"""Auth security helpers: Google ID-token verification and app JWTs.

Single-user platform — Google Sign-In proves identity, then we issue a
short-lived (24h) HS256 JWT. Day-trading sessions don't need week-long
tokens; re-auth daily.
"""

import asyncio
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any

import httpx
import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from google.auth import transport as google_transport
from google.oauth2 import id_token as google_id_token
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.database import get_db
from app.models.user import User

JWT_ALGORITHM = "HS256"
ACCESS_TOKEN_TTL = timedelta(hours=24)


class AuthError(Exception):
    """A credential (Google ID token or app JWT) failed verification."""


_CERT_FETCH_TIMEOUT_SECONDS = 10.0


class _HttpxResponse(google_transport.Response):
    """google-auth transport response adapter over :class:`httpx.Response`."""

    def __init__(self, response: httpx.Response) -> None:
        self._response = response

    @property
    def status(self) -> int:
        return self._response.status_code

    @property
    def headers(self) -> Mapping[str, str]:
        return self._response.headers

    @property
    def data(self) -> bytes:
        return self._response.content


class _HttpxRequest(google_transport.Request):
    """google-auth transport backed by httpx (used for Google cert fetches).

    google-auth's bundled ``requests``/``urllib3`` transports require extra
    packages this project doesn't depend on; httpx is already in the stack.
    """

    def __call__(
        self,
        url: str,
        method: str = "GET",
        body: bytes | None = None,
        headers: Mapping[str, str] | None = None,
        timeout: float | None = None,
        **kwargs: Any,
    ) -> _HttpxResponse:
        response = httpx.request(
            method,
            url,
            content=body,
            headers=headers,
            timeout=timeout if timeout is not None else _CERT_FETCH_TIMEOUT_SECONDS,
            **kwargs,
        )
        return _HttpxResponse(response)


def _verify_google_sync(credential: str) -> dict[str, Any]:
    """Blocking Google ID-token verification (network call for certs)."""
    settings = get_settings()
    try:
        # google-auth ships py.typed but this function is unannotated.
        idinfo: dict[str, Any] = google_id_token.verify_oauth2_token(  # type: ignore[no-untyped-call]
            credential,
            _HttpxRequest(),
            audience=settings.google_client_id,
        )
    except Exception as exc:
        msg = f"Google ID token verification failed: {exc}"
        raise AuthError(msg) from exc
    return idinfo


async def verify_google_id_token(credential: str) -> dict[str, Any]:
    """Verify a Google ID token; raises :class:`AuthError` on any failure.

    ``verify_oauth2_token`` does blocking HTTP (Google cert fetch), so it
    runs in a worker thread to keep the event loop free.
    """
    return await asyncio.to_thread(_verify_google_sync, credential)


def create_access_token(email: str) -> str:
    """Issue an HS256 JWT with ``sub=email`` and a 24-hour expiry."""
    now = datetime.now(UTC)
    payload: dict[str, Any] = {
        "sub": email,
        "iat": now,
        "exp": now + ACCESS_TOKEN_TTL,
    }
    return jwt.encode(payload, get_settings().secret_key, algorithm=JWT_ALGORITHM)


def decode_access_token(token: str) -> str:
    """Validate an app JWT (signature, expiry, required claims) → email."""
    try:
        payload = jwt.decode(
            token,
            get_settings().secret_key,
            algorithms=[JWT_ALGORITHM],
            options={"require": ["sub", "iat", "exp"]},
        )
    except jwt.InvalidTokenError as exc:
        msg = f"Invalid access token: {exc}"
        raise AuthError(msg) from exc
    email = payload.get("sub")
    if not isinstance(email, str) or not email:
        raise AuthError("Access token has no usable subject")
    return email


_bearer_scheme = HTTPBearer(auto_error=False)


async def get_current_user(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer_scheme)],
    db: Annotated[AsyncSession, Depends(get_db)],
) -> User:
    """FastAPI dependency: bearer JWT → :class:`User`, 401 on any failure."""
    unauthorized = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    if credentials is None:
        raise unauthorized
    try:
        email = decode_access_token(credentials.credentials)
    except AuthError as exc:
        raise unauthorized from exc
    user = (await db.execute(select(User).where(User.email == email))).scalar_one_or_none()
    if user is None:
        raise unauthorized
    return user
