"""Auth endpoints (Google OAuth → JWT). Single-user, fail-closed.

Login only succeeds when ``ALLOWED_EMAIL`` is configured AND matches the
Google-verified email (case-insensitive). An empty allow-list means
nobody gets in — never everybody.
"""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, ConfigDict
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.database import get_db
from app.core.logging import get_logger
from app.core.security import (
    AuthError,
    create_access_token,
    get_current_user,
    verify_google_id_token,
)
from app.models.user import User

router = APIRouter()
log = get_logger("auth")

_ACCESS_DENIED = HTTPException(
    status_code=status.HTTP_403_FORBIDDEN,
    detail="Access denied: this platform is restricted to a single user.",
)


class GoogleLoginRequest(BaseModel):
    """Google ID token (``credential``) from the frontend Sign-In flow."""

    credential: str


class UserResponse(BaseModel):
    """Public view of a user row."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    email: str
    display_name: str | None


class TokenResponse(BaseModel):
    """App JWT plus the authenticated user."""

    access_token: str
    token_type: str
    user: UserResponse


class ClientIdResponse(BaseModel):
    """Google OAuth client id for frontend bootstrap (safe to expose)."""

    client_id: str


@router.post("/google", response_model=TokenResponse)
async def google_login(
    payload: GoogleLoginRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> TokenResponse:
    """Verify a Google ID token, enforce the allow-list, and issue a JWT."""
    try:
        idinfo = await verify_google_id_token(payload.credential)
    except AuthError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid Google token",
        ) from exc

    raw_email = idinfo.get("email")
    if not isinstance(raw_email, str) or not raw_email:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Google token contained no email",
        )
    email = raw_email.lower()

    allowed_email = get_settings().allowed_email.strip().lower()
    if not allowed_email:
        # Fail CLOSED: an unset allow-list locks everyone out, not nobody.
        log.warning(
            "auth.allowed_email_unset",
            detail="ALLOWED_EMAIL is empty — rejecting ALL logins (fail closed)",
            attempted_email=email,
        )
        raise _ACCESS_DENIED
    if email != allowed_email:
        log.warning("auth.email_rejected", attempted_email=email)
        raise _ACCESS_DENIED

    user = (await db.execute(select(User).where(User.email == email))).scalar_one_or_none()
    if user is None:
        display_name = idinfo.get("name")
        user = User(
            email=email,
            display_name=display_name if isinstance(display_name, str) else None,
        )
        db.add(user)
        await db.commit()
        await db.refresh(user)
        log.info("auth.user_created", email=email)

    return TokenResponse(
        access_token=create_access_token(user.email),
        token_type="bearer",
        user=UserResponse.model_validate(user),
    )


@router.get("/me", response_model=UserResponse)
async def read_me(current_user: Annotated[User, Depends(get_current_user)]) -> UserResponse:
    """Current authenticated user."""
    return UserResponse.model_validate(current_user)


@router.get("/client-id", response_model=ClientIdResponse)
async def get_client_id() -> ClientIdResponse:
    """Google OAuth client id — public, used by the frontend to bootstrap."""
    return ClientIdResponse(client_id=get_settings().google_client_id)
