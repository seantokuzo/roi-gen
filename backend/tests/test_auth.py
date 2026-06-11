"""Auth flow: Google login (mocked), allow-list enforcement, JWT lifecycle.

``verify_google_id_token`` is monkeypatched at the endpoint module — these
tests never talk to Google.
"""

from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import jwt
import pytest

from app.api.v1.endpoints import auth as auth_endpoint
from app.core.config import get_settings
from app.core.security import ACCESS_TOKEN_TTL, JWT_ALGORITHM, AuthError, create_access_token

ALLOWED_EMAIL = "allowed@roigen.test"
GOOGLE_NAME = "Allowed Tester"
LOGIN_URL = "/api/v1/auth/google"
ME_URL = "/api/v1/auth/me"
CLIENT_ID_URL = "/api/v1/auth/client-id"


@pytest.fixture
def allowed_email(monkeypatch: pytest.MonkeyPatch) -> str:
    """Pin settings.allowed_email to a known test value."""
    monkeypatch.setattr(get_settings(), "allowed_email", ALLOWED_EMAIL)
    return ALLOWED_EMAIL


def _mock_google(
    monkeypatch: pytest.MonkeyPatch,
    *,
    email: str | None = ALLOWED_EMAIL,
    fail: bool = False,
) -> None:
    """Replace the Google verifier with a canned (or failing) response."""

    async def _fake(credential: str) -> dict[str, Any]:
        if fail:
            raise AuthError("forced verification failure")
        idinfo: dict[str, Any] = {"name": GOOGLE_NAME}
        if email is not None:
            idinfo["email"] = email
        return idinfo

    monkeypatch.setattr(auth_endpoint, "verify_google_id_token", _fake)


# ── POST /google ─────────────────────────────────────────────────


async def test_login_allowed_email_returns_valid_jwt(
    app_client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch, allowed_email: str
) -> None:
    _mock_google(monkeypatch)
    resp = await app_client.post(LOGIN_URL, json={"credential": "fake-google-credential"})
    assert resp.status_code == 200

    body = resp.json()
    assert body["token_type"] == "bearer"
    assert body["user"]["email"] == ALLOWED_EMAIL
    assert body["user"]["display_name"] == GOOGLE_NAME
    assert body["user"]["id"]

    decoded = jwt.decode(
        body["access_token"], get_settings().secret_key, algorithms=[JWT_ALGORITHM]
    )
    assert decoded["sub"] == ALLOWED_EMAIL
    assert decoded["exp"] - decoded["iat"] == int(ACCESS_TOKEN_TTL.total_seconds())


async def test_login_email_match_is_case_insensitive(
    app_client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch, allowed_email: str
) -> None:
    _mock_google(monkeypatch, email="ALLOWED@ROIGEN.TEST")
    resp = await app_client.post(LOGIN_URL, json={"credential": "fake"})
    assert resp.status_code == 200
    assert resp.json()["user"]["email"] == ALLOWED_EMAIL  # stored lowercase


async def test_login_is_idempotent_for_same_user(
    app_client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch, allowed_email: str
) -> None:
    _mock_google(monkeypatch)
    first = await app_client.post(LOGIN_URL, json={"credential": "fake"})
    second = await app_client.post(LOGIN_URL, json={"credential": "fake"})
    assert first.status_code == second.status_code == 200
    assert first.json()["user"]["id"] == second.json()["user"]["id"]


async def test_login_wrong_email_403(
    app_client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch, allowed_email: str
) -> None:
    _mock_google(monkeypatch, email="intruder@evil.test")
    resp = await app_client.post(LOGIN_URL, json={"credential": "fake"})
    assert resp.status_code == 403


async def test_login_empty_allowed_email_fails_closed(
    app_client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(get_settings(), "allowed_email", "")
    _mock_google(monkeypatch)  # Google says the email is fine — still 403
    resp = await app_client.post(LOGIN_URL, json={"credential": "fake"})
    assert resp.status_code == 403


async def test_login_bad_google_token_401(
    app_client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch, allowed_email: str
) -> None:
    _mock_google(monkeypatch, fail=True)
    resp = await app_client.post(LOGIN_URL, json={"credential": "garbage"})
    assert resp.status_code == 401


async def test_login_google_token_without_email_401(
    app_client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch, allowed_email: str
) -> None:
    _mock_google(monkeypatch, email=None)
    resp = await app_client.post(LOGIN_URL, json={"credential": "fake"})
    assert resp.status_code == 401


# ── GET /me ──────────────────────────────────────────────────────


async def test_me_without_token_401(app_client: httpx.AsyncClient) -> None:
    resp = await app_client.get(ME_URL)
    assert resp.status_code == 401


async def test_me_with_token_200(
    app_client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch, allowed_email: str
) -> None:
    _mock_google(monkeypatch)
    login = await app_client.post(LOGIN_URL, json={"credential": "fake"})
    token = login.json()["access_token"]

    resp = await app_client.get(ME_URL, headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["email"] == ALLOWED_EMAIL
    assert body["id"] == login.json()["user"]["id"]


async def test_me_with_expired_token_401(app_client: httpx.AsyncClient) -> None:
    now = datetime.now(UTC)
    expired = jwt.encode(
        {"sub": ALLOWED_EMAIL, "iat": now - timedelta(hours=25), "exp": now - timedelta(hours=1)},
        get_settings().secret_key,
        algorithm=JWT_ALGORITHM,
    )
    resp = await app_client.get(ME_URL, headers={"Authorization": f"Bearer {expired}"})
    assert resp.status_code == 401


async def test_me_with_garbage_token_401(app_client: httpx.AsyncClient) -> None:
    resp = await app_client.get(ME_URL, headers={"Authorization": "Bearer not.a.jwt"})
    assert resp.status_code == 401


async def test_me_valid_token_unknown_user_401(app_client: httpx.AsyncClient) -> None:
    # Well-formed, unexpired JWT — but no matching user row exists.
    token = create_access_token("ghost@roigen.test")
    resp = await app_client.get(ME_URL, headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 401


# ── GET /client-id ───────────────────────────────────────────────


async def test_client_id_is_public(
    app_client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(get_settings(), "google_client_id", "test-client-id.apps.example")
    resp = await app_client.get(CLIENT_ID_URL)
    assert resp.status_code == 200
    assert resp.json() == {"client_id": "test-client-id.apps.example"}
