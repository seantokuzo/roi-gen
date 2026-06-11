"""Portfolio + credentials endpoint tests.

``require_user`` is dependency-overridden to the seeded user, so these tests
are independent of the auth module's implementation progress.
"""

import uuid
from collections.abc import AsyncGenerator
from typing import Any

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.deps import require_user
from app.core.config import get_settings
from app.models import BrokerCredential, Portfolio, PortfolioMode, Strategy, User
from app.services.crypto import decrypt_str

API = "/api/v1/portfolios"


@pytest_asyncio.fixture
async def auth_client(
    app_client: httpx.AsyncClient, seeded_user: User
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """``app_client`` with ``require_user`` overridden to the seeded user."""
    from app.main import app

    app.dependency_overrides[require_user] = lambda: seeded_user
    try:
        yield app_client
    finally:
        app.dependency_overrides.pop(require_user, None)


async def _create(
    client: httpx.AsyncClient, name: str = "alpha", mode: str = "paper"
) -> dict[str, Any]:
    resp = await client.post(f"{API}/", json={"name": name, "mode": mode})
    assert resp.status_code == 201, resp.text
    data: dict[str, Any] = resp.json()
    return data


async def _seed_foreign_portfolio(db_session: AsyncSession) -> Portfolio:
    """A second user's portfolio, created directly in the DB."""
    other = User(email="other-user@roigen.test", display_name="Other User")
    db_session.add(other)
    await db_session.flush()
    foreign = Portfolio(user_id=other.id, name="theirs", mode=PortfolioMode.paper)
    db_session.add(foreign)
    await db_session.commit()
    await db_session.refresh(foreign)
    return foreign


# ── Create / list / get ──────────────────────────────────────────


async def test_create_first_portfolio_is_default(auth_client: httpx.AsyncClient) -> None:
    data = await _create(auth_client, name="alpha")
    assert data["name"] == "alpha"
    assert data["mode"] == "paper"
    assert data["is_default"] is True
    assert data["has_credentials"] is False
    assert data["description"] is None
    assert data["created_at"] is not None
    uuid.UUID(data["id"])  # well-formed id


async def test_second_portfolio_is_not_default(auth_client: httpx.AsyncClient) -> None:
    await _create(auth_client, name="alpha")
    second = await _create(auth_client, name="beta")
    assert second["is_default"] is False


async def test_create_live_mode_allowed(auth_client: httpx.AsyncClient) -> None:
    data = await _create(auth_client, name="real-money", mode="live")
    assert data["mode"] == "live"


async def test_create_duplicate_name_conflict(auth_client: httpx.AsyncClient) -> None:
    await _create(auth_client, name="alpha")
    resp = await auth_client.post(f"{API}/", json={"name": "alpha", "mode": "paper"})
    assert resp.status_code == 409


async def test_list_portfolios(auth_client: httpx.AsyncClient) -> None:
    await _create(auth_client, name="alpha")
    await _create(auth_client, name="beta")
    resp = await auth_client.get(f"{API}/")
    assert resp.status_code == 200
    names = [p["name"] for p in resp.json()]
    assert names == ["alpha", "beta"]


async def test_get_portfolio_by_id(auth_client: httpx.AsyncClient) -> None:
    created = await _create(auth_client, name="alpha")
    resp = await auth_client.get(f"{API}/{created['id']}")
    assert resp.status_code == 200
    assert resp.json()["id"] == created["id"]


async def test_get_missing_portfolio_404(auth_client: httpx.AsyncClient) -> None:
    resp = await auth_client.get(f"{API}/{uuid.uuid4()}")
    assert resp.status_code == 404


async def test_other_users_portfolio_is_404(
    auth_client: httpx.AsyncClient, db_session: AsyncSession
) -> None:
    foreign = await _seed_foreign_portfolio(db_session)
    assert (await auth_client.get(f"{API}/{foreign.id}")).status_code == 404
    patch = await auth_client.patch(f"{API}/{foreign.id}", json={"name": "mine-now"})
    assert patch.status_code == 404
    assert (await auth_client.delete(f"{API}/{foreign.id}")).status_code == 404


# ── Update ───────────────────────────────────────────────────────


async def test_patch_updates_name_and_description(auth_client: httpx.AsyncClient) -> None:
    created = await _create(auth_client, name="alpha")
    resp = await auth_client.patch(
        f"{API}/{created['id']}",
        json={"name": "renamed", "description": "now described"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["name"] == "renamed"
    assert data["description"] == "now described"
    assert data["is_default"] is True  # untouched


async def test_default_flag_is_exclusive(auth_client: httpx.AsyncClient) -> None:
    first = await _create(auth_client, name="alpha")
    second = await _create(auth_client, name="beta")
    assert first["is_default"] is True

    resp = await auth_client.patch(f"{API}/{second['id']}", json={"is_default": True})
    assert resp.status_code == 200
    assert resp.json()["is_default"] is True

    by_name = {p["name"]: p for p in (await auth_client.get(f"{API}/")).json()}
    assert by_name["alpha"]["is_default"] is False
    assert by_name["beta"]["is_default"] is True


# ── Delete ───────────────────────────────────────────────────────


async def test_delete_with_strategy_conflicts(
    auth_client: httpx.AsyncClient, db_session: AsyncSession
) -> None:
    created = await _create(auth_client, name="alpha")
    portfolio_id = uuid.UUID(created["id"])
    db_session.add(Strategy(portfolio_id=portfolio_id, name="orb-5m", kind="orb"))
    await db_session.commit()

    resp = await auth_client.delete(f"{API}/{portfolio_id}")
    assert resp.status_code == 409

    await db_session.execute(delete(Strategy).where(Strategy.portfolio_id == portfolio_id))
    await db_session.commit()
    assert (await auth_client.delete(f"{API}/{portfolio_id}")).status_code == 204
    assert (await auth_client.get(f"{API}/{portfolio_id}")).status_code == 404


async def test_delete_cascades_credentials(
    auth_client: httpx.AsyncClient, db_session: AsyncSession
) -> None:
    created = await _create(auth_client, name="alpha")
    portfolio_id = uuid.UUID(created["id"])
    put = await auth_client.put(
        f"{API}/{portfolio_id}/credentials",
        json={"api_key": "AK-cascade", "api_secret": "SK-cascade", "paper": True},
    )
    assert put.status_code == 200

    assert (await auth_client.delete(f"{API}/{portfolio_id}")).status_code == 204
    remaining = (
        await db_session.execute(
            select(BrokerCredential).where(BrokerCredential.portfolio_id == portfolio_id)
        )
    ).scalar_one_or_none()
    assert remaining is None


# ── Credentials ──────────────────────────────────────────────────


async def test_put_credentials_encrypts_at_rest(
    auth_client: httpx.AsyncClient, db_session: AsyncSession
) -> None:
    created = await _create(auth_client, name="alpha")
    portfolio_id = uuid.UUID(created["id"])
    resp = await auth_client.put(
        f"{API}/{portfolio_id}/credentials",
        json={"api_key": "AKPLAINTEXT", "api_secret": "SKPLAINTEXT", "paper": True},
    )
    assert resp.status_code == 200
    assert resp.json()["has_credentials"] is True
    # Secrets must never be echoed anywhere in the response.
    assert "AKPLAINTEXT" not in resp.text
    assert "SKPLAINTEXT" not in resp.text

    row = (
        await db_session.execute(
            select(BrokerCredential).where(BrokerCredential.portfolio_id == portfolio_id)
        )
    ).scalar_one()
    assert row.api_key_encrypted != "AKPLAINTEXT"
    assert row.api_secret_encrypted != "SKPLAINTEXT"
    assert decrypt_str(row.api_key_encrypted) == "AKPLAINTEXT"
    assert decrypt_str(row.api_secret_encrypted) == "SKPLAINTEXT"
    assert row.paper is True


async def test_put_credentials_upserts_single_row(
    auth_client: httpx.AsyncClient, db_session: AsyncSession
) -> None:
    created = await _create(auth_client, name="alpha")
    portfolio_id = uuid.UUID(created["id"])
    for key, secret in (("AK-one", "SK-one"), ("AK-two", "SK-two")):
        resp = await auth_client.put(
            f"{API}/{portfolio_id}/credentials",
            json={"api_key": key, "api_secret": secret, "paper": False},
        )
        assert resp.status_code == 200

    rows = (
        (
            await db_session.execute(
                select(BrokerCredential).where(BrokerCredential.portfolio_id == portfolio_id)
            )
        )
        .scalars()
        .all()
    )
    assert len(rows) == 1
    assert decrypt_str(rows[0].api_key_encrypted) == "AK-two"
    assert decrypt_str(rows[0].api_secret_encrypted) == "SK-two"
    assert rows[0].paper is False


# ── Bootstrap ────────────────────────────────────────────────────


async def test_bootstrap_is_idempotent(
    auth_client: httpx.AsyncClient,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = get_settings()
    monkeypatch.setattr(settings, "alpaca_api_key", "env-alpaca-key")
    monkeypatch.setattr(settings, "alpaca_secret_key", "env-alpaca-secret")

    first = await auth_client.post(f"{API}/bootstrap")
    assert first.status_code == 201, first.text
    data = first.json()
    assert data["name"] == "Primary Paper"
    assert data["mode"] == "paper"
    assert data["is_default"] is True
    assert data["has_credentials"] is True
    assert "env-alpaca-key" not in first.text

    second = await auth_client.post(f"{API}/bootstrap")
    assert second.status_code == 200
    assert second.json()["id"] == data["id"]

    portfolios = (await db_session.execute(select(Portfolio))).scalars().all()
    assert len(portfolios) == 1
    row = (
        await db_session.execute(
            select(BrokerCredential).where(BrokerCredential.portfolio_id == uuid.UUID(data["id"]))
        )
    ).scalar_one()
    assert row.api_key_encrypted != "env-alpaca-key"
    assert decrypt_str(row.api_key_encrypted) == "env-alpaca-key"
    assert decrypt_str(row.api_secret_encrypted) == "env-alpaca-secret"


async def test_bootstrap_returns_existing_default(
    auth_client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = get_settings()
    monkeypatch.setattr(settings, "alpaca_api_key", "env-alpaca-key")
    monkeypatch.setattr(settings, "alpaca_secret_key", "env-alpaca-secret")
    await _create(auth_client, name="alpha")
    beta = await _create(auth_client, name="beta")
    await auth_client.patch(f"{API}/{beta['id']}", json={"is_default": True})

    resp = await auth_client.post(f"{API}/bootstrap")
    assert resp.status_code == 200
    assert resp.json()["id"] == beta["id"]


async def test_bootstrap_without_env_keys_422(
    auth_client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = get_settings()
    monkeypatch.setattr(settings, "alpaca_api_key", "")
    monkeypatch.setattr(settings, "alpaca_secret_key", "")

    resp = await auth_client.post(f"{API}/bootstrap")
    assert resp.status_code == 422
    assert "ALPACA_API_KEY" in resp.json()["detail"]
