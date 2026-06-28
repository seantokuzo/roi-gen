"""AlpacaAdapterFactory: portfolio → bound adapter resolution.

Two angles:

- A DB-backed test that seeds a portfolio + encrypted credential row and proves
  ``get_adapter_for_portfolio`` decrypts them and returns an adapter bound to
  the right (paper/live) account.
- A pure-unit test that monkeypatches ``load_credentials`` so the factory's
  contract can be checked without a database.

It also pins the exact class/module path the API-injection agent depends on:
``app.brokers.alpaca.factory.AlpacaAdapterFactory``.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

import app.brokers.alpaca.factory as factory_mod
from app.brokers.alpaca.factory import AlpacaAdapterFactory, build_alpaca_adapter
from app.brokers.alpaca.rest import AlpacaBrokerAdapter
from app.brokers.base import BrokerAdapter, BrokerAdapterFactory
from app.brokers.credentials import BrokerCredentials
from app.brokers.errors import CredentialsNotFound
from app.models import BrokerCredential, Portfolio, PortfolioMode, User
from app.services.crypto import encrypt_str


def test_factory_class_path_is_pinned() -> None:
    # The injection agent imports this exact symbol — don't let it drift.
    assert AlpacaAdapterFactory.__module__ == "app.brokers.alpaca.factory"
    assert AlpacaAdapterFactory.__qualname__ == "AlpacaAdapterFactory"


def test_factory_satisfies_protocol() -> None:
    # runtime_checkable Protocol: an instance must structurally match.
    assert isinstance(AlpacaAdapterFactory(), BrokerAdapterFactory)


def test_build_alpaca_adapter_binds_paper_url() -> None:
    creds = BrokerCredentials(api_key="AK", api_secret="SK", paper=True)
    adapter = build_alpaca_adapter(creds)
    assert isinstance(adapter, AlpacaBrokerAdapter)
    assert isinstance(adapter, BrokerAdapter)
    assert adapter._base_url == "https://paper-api.alpaca.markets"


def test_build_alpaca_adapter_binds_live_url() -> None:
    creds = BrokerCredentials(api_key="AK", api_secret="SK", paper=False)
    adapter = build_alpaca_adapter(creds)
    assert adapter._base_url == "https://api.alpaca.markets"


async def _seed_portfolio_with_credentials(
    session: AsyncSession,
    user: User,
    *,
    api_key: str,
    api_secret: str,
    paper: bool,
) -> Portfolio:
    mode = PortfolioMode.paper if paper else PortfolioMode.live
    portfolio = Portfolio(user_id=user.id, name="factory-pf", mode=mode)
    session.add(portfolio)
    await session.flush()
    session.add(
        BrokerCredential(
            portfolio_id=portfolio.id,
            broker="alpaca",
            api_key_encrypted=encrypt_str(api_key),
            api_secret_encrypted=encrypt_str(api_secret),
            paper=paper,
        )
    )
    await session.commit()
    await session.refresh(portfolio)
    return portfolio


async def test_get_adapter_for_portfolio_db_backed(
    db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio = await _seed_portfolio_with_credentials(
        db_session,
        seeded_user,
        api_key="AK-PLAIN",
        api_secret="SK-PLAIN",
        paper=True,
    )

    factory = AlpacaAdapterFactory()
    adapter = await factory.get_adapter_for_portfolio(db_session, portfolio.id)

    assert isinstance(adapter, AlpacaBrokerAdapter)
    # Decrypted creds wired into the transport's auth headers.
    assert adapter._client.headers["APCA-API-KEY-ID"] == "AK-PLAIN"
    assert adapter._client.headers["APCA-API-SECRET-KEY"] == "SK-PLAIN"
    # Paper portfolio → paper base URL.
    assert adapter._base_url == "https://paper-api.alpaca.markets"
    await adapter.aclose()


async def test_get_adapter_for_portfolio_live_binds_live_url(
    db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio = await _seed_portfolio_with_credentials(
        db_session,
        seeded_user,
        api_key="AK-LIVE",
        api_secret="SK-LIVE",
        paper=False,
    )
    adapter = await AlpacaAdapterFactory().get_adapter_for_portfolio(db_session, portfolio.id)
    assert adapter._base_url == "https://api.alpaca.markets"
    await adapter.aclose()


async def test_get_adapter_for_portfolio_missing_creds_raises(
    db_session: AsyncSession, seeded_user: User
) -> None:
    # Portfolio with no credential row → CredentialsNotFound bubbles up.
    portfolio = Portfolio(user_id=seeded_user.id, name="no-creds", mode=PortfolioMode.paper)
    db_session.add(portfolio)
    await db_session.commit()
    await db_session.refresh(portfolio)

    with pytest.raises(CredentialsNotFound):
        await AlpacaAdapterFactory().get_adapter_for_portfolio(db_session, portfolio.id)


async def test_get_adapter_for_portfolio_monkeypatched(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Pure unit: no DB. The factory must call load_credentials with the given
    # session + portfolio_id, then hand the result to build_alpaca_adapter.
    pid = uuid.uuid4()
    sentinel_session = object()
    seen: dict[str, Any] = {}

    async def fake_load_credentials(session: Any, portfolio_id: uuid.UUID) -> BrokerCredentials:
        seen["session"] = session
        seen["portfolio_id"] = portfolio_id
        return BrokerCredentials(api_key="AK-FAKE", api_secret="SK-FAKE", paper=True)

    monkeypatch.setattr(factory_mod, "load_credentials", fake_load_credentials)

    adapter = await AlpacaAdapterFactory().get_adapter_for_portfolio(sentinel_session, pid)

    assert seen["session"] is sentinel_session
    assert seen["portfolio_id"] == pid
    assert isinstance(adapter, AlpacaBrokerAdapter)
    assert adapter._client.headers["APCA-API-KEY-ID"] == "AK-FAKE"
    await adapter.aclose()
