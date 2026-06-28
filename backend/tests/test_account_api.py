"""Account-state API tests.

A :class:`FakeBrokerAdapter` returns canned broker data and a
:class:`FakeFactory` hands it to the endpoints, so these tests never need the
real Alpaca adapter (built in parallel). ``require_user`` and
``get_broker_factory`` are dependency-overridden; ``get_db`` is overridden by
the ``app_client`` fixture to the test database.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, date, datetime
from decimal import Decimal

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.deps import get_broker_factory, require_user
from app.brokers.base import BrokerAdapter
from app.brokers.dto import (
    BrokerAccount,
    BrokerOrder,
    BrokerPosition,
    CalendarDay,
    MarketClock,
    OrderRequest,
)
from app.brokers.errors import BrokerAuthError, BrokerRateLimited, CredentialsNotFound
from app.models import (
    OrderClass,
    OrderSide,
    OrderStatus,
    OrderType,
    Portfolio,
    PortfolioMode,
    Position,
    TimeInForce,
    User,
)
from app.models.telemetry import EquitySnapshot, EventLog
from app.models.trading import Order

API = "/api/v1/portfolios"


def _account() -> BrokerAccount:
    return BrokerAccount(
        account_id="acct-1",
        status="ACTIVE",
        currency="USD",
        equity=Decimal("100000.00"),
        last_equity=Decimal("99000.00"),
        cash=Decimal("50000.00"),
        buying_power=Decimal("200000.00"),
        position_market_value=Decimal("50000.00"),
        trading_blocked=False,
        account_blocked=False,
        raw={"pattern_day_trader": "should-not-leak"},
    )


def _position(symbol: str = "AAPL", qty: str = "10") -> BrokerPosition:
    qd = Decimal(qty)
    return BrokerPosition(
        symbol=symbol,
        qty=qd,
        side="long" if qd >= 0 else "short",
        avg_entry_price=Decimal("190.00"),
        market_value=Decimal("2000.00"),
        cost_basis=Decimal("1900.00"),
        unrealized_pl=Decimal("100.00"),
        current_price=Decimal("200.00"),
    )


def _order(
    *,
    broker_order_id: str = "brk-1",
    client_order_id: str | None = "cli-1",
    symbol: str = "AAPL",
    status: OrderStatus = OrderStatus.accepted,
    filled_qty: str = "0",
    filled_avg_price: str | None = None,
) -> BrokerOrder:
    return BrokerOrder(
        broker_order_id=broker_order_id,
        client_order_id=client_order_id,
        symbol=symbol,
        side=OrderSide.buy,
        order_type=OrderType.market,
        order_class=OrderClass.simple,
        time_in_force=TimeInForce.day,
        status=status,
        qty=Decimal("10"),
        filled_qty=Decimal(filled_qty),
        filled_avg_price=Decimal(filled_avg_price) if filled_avg_price is not None else None,
        submitted_at=datetime(2026, 6, 23, 14, 0, tzinfo=UTC),
    )


class FakeBrokerAdapter(BrokerAdapter):
    """Canned read-only adapter for endpoint tests.

    Any read method may be told to ``raise`` a broker error to exercise the
    error-to-HTTP mapping. Order/position mutators raise to prove the account
    API never invokes them (iron law #1).
    """

    def __init__(
        self,
        *,
        account: BrokerAccount | None = None,
        positions: list[BrokerPosition] | None = None,
        open_orders: list[BrokerOrder] | None = None,
        client_lookup: dict[str, BrokerOrder] | None = None,
        raise_on: dict[str, Exception] | None = None,
    ) -> None:
        self._account = account or _account()
        self._positions = positions if positions is not None else [_position()]
        self._open_orders = open_orders if open_orders is not None else [_order()]
        self._client_lookup = client_lookup or {}
        self._raise_on = raise_on or {}
        self.entered = False
        self.closed = False
        self.list_orders_status: str | None = None

    def _maybe_raise(self, method: str) -> None:
        exc = self._raise_on.get(method)
        if exc is not None:
            raise exc

    async def __aenter__(self) -> FakeBrokerAdapter:
        self.entered = True
        return self

    async def get_account(self) -> BrokerAccount:
        self._maybe_raise("get_account")
        return self._account

    async def list_positions(self) -> list[BrokerPosition]:
        self._maybe_raise("list_positions")
        return list(self._positions)

    async def get_position(self, symbol: str) -> BrokerPosition | None:
        return next((p for p in self._positions if p.symbol == symbol), None)

    async def list_orders(
        self,
        *,
        status: str = "open",
        after: datetime | None = None,
        until: datetime | None = None,
        limit: int = 100,
        nested: bool = True,
    ) -> list[BrokerOrder]:
        self.list_orders_status = status
        self._maybe_raise("list_orders")
        return list(self._open_orders)

    async def get_order_by_client_id(self, client_order_id: str) -> BrokerOrder | None:
        return self._client_lookup.get(client_order_id)

    async def get_order(self, broker_order_id: str) -> BrokerOrder | None:
        return next((o for o in self._open_orders if o.broker_order_id == broker_order_id), None)

    async def get_clock(self) -> MarketClock:  # pragma: no cover
        raise NotImplementedError

    async def get_calendar(self, start: date, end: date) -> list[CalendarDay]:  # pragma: no cover
        raise NotImplementedError

    async def submit_order(self, req: OrderRequest) -> BrokerOrder:  # pragma: no cover
        raise AssertionError("account API must never submit orders (iron law #1)")

    async def cancel_order(self, broker_order_id: str) -> None:  # pragma: no cover
        raise AssertionError("account API must never cancel orders (iron law #1)")

    async def cancel_all_orders(self) -> None:  # pragma: no cover
        raise AssertionError("account API must never cancel orders (iron law #1)")

    async def close_position(
        self,
        symbol: str,
        *,
        qty: Decimal | None = None,
        percentage: Decimal | None = None,
    ) -> BrokerOrder:  # pragma: no cover
        raise AssertionError("account API must never close positions (iron law #1)")

    async def close_all_positions(self, *, cancel_orders: bool = True) -> None:  # pragma: no cover
        raise AssertionError("account API must never close positions (iron law #1)")

    async def aclose(self) -> None:
        self.closed = True


class FakeFactory:
    """A :class:`BrokerAdapterFactory` that always returns ``adapter``.

    When constructed with ``raise_exc`` it raises that on resolution instead —
    modelling the credential-lookup failure path (e.g. ``CredentialsNotFound``).
    """

    def __init__(
        self, adapter: FakeBrokerAdapter | None = None, *, raise_exc: Exception | None = None
    ) -> None:
        self.adapter = adapter or FakeBrokerAdapter()
        self._raise_exc = raise_exc
        self.requested: list[uuid.UUID] = []

    async def get_adapter_for_portfolio(
        self, session: object, portfolio_id: uuid.UUID
    ) -> BrokerAdapter:
        self.requested.append(portfolio_id)
        if self._raise_exc is not None:
            raise self._raise_exc
        return self.adapter


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


def _use_factory(factory: FakeFactory) -> None:
    from app.main import app

    app.dependency_overrides[get_broker_factory] = lambda: factory


def _clear_factory() -> None:
    from app.main import app

    app.dependency_overrides.pop(get_broker_factory, None)


@pytest_asyncio.fixture
async def portfolio(db_session: AsyncSession, seeded_user: User) -> Portfolio:
    """A committed portfolio owned by the seeded user."""
    p = Portfolio(user_id=seeded_user.id, name="acct", mode=PortfolioMode.paper)
    db_session.add(p)
    await db_session.commit()
    await db_session.refresh(p)
    return p


async def _foreign_portfolio(db_session: AsyncSession) -> Portfolio:
    other = User(email="other-acct@roigen.test", display_name="Other")
    db_session.add(other)
    await db_session.flush()
    foreign = Portfolio(user_id=other.id, name="theirs", mode=PortfolioMode.paper)
    db_session.add(foreign)
    await db_session.commit()
    await db_session.refresh(foreign)
    return foreign


# ── Account / positions / orders reads ───────────────────────────


async def test_get_account_maps_dto(auth_client: httpx.AsyncClient, portfolio: Portfolio) -> None:
    factory = FakeFactory()
    _use_factory(factory)
    try:
        resp = await auth_client.get(f"{API}/{portfolio.id}/account")
    finally:
        _clear_factory()

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["account_id"] == "acct-1"
    assert body["equity"] == "100000.00"
    assert body["buying_power"] == "200000.00"
    # No PDT field and no raw broker payload leaks through the projection.
    assert "pattern_day_trader" not in resp.text
    assert "raw" not in body
    # The adapter was resolved for THIS portfolio and entered/closed.
    assert factory.requested == [portfolio.id]
    assert factory.adapter.entered is True
    assert factory.adapter.closed is True


async def test_get_positions_maps_list(
    auth_client: httpx.AsyncClient, portfolio: Portfolio
) -> None:
    factory = FakeFactory(
        FakeBrokerAdapter(positions=[_position("AAPL", "10"), _position("MSFT", "-3")])
    )
    _use_factory(factory)
    try:
        resp = await auth_client.get(f"{API}/{portfolio.id}/positions")
    finally:
        _clear_factory()

    assert resp.status_code == 200, resp.text
    rows = {p["symbol"]: p for p in resp.json()}
    assert set(rows) == {"AAPL", "MSFT"}
    assert rows["AAPL"]["qty"] == "10"
    assert rows["MSFT"]["qty"] == "-3"
    assert rows["MSFT"]["side"] == "short"


async def test_get_orders_passes_status_filter(
    auth_client: httpx.AsyncClient, portfolio: Portfolio
) -> None:
    adapter = FakeBrokerAdapter(open_orders=[_order(status=OrderStatus.partially_filled)])
    factory = FakeFactory(adapter)
    _use_factory(factory)
    try:
        resp = await auth_client.get(f"{API}/{portfolio.id}/orders", params={"status": "open"})
    finally:
        _clear_factory()

    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data[0]["broker_order_id"] == "brk-1"
    assert data[0]["status"] == "partially_filled"
    assert adapter.list_orders_status == "open"


async def test_orders_default_status_is_open(
    auth_client: httpx.AsyncClient, portfolio: Portfolio
) -> None:
    adapter = FakeBrokerAdapter()
    factory = FakeFactory(adapter)
    _use_factory(factory)
    try:
        resp = await auth_client.get(f"{API}/{portfolio.id}/orders")
    finally:
        _clear_factory()
    assert resp.status_code == 200
    assert adapter.list_orders_status == "open"


# ── Ownership ────────────────────────────────────────────────────


async def test_account_foreign_portfolio_404(
    auth_client: httpx.AsyncClient, db_session: AsyncSession
) -> None:
    foreign = await _foreign_portfolio(db_session)
    factory = FakeFactory()
    _use_factory(factory)
    try:
        for path in ("account", "positions", "orders"):
            resp = await auth_client.get(f"{API}/{foreign.id}/{path}")
            assert resp.status_code == 404, path
        sync = await auth_client.post(f"{API}/{foreign.id}/sync")
        assert sync.status_code == 404
    finally:
        _clear_factory()
    # Ownership is checked BEFORE the broker is ever touched.
    assert factory.requested == []


async def test_account_missing_portfolio_404(
    auth_client: httpx.AsyncClient, portfolio: Portfolio
) -> None:
    factory = FakeFactory()
    _use_factory(factory)
    try:
        resp = await auth_client.get(f"{API}/{uuid.uuid4()}/account")
    finally:
        _clear_factory()
    assert resp.status_code == 404


# ── Broker error mapping ─────────────────────────────────────────


async def test_credentials_not_found_409(
    auth_client: httpx.AsyncClient, portfolio: Portfolio
) -> None:
    factory = FakeFactory(raise_exc=CredentialsNotFound("no keys"))
    _use_factory(factory)
    try:
        resp = await auth_client.get(f"{API}/{portfolio.id}/account")
    finally:
        _clear_factory()
    assert resp.status_code == 409
    assert "credentials" in resp.json()["detail"].lower()


async def test_bad_keys_map_to_502(auth_client: httpx.AsyncClient, portfolio: Portfolio) -> None:
    adapter = FakeBrokerAdapter(raise_on={"get_account": BrokerAuthError("401")})
    factory = FakeFactory(adapter)
    _use_factory(factory)
    try:
        resp = await auth_client.get(f"{API}/{portfolio.id}/account")
    finally:
        _clear_factory()
    assert resp.status_code == 502
    # Even on the error path the adapter context manager closed the transport.
    assert adapter.closed is True


async def test_rate_limited_maps_to_429_with_retry_after(
    auth_client: httpx.AsyncClient, portfolio: Portfolio
) -> None:
    adapter = FakeBrokerAdapter(
        raise_on={"list_positions": BrokerRateLimited("slow down", retry_after=3.0)}
    )
    factory = FakeFactory(adapter)
    _use_factory(factory)
    try:
        resp = await auth_client.get(f"{API}/{portfolio.id}/positions")
    finally:
        _clear_factory()
    assert resp.status_code == 429
    assert resp.headers.get("retry-after") == "3"


# ── Sync / reconciliation through the endpoint ───────────────────


async def test_sync_persists_snapshot_positions_and_event(
    auth_client: httpx.AsyncClient, db_session: AsyncSession, portfolio: Portfolio
) -> None:
    # Local holds GONE; an existing local order is accepted. Broker reports AAPL
    # only and the order now partially filled.
    db_session.add_all(
        [
            Position(
                portfolio_id=portfolio.id,
                symbol="GONE",
                qty=Decimal("4"),
                avg_entry_price=Decimal("10.00"),
            ),
            Order(
                client_order_id="cli-1",
                broker_order_id="brk-1",
                portfolio_id=portfolio.id,
                symbol="AAPL",
                side=OrderSide.buy,
                order_type=OrderType.market,
                order_class=OrderClass.simple,
                time_in_force=TimeInForce.day,
                status=OrderStatus.accepted,
                qty=Decimal("10"),
            ),
        ]
    )
    await db_session.commit()

    adapter = FakeBrokerAdapter(
        positions=[_position("AAPL", "10")],
        open_orders=[
            _order(
                broker_order_id="brk-1",
                client_order_id="cli-1",
                status=OrderStatus.partially_filled,
                filled_qty="4",
                filled_avg_price="190.50",
            )
        ],
    )
    factory = FakeFactory(adapter)
    _use_factory(factory)
    try:
        resp = await auth_client.post(f"{API}/{portfolio.id}/sync")
    finally:
        _clear_factory()

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["portfolio_id"] == str(portfolio.id)
    assert body["positions_synced"] == 1
    assert body["positions_removed"] == 1
    assert body["orders_updated"] == 1
    assert body["equity"] == "100000.00"

    # The endpoint committed: a fresh session sees the persisted effects.
    snap_count = len(
        (
            await db_session.execute(
                select(EquitySnapshot).where(EquitySnapshot.portfolio_id == portfolio.id)
            )
        )
        .scalars()
        .all()
    )
    assert snap_count == 1

    symbols = {
        p.symbol
        for p in (
            await db_session.execute(select(Position).where(Position.portfolio_id == portfolio.id))
        )
        .scalars()
        .all()
    }
    assert symbols == {"AAPL"}  # GONE removed, AAPL upserted

    order = (
        await db_session.execute(select(Order).where(Order.client_order_id == "cli-1"))
    ).scalar_one()
    assert order.status == OrderStatus.partially_filled.value
    assert order.filled_qty == Decimal("4")

    completed = (
        await db_session.execute(
            select(EventLog).where(EventLog.event_type == "reconcile.completed")
        )
    ).scalar_one()
    assert completed.payload["positions_removed"] == 1


async def test_sync_orphan_order_writes_event(
    auth_client: httpx.AsyncClient, db_session: AsyncSession, portfolio: Portfolio
) -> None:
    adapter = FakeBrokerAdapter(
        positions=[],
        open_orders=[_order(broker_order_id="brk-ext", client_order_id="cli-ext", symbol="NVDA")],
    )
    factory = FakeFactory(adapter)
    _use_factory(factory)
    try:
        resp = await auth_client.post(f"{API}/{portfolio.id}/sync")
    finally:
        _clear_factory()

    assert resp.status_code == 200, resp.text
    assert resp.json()["orphans"] == 1
    orphan = (
        await db_session.execute(
            select(EventLog).where(EventLog.event_type == "reconcile.orphan_broker_order")
        )
    ).scalar_one()
    assert orphan.source == "broker"
    assert orphan.payload["symbol"] == "NVDA"


async def test_sync_credentials_not_found_409(
    auth_client: httpx.AsyncClient, db_session: AsyncSession, portfolio: Portfolio
) -> None:
    factory = FakeFactory(raise_exc=CredentialsNotFound("no keys"))
    _use_factory(factory)
    try:
        resp = await auth_client.post(f"{API}/{portfolio.id}/sync")
    finally:
        _clear_factory()
    assert resp.status_code == 409
    # Nothing persisted when credentials are missing.
    snaps = (
        (
            await db_session.execute(
                select(EquitySnapshot).where(EquitySnapshot.portfolio_id == portfolio.id)
            )
        )
        .scalars()
        .all()
    )
    assert snaps == []


# ── Auth gate ────────────────────────────────────────────────────


async def test_account_requires_auth(app_client: httpx.AsyncClient) -> None:
    # No require_user override → the real auth dependency rejects the request.
    resp = await app_client.get(f"{API}/{uuid.uuid4()}/account")
    assert resp.status_code in (401, 403)


@pytest.mark.parametrize("path", ["account", "positions", "orders"])
async def test_read_endpoints_exist(
    auth_client: httpx.AsyncClient, portfolio: Portfolio, path: str
) -> None:
    factory = FakeFactory()
    _use_factory(factory)
    try:
        resp = await auth_client.get(f"{API}/{portfolio.id}/{path}")
    finally:
        _clear_factory()
    assert resp.status_code == 200
