"""Unit tests for :class:`ReconciliationService`.

These drive ``reconcile_portfolio`` directly with a :class:`FakeBrokerAdapter`
seeded with canned broker state, then assert on the rows the service stages on
the session. The service writes; the test commits (mirroring the endpoint's
ownership of the transaction).
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest_asyncio
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.brokers.base import BrokerAdapter
from app.brokers.dto import (
    BrokerAccount,
    BrokerOrder,
    BrokerPosition,
    CalendarDay,
    MarketClock,
    OrderRequest,
)
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
from app.services.reconciliation import ReconciliationService


def _account(equity: str = "100000.00") -> BrokerAccount:
    return BrokerAccount(
        account_id="acct-1",
        status="ACTIVE",
        currency="USD",
        equity=Decimal(equity),
        last_equity=Decimal("99000.00"),
        cash=Decimal("50000.00"),
        buying_power=Decimal("200000.00"),
        position_market_value=Decimal("50000.00"),
        trading_blocked=False,
        account_blocked=False,
    )


def _position(symbol: str, qty: str, avg: str = "100.00") -> BrokerPosition:
    qd = Decimal(qty)
    return BrokerPosition(
        symbol=symbol,
        qty=qd,
        side="long" if qd >= 0 else "short",
        avg_entry_price=Decimal(avg),
        market_value=Decimal("1000.00"),
        cost_basis=Decimal("900.00"),
        unrealized_pl=Decimal("100.00"),
        current_price=Decimal("105.00"),
    )


def _broker_order(
    *,
    broker_order_id: str,
    client_order_id: str | None,
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
    """A canned read-only adapter for reconciliation tests.

    Only the methods reconciliation calls return real data; the mutating
    methods raise to prove reconciliation never touches them (iron law #1).
    ``client_lookup`` maps a ``client_order_id`` to the terminal state the
    broker would report for a straggler (the post-restart recovery path).
    """

    def __init__(
        self,
        *,
        account: BrokerAccount,
        positions: list[BrokerPosition],
        open_orders: list[BrokerOrder],
        client_lookup: dict[str, BrokerOrder] | None = None,
    ) -> None:
        self._account = account
        self._positions = positions
        self._open_orders = open_orders
        self._client_lookup = client_lookup or {}
        self.client_lookups: list[str] = []
        self.closed = False

    async def get_account(self) -> BrokerAccount:
        return self._account

    async def list_positions(self) -> list[BrokerPosition]:
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
        return list(self._open_orders)

    async def get_order_by_client_id(self, client_order_id: str) -> BrokerOrder | None:
        self.client_lookups.append(client_order_id)
        return self._client_lookup.get(client_order_id)

    async def get_order(self, broker_order_id: str) -> BrokerOrder | None:
        return next((o for o in self._open_orders if o.broker_order_id == broker_order_id), None)

    async def get_clock(self) -> MarketClock:  # pragma: no cover - not exercised
        raise NotImplementedError

    async def get_calendar(self, start: date, end: date) -> list[CalendarDay]:  # pragma: no cover
        raise NotImplementedError

    async def submit_order(self, req: OrderRequest) -> BrokerOrder:  # pragma: no cover
        raise AssertionError("reconciliation must never submit orders (iron law #1)")

    async def cancel_order(self, broker_order_id: str) -> None:  # pragma: no cover
        raise AssertionError("reconciliation must never cancel orders (iron law #1)")

    async def cancel_all_orders(self) -> None:  # pragma: no cover
        raise AssertionError("reconciliation must never cancel orders (iron law #1)")

    async def close_position(
        self,
        symbol: str,
        *,
        qty: Decimal | None = None,
        percentage: Decimal | None = None,
    ) -> BrokerOrder:  # pragma: no cover
        raise AssertionError("reconciliation must never close positions (iron law #1)")

    async def close_all_positions(self, *, cancel_orders: bool = True) -> None:  # pragma: no cover
        raise AssertionError("reconciliation must never close positions (iron law #1)")

    async def aclose(self) -> None:
        self.closed = True


@pytest_asyncio.fixture
async def portfolio(db_session: AsyncSession, seeded_user: User) -> Portfolio:
    """A committed portfolio owned by the seeded user."""
    p = Portfolio(user_id=seeded_user.id, name="recon", mode=PortfolioMode.paper)
    db_session.add(p)
    await db_session.commit()
    await db_session.refresh(p)
    return p


async def _counts(db: AsyncSession, model: type, **filters: object) -> int:
    stmt = select(func.count()).select_from(model)
    for col, val in filters.items():
        stmt = stmt.where(getattr(model, col) == val)
    return (await db.scalar(stmt)) or 0


# ── Equity snapshot ──────────────────────────────────────────────


async def test_reconcile_writes_equity_snapshot(
    db_session: AsyncSession, portfolio: Portfolio
) -> None:
    adapter = FakeBrokerAdapter(account=_account("123456.78"), positions=[], open_orders=[])
    result = await ReconciliationService().reconcile_portfolio(db_session, portfolio.id, adapter)
    await db_session.commit()

    snap = (
        await db_session.execute(
            select(EquitySnapshot).where(EquitySnapshot.portfolio_id == portfolio.id)
        )
    ).scalar_one()
    assert snap.equity == Decimal("123456.78")
    assert snap.cash == Decimal("50000.00")
    assert snap.buying_power == Decimal("200000.00")
    assert snap.ts.tzinfo is not None  # tz-aware
    assert result.equity == Decimal("123456.78")


# ── Positions ────────────────────────────────────────────────────


async def test_reconcile_inserts_new_positions(
    db_session: AsyncSession, portfolio: Portfolio
) -> None:
    adapter = FakeBrokerAdapter(
        account=_account(),
        positions=[_position("AAPL", "10", "190.00"), _position("MSFT", "-5", "400.00")],
        open_orders=[],
    )
    result = await ReconciliationService().reconcile_portfolio(db_session, portfolio.id, adapter)
    await db_session.commit()

    rows = {
        p.symbol: p
        for p in (
            await db_session.execute(select(Position).where(Position.portfolio_id == portfolio.id))
        )
        .scalars()
        .all()
    }
    assert set(rows) == {"AAPL", "MSFT"}
    assert rows["AAPL"].qty == Decimal("10")
    assert rows["AAPL"].avg_entry_price == Decimal("190.00")
    # Signed qty preserved for shorts.
    assert rows["MSFT"].qty == Decimal("-5")
    assert result.positions_synced == 2
    assert result.positions_removed == 0


async def test_reconcile_updates_existing_position(
    db_session: AsyncSession, portfolio: Portfolio
) -> None:
    db_session.add(
        Position(
            portfolio_id=portfolio.id,
            symbol="AAPL",
            qty=Decimal("3"),
            avg_entry_price=Decimal("100.00"),
        )
    )
    await db_session.commit()

    adapter = FakeBrokerAdapter(
        account=_account(), positions=[_position("AAPL", "12", "195.50")], open_orders=[]
    )
    result = await ReconciliationService().reconcile_portfolio(db_session, portfolio.id, adapter)
    await db_session.commit()

    row = (
        await db_session.execute(
            select(Position).where(Position.portfolio_id == portfolio.id, Position.symbol == "AAPL")
        )
    ).scalar_one()
    assert row.qty == Decimal("12")
    assert row.avg_entry_price == Decimal("195.50")
    assert result.positions_synced == 1
    # Exactly one row — updated, not duplicated.
    assert await _counts(db_session, Position, portfolio_id=portfolio.id) == 1


async def test_reconcile_removes_vanished_positions(
    db_session: AsyncSession, portfolio: Portfolio
) -> None:
    # Local thinks we hold GONE; broker reports only AAPL → GONE must be deleted.
    db_session.add_all(
        [
            Position(
                portfolio_id=portfolio.id,
                symbol="GONE",
                qty=Decimal("7"),
                avg_entry_price=Decimal("50.00"),
            ),
            Position(
                portfolio_id=portfolio.id,
                symbol="AAPL",
                qty=Decimal("1"),
                avg_entry_price=Decimal("100.00"),
            ),
        ]
    )
    await db_session.commit()

    adapter = FakeBrokerAdapter(
        account=_account(), positions=[_position("AAPL", "1", "100.00")], open_orders=[]
    )
    result = await ReconciliationService().reconcile_portfolio(db_session, portfolio.id, adapter)
    await db_session.commit()

    symbols = {
        p.symbol
        for p in (
            await db_session.execute(select(Position).where(Position.portfolio_id == portfolio.id))
        )
        .scalars()
        .all()
    }
    assert symbols == {"AAPL"}
    assert result.positions_removed == 1


# ── Order diff ───────────────────────────────────────────────────


async def test_reconcile_updates_local_order_status_by_broker_id(
    db_session: AsyncSession, portfolio: Portfolio
) -> None:
    local = Order(
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
    )
    db_session.add(local)
    await db_session.commit()

    adapter = FakeBrokerAdapter(
        account=_account(),
        positions=[],
        open_orders=[
            _broker_order(
                broker_order_id="brk-1",
                client_order_id="cli-1",
                status=OrderStatus.partially_filled,
                filled_qty="4",
                filled_avg_price="190.25",
            )
        ],
    )
    result = await ReconciliationService().reconcile_portfolio(db_session, portfolio.id, adapter)
    await db_session.commit()
    await db_session.refresh(local)

    assert local.status == OrderStatus.partially_filled.value
    assert local.filled_qty == Decimal("4")
    assert local.filled_avg_price == Decimal("190.25")
    assert result.orders_updated == 1
    assert result.orphans == 0
    assert result.missing == 0


async def test_reconcile_matches_order_by_client_id_and_backfills_broker_id(
    db_session: AsyncSession, portfolio: Portfolio
) -> None:
    # Persist-before-submit left us with a client id but no broker id yet.
    local = Order(
        client_order_id="cli-2",
        broker_order_id=None,
        portfolio_id=portfolio.id,
        symbol="AAPL",
        side=OrderSide.buy,
        order_type=OrderType.market,
        order_class=OrderClass.simple,
        time_in_force=TimeInForce.day,
        status=OrderStatus.submitted,
        qty=Decimal("10"),
    )
    db_session.add(local)
    await db_session.commit()

    adapter = FakeBrokerAdapter(
        account=_account(),
        positions=[],
        open_orders=[
            _broker_order(
                broker_order_id="brk-2", client_order_id="cli-2", status=OrderStatus.accepted
            )
        ],
    )
    result = await ReconciliationService().reconcile_portfolio(db_session, portfolio.id, adapter)
    await db_session.commit()
    await db_session.refresh(local)

    assert local.broker_order_id == "brk-2"
    assert local.status == OrderStatus.accepted.value
    assert result.orders_updated == 1


async def test_reconcile_orphan_broker_order_logs_event(
    db_session: AsyncSession, portfolio: Portfolio
) -> None:
    # Broker reports an open order we have no local row for → orphan/external.
    adapter = FakeBrokerAdapter(
        account=_account(),
        positions=[],
        open_orders=[
            _broker_order(broker_order_id="brk-ext", client_order_id="cli-ext", symbol="TSLA")
        ],
    )
    result = await ReconciliationService().reconcile_portfolio(db_session, portfolio.id, adapter)
    await db_session.commit()

    assert result.orphans == 1
    events = (
        (
            await db_session.execute(
                select(EventLog).where(
                    EventLog.event_type == "reconcile.orphan_broker_order",
                    EventLog.portfolio_id == portfolio.id,
                )
            )
        )
        .scalars()
        .all()
    )
    assert len(events) == 1
    assert events[0].source == "broker"
    assert events[0].payload["broker_order_id"] == "brk-ext"


async def test_reconcile_recovers_terminal_order_via_client_lookup(
    db_session: AsyncSession, portfolio: Portfolio
) -> None:
    # Non-terminal locally, NOT in the broker's open list → look it up by client
    # id and adopt its true terminal state (post-restart recovery).
    local = Order(
        client_order_id="cli-3",
        broker_order_id="brk-3",
        portfolio_id=portfolio.id,
        symbol="AAPL",
        side=OrderSide.buy,
        order_type=OrderType.market,
        order_class=OrderClass.simple,
        time_in_force=TimeInForce.day,
        status=OrderStatus.submitted,
        qty=Decimal("10"),
    )
    db_session.add(local)
    await db_session.commit()

    adapter = FakeBrokerAdapter(
        account=_account(),
        positions=[],
        open_orders=[],
        client_lookup={
            "cli-3": _broker_order(
                broker_order_id="brk-3",
                client_order_id="cli-3",
                status=OrderStatus.filled,
                filled_qty="10",
                filled_avg_price="191.00",
            )
        },
    )
    result = await ReconciliationService().reconcile_portfolio(db_session, portfolio.id, adapter)
    await db_session.commit()
    await db_session.refresh(local)

    assert adapter.client_lookups == ["cli-3"]
    assert local.status == OrderStatus.filled.value
    assert local.filled_qty == Decimal("10")
    assert local.filled_avg_price == Decimal("191.00")
    assert result.orders_updated == 1
    assert result.missing == 0


async def test_reconcile_missing_order_left_untouched(
    db_session: AsyncSession, portfolio: Portfolio
) -> None:
    # Non-terminal locally, not open at broker, and the client-id lookup is empty
    # → never guess: status stays, a missing_order event is written.
    local = Order(
        client_order_id="cli-4",
        broker_order_id="brk-4",
        portfolio_id=portfolio.id,
        symbol="AAPL",
        side=OrderSide.buy,
        order_type=OrderType.market,
        order_class=OrderClass.simple,
        time_in_force=TimeInForce.day,
        status=OrderStatus.accepted,
        qty=Decimal("10"),
    )
    db_session.add(local)
    await db_session.commit()

    adapter = FakeBrokerAdapter(account=_account(), positions=[], open_orders=[], client_lookup={})
    result = await ReconciliationService().reconcile_portfolio(db_session, portfolio.id, adapter)
    await db_session.commit()
    await db_session.refresh(local)

    assert local.status == OrderStatus.accepted.value  # untouched
    assert result.missing == 1
    assert result.orders_updated == 0
    events = (
        (
            await db_session.execute(
                select(EventLog).where(EventLog.event_type == "reconcile.missing_order")
            )
        )
        .scalars()
        .all()
    )
    assert len(events) == 1
    assert events[0].order_id == local.id


async def test_reconcile_ignores_already_terminal_local_orders(
    db_session: AsyncSession, portfolio: Portfolio
) -> None:
    # A filled local order absent from the broker-open list must NOT trigger a
    # client-id lookup — it's already settled.
    local = Order(
        client_order_id="cli-5",
        broker_order_id="brk-5",
        portfolio_id=portfolio.id,
        symbol="AAPL",
        side=OrderSide.buy,
        order_type=OrderType.market,
        order_class=OrderClass.simple,
        time_in_force=TimeInForce.day,
        status=OrderStatus.filled,
        qty=Decimal("10"),
        filled_qty=Decimal("10"),
    )
    db_session.add(local)
    await db_session.commit()

    adapter = FakeBrokerAdapter(account=_account(), positions=[], open_orders=[], client_lookup={})
    result = await ReconciliationService().reconcile_portfolio(db_session, portfolio.id, adapter)
    await db_session.commit()

    assert adapter.client_lookups == []  # no lookup for a terminal order
    assert result.missing == 0
    assert result.orders_updated == 0


# ── Summary audit row ────────────────────────────────────────────


async def test_reconcile_writes_completed_event(
    db_session: AsyncSession, portfolio: Portfolio
) -> None:
    adapter = FakeBrokerAdapter(
        account=_account("100000.00"),
        positions=[_position("AAPL", "10")],
        open_orders=[],
    )
    await ReconciliationService().reconcile_portfolio(db_session, portfolio.id, adapter)
    await db_session.commit()

    event = (
        await db_session.execute(
            select(EventLog).where(
                EventLog.event_type == "reconcile.completed",
                EventLog.portfolio_id == portfolio.id,
            )
        )
    ).scalar_one()
    assert event.source == "system"
    assert event.payload["positions_synced"] == 1
    assert event.payload["equity"] == "100000.00"


async def test_reconcile_does_not_close_adapter(
    db_session: AsyncSession, portfolio: Portfolio
) -> None:
    # The service must NOT close the adapter — the caller's `async with` owns
    # its lifecycle (the service reconciles, the context manager closes).
    adapter = FakeBrokerAdapter(account=_account(), positions=[], open_orders=[])
    await ReconciliationService().reconcile_portfolio(db_session, portfolio.id, adapter)
    assert adapter.closed is False
