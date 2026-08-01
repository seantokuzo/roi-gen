"""Test builders + a fake adapter for the engine/risk suite.

Pure ``make_*`` builders construct domain / event / state objects with sensible
defaults so each test overrides only the field it exercises. The async ``seed_*``
helpers populate the test DB for the RiskStateProvider / RiskStage integration
tests. The :class:`FakeEngineAdapter` implements the full ``BrokerAdapter``
contract but raises on every order mutation — iron law #1 means risk tests must
never reach the broker.
"""

from __future__ import annotations

import dataclasses
import uuid
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from app.brokers.base import BrokerAdapter
from app.brokers.dto import (
    Bar,
    BrokerAccount,
    BrokerOrder,
    BrokerPosition,
    CalendarDay,
    MarketClock,
    OrderRequest,
    Quote,
    Trade,
    TradeUpdate,
)
from app.core.config import Settings
from app.engine.events import SignalEvent
from app.engine.risk.controls import RiskLimits
from app.engine.risk.state import RiskState
from app.models.enums import (
    OrderClass,
    OrderSide,
    OrderStatus,
    OrderType,
    PortfolioMode,
    StrategyStatus,
    TimeInForce,
)
from app.models.portfolio import Portfolio
from app.models.strategy import Strategy as StrategyModel
from app.models.telemetry import EquitySnapshot
from app.models.trading import Lot, LotClose, Order, Position

if TYPE_CHECKING:
    from collections.abc import Iterable

    from sqlalchemy.ext.asyncio import AsyncSession

# Anchor instant: Friday 2026-06-26, 11:00 ET = 15:00 UTC (EDT, UTC−4) — mid
# session, clear of the open and the flatten buffer, so the default state and
# clock cleanly approve. ET day starts at 04:00 UTC that date.
DEFAULT_NOW = datetime(2026, 6, 26, 15, 0, tzinfo=UTC)


# ── Broker DTO builders ──────────────────────────────────────────────


def make_account(**over: Any) -> BrokerAccount:
    fields: dict[str, Any] = {
        "account_id": "paper-acct",
        "status": "ACTIVE",
        "currency": "USD",
        "equity": Decimal("100000"),
        "last_equity": Decimal("100000"),
        "cash": Decimal("100000"),
        "buying_power": Decimal("400000"),
        "position_market_value": Decimal("0"),
        "trading_blocked": False,
        "account_blocked": False,
    }
    fields.update(over)
    return BrokerAccount(**fields)


def make_clock(
    *,
    is_open: bool = True,
    now: datetime = DEFAULT_NOW,
    next_close: datetime | None = None,
    next_open: datetime | None = None,
) -> MarketClock:
    return MarketClock(
        timestamp=now,
        is_open=is_open,
        next_open=next_open if next_open is not None else now + timedelta(hours=20),
        next_close=next_close if next_close is not None else now + timedelta(hours=5),
    )


def make_bar(
    symbol: str = "SPY", *, close: Decimal = Decimal("100"), ts: datetime = DEFAULT_NOW
) -> Bar:
    return Bar(
        symbol=symbol,
        timestamp=ts,
        open=close,
        high=close,
        low=close,
        close=close,
        volume=Decimal("1000"),
    )


def make_quote(symbol: str = "SPY", *, ts: datetime = DEFAULT_NOW) -> Quote:
    return Quote(
        symbol=symbol,
        timestamp=ts,
        bid_price=Decimal("99.99"),
        bid_size=Decimal("100"),
        ask_price=Decimal("100.01"),
        ask_size=Decimal("100"),
    )


def make_trade(symbol: str = "SPY", *, ts: datetime = DEFAULT_NOW) -> Trade:
    return Trade(symbol=symbol, timestamp=ts, price=Decimal("100"), size=Decimal("10"))


class FakeEngineAdapter(BrokerAdapter):
    """Read-only adapter returning canned account/clock; asserts on any mutation."""

    def __init__(self, *, account: BrokerAccount | None = None, clock: MarketClock | None = None):
        self._account = account if account is not None else make_account()
        self._clock = clock if clock is not None else make_clock()

    async def get_clock(self) -> MarketClock:
        return self._clock

    async def get_calendar(self, start: date, end: date) -> list[CalendarDay]:
        return []

    async def get_account(self) -> BrokerAccount:
        return self._account

    async def list_positions(self) -> list[BrokerPosition]:
        return []

    async def get_position(self, symbol: str) -> BrokerPosition | None:
        return None

    async def list_orders(
        self,
        *,
        status: str = "open",
        after: datetime | None = None,
        until: datetime | None = None,
        limit: int = 100,
        nested: bool = True,
    ) -> list[BrokerOrder]:
        return []

    async def get_order_by_client_id(self, client_order_id: str) -> BrokerOrder | None:
        return None

    async def get_order(self, broker_order_id: str) -> BrokerOrder | None:
        return None

    async def submit_order(self, req: OrderRequest) -> BrokerOrder:
        raise AssertionError("risk tests must never submit an order (iron law #1)")

    async def cancel_order(self, broker_order_id: str) -> None:
        raise AssertionError("risk tests must never mutate broker state")

    async def cancel_all_orders(self) -> None:
        raise AssertionError("risk tests must never mutate broker state")

    async def close_position(
        self, symbol: str, *, qty: Decimal | None = None, percentage: Decimal | None = None
    ) -> BrokerOrder:
        raise AssertionError("risk tests must never mutate broker state")

    async def close_all_positions(self, *, cancel_orders: bool = True) -> None:
        raise AssertionError("risk tests must never mutate broker state")

    async def aclose(self) -> None:
        return None


def make_broker_order(**over: Any) -> BrokerOrder:
    """A broker order read model; defaults mirror a fresh bracket parent."""
    fields: dict[str, Any] = {
        "broker_order_id": "bo-1",
        "client_order_id": "roigen-test",
        "symbol": "SPY",
        "side": OrderSide.buy,
        "order_type": OrderType.market,
        "order_class": OrderClass.bracket,
        "time_in_force": TimeInForce.day,
        "status": OrderStatus.accepted,
        "qty": Decimal("250"),
    }
    fields.update(over)
    return BrokerOrder(**fields)


def make_trade_update(**over: Any) -> TradeUpdate:
    """A trade-updates stream event; default is a full fill of the default order."""
    order_over = over.pop("order", None)
    fields: dict[str, Any] = {
        "event": "fill",
        "order": order_over
        if order_over is not None
        else make_broker_order(
            status=OrderStatus.filled,
            filled_qty=Decimal("250"),
            filled_avg_price=Decimal("100"),
        ),
        "execution_id": "exec-1",
        "price": Decimal("100"),
        "qty": Decimal("250"),
        "position_qty": Decimal("250"),
        "timestamp": DEFAULT_NOW,
    }
    fields.update(over)
    return TradeUpdate(**fields)


class RecordingAdapter(FakeEngineAdapter):
    """A :class:`FakeEngineAdapter` whose ``submit_order`` records and responds.

    Execution-core tests are the ONLY place broker mutations may be faked —
    the base class keeps asserting for risk tests. Configure per test:

    - ``submit_result``: the :class:`BrokerOrder` a submit returns, or an
      exception instance to raise.
    - ``lookup_results``: successive returns for ``get_order_by_client_id``
      (the ambiguous-resolve loop).
    - ``orders_by_id``: canned ``get_order`` responses (nested-leg fetches).
    """

    def __init__(
        self,
        *,
        account: BrokerAccount | None = None,
        clock: MarketClock | None = None,
        submit_result: BrokerOrder | Exception | None = None,
        lookup_results: list[BrokerOrder | None] | None = None,
        orders_by_id: dict[str, BrokerOrder] | None = None,
        open_orders: list[BrokerOrder] | None = None,
        positions: list[BrokerPosition] | None = None,
    ) -> None:
        super().__init__(account=account, clock=clock)
        self.submit_result = submit_result
        self.lookup_results = list(lookup_results or [])
        self.orders_by_id = dict(orders_by_id or {})
        self.open_orders = list(open_orders or [])
        self.positions = list(positions or [])
        self.submitted: list[OrderRequest] = []
        self.client_id_lookups: list[str] = []

    async def submit_order(self, req: OrderRequest) -> BrokerOrder:
        self.submitted.append(req)
        result = self.submit_result
        if isinstance(result, Exception):
            raise result
        if result is None:
            return make_broker_order(
                client_order_id=req.client_order_id,
                symbol=req.symbol,
                side=req.side,
                order_type=req.order_type,
                order_class=req.order_class,
                time_in_force=req.time_in_force,
                qty=req.qty,
            )
        return result

    async def get_order_by_client_id(self, client_order_id: str) -> BrokerOrder | None:
        self.client_id_lookups.append(client_order_id)
        if self.lookup_results:
            return self.lookup_results.pop(0)
        return None

    async def get_order(self, broker_order_id: str) -> BrokerOrder | None:
        return self.orders_by_id.get(broker_order_id)

    async def list_orders(
        self,
        *,
        status: str = "open",
        after: datetime | None = None,
        until: datetime | None = None,
        limit: int = 100,
        nested: bool = True,
    ) -> list[BrokerOrder]:
        return list(self.open_orders)

    async def list_positions(self) -> list[BrokerPosition]:
        return list(self.positions)


# ── Event / state / limits builders ──────────────────────────────────


def make_signal(**over: Any) -> SignalEvent:
    """Default: buy SPY, entry 100 / stop 99 (1.00/sh risk distance)."""
    fields: dict[str, Any] = {
        "portfolio_id": uuid.uuid4(),
        "strategy_id": uuid.uuid4(),
        "symbol": "SPY",
        "side": OrderSide.buy,
        "entry_price": Decimal("100"),
        "stop_price": Decimal("99"),
    }
    fields.update(over)
    return SignalEvent(**fields)


def make_state(**over: Any) -> RiskState:
    """A state that cleanly approves the default signal; override to trip a gate."""
    now: datetime = over.pop("now", DEFAULT_NOW)
    fields: dict[str, Any] = {
        "portfolio_id": uuid.uuid4(),
        "strategy_id": uuid.uuid4(),
        "symbol": "SPY",
        "now": now,
        "market_open": True,
        "next_close": now + timedelta(hours=5),
        "equity": Decimal("100000"),
        "last_equity": Decimal("100000"),
        "cash": Decimal("100000"),
        "buying_power": Decimal("400000"),
        "position_market_value": Decimal("0"),
        "trading_blocked": False,
        "account_blocked": False,
        "strategy_proven": False,
        "strategy_risk_pct": None,
        "strategy_max_positions": None,
        "strategy_open_qty": Decimal("0"),
        "open_positions_count": 0,
        "day_realized_pnl_strategy": Decimal("0"),
        "consecutive_losses": 0,
        "peak_equity": Decimal("100000"),
        "last_entry_at": None,
        "trading_halted": False,
    }
    fields.update(over)
    return RiskState(**fields)


def make_limits(**over: Any) -> RiskLimits:
    """RiskLimits at config defaults, with optional per-field overrides."""
    base = RiskLimits.from_settings(Settings(_env_file=None))
    return dataclasses.replace(base, **over)


# ── DB seeders (async; flush so the same session sees the rows) ──────


async def seed_portfolio(
    session: AsyncSession,
    user_id: uuid.UUID,
    *,
    name: str = "Risk Test",
    mode: PortfolioMode = PortfolioMode.paper,
) -> Portfolio:
    portfolio = Portfolio(user_id=user_id, name=name, mode=mode)
    session.add(portfolio)
    await session.flush()
    return portfolio


async def seed_strategy(
    session: AsyncSession,
    portfolio_id: uuid.UUID,
    *,
    name: str = "strat",
    kind: str = "test",
    status: StrategyStatus = StrategyStatus.paper,
    risk_per_trade_pct: Decimal | None = None,
    max_positions: int | None = None,
    symbols: Iterable[str] = ("SPY",),
    params: dict[str, Any] | None = None,
) -> StrategyModel:
    strategy = StrategyModel(
        portfolio_id=portfolio_id,
        name=name,
        kind=kind,
        status=status,
        params=params if params is not None else {},
        symbols=list(symbols),
        risk_per_trade_pct=risk_per_trade_pct,
        max_positions=max_positions,
    )
    session.add(strategy)
    await session.flush()
    return strategy


async def seed_lot(
    session: AsyncSession,
    portfolio_id: uuid.UUID,
    strategy_id: uuid.UUID | None,
    *,
    symbol: str = "SPY",
    side: OrderSide = OrderSide.buy,
    qty_orig: Decimal = Decimal("10"),
    qty_open: Decimal = Decimal("0"),
    entry_price: Decimal = Decimal("100"),
    opened_at: datetime | None = None,
    closed_at: datetime | None = None,
    realized_pnl: Decimal = Decimal("0"),
) -> Lot:
    lot = Lot(
        portfolio_id=portfolio_id,
        strategy_id=strategy_id,
        symbol=symbol,
        side=side,
        qty_orig=qty_orig,
        qty_open=qty_open,
        entry_price=entry_price,
        opened_at=opened_at if opened_at is not None else DEFAULT_NOW,
        closed_at=closed_at,
        realized_pnl=realized_pnl,
    )
    session.add(lot)
    await session.flush()
    return lot


async def seed_lot_close(
    session: AsyncSession,
    lot: Lot,
    *,
    qty: Decimal | None = None,
    realized_pnl: Decimal = Decimal("0"),
    closed_at: datetime | None = None,
) -> LotClose:
    """A per-close ledger row for ``lot`` (defaults to closing its full size)."""
    close = LotClose(
        lot_id=lot.id,
        portfolio_id=lot.portfolio_id,
        strategy_id=lot.strategy_id,
        symbol=lot.symbol,
        qty=qty if qty is not None else lot.qty_orig,
        realized_pnl=realized_pnl,
        closed_at=closed_at if closed_at is not None else DEFAULT_NOW,
    )
    session.add(close)
    await session.flush()
    return close


async def seed_order(
    session: AsyncSession,
    portfolio_id: uuid.UUID,
    strategy_id: uuid.UUID | None,
    *,
    client_order_id: str | None = None,
    broker_order_id: str | None = None,
    symbol: str = "SPY",
    side: OrderSide = OrderSide.buy,
    order_type: OrderType = OrderType.market,
    order_class: OrderClass = OrderClass.bracket,
    status: OrderStatus = OrderStatus.accepted,
    qty: Decimal = Decimal("250"),
    filled_qty: Decimal = Decimal("0"),
    parent_order_id: uuid.UUID | None = None,
    created_at: datetime | None = None,
) -> Order:
    order = Order(
        client_order_id=client_order_id
        if client_order_id is not None
        else f"roigen-{uuid.uuid4().hex}",
        broker_order_id=broker_order_id,
        portfolio_id=portfolio_id,
        strategy_id=strategy_id,
        symbol=symbol,
        side=side.value,
        order_type=order_type.value,
        order_class=order_class.value,
        time_in_force=TimeInForce.day.value,
        status=status.value,
        qty=qty,
        filled_qty=filled_qty,
        parent_order_id=parent_order_id,
    )
    if created_at is not None:  # override the server default for day-boundary tests
        order.created_at = created_at
    session.add(order)
    await session.flush()
    return order


async def seed_equity_snapshot(
    session: AsyncSession,
    portfolio_id: uuid.UUID,
    *,
    equity: Decimal,
    cash: Decimal = Decimal("0"),
    buying_power: Decimal = Decimal("0"),
    ts: datetime | None = None,
) -> EquitySnapshot:
    snapshot = EquitySnapshot(
        portfolio_id=portfolio_id,
        equity=equity,
        cash=cash,
        buying_power=buying_power,
        ts=ts if ts is not None else DEFAULT_NOW,
    )
    session.add(snapshot)
    await session.flush()
    return snapshot


async def seed_position(
    session: AsyncSession,
    portfolio_id: uuid.UUID,
    *,
    symbol: str = "SPY",
    qty: Decimal,
    avg_entry_price: Decimal = Decimal("100"),
) -> Position:
    position = Position(
        portfolio_id=portfolio_id,
        symbol=symbol,
        qty=qty,
        avg_entry_price=avg_entry_price,
    )
    session.add(position)
    await session.flush()
    return position
