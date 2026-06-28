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
)
from app.core.config import Settings
from app.engine.events import SignalEvent
from app.engine.risk.controls import RiskLimits
from app.engine.risk.state import RiskState
from app.models.enums import OrderSide, PortfolioMode, StrategyStatus
from app.models.portfolio import Portfolio
from app.models.strategy import Strategy as StrategyModel
from app.models.telemetry import EquitySnapshot
from app.models.trading import Lot, Position

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
        "day_realized_pnl_portfolio": Decimal("0"),
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
) -> StrategyModel:
    strategy = StrategyModel(
        portfolio_id=portfolio_id,
        name=name,
        kind=kind,
        status=status,
        params={},
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
