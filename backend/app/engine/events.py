"""Engine event taxonomy — the typed messages on the in-process FIFO bus.

One canonical cascade runs per market tick (game plan architecture):

    MarketEvent (Bar/Quote/Trade) → SignalEvent → OrderEvent → FillEvent

These are deliberately lightweight frozen-slotted dataclasses, NOT the Pydantic
broker DTOs. They are internal, high-frequency, and never serialized across a
process boundary as-is: the *live* market-data source decodes Redis JSON into
broker DTOs (``app.brokers.dto``) and wraps them here; the *simulator* (Phase 3)
constructs the very same events directly. That symmetry is the backtest/live
parity seam — identical events, identical bus, identical ``Strategy`` code (game
plan core principle #3).

Money/quantities are :class:`~decimal.Decimal`, datetimes tz-aware UTC (iron
laws #7, #5).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from app.models.enums import OrderSide, OrderType, TimeInForce

if TYPE_CHECKING:
    from app.brokers.dto import Bar, OrderRequest, Quote, Trade
    from app.engine.risk.approval import RiskApproval


def _now_utc() -> datetime:
    """tz-aware UTC now — the only wall-clock read events make (iron law #5)."""
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class Event:
    """Base marker for everything that travels on the bus."""


# ── Market data (strategy inputs) ────────────────────────────────────


@dataclass(frozen=True, slots=True)
class BarEvent(Event):
    """A completed OHLCV bar for one symbol."""

    bar: Bar

    @property
    def symbol(self) -> str:
        return self.bar.symbol


@dataclass(frozen=True, slots=True)
class QuoteEvent(Event):
    """A top-of-book quote update for one symbol."""

    quote: Quote

    @property
    def symbol(self) -> str:
        return self.quote.symbol


@dataclass(frozen=True, slots=True)
class TradeEvent(Event):
    """A single executed print on the tape for one symbol."""

    trade: Trade

    @property
    def symbol(self) -> str:
        return self.trade.symbol


MarketEvent = BarEvent | QuoteEvent | TradeEvent
"""Union of the market-data events a :class:`~app.engine.strategy.Strategy` consumes."""


# ── Strategy output → risk input ─────────────────────────────────────


@dataclass(frozen=True, slots=True)
class SignalEvent(Event):
    """A strategy's intent to OPEN a protected position.

    Strategies propose direction and protection only — ``(symbol, side, entry,
    stop)`` — and never size (game plan: "the risk engine returns qty or
    rejects"). ``entry_price`` is the reference price the risk engine sizes
    against (and the limit price for limit entries); ``stop_price`` is mandatory
    because a position without a stop is a bug (iron law #4) AND because
    fixed-fractional sizing is ``risk$ / |entry − stop|``.

    Exits are NOT signals in this design: protective legs ride on the bracket
    (broker-side) and the flatten/scheduler closes day sleeves (Phase 2c).
    """

    portfolio_id: uuid.UUID
    strategy_id: uuid.UUID
    symbol: str
    side: OrderSide
    entry_price: Decimal
    stop_price: Decimal
    take_profit_price: Decimal | None = None
    order_type: OrderType = OrderType.market
    time_in_force: TimeInForce = TimeInForce.day
    extended_hours: bool = False
    signal_id: uuid.UUID = field(default_factory=uuid.uuid4)
    created_at: datetime = field(default_factory=_now_utc)
    meta: dict[str, Any] = field(default_factory=dict)


# ── Risk output → execution input ────────────────────────────────────


@dataclass(frozen=True, slots=True)
class OrderEvent(Event):
    """A risk-approved, fully-sized order ready for the execution handler.

    Carries the broker :class:`~app.brokers.dto.OrderRequest` the risk engine
    built AND the :class:`~app.engine.risk.approval.RiskApproval` that authorized
    it. The execution handler (Phase 2b) requires the approval to act — that
    pairing is iron law #1 made physical: no approval, no order.
    """

    order_request: OrderRequest
    approval: RiskApproval

    @property
    def symbol(self) -> str:
        return self.order_request.symbol

    @property
    def side(self) -> OrderSide:
        return self.order_request.side


# ── Execution output → strategy input ────────────────────────────────


@dataclass(frozen=True, slots=True)
class FillEvent(Event):
    """A (partial) execution, sourced from the trade-updates stream (Phase 2b).

    Defined here in 2a so strategies can implement ``on_fill`` and the cascade
    type exists; the trade-updates → FillEvent producer lands in 2b.
    """

    portfolio_id: uuid.UUID
    order_id: uuid.UUID
    symbol: str
    side: OrderSide
    qty: Decimal
    price: Decimal
    occurred_at: datetime
    position_qty: Decimal | None = None
    strategy_id: uuid.UUID | None = None
