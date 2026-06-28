"""Broker-agnostic data-transfer objects — the vocabulary the engine speaks.

These Pydantic v2 models are the normalized shapes every concrete adapter
(Stage 2: Alpaca, future brokers) must produce and consume. Nothing here may
import ``alpaca`` or any vendor SDK: this is the abstraction the rest of the
system keys off, and it must stay vendor-neutral.

Conventions (project iron laws):
- Money, prices, and quantities are :class:`~decimal.Decimal`, never float (#7).
- Datetimes are timezone-aware UTC, never naive (#5). ``raw`` carries the
  untouched broker payload for debugging / forensic reconciliation.
- Snapshot reads are ``frozen=True`` (immutable value objects); request models
  that callers mutate while building are not.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from app.models.enums import (
    OrderClass,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)

# ── Market calendar / clock ──────────────────────────────────────────


class MarketClock(BaseModel):
    """The broker's view of the market clock at ``timestamp`` (snapshot)."""

    model_config = ConfigDict(frozen=True)

    timestamp: datetime
    is_open: bool
    next_open: datetime
    next_close: datetime


class CalendarDay(BaseModel):
    """A single trading session. ``trading_date`` is the calendar date; the
    open/close are tz-aware UTC instants for that session (early-close days
    carry their actual shortened close)."""

    model_config = ConfigDict(frozen=True)

    trading_date: date
    session_open: datetime
    session_close: datetime


# ── Account / positions ──────────────────────────────────────────────


class BrokerAccount(BaseModel):
    """Normalized account snapshot.

    Deliberately carries **no PDT fields** (iron law #3 — FINRA retired the
    pattern-day-trader rule and Alpaca deleted ``pattern_day_trader`` /
    ``daytrade_count`` / ``daytrading_buying_power`` from its API). Sizing keys
    off ``buying_power`` plus the margin-headroom guard, never daytrade counts.
    """

    model_config = ConfigDict(frozen=True)

    account_id: str
    status: str
    currency: str
    equity: Decimal
    last_equity: Decimal
    cash: Decimal
    buying_power: Decimal
    position_market_value: Decimal
    trading_blocked: bool
    account_blocked: bool
    raw: dict[str, Any] = Field(default_factory=dict)


class BrokerPosition(BaseModel):
    """Normalized open-position snapshot.

    ``qty`` is **signed**: negative means short. ``side`` ("long"/"short") is
    kept as a convenience mirror of the sign for readability at call sites.
    """

    model_config = ConfigDict(frozen=True)

    symbol: str
    qty: Decimal
    side: str
    avg_entry_price: Decimal
    market_value: Decimal
    cost_basis: Decimal
    unrealized_pl: Decimal
    current_price: Decimal | None = None
    raw: dict[str, Any] = Field(default_factory=dict)


# ── Orders ───────────────────────────────────────────────────────────


class OrderRequest(BaseModel):
    """An order to SUBMIT.

    The caller MUST persist ``client_order_id`` BEFORE handing this to
    :meth:`~app.brokers.base.BrokerAdapter.submit_order` (engine pattern:
    persist-before-submit so an ambiguous timeout can be reconciled).

    Protection / extended-hours rules (iron law #4): bracket and trailing-stop
    orders are RTH-only, and extended-hours entries must be ``limit`` type with
    self-managed exits. The broker is the ultimate enforcer of session rules;
    the validation here rejects the unambiguous contradiction
    (``order_class=bracket`` with ``extended_hours=True``) and leaves the rest
    to the broker, which knows the live session state.
    """

    client_order_id: str
    symbol: str
    side: OrderSide
    order_type: OrderType
    time_in_force: TimeInForce
    order_class: OrderClass = OrderClass.simple
    qty: Decimal | None = None
    notional: Decimal | None = None
    limit_price: Decimal | None = None
    stop_price: Decimal | None = None
    trail_percent: Decimal | None = None
    extended_hours: bool = False
    take_profit_limit_price: Decimal | None = None
    stop_loss_stop_price: Decimal | None = None
    stop_loss_limit_price: Decimal | None = None

    @model_validator(mode="after")
    def _validate(self) -> OrderRequest:
        # Exactly one of qty / notional.
        if (self.qty is None) == (self.notional is None):
            msg = "exactly one of qty or notional must be set"
            raise ValueError(msg)
        if self.qty is not None and self.qty <= 0:
            msg = "qty must be positive"
            raise ValueError(msg)
        if self.notional is not None and self.notional <= 0:
            msg = "notional must be positive"
            raise ValueError(msg)

        # Price requirements by order type.
        if self.order_type in (OrderType.limit, OrderType.stop_limit) and self.limit_price is None:
            msg = f"{self.order_type} order requires limit_price"
            raise ValueError(msg)
        if self.order_type in (OrderType.stop, OrderType.stop_limit) and self.stop_price is None:
            msg = f"{self.order_type} order requires stop_price"
            raise ValueError(msg)
        if self.order_type == OrderType.trailing_stop and self.trail_percent is None:
            msg = "trailing_stop order requires trail_percent"
            raise ValueError(msg)

        # Bracket must carry at least one protective leg.
        if self.order_class == OrderClass.bracket and not (
            self.take_profit_limit_price is not None
            or self.stop_loss_stop_price is not None
            or self.stop_loss_limit_price is not None
        ):
            msg = "bracket order requires at least one take-profit or stop-loss parameter"
            raise ValueError(msg)

        # Iron law #4: bracket is RTH-only — never extended hours.
        if self.order_class == OrderClass.bracket and self.extended_hours:
            msg = "bracket orders are RTH-only and cannot be extended_hours (iron law #4)"
            raise ValueError(msg)

        return self


class BrokerOrder(BaseModel):
    """Normalized order read model (the shape returned by reads + the stream).

    ``legs`` holds bracket/OCO children when the adapter requested nested
    representation. ``filled_qty`` defaults to zero (never ``None``) so callers
    can do arithmetic without null guards.
    """

    model_config = ConfigDict(frozen=True)

    broker_order_id: str
    client_order_id: str | None = None
    symbol: str
    side: OrderSide
    order_type: OrderType
    order_class: OrderClass
    time_in_force: TimeInForce
    status: OrderStatus
    qty: Decimal | None = None
    filled_qty: Decimal = Decimal("0")
    limit_price: Decimal | None = None
    stop_price: Decimal | None = None
    trail_percent: Decimal | None = None
    filled_avg_price: Decimal | None = None
    extended_hours: bool = False
    submitted_at: datetime | None = None
    filled_at: datetime | None = None
    canceled_at: datetime | None = None
    legs: list[BrokerOrder] = Field(default_factory=list)
    raw: dict[str, Any] = Field(default_factory=dict)


# ── Market data ──────────────────────────────────────────────────────


class Bar(BaseModel):
    """An OHLCV bar. ``vwap`` / ``trade_count`` are present on bars from feeds
    that supply them (and are ``None`` otherwise). Note IEX volume is unreliable
    (project CLAUDE.md) — that's a feed concern, not a shape concern."""

    model_config = ConfigDict(frozen=True)

    symbol: str
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    trade_count: int | None = None
    vwap: Decimal | None = None


class Quote(BaseModel):
    """A top-of-book (NBBO/IEX) quote snapshot."""

    model_config = ConfigDict(frozen=True)

    symbol: str
    timestamp: datetime
    bid_price: Decimal
    bid_size: Decimal
    ask_price: Decimal
    ask_size: Decimal


class Trade(BaseModel):
    """A single executed print on the tape."""

    model_config = ConfigDict(frozen=True)

    symbol: str
    timestamp: datetime
    price: Decimal
    size: Decimal


# ── Trade-updates stream ─────────────────────────────────────────────


class TradeUpdate(BaseModel):
    """A trade-updates stream event — the order-state source of truth.

    Per project CLAUDE.md the trade-updates stream is authoritative for order
    state and must never be replaced by polling. ``event`` is the broker event
    name (``new`` / ``fill`` / ``partial_fill`` / ``canceled`` / ``expired`` /
    ``rejected`` / ``replaced`` / ``pending_cancel`` / ...); ``order`` is the
    full normalized order at the moment of the event. ``price`` / ``qty`` /
    ``position_qty`` describe the execution that triggered a fill event (and are
    ``None`` for non-fill events)."""

    model_config = ConfigDict(frozen=True)

    event: str
    order: BrokerOrder
    execution_id: str | None = None
    price: Decimal | None = None
    qty: Decimal | None = None
    position_qty: Decimal | None = None
    timestamp: datetime
    raw: dict[str, Any] = Field(default_factory=dict)
