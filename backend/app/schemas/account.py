"""Pydantic v2 OUT schemas for the account-state API.

These are thin projections of the broker-agnostic DTOs
(:mod:`app.brokers.dto`) and the reconciliation result. They deliberately
carry **no PDT fields** (iron law #3) and keep money/qty as
:class:`~decimal.Decimal` (iron law #7) — Pydantic serializes ``Decimal``
to a JSON number losslessly. ``raw`` broker payloads are NOT echoed: the
API surface stays normalized and vendor-neutral.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel

from app.brokers.dto import BrokerAccount, BrokerOrder, BrokerPosition
from app.models.enums import OrderClass, OrderSide, OrderStatus, OrderType, TimeInForce
from app.services.reconciliation import ReconcileResult


class AccountOut(BaseModel):
    """Public account snapshot — mirror of :class:`BrokerAccount`, no PDT."""

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

    @classmethod
    def from_broker(cls, account: BrokerAccount) -> AccountOut:
        """Project a :class:`BrokerAccount` (drops ``raw``)."""
        return cls(
            account_id=account.account_id,
            status=account.status,
            currency=account.currency,
            equity=account.equity,
            last_equity=account.last_equity,
            cash=account.cash,
            buying_power=account.buying_power,
            position_market_value=account.position_market_value,
            trading_blocked=account.trading_blocked,
            account_blocked=account.account_blocked,
        )


class PositionOut(BaseModel):
    """Public position snapshot — mirror of :class:`BrokerPosition`."""

    symbol: str
    qty: Decimal
    side: str
    avg_entry_price: Decimal
    market_value: Decimal
    cost_basis: Decimal
    unrealized_pl: Decimal
    current_price: Decimal | None = None

    @classmethod
    def from_broker(cls, position: BrokerPosition) -> PositionOut:
        """Project a :class:`BrokerPosition` (drops ``raw``)."""
        return cls(
            symbol=position.symbol,
            qty=position.qty,
            side=position.side,
            avg_entry_price=position.avg_entry_price,
            market_value=position.market_value,
            cost_basis=position.cost_basis,
            unrealized_pl=position.unrealized_pl,
            current_price=position.current_price,
        )


class OrderOut(BaseModel):
    """Public order snapshot — mirror of :class:`BrokerOrder` (no ``raw``/``legs``)."""

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

    @classmethod
    def from_broker(cls, order: BrokerOrder) -> OrderOut:
        """Project a :class:`BrokerOrder` (drops ``raw`` and nested ``legs``)."""
        return cls(
            broker_order_id=order.broker_order_id,
            client_order_id=order.client_order_id,
            symbol=order.symbol,
            side=order.side,
            order_type=order.order_type,
            order_class=order.order_class,
            time_in_force=order.time_in_force,
            status=order.status,
            qty=order.qty,
            filled_qty=order.filled_qty,
            limit_price=order.limit_price,
            stop_price=order.stop_price,
            trail_percent=order.trail_percent,
            filled_avg_price=order.filled_avg_price,
            extended_hours=order.extended_hours,
            submitted_at=order.submitted_at,
            filled_at=order.filled_at,
            canceled_at=order.canceled_at,
        )


class ReconcileResultOut(BaseModel):
    """Public reconciliation summary — mirror of :class:`ReconcileResult`."""

    portfolio_id: uuid.UUID
    positions_synced: int
    positions_removed: int
    orders_updated: int
    orphans: int
    missing: int
    equity: Decimal

    @classmethod
    def from_result(cls, result: ReconcileResult) -> ReconcileResultOut:
        """Project a :class:`ReconcileResult`."""
        return cls(
            portfolio_id=result.portfolio_id,
            positions_synced=result.positions_synced,
            positions_removed=result.positions_removed,
            orders_updated=result.orders_updated,
            orphans=result.orphans,
            missing=result.missing,
            equity=result.equity,
        )
