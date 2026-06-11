"""Trading domain: orders, fills, positions, and FIFO lots.

Quantities are ``Numeric(18, 9)``; prices/money are ``Numeric(18, 6)`` —
always ``Decimal`` in code, never float (iron law #7).
"""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Numeric,
    String,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base
from app.models.base import TimestampMixin, UUIDPrimaryKeyMixin
from app.models.enums import OrderStatus


class Order(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """An order record — persisted (with ``client_order_id``) BEFORE submission.

    ``side`` / ``order_type`` / ``order_class`` / ``time_in_force`` / ``status``
    store string-enum values from :mod:`app.models.enums`. ``risk_approval`` is
    the risk-engine approval record (audit trail — iron law #1); ``raw`` is the
    last broker payload seen for this order.
    """

    __tablename__ = "orders"
    __table_args__ = (
        Index("ix_orders_portfolio_id_created_at", "portfolio_id", "created_at"),
        # Hot path: "open orders for this portfolio" during reconciliation/engine loops.
        Index("ix_orders_portfolio_status", "portfolio_id", "status"),
    )

    client_order_id: Mapped[str] = mapped_column(String(128), unique=True)
    broker_order_id: Mapped[str | None] = mapped_column(String(128), unique=True)
    portfolio_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("portfolios.id"))
    strategy_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("strategies.id"))
    symbol: Mapped[str] = mapped_column(String(16), index=True)
    side: Mapped[str] = mapped_column(String(32))
    order_type: Mapped[str] = mapped_column(String(32))
    order_class: Mapped[str] = mapped_column(String(32))
    time_in_force: Mapped[str] = mapped_column(String(32))
    status: Mapped[str] = mapped_column(String(32), default=OrderStatus.pending_submit, index=True)
    qty: Mapped[Decimal] = mapped_column(Numeric(18, 9))
    filled_qty: Mapped[Decimal] = mapped_column(Numeric(18, 9), default=Decimal("0"))
    limit_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 6))
    stop_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 6))
    trail_percent: Mapped[Decimal | None] = mapped_column(Numeric(18, 6))
    filled_avg_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 6))
    extended_hours: Mapped[bool] = mapped_column(default=False)
    parent_order_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("orders.id"))
    submitted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    filled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    canceled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    risk_approval: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    raw: Mapped[dict[str, Any] | None] = mapped_column(JSONB)


class Fill(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """A (partial) execution of an order, as reported by the broker stream."""

    __tablename__ = "fills"
    __table_args__ = (
        # Dedup guard: the broker stream can redeliver fill events.
        Index(
            "uq_fills_broker_fill_id",
            "broker_fill_id",
            unique=True,
            postgresql_where=text("broker_fill_id IS NOT NULL"),
        ),
    )

    order_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("orders.id"), index=True)
    broker_fill_id: Mapped[str | None] = mapped_column(String(128))
    qty: Mapped[Decimal] = mapped_column(Numeric(18, 9))
    price: Mapped[Decimal] = mapped_column(Numeric(18, 6))
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    raw: Mapped[dict[str, Any] | None] = mapped_column(JSONB)


class Position(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Net position per (portfolio, symbol) — the reconciliation-level view."""

    __tablename__ = "positions"
    __table_args__ = (
        UniqueConstraint("portfolio_id", "symbol", name="uq_positions_portfolio_id_symbol"),
    )

    portfolio_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("portfolios.id"))
    symbol: Mapped[str] = mapped_column(String(16))
    qty: Mapped[Decimal] = mapped_column(Numeric(18, 9))
    avg_entry_price: Mapped[Decimal] = mapped_column(Numeric(18, 6))


class Lot(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """A FIFO lot. ``side`` stores an :class:`app.models.enums.OrderSide` value:

    long lots are buys; short lots are sells.
    """

    __tablename__ = "lots"
    __table_args__ = (
        Index("ix_lots_portfolio_id_symbol_opened_at", "portfolio_id", "symbol", "opened_at"),
        CheckConstraint("qty_orig > 0", name="ck_lots_qty_orig_positive"),
        CheckConstraint("qty_open >= 0", name="ck_lots_qty_open_nonneg"),
        CheckConstraint("qty_open <= qty_orig", name="ck_lots_qty_open_le_orig"),
    )

    portfolio_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("portfolios.id"))
    strategy_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("strategies.id"))
    symbol: Mapped[str] = mapped_column(String(16), index=True)
    side: Mapped[str] = mapped_column(String(32))
    qty_orig: Mapped[Decimal] = mapped_column(Numeric(18, 9))
    qty_open: Mapped[Decimal] = mapped_column(Numeric(18, 9))
    entry_price: Mapped[Decimal] = mapped_column(Numeric(18, 6))
    entry_fill_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("fills.id"))
    opened_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    realized_pnl: Mapped[Decimal] = mapped_column(Numeric(18, 6), default=Decimal("0"))
