"""Telemetry: equity snapshots and the append-only event log."""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import BigInteger, DateTime, ForeignKey, Index, Numeric, String, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base
from app.models.base import UUIDPrimaryKeyMixin


class EquitySnapshot(UUIDPrimaryKeyMixin, Base):
    """Point-in-time equity/cash/buying-power snapshot for a portfolio."""

    __tablename__ = "equity_snapshots"
    __table_args__ = (Index("ix_equity_snapshots_portfolio_id_ts", "portfolio_id", "ts"),)

    portfolio_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("portfolios.id"))
    equity: Mapped[Decimal] = mapped_column(Numeric(18, 6))
    cash: Mapped[Decimal] = mapped_column(Numeric(18, 6))
    buying_power: Mapped[Decimal] = mapped_column(Numeric(18, 6))
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class EventLog(Base):
    """Append-only audit log.

    ``portfolio_id`` / ``strategy_id`` / ``order_id`` are plain UUID columns
    WITHOUT foreign-key constraints by design: the audit trail must never
    block (or be deleted by) entity deletes. ``source`` stores an
    :class:`app.models.enums.EventSource` value.
    """

    __tablename__ = "event_log"
    __table_args__ = (
        Index("ix_event_log_event_type_ts", "event_type", "ts"),
        Index("ix_event_log_ts", "ts"),
        # Recovery replay: "events for this portfolio in time order".
        Index("ix_event_log_portfolio_ts", "portfolio_id", "ts"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    source: Mapped[str] = mapped_column(String(32))
    event_type: Mapped[str] = mapped_column(String(64))
    portfolio_id: Mapped[uuid.UUID | None]
    strategy_id: Mapped[uuid.UUID | None]
    order_id: Mapped[uuid.UUID | None]
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
