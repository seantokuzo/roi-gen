"""Strategy model — a configured instance of an engine strategy class."""

from __future__ import annotations

import uuid
from decimal import Decimal
from typing import Any

from sqlalchemy import ForeignKey, Numeric, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base
from app.models.base import TimestampMixin, UUIDPrimaryKeyMixin
from app.models.enums import StrategyStatus


class Strategy(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """A strategy attached to a portfolio.

    ``kind`` is the engine registry key; ``status`` stores a
    :class:`app.models.enums.StrategyStatus` value. ``risk_per_trade_pct``
    of ``None`` means "use the global default from settings".
    """

    __tablename__ = "strategies"
    __table_args__ = (
        UniqueConstraint("portfolio_id", "name", name="uq_strategies_portfolio_id_name"),
    )

    portfolio_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("portfolios.id"))
    name: Mapped[str] = mapped_column(String(100))
    kind: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(32), default=StrategyStatus.draft)
    params: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    symbols: Mapped[list[str]] = mapped_column(ARRAY(String()), default=list)
    risk_per_trade_pct: Mapped[Decimal | None] = mapped_column(Numeric(6, 3))
    max_positions: Mapped[int | None]
