"""Portfolio and per-portfolio broker credentials."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from sqlalchemy import ForeignKey, Index, String, Text, UniqueConstraint, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base
from app.models.base import TimestampMixin, UUIDPrimaryKeyMixin

if TYPE_CHECKING:
    from app.models.user import User


class Portfolio(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """A trading portfolio (paper or live) owned by a user.

    ``mode`` stores a :class:`app.models.enums.PortfolioMode` value.
    """

    __tablename__ = "portfolios"
    __table_args__ = (
        UniqueConstraint("user_id", "name", name="uq_portfolios_user_id_name"),
        # DB-enforced "one default per user" — closes the race the PATCH
        # endpoint's clear-siblings UPDATE can't (partial unique index).
        Index(
            "uq_portfolios_one_default_per_user",
            "user_id",
            unique=True,
            postgresql_where=text("is_default"),
        ),
    )

    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"))
    name: Mapped[str] = mapped_column(String(100))
    mode: Mapped[str] = mapped_column(String(32))
    description: Mapped[str | None] = mapped_column(Text)
    is_default: Mapped[bool] = mapped_column(default=False)

    user: Mapped[User] = relationship(back_populates="portfolios")
    broker_credential: Mapped[BrokerCredential | None] = relationship(
        back_populates="portfolio",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class BrokerCredential(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Encrypted broker API credentials — exactly one set per portfolio.

    The portfolio FK is the only ``ON DELETE CASCADE`` in the schema:
    credentials must never outlive their portfolio (iron law #9).
    """

    __tablename__ = "broker_credentials"

    portfolio_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("portfolios.id", ondelete="CASCADE"), unique=True
    )
    broker: Mapped[str] = mapped_column(String(32), default="alpaca")
    api_key_encrypted: Mapped[str] = mapped_column(Text)
    api_secret_encrypted: Mapped[str] = mapped_column(Text)
    paper: Mapped[bool]

    portfolio: Mapped[Portfolio] = relationship(back_populates="broker_credential")
