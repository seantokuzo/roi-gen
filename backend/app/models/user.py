"""User model (single-user system, but the schema doesn't assume it)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base
from app.models.base import TimestampMixin, UUIDPrimaryKeyMixin

if TYPE_CHECKING:
    from app.models.portfolio import Portfolio


class User(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """An authenticated account (Google OAuth → JWT)."""

    __tablename__ = "users"

    email: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    display_name: Mapped[str | None] = mapped_column(String(255))

    portfolios: Mapped[list[Portfolio]] = relationship(back_populates="user")
