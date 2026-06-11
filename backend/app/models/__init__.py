"""Domain models.

Importing this package registers every table on ``Base.metadata`` —
required by Alembic autogenerate (``alembic/env.py`` imports this module).
"""

from app.models.base import TimestampMixin, UUIDPrimaryKeyMixin
from app.models.enums import (
    EventSource,
    OrderClass,
    OrderSide,
    OrderStatus,
    OrderType,
    PortfolioMode,
    StrategyStatus,
    TimeInForce,
)
from app.models.portfolio import BrokerCredential, Portfolio
from app.models.strategy import Strategy
from app.models.telemetry import EquitySnapshot, EventLog
from app.models.trading import Fill, Lot, Order, Position
from app.models.user import User

__all__ = [
    "BrokerCredential",
    "EquitySnapshot",
    "EventLog",
    "EventSource",
    "Fill",
    "Lot",
    "Order",
    "OrderClass",
    "OrderSide",
    "OrderStatus",
    "OrderType",
    "Portfolio",
    "PortfolioMode",
    "Position",
    "Strategy",
    "StrategyStatus",
    "TimeInForce",
    "TimestampMixin",
    "User",
    "UUIDPrimaryKeyMixin",
]
