"""Broker contract layer — the broker-agnostic spine.

Public surface every later stage (concrete adapters, execution handler,
reconciliation, account API) depends on. Nothing in this package imports a
vendor SDK; concrete adapters live in their own modules and implement
:class:`BrokerAdapter`.
"""

from app.brokers.base import BrokerAdapter, BrokerAdapterFactory
from app.brokers.credentials import BrokerCredentials, load_credentials
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
    TradeUpdate,
)
from app.brokers.errors import (
    AmbiguousOrderState,
    BrokerAuthError,
    BrokerError,
    BrokerRateLimited,
    BrokerUnavailable,
    CredentialsNotFound,
    OrderRejected,
)
from app.brokers.ratelimit import AsyncTokenBucket

__all__ = [
    "AmbiguousOrderState",
    "AsyncTokenBucket",
    "Bar",
    "BrokerAccount",
    "BrokerAdapter",
    "BrokerAdapterFactory",
    "BrokerAuthError",
    "BrokerCredentials",
    "BrokerError",
    "BrokerOrder",
    "BrokerPosition",
    "BrokerRateLimited",
    "BrokerUnavailable",
    "CalendarDay",
    "CredentialsNotFound",
    "MarketClock",
    "OrderRejected",
    "OrderRequest",
    "Quote",
    "Trade",
    "TradeUpdate",
    "load_credentials",
]
