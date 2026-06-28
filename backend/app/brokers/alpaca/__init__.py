"""Alpaca concrete broker implementation (REST adapter, streams, factory).

Concrete vendor code lives under this subpackage; the broker-agnostic contract
(``app.brokers.dto`` / ``base`` / ``errors``) must never import from here.
"""

from app.brokers.alpaca.factory import AlpacaAdapterFactory, build_alpaca_adapter
from app.brokers.alpaca.rest import AlpacaBrokerAdapter
from app.brokers.alpaca.status import ALPACA_STATUS_MAP, map_status

__all__ = [
    "ALPACA_STATUS_MAP",
    "AlpacaAdapterFactory",
    "AlpacaBrokerAdapter",
    "build_alpaca_adapter",
    "map_status",
]
