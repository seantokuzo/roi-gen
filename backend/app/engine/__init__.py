"""The deterministic trading engine (the fast loop).

Phase 2a delivered the decision spine: the FIFO :class:`EventBus`, the typed
event taxonomy, the :class:`Strategy` base + registry + runner, and the
:mod:`app.engine.risk` choke point wired together by :class:`RiskStage`. No LLM
calls ever run here (iron law #2 / game plan core principle #1) — this layer is
100% deterministic code. Phase 2b adds :mod:`app.engine.execution`: the
:class:`ExecutionStage` order-mutation boundary, the trade-updates writer, and
the FIFO lot → realized-P&L ledger.
"""

from app.engine.bus import EventBus
from app.engine.events import (
    BarEvent,
    Event,
    FillEvent,
    MarketEvent,
    OrderEvent,
    QuoteEvent,
    SignalEvent,
    TradeEvent,
)
from app.engine.execution import (
    ExecutionStage,
    RedisTradeUpdateSubscriber,
    TradeUpdateStage,
)
from app.engine.stage import RiskStage
from app.engine.strategy import Strategy, StrategyRegistry, StrategyRunner, registry

__all__ = [
    "BarEvent",
    "Event",
    "EventBus",
    "ExecutionStage",
    "FillEvent",
    "MarketEvent",
    "OrderEvent",
    "QuoteEvent",
    "RedisTradeUpdateSubscriber",
    "RiskStage",
    "SignalEvent",
    "Strategy",
    "StrategyRegistry",
    "StrategyRunner",
    "TradeEvent",
    "TradeUpdateStage",
    "registry",
]
