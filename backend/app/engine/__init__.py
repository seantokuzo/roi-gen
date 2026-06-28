"""The deterministic trading engine (the fast loop).

Phase 2a delivers the decision spine: the FIFO :class:`EventBus`, the typed
event taxonomy, the :class:`Strategy` base + registry + runner, and the
:mod:`app.engine.risk` choke point wired together by :class:`RiskStage`. No LLM
calls ever run here (iron law #2 / game plan core principle #1) — this layer is
100% deterministic code. The execution handler and order state machine that
consume the ``OrderEvent`` land in Phase 2b.
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
from app.engine.stage import RiskStage
from app.engine.strategy import Strategy, StrategyRegistry, StrategyRunner, registry

__all__ = [
    "BarEvent",
    "Event",
    "EventBus",
    "FillEvent",
    "MarketEvent",
    "OrderEvent",
    "QuoteEvent",
    "RiskStage",
    "SignalEvent",
    "Strategy",
    "StrategyRegistry",
    "StrategyRunner",
    "TradeEvent",
    "registry",
]
