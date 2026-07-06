"""Execution core — the order-mutation boundary and the fill → P&L ledger.

Public surface:

- :class:`~app.engine.execution.handler.ExecutionStage` — the ONLY caller of
  broker order mutations; consumes risk-approved ``OrderEvent``s (iron law #1).
- :class:`~app.engine.execution.trade_updates.TradeUpdateStage` /
  :class:`~app.engine.execution.trade_updates.RedisTradeUpdateSubscriber` —
  the trade-updates stream persisted: order state machine, fill ledger, FIFO
  lots, positions, ``FillEvent``s.
- :func:`~app.engine.execution.lots.apply_fill_to_lots` — the FIFO realized-P&L
  engine (also used by reconciliation's missed-fill synthesis).
- :func:`~app.engine.execution.state.plan_transition` — the shared order state
  machine every writer routes through.
"""

from app.engine.execution.handler import ExecutionStage
from app.engine.execution.lots import LotApplication, apply_fill_to_lots
from app.engine.execution.positions import apply_fill_to_position
from app.engine.execution.state import ABSORBING_STATUSES, TransitionPlan, plan_transition
from app.engine.execution.synthesis import synthesize_span
from app.engine.execution.trade_updates import RedisTradeUpdateSubscriber, TradeUpdateStage

__all__ = [
    "ABSORBING_STATUSES",
    "ExecutionStage",
    "LotApplication",
    "RedisTradeUpdateSubscriber",
    "TradeUpdateStage",
    "TransitionPlan",
    "apply_fill_to_lots",
    "apply_fill_to_position",
    "plan_transition",
    "synthesize_span",
]
