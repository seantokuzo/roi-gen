"""RiskEngine — the single choke point every order passes through (iron law #1).

Pure and synchronous: ``evaluate(signal, state) -> RiskDecision``. No IO, no DB,
no broker. The :class:`~app.engine.risk.state.RiskStateProvider` loads the state;
the stage persists the verdict and submits. Keeping the most consequential logic
in the project a pure function of its inputs makes it exhaustively and
deterministically testable — the simulator and the live path exercise the
identical engine.

Every evaluation runs *all* controls (no short-circuit) so the audit trail on an
approved order — and the rejection log on a blocked one — records each control's
verdict, not just the first trip. An order is built and an approval minted only
when every gate passed; that approval is the capability the execution handler
will demand (Phase 2b).
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from app.brokers.dto import OrderRequest
from app.engine.risk import controls
from app.engine.risk.approval import ControlCheck, RiskDecision, _mint_approval
from app.models.enums import OrderClass, OrderType

if TYPE_CHECKING:
    from app.engine.events import SignalEvent
    from app.engine.risk.controls import RiskLimits
    from app.engine.risk.state import RiskState

# Greppable client_order_id prefix — persisted before submission and the
# reconciliation key on an ambiguous timeout (never blind-resubmit).
_CLIENT_ORDER_ID_PREFIX = "roigen"


class RiskEngine:
    """Sizes and vets signals; the only minter of :class:`RiskApproval`."""

    def __init__(self, limits: RiskLimits) -> None:
        self._limits = limits

    def evaluate(self, signal: SignalEvent, state: RiskState) -> RiskDecision:
        """Return the verdict for ``signal`` given ``state`` (approve or reject)."""
        limits = self._limits
        sizing = controls.size_position(signal, state, limits)
        qty = sizing.qty

        checks: tuple[ControlCheck, ...] = (
            controls.check_account_tradeable(state),
            controls.check_extended_hours(signal),
            controls.check_session_open(state, limits),
            sizing.check,
            controls.check_daily_loss(state, limits, sizing.risk_amount),
            controls.check_consecutive_losses(state, limits),
            controls.check_drawdown_halt(state, limits),
            controls.check_max_positions(state),
            controls.check_no_pyramiding(state),
            controls.check_cooldown(state, limits),
            controls.check_margin_headroom(state, limits, qty=qty, entry=signal.entry_price),
            controls.check_volatility_halt(),
        )

        failed = [c for c in checks if not c.passed]
        if failed:
            reason = "; ".join(f"{c.name}: {c.detail}" for c in failed)
            return RiskDecision(checks=checks, reason=reason)

        client_order_id = f"{_CLIENT_ORDER_ID_PREFIX}-{uuid.uuid4().hex}"
        order_request = self._build_order_request(signal, qty, client_order_id)
        approval = _mint_approval(
            signal_id=signal.signal_id,
            portfolio_id=signal.portfolio_id,
            strategy_id=signal.strategy_id,
            symbol=signal.symbol,
            side=signal.side,
            qty=qty,
            entry_price=signal.entry_price,
            stop_price=signal.stop_price,
            take_profit_price=signal.take_profit_price,
            risk_pct=sizing.risk_pct,
            risk_amount=sizing.risk_amount,
            equity=state.equity,
            client_order_id=client_order_id,
            approved_at=datetime.now(UTC),
            checks=checks,
        )
        return RiskDecision(checks=checks, approval=approval, order_request=order_request)

    @staticmethod
    def _build_order_request(
        signal: SignalEvent, qty: Decimal, client_order_id: str
    ) -> OrderRequest:
        """Build a protected RTH bracket order (iron law #4).

        The stop-loss leg is mandatory (the signal carries it); take-profit is
        optional. Extended-hours signals are rejected upstream
        (``check_extended_hours``) until self-managed exits land in 2c, so every
        order built here is a bracket with broker-side protection.
        """
        limit_price = (
            signal.entry_price
            if signal.order_type in (OrderType.limit, OrderType.stop_limit)
            else None
        )
        return OrderRequest(
            client_order_id=client_order_id,
            symbol=signal.symbol,
            side=signal.side,
            order_type=signal.order_type,
            time_in_force=signal.time_in_force,
            order_class=OrderClass.bracket,
            qty=qty,
            limit_price=limit_price,
            stop_loss_stop_price=signal.stop_price,
            take_profit_limit_price=signal.take_profit_price,
        )
