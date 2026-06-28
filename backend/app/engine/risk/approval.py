"""The risk-approval capability object — iron law #1 made structural.

Iron law #1: *every* order passes through the Risk Engine. Legacy's fatal sin
was a risk layer that 2 of 3 order paths simply skipped. We make that bypass
impossible by accident and glaring on purpose:

- The execution handler (Phase 2b) requires a :class:`RiskApproval` to act.
- A :class:`RiskApproval` can be minted ONLY by the Risk Engine: its constructor
  demands a module-private capability key (:data:`_MINT_KEY`) that nothing
  outside this module holds, and :func:`_mint_approval` (the sole stamper) is
  underscore-private and called only by :class:`~app.engine.risk.engine.RiskEngine`
  after every control has run.

So "submit an order without risk approval" does not type-check (you have no
approval to pass) and does not run (you cannot forge one). For a single-user
system this is belt-and-suspenders, but the iron law earns it.
"""

from __future__ import annotations

import uuid
from dataclasses import InitVar, dataclass
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from app.models.enums import OrderSide

if TYPE_CHECKING:
    from app.brokers.dto import OrderRequest

# Held only by this module. _mint_approval() stamps it; the RiskApproval
# constructor rejects any instance built without it. Deliberately NOT exported —
# importing it to forge an approval is exactly what code review exists to catch.
_MINT_KEY = object()


@dataclass(frozen=True, slots=True)
class ControlCheck:
    """One risk control's verdict, captured for the order's audit trail.

    Every evaluation records a check per control (passed or not) so
    ``order.risk_approval`` and the rejection log tell the *whole* story of why
    an order was allowed or blocked — not just the first thing that tripped.
    """

    name: str
    passed: bool
    detail: str
    limit: str | None = None
    observed: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "passed": self.passed,
            "detail": self.detail,
            "limit": self.limit,
            "observed": self.observed,
        }


@dataclass(frozen=True)
class RiskApproval:
    """Proof that an order was sized and cleared by the Risk Engine.

    Construct ONLY via :func:`_mint_approval` (Risk Engine internal). The
    ``_mint`` init-var gates construction; a wrong/absent key raises.
    """

    approval_id: uuid.UUID
    signal_id: uuid.UUID
    portfolio_id: uuid.UUID
    strategy_id: uuid.UUID
    symbol: str
    side: OrderSide
    qty: Decimal
    entry_price: Decimal
    stop_price: Decimal
    take_profit_price: Decimal | None
    risk_pct: Decimal
    risk_amount: Decimal
    equity: Decimal
    client_order_id: str
    approved_at: datetime
    checks: tuple[ControlCheck, ...]
    _mint: InitVar[object]

    def __post_init__(self, _mint: object) -> None:
        if _mint is not _MINT_KEY:
            msg = "RiskApproval must be minted by the Risk Engine (iron law #1)"
            raise RuntimeError(msg)

    def audit_payload(self) -> dict[str, Any]:
        """JSON-serializable approval record for ``order.risk_approval`` (iron law #1)."""
        return {
            "approval_id": str(self.approval_id),
            "signal_id": str(self.signal_id),
            "strategy_id": str(self.strategy_id),
            "symbol": self.symbol,
            "side": self.side.value,
            "qty": str(self.qty),
            "entry_price": str(self.entry_price),
            "stop_price": str(self.stop_price),
            "take_profit_price": (
                str(self.take_profit_price) if self.take_profit_price is not None else None
            ),
            "risk_pct": str(self.risk_pct),
            "risk_amount": str(self.risk_amount),
            "equity": str(self.equity),
            "client_order_id": self.client_order_id,
            "approved_at": self.approved_at.isoformat(),
            "checks": [c.to_dict() for c in self.checks],
        }


def _mint_approval(
    *,
    signal_id: uuid.UUID,
    portfolio_id: uuid.UUID,
    strategy_id: uuid.UUID,
    symbol: str,
    side: OrderSide,
    qty: Decimal,
    entry_price: Decimal,
    stop_price: Decimal,
    take_profit_price: Decimal | None,
    risk_pct: Decimal,
    risk_amount: Decimal,
    equity: Decimal,
    client_order_id: str,
    approved_at: datetime,
    checks: tuple[ControlCheck, ...],
) -> RiskApproval:
    """Risk-Engine-internal: stamp the capability key onto a new approval.

    The ONLY caller is :class:`~app.engine.risk.engine.RiskEngine`, after all
    controls have passed. Do not call from anywhere else (iron law #1).
    """
    return RiskApproval(
        approval_id=uuid.uuid4(),
        signal_id=signal_id,
        portfolio_id=portfolio_id,
        strategy_id=strategy_id,
        symbol=symbol,
        side=side,
        qty=qty,
        entry_price=entry_price,
        stop_price=stop_price,
        take_profit_price=take_profit_price,
        risk_pct=risk_pct,
        risk_amount=risk_amount,
        equity=equity,
        client_order_id=client_order_id,
        approved_at=approved_at,
        checks=checks,
        _mint=_MINT_KEY,
    )


@dataclass(frozen=True, slots=True)
class RiskDecision:
    """The Risk Engine's verdict on one signal.

    ``approval`` and ``order_request`` are non-None iff approved; otherwise
    ``reason`` summarizes the block. ``checks`` always holds every control's
    verdict (the full audit), regardless of outcome.
    """

    checks: tuple[ControlCheck, ...]
    approval: RiskApproval | None = None
    order_request: OrderRequest | None = None
    reason: str | None = None

    @property
    def approved(self) -> bool:
        return self.approval is not None

    @property
    def failed_checks(self) -> tuple[ControlCheck, ...]:
        return tuple(c for c in self.checks if not c.passed)
