"""ExecutionStage — the ONLY code path that calls broker order mutations.

Iron law #1, made physical: this stage consumes ``OrderEvent``s, and an
``OrderEvent`` cannot exist without a mint-guarded
:class:`~app.engine.risk.approval.RiskApproval`. Nothing else in the codebase
may call :meth:`~app.brokers.base.BrokerAdapter.submit_order` — that is a
blocking review finding by project convention.

The submit discipline (game-plan core principle #4, "always recoverable"):

1. **Persist before submit.** The ``Order`` row (status ``pending_submit``,
   the risk approval as its audit payload) is committed BEFORE the broker call,
   so a crash or timeout can always be reconciled by ``client_order_id``.
2. **Never blind-resubmit.** An ambiguous failure (timeout / dropped connection
   / 5xx response mid-submit) starts a bounded lookup loop against
   ``get_order_by_client_id``; if the broker knows the order we adopt its
   truth, otherwise the row stays ``pending_submit`` for reconciliation to age
   out. A resubmit after a silently-accepted order would double real money.
3. **Only provable non-placement is terminal-``failed``.** A definitive 422 is
   ``rejected``; a 429 was refused before processing (``failed``); everything
   else — including unexpected exceptions — is treated as ambiguous, because
   guessing "not placed" about a live order orphans its fills.

The post-submit apply re-reads the row under ``SELECT … FOR UPDATE`` and routes
state through the shared transition rules: on paper, Alpaca's trade-updates
stream regularly beats the REST submit response, so the row may already be
``filled`` by the time the response lands — the apply must not regress it.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from app.brokers.errors import (
    AmbiguousOrderState,
    BrokerRateLimited,
    OrderRejected,
)
from app.core.logging import get_logger
from app.engine.events import OrderEvent
from app.engine.execution.apply import (
    apply_snapshot,
    lock_order_by_client_id,
    persist_bracket_legs,
)
from app.models.enums import EventSource, OrderClass, OrderStatus
from app.models.telemetry import EventLog
from app.models.trading import Order

if TYPE_CHECKING:
    import uuid
    from collections.abc import Sequence

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from app.brokers.base import BrokerAdapter
    from app.brokers.dto import BrokerOrder, OrderRequest
    from app.engine.bus import EventBus
    from app.engine.risk.approval import RiskApproval

log = get_logger("engine.execution")

# Ambiguous-submit resolution: lookup delays between get_order_by_client_id
# attempts. Bounded and short — anything unresolved is reconciliation's job.
_DEFAULT_RESOLVE_DELAYS: tuple[float, ...] = (1.0, 2.0, 4.0)


class ExecutionStage:
    """Subscribes to ``OrderEvent`` and owns the order-mutation boundary."""

    def __init__(
        self,
        *,
        bus: EventBus,
        session_factory: async_sessionmaker[AsyncSession],
        adapter: BrokerAdapter,
        resolve_delays: Sequence[float] = _DEFAULT_RESOLVE_DELAYS,
    ) -> None:
        self._bus = bus
        self._session_factory = session_factory
        self._adapter = adapter
        self._resolve_delays = tuple(resolve_delays)

    def register_handlers(self) -> None:
        self._bus.subscribe(OrderEvent, self._on_order)

    # ── The one order path ───────────────────────────────────────────

    async def _on_order(self, event: OrderEvent) -> None:
        """Persist → submit → apply, self-auditing every outcome.

        The bus swallows handler exceptions, so nothing may escape silently:
        every branch below either commits an audit row or logs + audits on a
        fresh session.
        """
        req = event.order_request
        approval = event.approval
        try:
            mismatch = _pairing_mismatch(req, approval)
            if mismatch is not None:
                log.error(
                    "engine.execution.pairing_mismatch",
                    client_order_id=req.client_order_id,
                    detail=mismatch,
                )
                await self._audit(
                    "order.error",
                    portfolio_id=approval.portfolio_id,
                    strategy_id=approval.strategy_id,
                    payload={
                        "client_order_id": req.client_order_id,
                        "error": f"order/approval pairing mismatch: {mismatch}",
                    },
                )
                return

            async with self._session_factory() as session:
                order = await self._persist_pending(session, req, approval)
                await self._submit_and_apply(session, order, req, approval)
        except Exception as exc:  # noqa: BLE001 — a dropped order must be auditable, not silent
            log.exception(
                "engine.execution.order_error",
                client_order_id=req.client_order_id,
                symbol=req.symbol,
            )
            await self._audit(
                "order.error",
                portfolio_id=approval.portfolio_id,
                strategy_id=approval.strategy_id,
                payload={
                    "client_order_id": req.client_order_id,
                    "symbol": req.symbol,
                    "error": repr(exc),
                },
            )

    async def _persist_pending(
        self, session: AsyncSession, req: OrderRequest, approval: RiskApproval
    ) -> Order:
        """Commit the ``pending_submit`` row — the recovery anchor — pre-broker."""
        order = Order(
            client_order_id=req.client_order_id,
            portfolio_id=approval.portfolio_id,
            strategy_id=approval.strategy_id,
            symbol=req.symbol,
            side=req.side.value,
            order_type=req.order_type.value,
            order_class=req.order_class.value,
            time_in_force=req.time_in_force.value,
            status=OrderStatus.pending_submit.value,
            qty=approval.qty,
            limit_price=req.limit_price,
            stop_price=req.stop_loss_stop_price,
            extended_hours=req.extended_hours,
            risk_approval=approval.audit_payload(),
        )
        session.add(order)
        await session.commit()
        log.info(
            "engine.execution.persisted",
            client_order_id=req.client_order_id,
            symbol=req.symbol,
            side=req.side.value,
            qty=str(approval.qty),
        )
        return order

    async def _submit_and_apply(
        self,
        session: AsyncSession,
        order: Order,
        req: OrderRequest,
        approval: RiskApproval,
    ) -> None:
        try:
            broker_order = await self._adapter.submit_order(req)
        except OrderRejected as exc:
            await self._mark_terminal(
                session,
                order,
                approval,
                status=OrderStatus.rejected,
                event_type="order.rejected_by_broker",
                error=str(exc),
            )
            return
        except BrokerRateLimited as exc:
            # 429 = refused before processing: provably not placed.
            await self._mark_terminal(
                session,
                order,
                approval,
                status=OrderStatus.failed,
                event_type="order.submit_failed",
                error=str(exc),
            )
            return
        except AmbiguousOrderState as exc:
            await self._resolve_ambiguous(session, order, approval, error=str(exc))
            return
        except Exception as exc:  # noqa: BLE001 — unknown ≠ not-placed; treat as ambiguous
            log.exception(
                "engine.execution.submit_unexpected",
                client_order_id=order.client_order_id,
            )
            await self._resolve_ambiguous(session, order, approval, error=repr(exc))
            return

        await self._apply_submit_result(session, order.id, approval, broker_order)

    async def _apply_submit_result(
        self,
        session: AsyncSession,
        order_id: uuid.UUID,
        approval: RiskApproval,
        broker_order: BrokerOrder,
    ) -> None:
        """Adopt the submit response under a row lock (the stream may have won)."""
        locked = await lock_order_by_client_id(session, approval.client_order_id)
        if locked is None:  # pragma: no cover — we just committed it
            msg = f"order row vanished for {approval.client_order_id}"
            raise RuntimeError(msg)

        if not broker_order.legs and broker_order.order_class is OrderClass.bracket:
            # Some read paths omit nested legs; fetch them so protective exits
            # have local rows before their fills arrive.
            broker_order = await self._fetch_nested(broker_order)

        plan = apply_snapshot(locked, broker_order)
        legs_created = await persist_bracket_legs(session, locked, broker_order)
        session.add(
            EventLog(
                source=EventSource.engine.value,
                event_type="order.submitted",
                portfolio_id=approval.portfolio_id,
                strategy_id=approval.strategy_id,
                order_id=locked.id,
                payload={
                    "client_order_id": locked.client_order_id,
                    "broker_order_id": locked.broker_order_id,
                    "status": locked.status,
                    "transition_plan": plan.value,
                    "legs_created": legs_created,
                },
            )
        )
        await session.commit()
        log.info(
            "engine.execution.submitted",
            client_order_id=locked.client_order_id,
            broker_order_id=locked.broker_order_id,
            status=locked.status,
            legs=legs_created,
        )

    async def _fetch_nested(self, broker_order: BrokerOrder) -> BrokerOrder:
        """Re-read the order with nested legs; fall back to what we have."""
        nested = await self._adapter.get_order(broker_order.broker_order_id)
        return nested if nested is not None else broker_order

    async def _resolve_ambiguous(
        self,
        session: AsyncSession,
        order: Order,
        approval: RiskApproval,
        *,
        error: str,
    ) -> None:
        """The order's fate is unknown: look it up by client id, NEVER resubmit.

        Found → adopt broker truth (including legs). Not found after the loop →
        leave ``pending_submit``; reconciliation ages it into ``failed`` after
        its grace window, or adopts it if it materializes late.
        """
        log.warning(
            "engine.execution.submit_ambiguous",
            client_order_id=order.client_order_id,
            error=error,
        )
        for delay in self._resolve_delays:
            await asyncio.sleep(delay)
            found = await self._adapter.get_order_by_client_id(order.client_order_id)
            if found is not None:
                await self._apply_submit_result(session, order.id, approval, found)
                await self._audit(
                    "order.submit_ambiguous_resolved",
                    portfolio_id=approval.portfolio_id,
                    strategy_id=approval.strategy_id,
                    order_id=order.id,
                    payload={
                        "client_order_id": order.client_order_id,
                        "broker_order_id": found.broker_order_id,
                        "status": found.status.value,
                        "error": error,
                    },
                )
                return

        await self._audit(
            "order.submit_ambiguous",
            portfolio_id=approval.portfolio_id,
            strategy_id=approval.strategy_id,
            order_id=order.id,
            payload={
                "client_order_id": order.client_order_id,
                "error": error,
                "resolution": "unresolved — left pending_submit for reconciliation",
            },
        )

    async def _mark_terminal(
        self,
        session: AsyncSession,
        order: Order,
        approval: RiskApproval,
        *,
        status: OrderStatus,
        event_type: str,
        error: str,
    ) -> None:
        locked = await lock_order_by_client_id(session, approval.client_order_id)
        if locked is None:  # pragma: no cover — we just committed it
            msg = f"order row vanished for {approval.client_order_id}"
            raise RuntimeError(msg)
        locked.status = status.value
        session.add(
            EventLog(
                source=EventSource.engine.value,
                event_type=event_type,
                portfolio_id=approval.portfolio_id,
                strategy_id=approval.strategy_id,
                order_id=locked.id,
                payload={
                    "client_order_id": locked.client_order_id,
                    "status": status.value,
                    "error": error,
                },
            )
        )
        await session.commit()
        log.warning(
            "engine.execution.terminal",
            client_order_id=locked.client_order_id,
            status=status.value,
            error=error,
        )

    async def _audit(
        self,
        event_type: str,
        *,
        portfolio_id: uuid.UUID,
        strategy_id: uuid.UUID | None,
        payload: dict[str, Any],
        order_id: uuid.UUID | None = None,
    ) -> None:
        """Write an audit row on a FRESH session (the working one may be poisoned)."""
        try:
            async with self._session_factory() as session:
                session.add(
                    EventLog(
                        source=EventSource.engine.value,
                        event_type=event_type,
                        portfolio_id=portfolio_id,
                        strategy_id=strategy_id,
                        order_id=order_id,
                        payload=payload,
                    )
                )
                await session.commit()
        except Exception:  # noqa: BLE001 — last-ditch; never raise out of the audit path
            log.exception("engine.execution.audit_failed", event_type=event_type)


def _pairing_mismatch(req: OrderRequest, approval: RiskApproval) -> str | None:
    """Belt-and-suspenders check that the request is the one the approval sized.

    The approval cannot be forged (mint guard), but a bug could pair it with
    the wrong request; a mismatch here means the risk audit would not describe
    the order actually sent.
    """
    if req.client_order_id != approval.client_order_id:
        return f"client_order_id {req.client_order_id!r} != {approval.client_order_id!r}"
    if req.symbol != approval.symbol:
        return f"symbol {req.symbol!r} != {approval.symbol!r}"
    if req.side is not approval.side:
        return f"side {req.side.value!r} != {approval.side.value!r}"
    if req.qty != approval.qty:
        return f"qty {req.qty} != {approval.qty}"
    return None
