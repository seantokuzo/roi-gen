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
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from app.brokers.dto import OrderRequest
from app.brokers.errors import (
    AmbiguousOrderState,
    BrokerRateLimited,
    OrderRejected,
)
from app.core.logging import get_logger
from app.engine.events import FlattenOrderEvent, OrderEvent
from app.engine.execution.apply import (
    apply_snapshot,
    lock_order_by_client_id,
    persist_bracket_legs,
)
from app.engine.risk.approval import FlattenApproval as _FlattenApproval
from app.engine.risk.approval import RiskApproval as _RiskApproval
from app.models.enums import (
    EventSource,
    OrderClass,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from app.models.telemetry import EventLog
from app.models.trading import Order

if TYPE_CHECKING:
    import uuid
    from collections.abc import Callable, Sequence

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from app.brokers.base import BrokerAdapter
    from app.brokers.dto import BrokerOrder
    from app.engine.bus import EventBus
    from app.engine.risk.approval import FlattenApproval, RiskApproval

log = get_logger("engine.execution")

# Ambiguous-submit resolution: lookup delays between get_order_by_client_id
# attempts. Bounded and short — anything unresolved is reconciliation's job.
_DEFAULT_RESOLVE_DELAYS: tuple[float, ...] = (1.0, 2.0, 4.0)

# Cancel-confirmation polling during a flatten: Alpaca cancels are async, and a
# liquidation submitted while a leg still holds the qty is refused (403). Bounded;
# a leg that outlives the budget just fails this pass and the controller re-drives.
_DEFAULT_CANCEL_CONFIRM_DELAYS: tuple[float, ...] = (0.25,) * 8

# Liquidation client_order_id prefix. Doubles as flatten provenance in the DB
# (queryable audit chain) and as the re-drive guard: the cancel sweep skips
# working orders with this prefix so a re-driven flatten never cancels its
# predecessor's still-working liquidation (that loop would prevent completion).
_FLATTEN_CLIENT_ID_PREFIX = "roigen-flatten"

# Broker order states that end the cancel-confirmation wait. `filled` is a
# success here — a stop leg that filled during the race shrank the position,
# and the fresh position re-read below sizes the liquidation accordingly.
_CANCEL_SETTLED_STATUSES = frozenset(
    {
        OrderStatus.canceled,
        OrderStatus.filled,
        OrderStatus.expired,
        OrderStatus.rejected,
    }
)

# A flatten pass should fit well inside the shared 200 req/min bucket; past this
# many broker calls we keep going (safety first) but flag the budget breach.
_FLATTEN_REQUEST_BUDGET = 50


@dataclass(frozen=True, slots=True)
class _SubmitIdentity:
    """What the submit/apply/resolve discipline needs to know about its caller.

    Entry orders (RiskApproval) and liquidations (FlattenApproval) share the
    identical persist-before-submit + never-blind-resubmit machinery; this is
    the seam that lets them, since a FlattenApproval has no client_order_id or
    strategy of its own.
    """

    client_order_id: str
    portfolio_id: uuid.UUID
    strategy_id: uuid.UUID | None


@dataclass(frozen=True, slots=True)
class _SymbolOutcome:
    """Per-symbol result of one flatten pass, for the audit row."""

    symbol: str
    outcome: str
    detail: str = ""

    def to_dict(self) -> dict[str, str]:
        return {"symbol": self.symbol, "outcome": self.outcome, "detail": self.detail}


class ExecutionStage:
    """Subscribes to ``OrderEvent`` and owns the order-mutation boundary."""

    def __init__(
        self,
        *,
        bus: EventBus,
        session_factory: async_sessionmaker[AsyncSession],
        adapter: BrokerAdapter,
        resolve_delays: Sequence[float] = _DEFAULT_RESOLVE_DELAYS,
        cancel_confirm_delays: Sequence[float] = _DEFAULT_CANCEL_CONFIRM_DELAYS,
        halted: Callable[[], bool] | None = None,
    ) -> None:
        self._bus = bus
        self._session_factory = session_factory
        self._adapter = adapter
        self._resolve_delays = tuple(resolve_delays)
        self._cancel_confirm_delays = tuple(cancel_confirm_delays)
        self._halted: Callable[[], bool] = halted if halted is not None else _never_halted
        # Flatten is single-flight: the guarded task runs off-bus (cancel-confirm
        # polling must not stall fill processing), the lock coalesces overlapping
        # drives, and seen ids make a replayed FlattenOrderEvent an audited no-op.
        self._flatten_lock = asyncio.Lock()
        self._seen_flatten_ids: set[uuid.UUID] = set()
        self._flatten_tasks: set[asyncio.Task[None]] = set()

    def register_handlers(self) -> None:
        self._bus.subscribe(OrderEvent, self._on_order)
        self._bus.subscribe(FlattenOrderEvent, self._on_flatten)

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
            if not isinstance(approval, _RiskApproval):
                # The mint guard stops forged RiskApprovals; this stops
                # duck-typed impostors that never went through the constructor.
                log.error(
                    "engine.execution.approval_not_minted",
                    client_order_id=req.client_order_id,
                    approval_type=type(approval).__name__,
                )
                return
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

            if self._halted():
                # Approval-time and submit-time are different instants: a signal
                # evaluated just before the kill switch flipped can queue an
                # OrderEvent behind the FlattenEvent. Re-checking here closes
                # that TOCTOU — a halted engine never submits an entry, however
                # the events interleaved. (Flatten has its own handler and is
                # deliberately exempt.)
                log.warning(
                    "engine.execution.suppressed_halted",
                    client_order_id=req.client_order_id,
                    symbol=req.symbol,
                )
                await self._audit(
                    "order.suppressed_halted",
                    portfolio_id=approval.portfolio_id,
                    strategy_id=approval.strategy_id,
                    payload={
                        "client_order_id": req.client_order_id,
                        "symbol": req.symbol,
                        "side": req.side.value,
                        "qty": str(approval.qty),
                    },
                )
                return

            identity = _SubmitIdentity(
                client_order_id=req.client_order_id,
                portfolio_id=approval.portfolio_id,
                strategy_id=approval.strategy_id,
            )
            async with self._session_factory() as session:
                order = await self._persist_pending(
                    session, req, identity=identity, risk_approval=approval.audit_payload()
                )
                await self._submit_and_apply(session, order, req, identity)
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
        self,
        session: AsyncSession,
        req: OrderRequest,
        *,
        identity: _SubmitIdentity,
        risk_approval: dict[str, Any],
    ) -> Order:
        """Commit the ``pending_submit`` row — the recovery anchor — pre-broker.

        Shared by entries and liquidations: BOTH kinds of broker mutation get
        the same recovery anchor, so a crash mid-flatten reconciles by
        ``client_order_id`` exactly like a crash mid-entry (2b discipline).
        """
        if req.qty is None:  # pragma: no cover — both builders always size by qty
            msg = f"orders must be qty-sized, got notional for {req.client_order_id}"
            raise RuntimeError(msg)
        order = Order(
            client_order_id=req.client_order_id,
            portfolio_id=identity.portfolio_id,
            strategy_id=identity.strategy_id,
            symbol=req.symbol,
            side=req.side.value,
            order_type=req.order_type.value,
            order_class=req.order_class.value,
            time_in_force=req.time_in_force.value,
            status=OrderStatus.pending_submit.value,
            qty=req.qty,
            limit_price=req.limit_price,
            stop_price=req.stop_loss_stop_price,
            extended_hours=req.extended_hours,
            risk_approval=risk_approval,
        )
        session.add(order)
        await session.commit()
        log.info(
            "engine.execution.persisted",
            client_order_id=req.client_order_id,
            symbol=req.symbol,
            side=req.side.value,
            qty=str(req.qty),
        )
        return order

    # ── The flatten path (Phase 2c) ──────────────────────────────────

    async def _on_flatten(self, event: FlattenOrderEvent) -> None:
        """Verify the approval, then run the flatten OFF the bus consumer.

        Cancel-confirmation polling takes seconds; run inline it would stall
        every fill event behind it exactly when latency matters most. The
        spawned task is single-flight (lock) and replay-proof (seen ids):
        overlapping or duplicated FlattenOrderEvents become audited no-ops, and
        the FlattenController re-drives from broker truth if this pass leaves
        anything exposed.
        """
        approval = event.approval
        if not isinstance(approval, _FlattenApproval):
            log.error(
                "engine.execution.flatten_approval_not_minted",
                approval_type=type(approval).__name__,
            )
            return
        if approval.flatten_id in self._seen_flatten_ids:
            await self._audit(
                "flatten.duplicate_ignored",
                portfolio_id=approval.portfolio_id,
                strategy_id=None,
                payload={"flatten_id": str(approval.flatten_id)},
            )
            return
        self._seen_flatten_ids.add(approval.flatten_id)
        task = asyncio.create_task(self._run_flatten(approval))
        self._flatten_tasks.add(task)
        task.add_done_callback(self._flatten_tasks.discard)

    async def _run_flatten(self, approval: FlattenApproval) -> None:
        if self._flatten_lock.locked():
            # A drive is already in flight; this one adds nothing the
            # controller's next broker-truth check won't cover.
            await self._audit(
                "flatten.coalesced",
                portfolio_id=approval.portfolio_id,
                strategy_id=None,
                payload={"flatten_id": str(approval.flatten_id), "source": approval.source},
            )
            return
        async with self._flatten_lock:
            try:
                outcomes = await self._execute_flatten(approval)
            except Exception as exc:  # noqa: BLE001 — the safety path must be auditable, not silent
                log.exception("engine.execution.flatten_error", flatten_id=str(approval.flatten_id))
                await self._audit(
                    "flatten.error",
                    portfolio_id=approval.portfolio_id,
                    strategy_id=None,
                    payload={"flatten_id": str(approval.flatten_id), "error": repr(exc)},
                )
                return
        # Anything short of an accepted submit is a non-close for summary
        # purposes — the controller re-drives, and the operator must see why.
        failures = [o for o in outcomes if o.outcome != "submitted"]
        await self._audit(
            "flatten.partial" if failures else "flatten.executed",
            portfolio_id=approval.portfolio_id,
            strategy_id=None,
            payload={
                "flatten_id": str(approval.flatten_id),
                "source": approval.source,
                "command_seq": approval.command_seq,
                "outcomes": [o.to_dict() for o in outcomes],
            },
        )
        log_fn = log.warning if failures else log.info
        log_fn(
            "engine.execution.flatten_done",
            flatten_id=str(approval.flatten_id),
            symbols=len(outcomes),
            failures=len(failures),
        )

    async def _execute_flatten(self, approval: FlattenApproval) -> list[_SymbolOutcome]:
        """One flatten pass: cancel → confirm → liquidate, all from broker truth.

        Broker truth decides at every step (the local ``Position`` table lags
        fills and must never pick what to close — a dropped fill event would
        otherwise mean cancel-the-protection-then-skip-the-close). Per-symbol
        isolation: one symbol's failure never blocks the rest; the controller
        owns completion and re-drives whatever this pass left exposed.
        """
        requests = 0

        # 1. Cancel every working order EXCEPT still-working liquidations from a
        # prior drive — canceling those would restart their fill clock every
        # re-drive and prevent the flatten from ever completing. Account-wide by
        # policy: the engine's account is engine-only (manual orders get swept).
        open_orders = await self._adapter.list_orders(status="open")
        requests += 1
        to_cancel = [
            o
            for o in open_orders
            if not (o.client_order_id or "").startswith(_FLATTEN_CLIENT_ID_PREFIX)
        ]
        for order in to_cancel:
            await self._adapter.cancel_order(order.broker_order_id)  # idempotent
            requests += 1

        # 2. Confirm cancels settled: Alpaca cancels are async, and a liquidation
        # submitted while a leg still reserves the qty is refused. ANY terminal
        # state settles it — `filled` means the leg won the race, which the fresh
        # position read below simply absorbs. Unsettled leftovers are not fatal:
        # their symbol's liquidation gets refused and the controller re-drives.
        pending = {o.broker_order_id for o in to_cancel}
        for delay in self._cancel_confirm_delays:
            if not pending:
                break
            await asyncio.sleep(delay)
            for broker_order_id in list(pending):
                found = await self._adapter.get_order(broker_order_id)
                requests += 1
                if found is None or found.status in _CANCEL_SETTLED_STATUSES:
                    pending.discard(broker_order_id)
        if pending:
            log.warning(
                "engine.execution.flatten_cancels_unsettled",
                flatten_id=str(approval.flatten_id),
                count=len(pending),
            )

        # 3. Liquidate whatever the broker says is open — read fresh AFTER the
        # cancels settle (a stop leg may have closed a position during step 2).
        positions = await self._adapter.list_positions()
        requests += 1
        outcomes: list[_SymbolOutcome] = []
        for position in positions:
            if position.qty == 0:
                continue
            outcome = await self._liquidate_symbol(approval, position.symbol, position.qty)
            outcomes.append(outcome)
            requests += 1

        if requests > _FLATTEN_REQUEST_BUDGET:
            log.warning(
                "engine.execution.flatten_budget_exceeded",
                flatten_id=str(approval.flatten_id),
                requests=requests,
                budget=_FLATTEN_REQUEST_BUDGET,
            )
        return outcomes

    async def _liquidate_symbol(
        self, approval: FlattenApproval, symbol: str, signed_qty: Decimal
    ) -> _SymbolOutcome:
        """Submit one reduce-direction market liquidation with the 2b discipline.

        A plain ``submit_order`` with OUR client_order_id — never the broker's
        close-position endpoint, whose broker-generated id would make a response
        timeout an untracked live order with orphaned fills. Persist-before-
        submit + resolve-by-client-id apply unchanged; the client-id prefix is
        the flatten's provenance in every downstream row.
        """
        side = OrderSide.sell if signed_qty > 0 else OrderSide.buy
        req = OrderRequest(
            client_order_id=(
                f"{_FLATTEN_CLIENT_ID_PREFIX}-{approval.flatten_id.hex[:12]}-{symbol.lower()}"
            ),
            symbol=symbol,
            side=side,
            order_type=OrderType.market,
            time_in_force=TimeInForce.day,
            order_class=OrderClass.simple,
            qty=abs(signed_qty),
            # *_to_close: the broker refuses to let a qty race flip this into
            # an accidental short/long — a liquidation may only reduce.
            position_intent="sell_to_close" if side is OrderSide.sell else "buy_to_close",
        )
        identity = _SubmitIdentity(
            client_order_id=req.client_order_id,
            portfolio_id=approval.portfolio_id,
            strategy_id=None,
        )
        try:
            async with self._session_factory() as session:
                order = await self._persist_pending(
                    session, req, identity=identity, risk_approval=approval.audit_payload()
                )
                await self._submit_and_apply(session, order, req, identity)
        except Exception as exc:  # noqa: BLE001 — isolate per symbol; the rest must still close
            log.exception(
                "engine.execution.flatten_symbol_error",
                flatten_id=str(approval.flatten_id),
                symbol=symbol,
            )
            return _SymbolOutcome(symbol=symbol, outcome="failed", detail=repr(exc))
        # _submit_and_apply absorbs broker refusals into the row (rejected /
        # failed / left pending_submit on ambiguity) and returns normally — the
        # flatten summary must report the row's TRUTH, not "we tried": a
        # held-qty 403 here is the expected cancel-race outcome the controller
        # re-drives on, and the audit row is what the operator reads.
        async with self._session_factory() as session:
            row = await session.scalar(
                select(Order.status).where(Order.client_order_id == req.client_order_id)
            )
        status = row or OrderStatus.pending_submit.value
        if status in (OrderStatus.rejected.value, OrderStatus.failed.value):
            return _SymbolOutcome(symbol=symbol, outcome=status, detail=req.client_order_id)
        if status == OrderStatus.pending_submit.value:
            return _SymbolOutcome(symbol=symbol, outcome="ambiguous", detail=req.client_order_id)
        return _SymbolOutcome(symbol=symbol, outcome="submitted", detail=req.client_order_id)

    async def _submit_and_apply(
        self,
        session: AsyncSession,
        order: Order,
        req: OrderRequest,
        identity: _SubmitIdentity,
    ) -> None:
        try:
            broker_order = await self._adapter.submit_order(req)
        except OrderRejected as exc:
            await self._mark_terminal(
                session,
                identity,
                status=OrderStatus.rejected,
                event_type="order.rejected_by_broker",
                error=str(exc),
            )
            return
        except BrokerRateLimited as exc:
            # 429 = refused before processing: provably not placed.
            await self._mark_terminal(
                session,
                identity,
                status=OrderStatus.failed,
                event_type="order.submit_failed",
                error=str(exc),
            )
            return
        except AmbiguousOrderState as exc:
            await self._resolve_ambiguous(session, order, identity, error=str(exc))
            return
        except Exception as exc:  # noqa: BLE001 — unknown ≠ not-placed; treat as ambiguous
            log.exception(
                "engine.execution.submit_unexpected",
                client_order_id=order.client_order_id,
            )
            await self._resolve_ambiguous(session, order, identity, error=repr(exc))
            return

        await self._apply_submit_result(session, identity, broker_order)

    async def _apply_submit_result(
        self,
        session: AsyncSession,
        identity: _SubmitIdentity,
        broker_order: BrokerOrder,
    ) -> None:
        """Adopt the submit response under a row lock (the stream may have won)."""
        locked = await lock_order_by_client_id(session, identity.client_order_id)
        if locked is None:  # pragma: no cover — we just committed it
            msg = f"order row vanished for {identity.client_order_id}"
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
                portfolio_id=identity.portfolio_id,
                strategy_id=identity.strategy_id,
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
        identity: _SubmitIdentity,
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
                await self._apply_submit_result(session, identity, found)
                await self._audit(
                    "order.submit_ambiguous_resolved",
                    portfolio_id=identity.portfolio_id,
                    strategy_id=identity.strategy_id,
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
            portfolio_id=identity.portfolio_id,
            strategy_id=identity.strategy_id,
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
        identity: _SubmitIdentity,
        *,
        status: OrderStatus,
        event_type: str,
        error: str,
    ) -> None:
        locked = await lock_order_by_client_id(session, identity.client_order_id)
        if locked is None:  # pragma: no cover — we just committed it
            msg = f"order row vanished for {identity.client_order_id}"
            raise RuntimeError(msg)
        locked.status = status.value
        session.add(
            EventLog(
                source=EventSource.engine.value,
                event_type=event_type,
                portfolio_id=identity.portfolio_id,
                strategy_id=identity.strategy_id,
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


def _never_halted() -> bool:
    return False


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
