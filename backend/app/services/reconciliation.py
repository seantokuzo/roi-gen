"""Boot / periodic reconciliation: make local state agree with the broker.

The broker is the source of truth for account equity, open positions, and
order state (project CLAUDE.md — the trade-updates stream is authoritative;
this service is the *snapshot* counterpart that runs on boot and periodically
to catch anything that happened while we were down or that a missed stream
event left stale).

This module is **read / diff / persist ONLY**. It NEVER submits or cancels an
order — that capability lives behind the execution handler and the risk engine
(iron law #1). The only broker calls here are reads (``get_account``,
``list_positions``, ``list_orders``, ``get_order``, ``get_order_by_client_id``).

Concurrency: the engine's trade-updates writer runs while periodic reconciles
do. Every order mutation here happens under ``SELECT … FOR UPDATE`` and routes
state through the shared transition rules
(:mod:`app.engine.execution.state`) — a stale REST snapshot must never clobber
newer stream truth (verified design-review finding).

Missed-fill synthesis (``synthesize_fills=True``, the engine's mode): executions
that printed while we were deaf exist only as the broker's cumulative
``filled_qty``. The detection cursor is ``SUM(Fill.qty)`` per order — the
durable fill ledger, never ``Order.filled_qty`` — and the synthetic fill + FIFO
lot application land in the SAME transaction as the order update, so a crash
can't separate evidence from ledger. Synthesis never touches Position rows:
the broker position overwrite in this same transaction already includes the
missed quantity.

Post-restart order recovery: a local order in a non-absorbing status that the
broker does NOT list as open is looked up (by ``broker_order_id`` when we have
one — it works for bracket legs and returns nested children — else by
``client_order_id``, the key persisted before submission) and adopted at its
true state. A ``pending_submit`` row with no ``broker_order_id`` that the
broker has never heard of ages into ``failed`` after a grace window; anything
else unknown is left UNTOUCHED and audited — we never guess an order's fate.

All writes go to the passed ``session``; the caller owns the transaction and
commits (repo convention: ``get_db`` does not commit — endpoints do).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import delete, func, select

from app.core.logging import get_logger
from app.engine.execution.apply import apply_snapshot, persist_bracket_legs
from app.engine.execution.lots import UNAPPLIED_REMAINDER_EVENT, apply_fill_to_lots
from app.engine.execution.synthesis import applied_ledger, span_price, synthesize_span
from app.models.enums import EventSource, OrderClass, OrderSide, OrderStatus
from app.models.telemetry import EquitySnapshot, EventLog
from app.models.trading import Fill, Order, Position

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from app.brokers.base import BrokerAdapter
    from app.brokers.dto import BrokerAccount, BrokerOrder, BrokerPosition

log = get_logger("reconciliation")

# Statuses with no transitions out (shared with the order state machine).
# ``done_for_day`` and ``stopped`` are deliberately NOT here: done_for_day is
# dormancy (a GTC order fills next session) and stopped means a fill is
# guaranteed but hasn't printed — treating either as final would skip the
# recovery lookup that finds those fills. ``failed`` IS here (provably never
# placed); the broker-open match above still rescues one that materializes.
TERMINAL_STATUSES: frozenset[OrderStatus] = frozenset(
    {
        OrderStatus.filled,
        OrderStatus.canceled,
        OrderStatus.expired,
        OrderStatus.rejected,
        OrderStatus.replaced,
        OrderStatus.failed,
    }
)

# A pending_submit row with no broker_order_id whose client id the broker does
# not recognize is declared failed after this age — long past any submit
# latency, short enough that the row can't wedge the reconciler forever.
_NEVER_SUBMITTED_GRACE = timedelta(seconds=120)

# Companion event types for the parked-remainder retry pass. The event log is
# append-only (model docstring), so an anomaly's lifecycle is tracked with
# companion rows referencing its ``id`` — never by mutating its payload.
_REMAINDER_RESOLVED_EVENT = "lots.unapplied_remainder_resolved"
_REMAINDER_RETRY_FAILED_EVENT = "lots.unapplied_remainder_retry_failed"
_REMAINDER_ALERT_EVENT = "lots.unapplied_remainder_alert"
# A remainder still unapplied after this many reconcile cycles escalates.
_REMAINDER_ALERT_CYCLES = 3


@dataclass(frozen=True, slots=True)
class ReconcileResult:
    """Outcome counts from one :meth:`ReconciliationService.reconcile_portfolio`."""

    portfolio_id: uuid.UUID
    positions_synced: int
    positions_removed: int
    orders_updated: int
    orphans: int
    missing: int
    fills_synthesized: int
    equity: Decimal


def _is_terminal(status: str) -> bool:
    """Whether a stored status string is a broker-terminal state."""
    try:
        return OrderStatus(status) in TERMINAL_STATUSES
    except ValueError:
        # Unknown/garbage status: treat as non-terminal so it gets investigated
        # rather than silently skipped.
        return False


class ReconciliationService:
    """Snapshots the broker and reconciles a single portfolio's local state.

    Stateless: one instance can reconcile many portfolios. Construct once and
    reuse, or instantiate per call — there is no shared mutable state.
    """

    async def reconcile_portfolio(
        self,
        session: AsyncSession,
        portfolio_id: uuid.UUID,
        adapter: BrokerAdapter,
        *,
        synthesize_fills: bool = False,
    ) -> ReconcileResult:
        """Reconcile ``portfolio_id`` against ``adapter``; return outcome counts.

        Writes (equity snapshot, position upserts/deletes, order updates, audit
        rows, synthetic fills + lots when ``synthesize_fills``) are staged on
        ``session``; the CALLER commits. ``synthesize_fills`` is the engine's
        mode — the API sync endpoint leaves it off (no lot side-effects from a
        UI action; the engine's next reconcile still catches everything,
        because the detection cursor is the fill ledger, not order columns).
        """
        account = await adapter.get_account()
        broker_positions = await adapter.list_positions()
        broker_open_orders = await adapter.list_orders(status="open")

        now = datetime.now(UTC)

        # ── Equity snapshot ──────────────────────────────────────────
        session.add(
            EquitySnapshot(
                portfolio_id=portfolio_id,
                equity=account.equity,
                cash=account.cash,
                buying_power=account.buying_power,
                ts=now,
            )
        )

        # Orders BEFORE positions — the trade-updates writer locks in that
        # order (order row → position row) per fill, and both tasks run
        # concurrently; touching positions first here would be the classic
        # AB/BA lock inversion → recurring Postgres deadlocks (design review
        # C4). The position overwrite still lands in this same transaction,
        # so it remains authoritative over anything synthesis staged.
        orders_updated, orphans, missing, fills_synthesized = await self._reconcile_orders(
            session,
            portfolio_id,
            adapter,
            broker_open_orders,
            now=now,
            synthesize_fills=synthesize_fills,
        )
        positions_synced, positions_removed = await self._reconcile_positions(
            session, portfolio_id, account, broker_positions
        )

        session.add(
            EventLog(
                source=EventSource.system.value,
                event_type="reconcile.completed",
                portfolio_id=portfolio_id,
                payload={
                    "positions_synced": positions_synced,
                    "positions_removed": positions_removed,
                    "orders_updated": orders_updated,
                    "orphans": orphans,
                    "missing": missing,
                    "fills_synthesized": fills_synthesized,
                    "equity": str(account.equity),
                    "cash": str(account.cash),
                    "buying_power": str(account.buying_power),
                    "ts": now.isoformat(),
                },
            )
        )

        log.info(
            "reconcile.completed",
            portfolio_id=str(portfolio_id),
            positions_synced=positions_synced,
            positions_removed=positions_removed,
            orders_updated=orders_updated,
            orphans=orphans,
            missing=missing,
            fills_synthesized=fills_synthesized,
            equity=str(account.equity),
        )

        return ReconcileResult(
            portfolio_id=portfolio_id,
            positions_synced=positions_synced,
            positions_removed=positions_removed,
            orders_updated=orders_updated,
            orphans=orphans,
            missing=missing,
            fills_synthesized=fills_synthesized,
            equity=account.equity,
        )

    async def _reconcile_positions(
        self,
        session: AsyncSession,
        portfolio_id: uuid.UUID,
        account: BrokerAccount,
        broker_positions: list[BrokerPosition],
    ) -> tuple[int, int]:
        """Upsert positions the broker reports; delete locals it no longer reports.

        Safety guard: an empty position list that CONTRADICTS the account
        snapshot (broker reports no positions, yet ``position_market_value`` is
        non-zero) is treated as a suspect/transient response — we skip all
        deletes rather than wipe the local book on a glitch. Alpaca has a
        documented multi-hour-outage history (project CLAUDE.md); a successful
        but momentarily-empty body must not be read as "we went flat."
        """
        local_positions = (
            (await session.execute(select(Position).where(Position.portfolio_id == portfolio_id)))
            .scalars()
            .all()
        )
        local_by_symbol = {p.symbol: p for p in local_positions}
        broker_symbols = {bp.symbol for bp in broker_positions}

        synced = 0
        for bp in broker_positions:
            existing = local_by_symbol.get(bp.symbol)
            if existing is None:
                session.add(
                    Position(
                        portfolio_id=portfolio_id,
                        symbol=bp.symbol,
                        qty=bp.qty,  # signed (negative == short)
                        avg_entry_price=bp.avg_entry_price,
                    )
                )
            else:
                existing.qty = bp.qty
                existing.avg_entry_price = bp.avg_entry_price
            synced += 1

        if not broker_positions and account.position_market_value != 0 and local_by_symbol:
            # Empty list but the account still carries market value → don't trust
            # it. Leave the local book intact for the next reconcile.
            log.warning(
                "reconcile.positions_suspect_empty",
                portfolio_id=str(portfolio_id),
                position_market_value=str(account.position_market_value),
                local_positions=len(local_by_symbol),
            )
            return synced, 0

        # Anything local that the broker no longer reports: we're flat — remove it.
        vanished = [sym for sym in local_by_symbol if sym not in broker_symbols]
        if vanished:
            await session.execute(
                delete(Position).where(
                    Position.portfolio_id == portfolio_id,
                    Position.symbol.in_(vanished),
                )
            )

        return synced, len(vanished)

    async def _reconcile_orders(
        self,
        session: AsyncSession,
        portfolio_id: uuid.UUID,
        adapter: BrokerAdapter,
        broker_open_orders: list[BrokerOrder],
        *,
        now: datetime,
        synthesize_fills: bool,
    ) -> tuple[int, int, int, int]:
        """Diff broker-open orders against locals; recover non-terminal stragglers.

        Returns ``(orders_updated, orphans, missing, fills_synthesized)``.
        """
        local_orders = (
            (await session.execute(select(Order).where(Order.portfolio_id == portfolio_id)))
            .scalars()
            .all()
        )
        local_by_broker_id = {o.broker_order_id: o for o in local_orders if o.broker_order_id}
        local_by_client_id = {o.client_order_id: o for o in local_orders}

        def _find_local(bo: BrokerOrder) -> Order | None:
            local = local_by_broker_id.get(bo.broker_order_id)
            if local is None and bo.client_order_id is not None:
                local = local_by_client_id.get(bo.client_order_id)
            return local

        updated = 0
        orphans = 0
        synthesized = 0
        matched_local_ids: set[uuid.UUID] = set()

        # 1) Each broker-open order (parents AND their nested legs) → local twin.
        flat: list[tuple[BrokerOrder, BrokerOrder | None]] = []
        for bo in broker_open_orders:
            flat.append((bo, None))
            flat.extend((leg, bo) for leg in bo.legs)

        for bo, parent_bo in flat:
            local = _find_local(bo)
            if local is None and parent_bo is not None:
                # A leg with no local row but a known local parent: adopt it —
                # its fills are how protective exits reach the lot ledger.
                local_parent = _find_local(parent_bo)
                if local_parent is not None:
                    created = await persist_bracket_legs(session, local_parent, parent_bo)
                    if created:
                        await session.flush()
                        local = (
                            await session.execute(
                                select(Order).where(Order.broker_order_id == bo.broker_order_id)
                            )
                        ).scalar_one_or_none()
                        if local is not None:
                            if local.broker_order_id:
                                local_by_broker_id[local.broker_order_id] = local
                            local_by_client_id[local.client_order_id] = local
                            log.info(
                                "reconcile.leg_adopted",
                                portfolio_id=str(portfolio_id),
                                broker_order_id=bo.broker_order_id,
                            )
            if local is None:
                # No local row → an order placed outside the system (manual /
                # legacy / another process). Record it; never act on it here.
                orphans += 1
                session.add(
                    EventLog(
                        source=EventSource.broker.value,
                        event_type="reconcile.orphan_broker_order",
                        portfolio_id=portfolio_id,
                        payload={
                            "broker_order_id": bo.broker_order_id,
                            "client_order_id": bo.client_order_id,
                            "symbol": bo.symbol,
                            "side": bo.side.value,
                            "status": bo.status.value,
                            "qty": str(bo.qty) if bo.qty is not None else None,
                        },
                    )
                )
                log.warning(
                    "reconcile.orphan_broker_order",
                    portfolio_id=str(portfolio_id),
                    broker_order_id=bo.broker_order_id,
                    symbol=bo.symbol,
                    status=bo.status.value,
                )
                continue

            matched_local_ids.add(local.id)
            changed, synth = await self._adopt(
                session, local, bo, now=now, synthesize_fills=synthesize_fills
            )
            updated += int(changed)
            synthesized += synth

        # 2) Local non-terminal orders the broker did NOT list as open. Either
        # they reached a terminal state while we were down, or the lookup is
        # unknown.
        open_broker_ids = {bo.broker_order_id for bo, _ in flat}
        missing = 0
        for local in local_orders:
            if local.id in matched_local_ids:
                continue
            if _is_terminal(local.status):
                continue
            if local.broker_order_id and local.broker_order_id in open_broker_ids:
                continue

            true_state = await self._lookup(adapter, local)
            if true_state is None:
                missing += await self._handle_unknown(session, portfolio_id, local, now=now)
                continue
            changed, synth = await self._adopt(
                session, local, true_state, now=now, synthesize_fills=synthesize_fills
            )
            updated += int(changed)
            synthesized += synth
            if (
                true_state.order_class is OrderClass.bracket
                and local.parent_order_id is None
                and true_state.legs
            ):
                await persist_bracket_legs(session, local, true_state)

        # 3) Ledger-deficit sweep: orders whose filled_qty exceeds their fill
        # ledger. Steps 1-2 skip locally-TERMINAL orders, but a terminal order
        # can carry a deficit (e.g. the API sync adopted 'filled' without
        # synthesis while the stream events were lost) — and without this
        # sweep those fills would never reach the lot ledger.
        if synthesize_fills:
            synthesized += await self._sweep_ledger_deficits(
                session, portfolio_id, adapter, now=now
            )
            # 4) Parked-remainder retry — AFTER the deficit sweep so lots the
            # sweep just synthesized (e.g. a flatten's missing entry) are
            # visible, and still inside the orders phase so the transaction's
            # lock order stays orders → lots → positions (the trade-updates
            # writer's order — AB/BA inversion would deadlock).
            await self._retry_unapplied_remainders(session, portfolio_id)

        return updated, orphans, missing, synthesized

    async def _sweep_ledger_deficits(
        self,
        session: AsyncSession,
        portfolio_id: uuid.UUID,
        adapter: BrokerAdapter,
        *,
        now: datetime,
    ) -> int:
        """Synthesize fills for any order whose ledger trails its filled_qty.

        Orders already repaired earlier in this reconcile no longer show a
        deficit (their synthetic Fill rows are flushed), so this only touches
        genuine stragglers. Broker truth is preferred; the order's own
        filled_qty/filled_avg_price (which themselves came from a broker
        snapshot) are the fallback when the broker no longer returns the order.
        """
        ledger = (
            select(Fill.order_id, func.sum(Fill.qty).label("applied"))
            .group_by(Fill.order_id)
            .subquery()
        )
        deficits = (
            (
                await session.execute(
                    select(Order)
                    .outerjoin(ledger, ledger.c.order_id == Order.id)
                    .where(
                        Order.portfolio_id == portfolio_id,
                        Order.filled_qty > func.coalesce(ledger.c.applied, 0),
                    )
                )
            )
            .scalars()
            .all()
        )

        synthesized = 0
        for local in deficits:
            true_state = await self._lookup(adapter, local)
            if true_state is not None:
                _, synth = await self._adopt(
                    session, local, true_state, now=now, synthesize_fills=True
                )
                synthesized += synth
                continue
            if local.filled_avg_price is None:
                session.add(
                    EventLog(
                        source=EventSource.system.value,
                        event_type="reconcile.ledger_deficit_unresolved",
                        portfolio_id=portfolio_id,
                        order_id=local.id,
                        payload={
                            "client_order_id": local.client_order_id,
                            "filled_qty": str(local.filled_qty),
                        },
                    )
                )
                log.warning(
                    "reconcile.ledger_deficit_unresolved",
                    portfolio_id=str(portfolio_id),
                    client_order_id=local.client_order_id,
                )
                continue
            await session.refresh(local, with_for_update=True)
            applied = await applied_ledger(session, local.id)
            delta = local.filled_qty - applied.qty
            if delta <= 0:  # repaired concurrently
                continue
            price = span_price(
                cum_qty=local.filled_qty,
                cum_avg_price=local.filled_avg_price,
                applied=applied,
                span_qty=delta,
            )
            fill = await synthesize_span(
                session,
                local,
                span_qty=delta,
                price=price,
                occurred_at=local.filled_at or now,
                tag="recon",
                apply_position=False,
            )
            if fill is not None:
                synthesized += 1
                session.add(
                    EventLog(
                        source=EventSource.system.value,
                        event_type="reconcile.fill_synthesized",
                        portfolio_id=portfolio_id,
                        strategy_id=local.strategy_id,
                        order_id=local.id,
                        payload={
                            "qty": str(delta),
                            "price": str(price),
                            "source": "local_columns_fallback",
                        },
                    )
                )
        return synthesized

    async def _retry_unapplied_remainders(
        self, session: AsyncSession, portfolio_id: uuid.UUID
    ) -> int:
        """Retry parked strategy-less liquidation remainders (lot overshoot).

        A flatten fill whose qty overran every matched open lot parked the
        remainder as an ``UNAPPLIED_REMAINDER_EVENT`` anomaly instead of
        minting a phantom opposite-side lot (:mod:`app.engine.execution.lots`);
        its Fill row already carries the full qty, so the synthesis cursor is
        satisfied and only the LOT application is outstanding. By this point
        in the reconcile the missed-entry synthesis has run, so the lots the
        fill was racing may now exist — re-apply each open remainder.

        Bookkeeping is append-only companion EventLog rows (anomaly payloads
        are never mutated — the event log is documented append-only):

        - applied in full → a resolved row referencing the anomaly id;
        - applied in part → a resolved row PLUS a fresh, smaller anomaly for
          the residual, so an open anomaly's payload ``qty`` is always exactly
          what remains to retry (no cross-row arithmetic; the residual's
          retry-cycle counter restarts — acceptable, since partial progress
          means lots ARE materializing);
        - applied not at all → a retry-failed row (cycle N); at cycle
          ``_REMAINDER_ALERT_CYCLES`` an alert row, once per anomaly. Retries
          continue after the alert — the missing entry can surface on any
          later reconcile — and every failed cycle stays on the audit trail.

        Returns the number of anomalies (fully or partially) applied.
        """
        anomalies = (
            (
                await session.execute(
                    select(EventLog)
                    .where(
                        EventLog.portfolio_id == portfolio_id,
                        EventLog.event_type == UNAPPLIED_REMAINDER_EVENT,
                    )
                    .order_by(EventLog.id)
                )
            )
            .scalars()
            .all()
        )
        if not anomalies:
            return 0

        companions = (
            await session.execute(
                select(EventLog.event_type, EventLog.payload).where(
                    EventLog.portfolio_id == portfolio_id,
                    EventLog.event_type.in_(
                        (
                            _REMAINDER_RESOLVED_EVENT,
                            _REMAINDER_RETRY_FAILED_EVENT,
                            _REMAINDER_ALERT_EVENT,
                        )
                    ),
                )
            )
        ).all()
        resolved: set[int] = set()
        failed_cycles: dict[int, int] = {}
        alerted: set[int] = set()
        for event_type, companion_payload in companions:
            ref = companion_payload.get("anomaly_event_id")
            if ref is None:  # pragma: no cover — companions always carry the ref
                continue
            anomaly_id = int(ref)
            if event_type == _REMAINDER_RESOLVED_EVENT:
                resolved.add(anomaly_id)
            elif event_type == _REMAINDER_RETRY_FAILED_EVENT:
                failed_cycles[anomaly_id] = failed_cycles.get(anomaly_id, 0) + 1
            else:
                alerted.add(anomaly_id)

        applied_count = 0
        for anomaly in anomalies:
            if anomaly.id in resolved:
                continue
            parked = anomaly.payload
            qty = Decimal(parked["qty"])
            order_id = uuid.UUID(parked["order_id"]) if parked.get("order_id") else None
            fill_id = uuid.UUID(parked["fill_id"]) if parked.get("fill_id") else None

            application = await apply_fill_to_lots(
                session,
                portfolio_id=portfolio_id,
                strategy_id=None,
                symbol=parked["symbol"],
                side=OrderSide(parked["side"]),
                qty=qty,
                price=Decimal(parked["price"]),
                occurred_at=datetime.fromisoformat(parked["occurred_at"]),
                order_id=order_id,
                fill_id=fill_id,
                record_unapplied=False,  # this pass owns the parked bookkeeping
            )

            if application.qty_closed > 0:
                applied_count += 1
                session.add(
                    EventLog(
                        source=EventSource.system.value,
                        event_type=_REMAINDER_RESOLVED_EVENT,
                        portfolio_id=portfolio_id,
                        order_id=order_id,
                        payload={
                            "anomaly_event_id": anomaly.id,
                            "symbol": parked["symbol"],
                            "applied_qty": str(application.qty_closed),
                            "realized_pnl": str(application.realized_pnl),
                            "residual_qty": str(application.unapplied_qty),
                        },
                    )
                )
                log.warning(
                    "reconcile.remainder_applied",
                    portfolio_id=str(portfolio_id),
                    anomaly_event_id=anomaly.id,
                    symbol=parked["symbol"],
                    applied_qty=str(application.qty_closed),
                    residual_qty=str(application.unapplied_qty),
                )
                if application.unapplied_qty > 0:
                    residual = dict(parked)
                    residual["qty"] = str(application.unapplied_qty)
                    residual["superseded_anomaly_event_id"] = anomaly.id
                    session.add(
                        EventLog(
                            source=EventSource.system.value,
                            event_type=UNAPPLIED_REMAINDER_EVENT,
                            portfolio_id=portfolio_id,
                            order_id=order_id,
                            payload=residual,
                        )
                    )
                continue

            cycle = failed_cycles.get(anomaly.id, 0) + 1
            session.add(
                EventLog(
                    source=EventSource.system.value,
                    event_type=_REMAINDER_RETRY_FAILED_EVENT,
                    portfolio_id=portfolio_id,
                    order_id=order_id,
                    payload={
                        "anomaly_event_id": anomaly.id,
                        "symbol": parked["symbol"],
                        "qty": str(qty),
                        "cycle": cycle,
                    },
                )
            )
            if cycle >= _REMAINDER_ALERT_CYCLES and anomaly.id not in alerted:
                session.add(
                    EventLog(
                        source=EventSource.system.value,
                        event_type=_REMAINDER_ALERT_EVENT,
                        portfolio_id=portfolio_id,
                        order_id=order_id,
                        payload={
                            "anomaly_event_id": anomaly.id,
                            "symbol": parked["symbol"],
                            "qty": str(qty),
                            "cycles": cycle,
                        },
                    )
                )
                log.error(
                    "reconcile.remainder_alert",
                    portfolio_id=str(portfolio_id),
                    anomaly_event_id=anomaly.id,
                    symbol=parked["symbol"],
                    qty=str(qty),
                    cycles=cycle,
                )
        return applied_count

    @staticmethod
    async def _lookup(adapter: BrokerAdapter, local: Order) -> BrokerOrder | None:
        """Recover an order's true state — broker id first (works for legs and
        returns nested children), client id as the pre-submit-crash fallback."""
        if local.broker_order_id:
            found = await adapter.get_order(local.broker_order_id)
            if found is not None:
                return found
        return await adapter.get_order_by_client_id(local.client_order_id)

    async def _adopt(
        self,
        session: AsyncSession,
        local: Order,
        broker: BrokerOrder,
        *,
        now: datetime,
        synthesize_fills: bool,
    ) -> tuple[bool, int]:
        """Apply broker truth onto ``local`` under a row lock, synthesizing any
        fills the ledger never saw. Returns ``(changed, fills_synthesized)``."""
        await session.refresh(local, with_for_update=True)
        before = (local.status, local.filled_qty, local.broker_order_id)

        if local.status == OrderStatus.failed.value:
            # We guessed "not placed"; the broker just reported the order.
            # ``failed`` is soft-terminal — broker reality trumps the guess
            # (mirrors the trade-updates writer's resurrection path).
            local.status = OrderStatus.pending_submit.value
            session.add(
                EventLog(
                    source=EventSource.broker.value,
                    event_type="order.failed_resurrected",
                    portfolio_id=local.portfolio_id,
                    strategy_id=local.strategy_id,
                    order_id=local.id,
                    payload={
                        "client_order_id": local.client_order_id,
                        "broker_order_id": broker.broker_order_id,
                        "broker_status": broker.status.value,
                        "via": "reconciliation",
                    },
                )
            )
            log.warning(
                "reconcile.failed_resurrected",
                client_order_id=local.client_order_id,
                broker_status=broker.status.value,
            )

        synthesized = 0
        if synthesize_fills:
            applied = await applied_ledger(session, local.id)
            delta = broker.filled_qty - applied.qty
            if delta > 0:
                price = span_price(
                    cum_qty=broker.filled_qty,
                    cum_avg_price=broker.filled_avg_price,
                    applied=applied,
                    span_qty=delta,
                )
                fill = await synthesize_span(
                    session,
                    local,
                    span_qty=delta,
                    price=price,
                    occurred_at=broker.filled_at or now,
                    tag="recon",
                    apply_position=False,
                )
                if fill is not None:
                    synthesized = 1
                    session.add(
                        EventLog(
                            source=EventSource.system.value,
                            event_type="reconcile.fill_synthesized",
                            portfolio_id=local.portfolio_id,
                            strategy_id=local.strategy_id,
                            order_id=local.id,
                            payload={
                                "qty": str(delta),
                                "price": str(price),
                                "broker_cumulative": str(broker.filled_qty),
                            },
                        )
                    )

        apply_snapshot(local, broker)
        changed = before != (local.status, local.filled_qty, local.broker_order_id)
        return changed, synthesized

    async def _handle_unknown(
        self,
        session: AsyncSession,
        portfolio_id: uuid.UUID,
        local: Order,
        *,
        now: datetime,
    ) -> int:
        """The broker has no record of a non-terminal local order.

        A ``pending_submit`` row that never got a ``broker_order_id`` and has
        aged past the grace window provably never landed (the client-id lookup
        is authoritative for orders we key ourselves) → ``failed``. Anything
        else is left untouched and audited — never guess.
        Returns 1 when the order remains unresolved (the ``missing`` count).
        """
        age_ok = local.created_at is not None and (now - local.created_at) > _NEVER_SUBMITTED_GRACE
        if (
            local.status == OrderStatus.pending_submit.value
            and local.broker_order_id is None
            and age_ok
        ):
            await session.refresh(local, with_for_update=True)
            if local.status == OrderStatus.pending_submit.value:  # re-check under lock
                local.status = OrderStatus.failed.value
                session.add(
                    EventLog(
                        source=EventSource.system.value,
                        event_type="reconcile.never_submitted",
                        portfolio_id=portfolio_id,
                        order_id=local.id,
                        payload={
                            "client_order_id": local.client_order_id,
                            "symbol": local.symbol,
                            "age_seconds": (now - local.created_at).total_seconds(),
                        },
                    )
                )
                log.warning(
                    "reconcile.never_submitted",
                    portfolio_id=str(portfolio_id),
                    client_order_id=local.client_order_id,
                )
                return 0

        session.add(
            EventLog(
                source=EventSource.system.value,
                event_type="reconcile.missing_order",
                portfolio_id=portfolio_id,
                order_id=local.id,
                payload={
                    "client_order_id": local.client_order_id,
                    "broker_order_id": local.broker_order_id,
                    "symbol": local.symbol,
                    "local_status": local.status,
                },
            )
        )
        log.warning(
            "reconcile.missing_order",
            portfolio_id=str(portfolio_id),
            client_order_id=local.client_order_id,
            local_status=local.status,
        )
        return 1
