"""Boot / periodic reconciliation: make local state agree with the broker.

The broker is the source of truth for account equity, open positions, and
order state (project CLAUDE.md — the trade-updates stream is authoritative;
this service is the *snapshot* counterpart that runs on boot and periodically
to catch anything that happened while we were down or that a missed stream
event left stale).

This module is **read / diff / persist ONLY**. It NEVER submits or cancels an
order — that capability lives behind the execution handler and the risk engine
(iron law #1). The only broker calls here are reads (``get_account``,
``list_positions``, ``list_orders``, ``get_order_by_client_id``).

Post-restart order recovery (the "what happened while we were down" case): a
local order in a non-terminal status that the broker does NOT list as open is
looked up by its ``client_order_id`` — the reconciliation key persisted before
submission — and adopted at its true terminal state. If even that lookup comes
back empty the order is left UNTOUCHED and an audit row is written; we never
guess an order's fate (iron law spirit: ambiguity is reconciled, not assumed).

All writes go to the passed ``session``; the caller owns the transaction and
commits (repo convention: ``get_db`` does not commit — endpoints do).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import delete, select

from app.core.logging import get_logger
from app.models.enums import EventSource, OrderStatus
from app.models.telemetry import EquitySnapshot, EventLog
from app.models.trading import Order, Position

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from app.brokers.base import BrokerAdapter
    from app.brokers.dto import BrokerOrder, BrokerPosition

log = get_logger("reconciliation")

# Statuses the broker considers "done" — it will not list these as open. Any
# local order in one of these is already settled; nothing to recover.
TERMINAL_STATUSES: frozenset[OrderStatus] = frozenset(
    {
        OrderStatus.filled,
        OrderStatus.canceled,
        OrderStatus.expired,
        OrderStatus.rejected,
        OrderStatus.replaced,
        OrderStatus.done_for_day,
        OrderStatus.stopped,
    }
)


@dataclass(frozen=True, slots=True)
class ReconcileResult:
    """Outcome counts from one :meth:`ReconciliationService.reconcile_portfolio`."""

    portfolio_id: uuid.UUID
    positions_synced: int
    positions_removed: int
    orders_updated: int
    orphans: int
    missing: int
    equity: Decimal


def _is_terminal(status: str) -> bool:
    """Whether a stored status string is a broker-terminal state."""
    try:
        return OrderStatus(status) in TERMINAL_STATUSES
    except ValueError:
        # Unknown/garbage status: treat as non-terminal so it gets investigated
        # rather than silently skipped.
        return False


def _apply_broker_order(local: Order, broker: BrokerOrder) -> bool:
    """Copy mutable broker order fields onto ``local``; return whether anything changed."""
    changed = False
    new_status = broker.status.value
    if local.status != new_status:
        local.status = new_status
        changed = True
    if local.filled_qty != broker.filled_qty:
        local.filled_qty = broker.filled_qty
        changed = True
    if broker.filled_avg_price is not None and local.filled_avg_price != broker.filled_avg_price:
        local.filled_avg_price = broker.filled_avg_price
        changed = True
    if local.broker_order_id != broker.broker_order_id:
        local.broker_order_id = broker.broker_order_id
        changed = True
    if broker.submitted_at is not None and local.submitted_at != broker.submitted_at:
        local.submitted_at = broker.submitted_at
        changed = True
    if broker.filled_at is not None and local.filled_at != broker.filled_at:
        local.filled_at = broker.filled_at
        changed = True
    if broker.canceled_at is not None and local.canceled_at != broker.canceled_at:
        local.canceled_at = broker.canceled_at
        changed = True
    return changed


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
    ) -> ReconcileResult:
        """Reconcile ``portfolio_id`` against ``adapter``; return outcome counts.

        Writes (equity snapshot, position upserts/deletes, order updates, audit
        rows) are staged on ``session``; the CALLER commits.
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

        positions_synced, positions_removed = await self._reconcile_positions(
            session, portfolio_id, broker_positions
        )
        orders_updated, orphans, missing = await self._reconcile_orders(
            session, portfolio_id, adapter, broker_open_orders
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
            equity=str(account.equity),
        )

        return ReconcileResult(
            portfolio_id=portfolio_id,
            positions_synced=positions_synced,
            positions_removed=positions_removed,
            orders_updated=orders_updated,
            orphans=orphans,
            missing=missing,
            equity=account.equity,
        )

    async def _reconcile_positions(
        self,
        session: AsyncSession,
        portfolio_id: uuid.UUID,
        broker_positions: list[BrokerPosition],
    ) -> tuple[int, int]:
        """Upsert positions the broker reports; delete locals it no longer reports."""
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
    ) -> tuple[int, int, int]:
        """Diff broker-open orders against locals; recover non-terminal stragglers.

        Returns ``(orders_updated, orphans, missing)``.
        """
        local_orders = (
            (await session.execute(select(Order).where(Order.portfolio_id == portfolio_id)))
            .scalars()
            .all()
        )
        local_by_broker_id = {o.broker_order_id: o for o in local_orders if o.broker_order_id}
        local_by_client_id = {o.client_order_id: o for o in local_orders}

        updated = 0
        orphans = 0
        matched_local_ids: set[uuid.UUID] = set()

        # 1) Each broker-open order → find its local twin (broker id, then client id).
        for bo in broker_open_orders:
            local = local_by_broker_id.get(bo.broker_order_id)
            if local is None and bo.client_order_id is not None:
                local = local_by_client_id.get(bo.client_order_id)
            if local is None:
                # No local row → an order placed outside the system (manual / legacy
                # / another process). Record it; never act on it here.
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
            if _apply_broker_order(local, bo):
                updated += 1

        # 2) Local non-terminal orders the broker did NOT list as open. Either they
        # reached a terminal state while we were down, or the lookup is unknown.
        open_broker_ids = {bo.broker_order_id for bo in broker_open_orders}
        missing = 0
        for local in local_orders:
            if local.id in matched_local_ids:
                continue
            if _is_terminal(local.status):
                continue
            # Belt-and-suspenders: skip if it was actually in the broker-open set
            # under its broker id (shouldn't happen given step 1, but cheap).
            if local.broker_order_id and local.broker_order_id in open_broker_ids:
                continue

            true_state = await adapter.get_order_by_client_id(local.client_order_id)
            if true_state is None:
                # Broker has no record under our client id. Do NOT guess — record
                # and leave status untouched for human / later inspection.
                missing += 1
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
                continue
            if _apply_broker_order(local, true_state):
                updated += 1

        return updated, orphans, missing
