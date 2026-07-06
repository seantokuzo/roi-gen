"""Shared broker-snapshot → Order-row application, used by every writer.

Three writers copy broker order state onto local rows (the ExecutionStage's
post-submit apply, the trade-updates writer, reconciliation). They all split
the copy the same way:

- **Identity fields** — ``broker_order_id``, timestamps the broker reports,
  ``raw`` — are ALWAYS safe to persist: they cannot regress money state and
  losing them orphans the row (a bracket parent without ``broker_order_id``
  can never have its legs adopted).
- **State fields** — ``status`` / ``filled_qty`` / ``filled_avg_price`` — go
  through :func:`~app.engine.execution.state.plan_transition` so a stale
  snapshot or late event can't clobber newer truth written by another writer.

Bracket legs are persisted as child ``Order`` rows keyed by the leg's
broker-generated ``client_order_id`` — never a fabricated one, which would be
unreconcilable (the recovery path looks orders up by ``client_order_id``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select

from app.core.logging import get_logger
from app.engine.execution.state import TransitionPlan, plan_transition
from app.models.trading import Order

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from app.brokers.dto import BrokerOrder

log = get_logger("engine.execution")


def apply_identity(local: Order, broker: BrokerOrder) -> None:
    """Copy identity/timestamp fields the broker reports; never regresses state."""
    if local.broker_order_id is None and broker.broker_order_id:
        local.broker_order_id = broker.broker_order_id
    if broker.submitted_at is not None:
        local.submitted_at = broker.submitted_at
    if broker.filled_at is not None:
        local.filled_at = broker.filled_at
    if broker.canceled_at is not None:
        local.canceled_at = broker.canceled_at
    if broker.raw:
        local.raw = broker.raw


def apply_snapshot(local: Order, broker: BrokerOrder) -> TransitionPlan:
    """Apply a full broker snapshot: identity always, state only when the
    transition plan allows. Returns the plan so callers can audit skips."""
    apply_identity(local, broker)
    plan = plan_transition(local.status, local.filled_qty, broker.status, broker.filled_qty)
    if plan is TransitionPlan.apply:
        if local.status != broker.status.value:
            log.info(
                "engine.execution.order_status",
                client_order_id=local.client_order_id,
                from_status=local.status,
                to_status=broker.status.value,
            )
        local.status = broker.status.value
        local.filled_qty = broker.filled_qty
        if broker.filled_avg_price is not None:
            local.filled_avg_price = broker.filled_avg_price
    return plan


async def persist_bracket_legs(session: AsyncSession, parent: Order, broker: BrokerOrder) -> int:
    """Create local child rows for ``broker.legs`` not yet persisted.

    Legs inherit ``portfolio_id`` / ``strategy_id`` from the parent (that
    attribution is how a protective exit's fill finds the right lots). A leg
    without a broker-supplied ``client_order_id`` is skipped and logged —
    fabricating one would make the row unreconcilable; the nested
    ``get_order`` fetch or the next reconcile retries it.
    Returns the number of rows created.
    """
    if not broker.legs:
        return 0

    existing = (
        (await session.execute(select(Order).where(Order.parent_order_id == parent.id)))
        .scalars()
        .all()
    )
    known_broker_ids = {o.broker_order_id for o in existing if o.broker_order_id}
    known_client_ids = {o.client_order_id for o in existing}

    created = 0
    for leg in broker.legs:
        if leg.broker_order_id in known_broker_ids:
            continue
        if leg.client_order_id is None:
            log.warning(
                "engine.execution.leg_missing_client_id",
                parent_client_order_id=parent.client_order_id,
                leg_broker_order_id=leg.broker_order_id,
            )
            continue
        if leg.client_order_id in known_client_ids:
            continue
        session.add(
            Order(
                client_order_id=leg.client_order_id,
                broker_order_id=leg.broker_order_id,
                portfolio_id=parent.portfolio_id,
                strategy_id=parent.strategy_id,
                symbol=leg.symbol,
                side=leg.side.value,
                order_type=leg.order_type.value,
                order_class=leg.order_class.value,
                time_in_force=leg.time_in_force.value,
                status=leg.status.value,
                qty=leg.qty if leg.qty is not None else parent.qty,
                filled_qty=leg.filled_qty,
                limit_price=leg.limit_price,
                stop_price=leg.stop_price,
                filled_avg_price=leg.filled_avg_price,
                extended_hours=leg.extended_hours,
                parent_order_id=parent.id,
                submitted_at=leg.submitted_at,
                raw=leg.raw or None,
            )
        )
        created += 1
    return created


# The locked lookups MUST populate_existing: without it, a row already in the
# session's identity map (e.g. the one ExecutionStage just persisted) is
# returned with its STALE in-memory attributes even though the SELECT saw newer
# DB truth — and a concurrent writer's committed state (a fill that beat the
# submit response) would be silently regressed. Caught by
# test_submit_response_never_regresses_a_filled_row.


async def lock_order(session: AsyncSession, order_id: object) -> Order | None:
    """SELECT ... FOR UPDATE the order row so concurrent writers serialize."""
    return (
        await session.execute(
            select(Order)
            .where(Order.id == order_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()


async def lock_order_by_client_id(session: AsyncSession, client_order_id: str) -> Order | None:
    """Locked lookup by the persisted-before-submit reconciliation key."""
    return (
        await session.execute(
            select(Order)
            .where(Order.client_order_id == client_order_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()


async def lock_order_by_broker_id(session: AsyncSession, broker_order_id: str) -> Order | None:
    """Locked lookup by the broker's id (how leg events find their rows)."""
    return (
        await session.execute(
            select(Order)
            .where(Order.broker_order_id == broker_order_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
