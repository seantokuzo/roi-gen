"""Missed-fill synthesis — recovering executions the stream never delivered.

Alpaca's trade-updates stream has no replay and Redis pub/sub is
fire-and-forget: any execution that prints while the engine is down, the
websocket is reconnecting, or the subscriber is briefly deaf is gone as an
event. The durable evidence that survives is the broker's CUMULATIVE
``filled_qty`` / ``filled_avg_price`` on the order. Synthesis turns that
evidence back into ledger rows.

The detection cursor is ``SUM(Fill.qty)`` per order — the durable fill ledger,
NEVER ``Order.filled_qty``. Multiple writers legitimately advance
``Order.filled_qty`` (trade updates, reconciliation, the API sync endpoint)
without applying lots; keying on it makes missed fills permanently invisible
after any such advance (a verified design-review finding). The ledger invariant
this file maintains: **lots-applied quantity ≡ SUM(Fill.qty) per order**.

Span pricing backs the missed quantity's price out of the notional difference:
``(cum_qty × cum_avg − Σ recorded qty×price) / span_qty``. Exact in aggregate
regardless of how many real fills were already recorded; NOT quantized to the
sub-penny order grid (broker avg prices are legitimately sub-penny — only
order submission prices get quantized).

Synthetic ``broker_fill_id``s are deterministic
(``{tag}-{order_id}-{cumulative}``) so the partial unique index makes re-runs
idempotent. Decimals are normalized first — ``100`` and ``100.000000000``
must produce the same id from any code path.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import func, select

from app.core.logging import get_logger
from app.engine.execution.lots import apply_fill_to_lots
from app.engine.execution.positions import apply_fill_to_position
from app.models.enums import OrderSide
from app.models.trading import Fill, Order

if TYPE_CHECKING:
    from datetime import datetime

    from sqlalchemy.ext.asyncio import AsyncSession

log = get_logger("engine.synthesis")


@dataclass(frozen=True, slots=True)
class AppliedLedger:
    """The durable per-order fill ledger: what lots have actually seen."""

    qty: Decimal
    notional: Decimal


async def applied_ledger(session: AsyncSession, order_id: uuid.UUID) -> AppliedLedger:
    """Read the order's recorded fill quantity and notional (the cursor)."""
    row = (
        await session.execute(
            select(
                func.coalesce(func.sum(Fill.qty), 0),
                func.coalesce(func.sum(Fill.qty * Fill.price), 0),
            ).where(Fill.order_id == order_id)
        )
    ).one()
    qty = row[0] if row[0] is not None else Decimal("0")
    notional = row[1] if row[1] is not None else Decimal("0")
    return AppliedLedger(qty=Decimal(qty), notional=Decimal(notional))


def span_price(
    *,
    cum_qty: Decimal,
    cum_avg_price: Decimal | None,
    applied: AppliedLedger,
    span_qty: Decimal,
) -> Decimal:
    """Back the missed span's average price out of the notional difference."""
    if span_qty <= 0:
        msg = "span_qty must be positive"
        raise ValueError(msg)
    if cum_avg_price is None:
        # No broker average at all (shouldn't happen for a filled span); the
        # only price we have is what we recorded — degenerate to it, or zero
        # cost basis if nothing was recorded. Callers log this.
        if applied.qty > 0:
            return applied.notional / applied.qty
        return Decimal("0")
    if applied.qty == 0:
        return cum_avg_price  # exact: the span IS the whole cumulative fill
    return (cum_qty * cum_avg_price - applied.notional) / span_qty


def synthetic_fill_id(tag: str, order_id: uuid.UUID, cumulative: Decimal) -> str:
    """Deterministic id for a synthesized fill (idempotent under the unique index)."""
    return f"{tag}-{order_id}-{cumulative.normalize()}"


async def synthesize_span(
    session: AsyncSession,
    order: Order,
    *,
    span_qty: Decimal,
    price: Decimal,
    occurred_at: datetime,
    tag: str,
    apply_position: bool,
) -> Fill | None:
    """Insert a synthetic Fill for a missed span and apply it to the lot ledger.

    ``apply_position=False`` for boot/reconcile synthesis (the broker position
    overwrite in the same transaction already includes the missed quantity —
    applying the delta again double-counts, a verified design-review finding);
    ``True`` for mid-session gap synthesis, which must keep the live Position
    row current between reconciles.

    Idempotent: if the deterministic fill id already exists, does nothing.
    Staged on ``session``; caller commits (atomically with whatever detection
    evidence produced the span).
    """
    applied = await applied_ledger(session, order.id)
    cumulative = applied.qty + span_qty
    fill_key = synthetic_fill_id(tag, order.id, cumulative)
    existing = (
        await session.execute(select(Fill.id).where(Fill.broker_fill_id == fill_key))
    ).scalar_one_or_none()
    if existing is not None:
        return None

    fill = Fill(
        order_id=order.id,
        broker_fill_id=fill_key,
        qty=span_qty,
        price=price,
        occurred_at=occurred_at,
        raw={"synthesized": tag, "cumulative": str(cumulative)},
    )
    session.add(fill)
    await session.flush()

    side = OrderSide(order.side)
    await apply_fill_to_lots(
        session,
        portfolio_id=order.portfolio_id,
        strategy_id=order.strategy_id,
        symbol=order.symbol,
        side=side,
        qty=span_qty,
        price=price,
        occurred_at=occurred_at,
        order_id=order.id,
        fill_id=fill.id,
    )
    if apply_position:
        await apply_fill_to_position(
            session,
            portfolio_id=order.portfolio_id,
            symbol=order.symbol,
            side=side,
            qty=span_qty,
            price=price,
        )

    log.warning(
        "engine.synthesis.fill",
        tag=tag,
        client_order_id=order.client_order_id,
        symbol=order.symbol,
        qty=str(span_qty),
        price=str(price),
    )
    return fill
