"""FIFO lot engine — where fills become realized P&L (the ledger legacy never had).

Every applied fill either consumes open opposite-side lots (front of the FIFO
queue first) and books realized P&L, opens a new lot with the remainder, or
both (a cross-through-zero flip). Two artifacts are written per consumption:

- the consumed :class:`~app.models.trading.Lot` (``qty_open`` decremented,
  ``realized_pnl`` accumulated, ``closed_at`` stamped at FULL close), and
- a :class:`~app.models.trading.LotClose` row — the per-close ledger the
  same-day risk queries read, because the lot accumulator alone hides a
  partial-close loss until the lot fully closes.

Scoping: FIFO matching is per ``(portfolio_id, symbol, strategy_id)`` for
strategy-scoped fills. Two strategies may hold the same symbol (the broker
nets per account; lots are the per-strategy analytical ledger), and P&L
attribution must never bleed across strategies.

A **strategy-less fill** (``strategy_id=None`` — flatten liquidations; the
provenance marker is the ``roigen-flatten-`` client-id prefix, never
``strategy_id IS NULL``) is different: the broker liquidates the NET account
position, so the fill matches open lots across ALL strategies in
``(portfolio_id, symbol)``, FIFO by ``opened_at``. Each ``LotClose`` carries
the **closed lot's** ``strategy_id`` so flatten-realized P&L lands on the
right strategy's same-day breaker. Overshoot rule: a strategy-less fill's
remainder beyond every matched open lot NEVER opens a lot — the broker sized
the liquidation, so overshoot is always ledger divergence (a missing entry
we haven't recorded yet), not a real short. The remainder is parked as an
:class:`~app.models.telemetry.EventLog` anomaly
(:data:`UNAPPLIED_REMAINDER_EVENT`) and retried by the periodic reconcile
(:mod:`app.services.reconciliation`). Strategy-scoped remainders still open a
new lot (legitimate short-entry / cross-through-zero flow); NULL-strategy
LOTS stay reserved for manual/unattributed entries.

Sign conventions: long lots are buys, short lots are sells (``Lot.side`` stores
the :class:`~app.models.enums.OrderSide` value). Realized P&L on a long close is
``(sell_price − entry) × qty``; on a short cover ``(entry − buy_price) × qty``.
All arithmetic is :class:`~decimal.Decimal` (iron law #7).

Consumption order is ``(opened_at, created_at, id)`` — ``opened_at`` alone can
collide (paper partial fills in the same second; boot-synthesized fills sharing
a timestamp), and FIFO must be deterministic.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import select

from app.core.logging import get_logger
from app.models.enums import EventSource, OrderSide
from app.models.telemetry import EventLog
from app.models.trading import Lot, LotClose

if TYPE_CHECKING:
    from datetime import datetime

    from sqlalchemy.ext.asyncio import AsyncSession

log = get_logger("engine.lots")

# The parked-remainder anomaly (see module docstring). Written here; read and
# retried by the reconciliation service's parked-remainder pass.
UNAPPLIED_REMAINDER_EVENT = "lots.unapplied_liquidation_remainder"


@dataclass(frozen=True, slots=True)
class LotApplication:
    """What one applied fill did to the lot ledger."""

    realized_pnl: Decimal
    qty_closed: Decimal
    lots_fully_closed: int
    opened_lot_id: uuid.UUID | None
    # Strategy-less overshoot only: quantity that matched no open lot and was
    # parked (never opens a lot). Always 0 for strategy-scoped fills.
    unapplied_qty: Decimal = Decimal("0")


async def apply_fill_to_lots(
    session: AsyncSession,
    *,
    portfolio_id: uuid.UUID,
    strategy_id: uuid.UUID | None,
    symbol: str,
    side: OrderSide,
    qty: Decimal,
    price: Decimal,
    occurred_at: datetime,
    order_id: uuid.UUID | None = None,
    fill_id: uuid.UUID | None = None,
    record_unapplied: bool = True,
) -> LotApplication:
    """Apply one fill FIFO against the ``(portfolio, symbol, strategy)`` lots.

    Consumes open opposite-side lots first (booking a ``LotClose`` per
    consumption); a strategy-scoped remainder opens a new same-side lot, a
    strategy-less remainder is parked (module docstring). Rows are staged on
    ``session``; the caller owns the transaction and must already hold whatever
    serialization it needs (the writers lock the Order row).

    ``record_unapplied=False`` suppresses the parked-remainder EventLog row
    ONLY — for the reconcile retry pass, which re-applies an already-parked
    remainder and owns its own resolve/supersede bookkeeping (a fresh anomaly
    row from in here would double-park the same quantity). Every other caller
    leaves it True.
    """
    opposite = OrderSide.sell if side is OrderSide.buy else OrderSide.buy
    # Strategy-scoped fills match their own ledger; strategy-less fills
    # (flatten liquidations of the NET broker position) match every
    # strategy's lots in (portfolio, symbol) — the old NULL-scope-only match
    # would close nothing and mint a phantom opposite-side lot.
    conditions = [
        Lot.portfolio_id == portfolio_id,
        Lot.symbol == symbol,
        Lot.side == opposite.value,
        Lot.qty_open > 0,
    ]
    if strategy_id is not None:
        conditions.append(Lot.strategy_id == strategy_id)
    # FOR UPDATE: the Order-row locks writers hold only serialize fills of the
    # SAME order — two writers filling DIFFERENT orders in this same
    # (portfolio, strategy, symbol) scope (live writer vs periodic-reconcile
    # synthesis) would otherwise read-modify-write the same lot concurrently
    # and double-book realized P&L. The deterministic ORDER BY keeps lock
    # acquisition order consistent across writers (no deadlock by ordering) —
    # the strategy-less widened set locks a superset of rows in the SAME
    # global order, so it composes with strategy-scoped writers too.
    open_lots = (
        (
            await session.execute(
                select(Lot)
                .where(*conditions)
                .order_by(Lot.opened_at, Lot.created_at, Lot.id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        )
        .scalars()
        .all()
    )

    remaining = qty
    realized = Decimal("0")
    qty_closed = Decimal("0")
    fully_closed = 0

    for lot in open_lots:
        if remaining <= 0:
            break
        consumed = min(lot.qty_open, remaining)
        # Long lot (buy) closed by a sell: (exit − entry) × qty.
        # Short lot (sell) covered by a buy: (entry − exit) × qty.
        if lot.side == OrderSide.buy.value:
            pnl = (price - lot.entry_price) * consumed
        else:
            pnl = (lot.entry_price - price) * consumed

        lot.qty_open = lot.qty_open - consumed
        lot.realized_pnl = lot.realized_pnl + pnl
        if lot.qty_open == 0:
            lot.closed_at = occurred_at
            fully_closed += 1

        session.add(
            LotClose(
                lot_id=lot.id,
                order_id=order_id,
                fill_id=fill_id,
                portfolio_id=portfolio_id,
                # The CLOSED LOT's strategy, never the fill's parameter: a
                # strategy-less liquidation fill would otherwise stamp NULL
                # and hide the realized loss from that strategy's same-day
                # breaker. (Lot.realized_pnl/closed_at above need no such
                # treatment — they live on the lot row itself.)
                strategy_id=lot.strategy_id,
                symbol=symbol,
                qty=consumed,
                realized_pnl=pnl,
                closed_at=occurred_at,
            )
        )

        realized += pnl
        qty_closed += consumed
        remaining -= consumed

    opened_lot_id: uuid.UUID | None = None
    unapplied = Decimal("0")
    if remaining > 0:
        if strategy_id is None:
            # Overshoot rule: NEVER open a lot from a strategy-less remainder.
            # Park it; the periodic reconcile retries it against lots that
            # appear later (e.g. a synthesized missing entry).
            unapplied = remaining
            log.warning(
                "engine.lots.unapplied_remainder",
                symbol=symbol,
                side=side.value,
                qty=str(remaining),
                price=str(price),
                order_id=str(order_id) if order_id else None,
                fill_id=str(fill_id) if fill_id else None,
            )
            if record_unapplied:
                session.add(
                    EventLog(
                        source=EventSource.engine.value,
                        event_type=UNAPPLIED_REMAINDER_EVENT,
                        portfolio_id=portfolio_id,
                        order_id=order_id,
                        payload={
                            "portfolio_id": str(portfolio_id),
                            "symbol": symbol,
                            "side": side.value,
                            "qty": str(remaining),
                            "price": str(price),
                            "occurred_at": occurred_at.isoformat(),
                            "order_id": str(order_id) if order_id else None,
                            "fill_id": str(fill_id) if fill_id else None,
                        },
                    )
                )
        else:
            new_lot = Lot(
                portfolio_id=portfolio_id,
                strategy_id=strategy_id,
                symbol=symbol,
                side=side.value,
                qty_orig=remaining,
                qty_open=remaining,
                entry_price=price,
                entry_fill_id=fill_id,
                opened_at=occurred_at,
            )
            session.add(new_lot)
            await session.flush()
            opened_lot_id = new_lot.id

    log.debug(
        "engine.lots.applied",
        symbol=symbol,
        side=side.value,
        qty=str(qty),
        price=str(price),
        realized=str(realized),
        closed=str(qty_closed),
        opened=str(remaining) if opened_lot_id is not None else None,
        unapplied=str(unapplied) if unapplied > 0 else None,
    )
    return LotApplication(
        realized_pnl=realized,
        qty_closed=qty_closed,
        lots_fully_closed=fully_closed,
        opened_lot_id=opened_lot_id,
        unapplied_qty=unapplied,
    )
