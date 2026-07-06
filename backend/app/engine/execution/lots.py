"""FIFO lot engine — where fills become realized P&L (the ledger legacy never had).

Every applied fill either consumes open opposite-side lots (front of the FIFO
queue first) and books realized P&L, opens a new lot with the remainder, or
both (a cross-through-zero flip). Two artifacts are written per consumption:

- the consumed :class:`~app.models.trading.Lot` (``qty_open`` decremented,
  ``realized_pnl`` accumulated, ``closed_at`` stamped at FULL close), and
- a :class:`~app.models.trading.LotClose` row — the per-close ledger the
  same-day risk queries read, because the lot accumulator alone hides a
  partial-close loss until the lot fully closes.

Scoping: FIFO matching is per ``(portfolio_id, symbol, strategy_id)``. Two
strategies may hold the same symbol (the broker nets per account; lots are the
per-strategy analytical ledger), and P&L attribution must never bleed across
strategies. A NULL ``strategy_id`` (manual / unattributed) is its own scope.

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
from app.models.enums import OrderSide
from app.models.trading import Lot, LotClose

if TYPE_CHECKING:
    from datetime import datetime

    from sqlalchemy.ext.asyncio import AsyncSession

log = get_logger("engine.lots")


@dataclass(frozen=True, slots=True)
class LotApplication:
    """What one applied fill did to the lot ledger."""

    realized_pnl: Decimal
    qty_closed: Decimal
    lots_fully_closed: int
    opened_lot_id: uuid.UUID | None


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
) -> LotApplication:
    """Apply one fill FIFO against the ``(portfolio, symbol, strategy)`` lots.

    Consumes open opposite-side lots first (booking a ``LotClose`` per
    consumption); any remainder opens a new same-side lot. Rows are staged on
    ``session``; the caller owns the transaction and must already hold whatever
    serialization it needs (the writers lock the Order row).
    """
    opposite = OrderSide.sell if side is OrderSide.buy else OrderSide.buy
    open_lots = (
        (
            await session.execute(
                select(Lot)
                .where(
                    Lot.portfolio_id == portfolio_id,
                    Lot.strategy_id == strategy_id,
                    Lot.symbol == symbol,
                    Lot.side == opposite.value,
                    Lot.qty_open > 0,
                )
                .order_by(Lot.opened_at, Lot.created_at, Lot.id)
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
                strategy_id=strategy_id,
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
    if remaining > 0:
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
        opened=str(remaining) if remaining > 0 else None,
    )
    return LotApplication(
        realized_pnl=realized,
        qty_closed=qty_closed,
        lots_fully_closed=fully_closed,
        opened_lot_id=opened_lot_id,
    )
