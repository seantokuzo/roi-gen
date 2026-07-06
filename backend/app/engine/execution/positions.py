"""Net position tracker — the (portfolio, symbol) mirror of broker positions.

:class:`~app.models.trading.Position` is the reconciliation-level net view
(signed qty + avg entry), and the broker snapshot is authoritative for it:
reconciliation overwrites qty/avg and deletes flat rows wholesale. This applier
keeps the row LIVE between reconciles as fills stream in. It is deliberately
NOT called for boot-synthesized fills — at boot the broker overwrite already
includes them, and applying the delta again would double-count (a verified
design-review finding).

Avg-entry math is the standard convention: growing the position in its current
direction re-weights the average; reducing leaves it unchanged (P&L attribution
belongs to lots, not the net view); crossing through zero resets the average to
the fill price for the residual.
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import select

from app.models.enums import OrderSide
from app.models.trading import Position

if TYPE_CHECKING:
    import uuid

    from sqlalchemy.ext.asyncio import AsyncSession


async def apply_fill_to_position(
    session: AsyncSession,
    *,
    portfolio_id: uuid.UUID,
    symbol: str,
    side: OrderSide,
    qty: Decimal,
    price: Decimal,
) -> None:
    """Apply a signed fill delta onto the ``(portfolio, symbol)`` position row.

    Buy is positive, sell negative. A position landing exactly flat deletes the
    row (matching reconciliation's semantics). Staged on ``session``; caller
    commits.
    """
    delta = qty if side is OrderSide.buy else -qty
    position = (
        await session.execute(
            select(Position).where(
                Position.portfolio_id == portfolio_id,
                Position.symbol == symbol,
            )
        )
    ).scalar_one_or_none()

    if position is None:
        session.add(
            Position(
                portfolio_id=portfolio_id,
                symbol=symbol,
                qty=delta,
                avg_entry_price=price,
            )
        )
        return

    old_qty = position.qty
    new_qty = old_qty + delta

    if new_qty == 0:
        await session.delete(position)
        return

    same_direction = (old_qty > 0 and delta > 0) or (old_qty < 0 and delta < 0)
    crossed_zero = (old_qty > 0 > new_qty) or (old_qty < 0 < new_qty)
    if same_direction:
        # Weighted average over the grown position (abs weights; signs match).
        total = abs(old_qty) + abs(delta)
        position.avg_entry_price = (
            position.avg_entry_price * abs(old_qty) + price * abs(delta)
        ) / total
    elif crossed_zero:
        # The residual is a fresh position opened at this fill's price.
        position.avg_entry_price = price
    # Plain reduction: avg entry unchanged (realized P&L lives in lots).

    position.qty = new_qty


def signed_qty(side: OrderSide, qty: Decimal) -> Decimal:
    """Signed quantity convention used across the position ledger."""
    return qty if side is OrderSide.buy else -qty
