"""Net position tracker: upsert, avg-entry math, cross-zero, delete-at-flat."""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import select

from app.engine.execution.positions import apply_fill_to_position
from app.models.enums import OrderSide
from app.models.trading import Position
from tests.engine.builders import seed_portfolio

if TYPE_CHECKING:
    import uuid

    from sqlalchemy.ext.asyncio import AsyncSession

    from app.models.user import User


async def _fill(
    session: AsyncSession,
    portfolio_id: uuid.UUID,
    *,
    side: OrderSide,
    qty: str,
    price: str,
) -> None:
    await apply_fill_to_position(
        session,
        portfolio_id=portfolio_id,
        symbol="SPY",
        side=side,
        qty=Decimal(qty),
        price=Decimal(price),
    )


async def _position(session: AsyncSession, portfolio_id: uuid.UUID) -> Position | None:
    return (
        await session.execute(select(Position).where(Position.portfolio_id == portfolio_id))
    ).scalar_one_or_none()


async def test_first_fill_creates_the_row(db_session: AsyncSession, seeded_user: User) -> None:
    portfolio = await seed_portfolio(db_session, seeded_user.id)
    await _fill(db_session, portfolio.id, side=OrderSide.buy, qty="100", price="50")
    position = await _position(db_session, portfolio.id)
    assert position is not None
    assert position.qty == Decimal("100")
    assert position.avg_entry_price == Decimal("50")


async def test_same_direction_growth_reweights_average(
    db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio = await seed_portfolio(db_session, seeded_user.id)
    await _fill(db_session, portfolio.id, side=OrderSide.buy, qty="100", price="50")
    await _fill(db_session, portfolio.id, side=OrderSide.buy, qty="100", price="60")
    position = await _position(db_session, portfolio.id)
    assert position is not None
    assert position.qty == Decimal("200")
    assert position.avg_entry_price == Decimal("55")


async def test_reduction_leaves_average_unchanged(
    db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio = await seed_portfolio(db_session, seeded_user.id)
    await _fill(db_session, portfolio.id, side=OrderSide.buy, qty="100", price="50")
    await _fill(db_session, portfolio.id, side=OrderSide.sell, qty="40", price="60")
    position = await _position(db_session, portfolio.id)
    assert position is not None
    assert position.qty == Decimal("60")
    assert position.avg_entry_price == Decimal("50")  # P&L belongs to lots


async def test_flat_deletes_the_row(db_session: AsyncSession, seeded_user: User) -> None:
    portfolio = await seed_portfolio(db_session, seeded_user.id)
    await _fill(db_session, portfolio.id, side=OrderSide.buy, qty="100", price="50")
    await _fill(db_session, portfolio.id, side=OrderSide.sell, qty="100", price="55")
    assert await _position(db_session, portfolio.id) is None  # matches reconciliation


async def test_cross_zero_resets_average_to_fill_price(
    db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio = await seed_portfolio(db_session, seeded_user.id)
    await _fill(db_session, portfolio.id, side=OrderSide.buy, qty="100", price="50")
    await _fill(db_session, portfolio.id, side=OrderSide.sell, qty="150", price="55")
    position = await _position(db_session, portfolio.id)
    assert position is not None
    assert position.qty == Decimal("-50")  # net short now
    assert position.avg_entry_price == Decimal("55")  # fresh basis at the flip


async def test_short_growth_reweights_like_longs(
    db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio = await seed_portfolio(db_session, seeded_user.id)
    await _fill(db_session, portfolio.id, side=OrderSide.sell, qty="100", price="40")
    await _fill(db_session, portfolio.id, side=OrderSide.sell, qty="100", price="50")
    position = await _position(db_session, portfolio.id)
    assert position is not None
    assert position.qty == Decimal("-200")
    assert position.avg_entry_price == Decimal("45")
