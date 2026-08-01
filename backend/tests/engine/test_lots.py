"""FIFO lot engine: open/close/flip mechanics, P&L signs, per-close ledger, scoping."""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import select

from app.engine.execution.lots import UNAPPLIED_REMAINDER_EVENT, apply_fill_to_lots
from app.engine.risk.state import RiskStateProvider
from app.models.enums import OrderSide
from app.models.telemetry import EventLog
from app.models.trading import Lot, LotClose
from tests.engine.builders import (
    DEFAULT_NOW,
    FakeEngineAdapter,
    seed_lot,
    seed_portfolio,
    seed_strategy,
)

if TYPE_CHECKING:
    import uuid

    from sqlalchemy.ext.asyncio import AsyncSession

    from app.models.portfolio import Portfolio
    from app.models.strategy import Strategy as StrategyModel
    from app.models.user import User


async def _scoped(db_session: AsyncSession, seeded_user: User) -> tuple[Portfolio, StrategyModel]:
    portfolio = await seed_portfolio(db_session, seeded_user.id)
    strategy = await seed_strategy(db_session, portfolio.id)
    return portfolio, strategy


async def _apply(
    session: AsyncSession,
    portfolio_id: uuid.UUID,
    strategy_id: uuid.UUID | None,
    *,
    side: OrderSide,
    qty: str,
    price: str,
    at_offset_min: int = 0,
) -> object:
    return await apply_fill_to_lots(
        session,
        portfolio_id=portfolio_id,
        strategy_id=strategy_id,
        symbol="SPY",
        side=side,
        qty=Decimal(qty),
        price=Decimal(price),
        occurred_at=DEFAULT_NOW + timedelta(minutes=at_offset_min),
    )


async def _open_lots(session: AsyncSession, portfolio_id: uuid.UUID) -> list[Lot]:
    return list(
        (
            await session.execute(
                select(Lot)
                .where(Lot.portfolio_id == portfolio_id, Lot.qty_open > 0)
                .order_by(Lot.opened_at, Lot.created_at, Lot.id)
            )
        )
        .scalars()
        .all()
    )


async def test_buy_with_no_shorts_opens_a_long_lot(
    db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio, strategy = await _scoped(db_session, seeded_user)
    result = await _apply(
        db_session, portfolio.id, strategy.id, side=OrderSide.buy, qty="100", price="50.00"
    )
    assert result.realized_pnl == Decimal("0")
    assert result.opened_lot_id is not None

    lots = await _open_lots(db_session, portfolio.id)
    assert len(lots) == 1
    assert lots[0].side == OrderSide.buy.value
    assert lots[0].qty_orig == Decimal("100")
    assert lots[0].qty_open == Decimal("100")
    assert lots[0].entry_price == Decimal("50.00")
    assert lots[0].closed_at is None


async def test_sell_closes_long_lot_and_books_pnl_and_close_row(
    db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio, strategy = await _scoped(db_session, seeded_user)
    await _apply(db_session, portfolio.id, strategy.id, side=OrderSide.buy, qty="100", price="50")
    result = await _apply(
        db_session, portfolio.id, strategy.id, side=OrderSide.sell, qty="100", price="52"
    )

    assert result.realized_pnl == Decimal("200")  # (52 − 50) × 100
    assert result.lots_fully_closed == 1
    assert result.opened_lot_id is None

    lot = (await db_session.execute(select(Lot))).scalars().one()
    assert lot.qty_open == Decimal("0")
    assert lot.closed_at is not None
    assert lot.realized_pnl == Decimal("200")

    close = (await db_session.execute(select(LotClose))).scalars().one()
    assert close.lot_id == lot.id
    assert close.qty == Decimal("100")
    assert close.realized_pnl == Decimal("200")
    assert close.strategy_id == strategy.id


async def test_partial_close_books_pnl_but_lot_stays_open(
    db_session: AsyncSession, seeded_user: User
) -> None:
    """The C1 scenario: a scale-out at a loss must be visible in lot_closes
    immediately, while the lot itself stays open (closed_at NULL)."""
    portfolio, strategy = await _scoped(db_session, seeded_user)
    await _apply(db_session, portfolio.id, strategy.id, side=OrderSide.buy, qty="1000", price="10")
    result = await _apply(
        db_session, portfolio.id, strategy.id, side=OrderSide.sell, qty="900", price="8.50"
    )

    assert result.realized_pnl == Decimal("-1350.00")
    assert result.lots_fully_closed == 0

    lot = (await db_session.execute(select(Lot))).scalars().one()
    assert lot.qty_open == Decimal("100")
    assert lot.closed_at is None  # NOT fully closed...
    close = (await db_session.execute(select(LotClose))).scalars().one()
    assert close.realized_pnl == Decimal("-1350.00")  # ...but the loss is on the ledger


async def test_fifo_consumes_oldest_lot_first(db_session: AsyncSession, seeded_user: User) -> None:
    portfolio, strategy = await _scoped(db_session, seeded_user)
    await _apply(db_session, portfolio.id, strategy.id, side=OrderSide.buy, qty="10", price="100")
    await _apply(
        db_session,
        portfolio.id,
        strategy.id,
        side=OrderSide.buy,
        qty="10",
        price="110",
        at_offset_min=1,
    )
    # Sell 15: consumes all of lot1 (entry 100) + 5 of lot2 (entry 110).
    result = await _apply(
        db_session,
        portfolio.id,
        strategy.id,
        side=OrderSide.sell,
        qty="15",
        price="120",
        at_offset_min=2,
    )

    # (120−100)×10 + (120−110)×5 = 200 + 50
    assert result.realized_pnl == Decimal("250")
    assert result.lots_fully_closed == 1
    remaining = await _open_lots(db_session, portfolio.id)
    assert len(remaining) == 1
    assert remaining[0].entry_price == Decimal("110")
    assert remaining[0].qty_open == Decimal("5")


async def test_cross_zero_flip_closes_long_and_opens_short(
    db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio, strategy = await _scoped(db_session, seeded_user)
    await _apply(db_session, portfolio.id, strategy.id, side=OrderSide.buy, qty="10", price="100")
    result = await _apply(
        db_session,
        portfolio.id,
        strategy.id,
        side=OrderSide.sell,
        qty="25",
        price="105",
        at_offset_min=1,
    )

    assert result.realized_pnl == Decimal("50")  # (105−100)×10
    assert result.qty_closed == Decimal("10")
    assert result.opened_lot_id is not None  # residual 15 opens a short

    shorts = [
        lot
        for lot in await _open_lots(db_session, portfolio.id)
        if lot.side == OrderSide.sell.value
    ]
    assert len(shorts) == 1
    assert shorts[0].qty_open == Decimal("15")
    assert shorts[0].entry_price == Decimal("105")


async def test_short_cover_pnl_sign_convention(db_session: AsyncSession, seeded_user: User) -> None:
    portfolio, strategy = await _scoped(db_session, seeded_user)
    # Short 100 @ 50, cover @ 45 → profit (entry − exit) × qty = +500.
    await _apply(db_session, portfolio.id, strategy.id, side=OrderSide.sell, qty="100", price="50")
    result = await _apply(
        db_session,
        portfolio.id,
        strategy.id,
        side=OrderSide.buy,
        qty="100",
        price="45",
        at_offset_min=1,
    )
    assert result.realized_pnl == Decimal("500")

    # And a losing cover: short 100 @ 50, cover @ 53 → −300.
    await _apply(
        db_session,
        portfolio.id,
        strategy.id,
        side=OrderSide.sell,
        qty="100",
        price="50",
        at_offset_min=2,
    )
    losing = await _apply(
        db_session,
        portfolio.id,
        strategy.id,
        side=OrderSide.buy,
        qty="100",
        price="53",
        at_offset_min=3,
    )
    assert losing.realized_pnl == Decimal("-300")


async def test_strategy_scoping_never_bleeds_across_ledgers(
    db_session: AsyncSession, seeded_user: User
) -> None:
    """Strategy B selling SPY must not consume strategy A's lots. (Strategy-less
    fills deliberately DO cross scopes — covered by the flatten tests below.)"""
    portfolio = await seed_portfolio(db_session, seeded_user.id)
    strat_a = await seed_strategy(db_session, portfolio.id, name="a")
    strat_b = await seed_strategy(db_session, portfolio.id, name="b")
    await _apply(db_session, portfolio.id, strat_a.id, side=OrderSide.buy, qty="10", price="100")

    result_b = await _apply(
        db_session, portfolio.id, strat_b.id, side=OrderSide.sell, qty="10", price="105"
    )
    # B had no long lots: its sell opens a SHORT, realizes nothing, touches
    # nothing of A's.
    assert result_b.realized_pnl == Decimal("0")
    assert result_b.opened_lot_id is not None

    a_lots = (
        (await db_session.execute(select(Lot).where(Lot.strategy_id == strat_a.id))).scalars().all()
    )
    assert len(a_lots) == 1
    assert a_lots[0].qty_open == Decimal("10")  # untouched


async def test_fifo_tiebreak_is_deterministic_on_equal_opened_at(
    db_session: AsyncSession, seeded_user: User
) -> None:
    # Two lots with the SAME opened_at (paper partials in one second): FIFO
    # falls back to created_at, so consumption order can't depend on row luck.
    portfolio, strategy = await _scoped(db_session, seeded_user)
    first = await seed_lot(
        db_session,
        portfolio.id,
        strategy.id,
        qty_orig=Decimal("10"),
        qty_open=Decimal("10"),
        entry_price=Decimal("100"),
        opened_at=DEFAULT_NOW,
    )
    second = await seed_lot(
        db_session,
        portfolio.id,
        strategy.id,
        qty_orig=Decimal("10"),
        qty_open=Decimal("10"),
        entry_price=Decimal("110"),
        opened_at=DEFAULT_NOW,
    )
    # server-default created_at is transaction-start time (identical here) —
    # set distinct instants explicitly so the tiebreak is actually exercised.
    first.created_at = DEFAULT_NOW
    second.created_at = DEFAULT_NOW + timedelta(seconds=1)
    await db_session.flush()

    result = await _apply(
        db_session,
        portfolio.id,
        strategy.id,
        side=OrderSide.sell,
        qty="10",
        price="120",
        at_offset_min=1,
    )
    # The earlier-created lot (entry 100) is consumed: (120−100)×10.
    assert result.realized_pnl == Decimal("200")
    await db_session.flush()  # refresh() discards unflushed mutations otherwise
    await db_session.refresh(first)
    assert first.qty_open == Decimal("0")


# ── Strategy-less (flatten liquidation) fills ────────────────────────


async def test_none_strategy_fill_closes_fifo_across_strategies(
    db_session: AsyncSession, seeded_user: User
) -> None:
    """A strategy-less fill liquidates the NET position: it consumes open lots
    across ALL strategies FIFO by opened_at, and each LotClose is attributed to
    the CLOSED LOT's strategy — not the fill's NULL parameter."""
    portfolio = await seed_portfolio(db_session, seeded_user.id)
    strat_a = await seed_strategy(db_session, portfolio.id, name="a")
    strat_b = await seed_strategy(db_session, portfolio.id, name="b")
    await _apply(db_session, portfolio.id, strat_a.id, side=OrderSide.buy, qty="10", price="100")
    await _apply(
        db_session,
        portfolio.id,
        strat_b.id,
        side=OrderSide.buy,
        qty="10",
        price="110",
        at_offset_min=1,
    )

    result = await _apply(
        db_session, portfolio.id, None, side=OrderSide.sell, qty="15", price="120", at_offset_min=2
    )

    # A's older lot is fully consumed first, then 5 of B's:
    # (120−100)×10 + (120−110)×5 = 200 + 50.
    assert result.realized_pnl == Decimal("250")
    assert result.qty_closed == Decimal("15")
    assert result.lots_fully_closed == 1
    assert result.opened_lot_id is None
    assert result.unapplied_qty == Decimal("0")

    closes = (await db_session.execute(select(LotClose))).scalars().all()
    by_strategy = {c.strategy_id: c for c in closes}
    assert set(by_strategy) == {strat_a.id, strat_b.id}  # never NULL
    assert by_strategy[strat_a.id].qty == Decimal("10")
    assert by_strategy[strat_a.id].realized_pnl == Decimal("200")
    assert by_strategy[strat_b.id].qty == Decimal("5")
    assert by_strategy[strat_b.id].realized_pnl == Decimal("50")

    remaining = await _open_lots(db_session, portfolio.id)
    assert len(remaining) == 1
    assert remaining[0].strategy_id == strat_b.id
    assert remaining[0].qty_open == Decimal("5")


async def test_breaker_sees_flatten_realized_pnl_per_strategy(
    db_session: AsyncSession, seeded_user: User
) -> None:
    """The per-strategy same-day breaker input (day_realized_pnl_strategy reads
    lot_closes) must see a flatten loss on the strategy whose lots were closed."""
    portfolio = await seed_portfolio(db_session, seeded_user.id)
    strat_a = await seed_strategy(db_session, portfolio.id, name="a")
    strat_b = await seed_strategy(db_session, portfolio.id, name="b")
    await _apply(db_session, portfolio.id, strat_a.id, side=OrderSide.buy, qty="10", price="100")
    await _apply(
        db_session,
        portfolio.id,
        strat_b.id,
        side=OrderSide.buy,
        qty="10",
        price="110",
        at_offset_min=1,
    )
    # Flatten at 95: A loses (95−100)×10 = −50; B loses (95−110)×5 = −75.
    await _apply(
        db_session, portfolio.id, None, side=OrderSide.sell, qty="15", price="95", at_offset_min=2
    )

    provider = RiskStateProvider()
    state_a = await provider.load(
        db_session,
        FakeEngineAdapter(),
        portfolio_id=portfolio.id,
        strategy_id=strat_a.id,
        symbol="SPY",
    )
    state_b = await provider.load(
        db_session,
        FakeEngineAdapter(),
        portfolio_id=portfolio.id,
        strategy_id=strat_b.id,
        symbol="SPY",
    )
    assert state_a.day_realized_pnl_strategy == Decimal("-50")
    assert state_b.day_realized_pnl_strategy == Decimal("-75")


async def test_none_strategy_overshoot_parks_remainder_and_never_opens_a_lot(
    db_session: AsyncSession, seeded_user: User
) -> None:
    """A strategy-less fill beyond every matched lot must NOT mint a phantom
    opposite-side lot — the remainder is parked as an EventLog anomaly."""
    portfolio, strategy = await _scoped(db_session, seeded_user)
    await _apply(db_session, portfolio.id, strategy.id, side=OrderSide.buy, qty="100", price="100")

    result = await _apply(
        db_session, portfolio.id, None, side=OrderSide.sell, qty="150", price="110", at_offset_min=1
    )

    assert result.qty_closed == Decimal("100")
    assert result.realized_pnl == Decimal("1000")
    assert result.opened_lot_id is None
    assert result.unapplied_qty == Decimal("50")

    lots = (await db_session.execute(select(Lot))).scalars().all()
    assert len(lots) == 1  # the entry lot only — no phantom short
    assert lots[0].qty_open == Decimal("0")

    anomaly = (
        (
            await db_session.execute(
                select(EventLog).where(EventLog.event_type == UNAPPLIED_REMAINDER_EVENT)
            )
        )
        .scalars()
        .one()
    )
    assert anomaly.portfolio_id == portfolio.id
    assert anomaly.payload["symbol"] == "SPY"
    assert anomaly.payload["side"] == OrderSide.sell.value
    # Decimal-compare: quantities routed through Numeric columns carry scale.
    assert Decimal(anomaly.payload["qty"]) == Decimal("50")
    assert Decimal(anomaly.payload["price"]) == Decimal("110")


async def test_strategy_scoped_overshoot_still_opens_a_lot_and_parks_nothing(
    db_session: AsyncSession, seeded_user: User
) -> None:
    """Regression: the cross-through-zero flip is legitimate for strategy-scoped
    fills — remainder opens a lot, no anomaly row, unapplied_qty stays 0."""
    portfolio, strategy = await _scoped(db_session, seeded_user)
    await _apply(db_session, portfolio.id, strategy.id, side=OrderSide.buy, qty="100", price="100")

    result = await _apply(
        db_session,
        portfolio.id,
        strategy.id,
        side=OrderSide.sell,
        qty="150",
        price="110",
        at_offset_min=1,
    )

    assert result.qty_closed == Decimal("100")
    assert result.opened_lot_id is not None
    assert result.unapplied_qty == Decimal("0")
    anomalies = (
        (
            await db_session.execute(
                select(EventLog).where(EventLog.event_type == UNAPPLIED_REMAINDER_EVENT)
            )
        )
        .scalars()
        .all()
    )
    assert anomalies == []
