"""RiskStateProvider: control inputs loaded from broker + DB, ET day boundaries."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

from app.engine.risk.state import RiskStateProvider
from app.models.enums import StrategyStatus
from tests.engine.builders import (
    DEFAULT_NOW,
    FakeEngineAdapter,
    make_account,
    make_clock,
    seed_equity_snapshot,
    seed_lot,
    seed_portfolio,
    seed_strategy,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from app.models.user import User

provider = RiskStateProvider()


async def test_loads_strategy_config(db_session: AsyncSession, seeded_user: User) -> None:
    portfolio = await seed_portfolio(db_session, seeded_user.id)
    strategy = await seed_strategy(
        db_session,
        portfolio.id,
        status=StrategyStatus.paper,
        risk_per_trade_pct=Decimal("0.5"),
        max_positions=3,
    )
    state = await provider.load(
        db_session,
        FakeEngineAdapter(),
        portfolio_id=portfolio.id,
        strategy_id=strategy.id,
        symbol="SPY",
    )
    assert state.strategy_proven is False
    assert state.strategy_risk_pct == Decimal("0.5")
    assert state.strategy_max_positions == 3


async def test_live_strategy_is_proven(db_session: AsyncSession, seeded_user: User) -> None:
    portfolio = await seed_portfolio(db_session, seeded_user.id)
    strategy = await seed_strategy(db_session, portfolio.id, status=StrategyStatus.live)
    state = await provider.load(
        db_session,
        FakeEngineAdapter(),
        portfolio_id=portfolio.id,
        strategy_id=strategy.id,
        symbol="SPY",
    )
    assert state.strategy_proven is True


async def test_account_and_clock_snapshot_flow_through(
    db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio = await seed_portfolio(db_session, seeded_user.id)
    strategy = await seed_strategy(db_session, portfolio.id)
    adapter = FakeEngineAdapter(
        account=make_account(
            equity=Decimal("123456"),
            buying_power=Decimal("200000"),
            position_market_value=Decimal("5000"),
            trading_blocked=True,
        ),
        clock=make_clock(is_open=False),
    )
    state = await provider.load(
        db_session,
        adapter,
        portfolio_id=portfolio.id,
        strategy_id=strategy.id,
        symbol="SPY",
        trading_halted=True,
    )
    assert state.equity == Decimal("123456")
    assert state.buying_power == Decimal("200000")
    assert state.position_market_value == Decimal("5000")
    assert state.trading_blocked is True
    assert state.market_open is False
    assert state.now == DEFAULT_NOW
    assert state.trading_halted is True


async def test_day_realized_pnl_respects_et_day_boundary(
    db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio = await seed_portfolio(db_session, seeded_user.id)
    strategy = await seed_strategy(db_session, portfolio.id)
    # Closed today (11:00 ET) — counts.
    await seed_lot(
        db_session, portfolio.id, strategy.id, closed_at=DEFAULT_NOW, realized_pnl=Decimal("-50")
    )
    # Closed yesterday evening ET (2026-06-25 19:00 ET = 23:00 UTC) — excluded.
    yesterday = datetime(2026, 6, 25, 23, 0, tzinfo=UTC)
    await seed_lot(
        db_session, portfolio.id, strategy.id, closed_at=yesterday, realized_pnl=Decimal("-1000")
    )
    # A manual (no-strategy) lot closed today — must NOT count in the strategy total.
    await seed_lot(
        db_session, portfolio.id, None, closed_at=DEFAULT_NOW, realized_pnl=Decimal("-20")
    )

    state = await provider.load(
        db_session,
        FakeEngineAdapter(),
        portfolio_id=portfolio.id,
        strategy_id=strategy.id,
        symbol="SPY",
    )
    # Only today's strategy lot counts: yesterday excluded by ET boundary, the
    # manual lot excluded by strategy scope.
    assert state.day_realized_pnl_strategy == Decimal("-50")


async def test_consecutive_losses_counts_leading_negatives(
    db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio = await seed_portfolio(db_session, seeded_user.id)
    strategy = await seed_strategy(db_session, portfolio.id)
    base = DEFAULT_NOW
    await seed_lot(
        db_session,
        portfolio.id,
        strategy.id,
        closed_at=base - timedelta(hours=3),
        realized_pnl=Decimal("5"),
    )
    await seed_lot(
        db_session,
        portfolio.id,
        strategy.id,
        closed_at=base - timedelta(hours=2),
        realized_pnl=Decimal("-1"),
    )
    await seed_lot(
        db_session,
        portfolio.id,
        strategy.id,
        closed_at=base - timedelta(hours=1),
        realized_pnl=Decimal("-2"),
    )
    await seed_lot(
        db_session, portfolio.id, strategy.id, closed_at=base, realized_pnl=Decimal("-3")
    )

    state = await provider.load(
        db_session,
        FakeEngineAdapter(),
        portfolio_id=portfolio.id,
        strategy_id=strategy.id,
        symbol="SPY",
    )
    # Three most-recent closes are losses; the streak stops at the older win.
    assert state.consecutive_losses == 3


async def test_peak_equity_is_max_of_snapshots_and_current(
    db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio = await seed_portfolio(db_session, seeded_user.id)
    strategy = await seed_strategy(db_session, portfolio.id)
    for equity in (Decimal("100000"), Decimal("120000"), Decimal("90000")):
        await seed_equity_snapshot(db_session, portfolio.id, equity=equity)

    adapter = FakeEngineAdapter(account=make_account(equity=Decimal("95000")))
    state = await provider.load(
        db_session,
        adapter,
        portfolio_id=portfolio.id,
        strategy_id=strategy.id,
        symbol="SPY",
    )
    assert state.peak_equity == Decimal("120000")
    assert state.drawdown_pct > Decimal("20")  # (120k − 95k) / 120k ≈ 20.8%


async def test_strategy_open_qty_and_count(db_session: AsyncSession, seeded_user: User) -> None:
    portfolio = await seed_portfolio(db_session, seeded_user.id)
    strategy = await seed_strategy(db_session, portfolio.id)
    # Two open SPY lots sum; QQQ open; IWM fully closed (qty_open 0).
    await seed_lot(db_session, portfolio.id, strategy.id, symbol="SPY", qty_open=Decimal("5"))
    await seed_lot(db_session, portfolio.id, strategy.id, symbol="SPY", qty_open=Decimal("2"))
    await seed_lot(db_session, portfolio.id, strategy.id, symbol="QQQ", qty_open=Decimal("3"))
    await seed_lot(db_session, portfolio.id, strategy.id, symbol="IWM", qty_open=Decimal("0"))

    state = await provider.load(
        db_session,
        FakeEngineAdapter(),
        portfolio_id=portfolio.id,
        strategy_id=strategy.id,
        symbol="SPY",
    )
    assert state.strategy_open_qty == Decimal("7")  # 5 + 2 open SPY lots
    assert state.open_positions_count == 2  # SPY + QQQ have open lots; IWM is closed


async def test_last_entry_at_is_most_recent_open(
    db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio = await seed_portfolio(db_session, seeded_user.id)
    strategy = await seed_strategy(db_session, portfolio.id)
    newer = DEFAULT_NOW - timedelta(hours=1)
    await seed_lot(
        db_session,
        portfolio.id,
        strategy.id,
        symbol="SPY",
        opened_at=DEFAULT_NOW - timedelta(hours=2),
    )
    await seed_lot(db_session, portfolio.id, strategy.id, symbol="SPY", opened_at=newer)

    state = await provider.load(
        db_session,
        FakeEngineAdapter(),
        portfolio_id=portfolio.id,
        strategy_id=strategy.id,
        symbol="SPY",
    )
    assert state.last_entry_at == newer
