"""load_active_strategies — the lifecycle gate (iron law #8) and bad-row survival."""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.engine.bus import EventBus
from app.engine.events import SignalEvent
from app.engine.loader import load_active_strategies
from app.engine.strategies import ProbeStrategy
from app.models.enums import PortfolioMode, StrategyStatus
from tests.engine.builders import (
    DEFAULT_NOW,
    make_bar,
    seed_order,
    seed_portfolio,
    seed_strategy,
)

if TYPE_CHECKING:
    import uuid

    from sqlalchemy.ext.asyncio import AsyncEngine

    from app.models.user import User


async def _seed_all_statuses(
    db_session: AsyncSession, portfolio_id: uuid.UUID
) -> dict[StrategyStatus, uuid.UUID]:
    """One probe row per lifecycle status; returns status → row id."""
    rows: dict[StrategyStatus, uuid.UUID] = {}
    for status in StrategyStatus:
        row = await seed_strategy(
            db_session, portfolio_id, name=f"probe-{status}", kind="probe", status=status
        )
        rows[status] = row.id
    await db_session.commit()
    return rows


# ── Lifecycle gate matrix ────────────────────────────────────────────


async def test_paper_portfolio_loads_paper_and_live_only(
    db_engine: AsyncEngine, db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio = await seed_portfolio(db_session, seeded_user.id, mode=PortfolioMode.paper)
    rows = await _seed_all_statuses(db_session, portfolio.id)

    loaded = await load_active_strategies(
        db_session,
        portfolio,
        bus=EventBus(),
        session_factory=async_sessionmaker(db_engine, expire_on_commit=False),
    )

    assert {s.strategy_id for s in loaded} == {
        rows[StrategyStatus.paper],
        rows[StrategyStatus.live],
    }


async def test_live_portfolio_loads_live_only(
    db_engine: AsyncEngine, db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio = await seed_portfolio(db_session, seeded_user.id, mode=PortfolioMode.live)
    rows = await _seed_all_statuses(db_session, portfolio.id)

    loaded = await load_active_strategies(
        db_session,
        portfolio,
        bus=EventBus(),
        session_factory=async_sessionmaker(db_engine, expire_on_commit=False),
    )

    # Paper-only rows never trade a live portfolio; draft/backtesting/paused/
    # stopped never load anywhere.
    assert {s.strategy_id for s in loaded} == {rows[StrategyStatus.live]}


# ── Bad rows: engine survives, gap is visible ────────────────────────


async def test_unknown_kind_is_skipped_and_engine_survives(
    db_engine: AsyncEngine, db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio = await seed_portfolio(db_session, seeded_user.id)
    await seed_strategy(db_session, portfolio.id, name="mystery", kind="does-not-exist")
    good = await seed_strategy(db_session, portfolio.id, name="probe", kind="probe")
    await db_session.commit()

    loaded = await load_active_strategies(
        db_session,
        portfolio,
        bus=EventBus(),
        session_factory=async_sessionmaker(db_engine, expire_on_commit=False),
    )

    assert [s.strategy_id for s in loaded] == [good.id]


async def test_unparseable_params_row_is_skipped_and_engine_survives(
    db_engine: AsyncEngine, db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio = await seed_portfolio(db_session, seeded_user.id)
    await seed_strategy(
        db_session, portfolio.id, name="broken", kind="probe", params={"stop_pct": "garbage"}
    )
    good = await seed_strategy(db_session, portfolio.id, name="probe", kind="probe")
    await db_session.commit()

    loaded = await load_active_strategies(
        db_session,
        portfolio,
        bus=EventBus(),
        session_factory=async_sessionmaker(db_engine, expire_on_commit=False),
    )

    assert [s.strategy_id for s in loaded] == [good.id]


# ── Construction wiring ──────────────────────────────────────────────


async def test_probe_gets_row_params_and_a_working_session_factory(
    db_engine: AsyncEngine, db_session: AsyncSession, seeded_user: User
) -> None:
    """The loader forwards JSONB params AND the real session factory: a probe
    loaded after an entry order exists today must stay out of the market."""
    portfolio = await seed_portfolio(db_session, seeded_user.id)
    row = await seed_strategy(
        db_session,
        portfolio.id,
        kind="probe",
        symbols=("SPY", "QQQ"),
        params={"stop_pct": 0.007},
    )
    await seed_order(db_session, portfolio.id, row.id, symbol="SPY", created_at=DEFAULT_NOW)
    await db_session.commit()

    factory = async_sessionmaker(db_engine, expire_on_commit=False)
    bus = EventBus()
    signals: list[SignalEvent] = []

    async def handler(event: SignalEvent) -> None:
        signals.append(event)

    bus.subscribe(SignalEvent, handler)

    loaded = await load_active_strategies(db_session, portfolio, bus=bus, session_factory=factory)

    assert len(loaded) == 1
    probe = loaded[0]
    assert isinstance(probe, ProbeStrategy)
    assert probe.portfolio_id == portfolio.id
    assert probe.symbols == ("SPY", "QQQ")
    assert probe.stop_pct == Decimal("0.007")  # JSONB float → exact Decimal

    await probe.on_bar(make_bar("SPY"))  # today's slot already consumed in the DB
    await probe.on_bar(make_bar("QQQ"))  # untouched symbol still enters
    await bus.drain()

    assert [s.symbol for s in signals] == ["QQQ"]
