"""ProbeStrategy — RTH filter, ET-day dedup, params, and the DB session bound."""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.brokers.pricing import quantize_price
from app.engine.bus import EventBus
from app.engine.events import SignalEvent
from app.engine.strategies import ProbeStrategy
from app.models.enums import OrderSide, OrderType, TimeInForce
from tests.engine.builders import (
    DEFAULT_NOW,
    make_bar,
    seed_order,
    seed_portfolio,
    seed_strategy,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

    from app.models.user import User

# DEFAULT_NOW is FRIDAY 2026-06-26 15:00 UTC = 11:00 ET (EDT), mid-RTH. The
# probe drops non-RTH bars outright (weekends, pre/post market), so "the next
# ET trading day" is MONDAY 06-29 at the same wall clock — +1 day would be a
# Saturday bar the RTH filter (correctly) swallows.
NEXT_ET_DAY = DEFAULT_NOW + timedelta(days=3)  # Monday 2026-06-29, 11:00 ET


def _probe(
    bus: EventBus,
    *,
    params: dict[str, Any] | None = None,
    session_factory: async_sessionmaker[AsyncSession] | None = None,
    strategy_id: uuid.UUID | None = None,
    portfolio_id: uuid.UUID | None = None,
    symbols: tuple[str, ...] = ("SPY",),
) -> ProbeStrategy:
    return ProbeStrategy(
        strategy_id=strategy_id if strategy_id is not None else uuid.uuid4(),
        portfolio_id=portfolio_id if portfolio_id is not None else uuid.uuid4(),
        symbols=symbols,
        bus=bus,
        params=params,
        session_factory=session_factory,
    )


def _capture(bus: EventBus) -> list[SignalEvent]:
    signals: list[SignalEvent] = []

    async def handler(event: SignalEvent) -> None:
        signals.append(event)

    bus.subscribe(SignalEvent, handler)
    return signals


# ── Signal shape ─────────────────────────────────────────────────────


async def test_probe_emits_protected_buy_with_default_params() -> None:
    bus = EventBus()
    signals = _capture(bus)
    probe = _probe(bus)

    await probe.on_bar(make_bar("SPY", close=Decimal("100")))
    await bus.drain()

    assert len(signals) == 1
    signal = signals[0]
    assert signal.symbol == "SPY"
    assert signal.side is OrderSide.buy
    assert signal.entry_price == Decimal("100")
    assert signal.stop_price == Decimal("99.50")
    assert signal.take_profit_price == Decimal("101.00")
    assert signal.stop_price < signal.entry_price  # protection below entry
    assert signal.take_profit_price is not None
    assert signal.take_profit_price > signal.entry_price
    assert signal.order_type is OrderType.market
    assert signal.time_in_force is TimeInForce.day
    assert signal.extended_hours is False


async def test_probe_ignores_symbols_it_does_not_trade() -> None:
    bus = EventBus()
    signals = _capture(bus)
    probe = _probe(bus, symbols=("SPY",))

    await probe.on_bar(make_bar("QQQ"))
    await bus.drain()

    assert signals == []


# ── ET-day dedup ─────────────────────────────────────────────────────


async def test_probe_emits_once_per_et_day() -> None:
    bus = EventBus()
    signals = _capture(bus)
    probe = _probe(bus)

    await probe.on_bar(make_bar("SPY"))
    await probe.on_bar(make_bar("SPY", ts=DEFAULT_NOW + timedelta(minutes=5)))
    await bus.drain()

    assert len(signals) == 1


async def test_probe_next_et_day_emits_again() -> None:
    bus = EventBus()
    signals = _capture(bus)
    probe = _probe(bus)

    await probe.on_bar(make_bar("SPY"))
    await probe.on_bar(make_bar("SPY", ts=NEXT_ET_DAY))
    await bus.drain()

    assert len(signals) == 2


async def test_probe_dedup_key_is_the_et_calendar_date() -> None:
    """The dedup key is the ET trading DATE, not a rolling window: a Monday
    late-afternoon bar and Tuesday's open bar are <18h apart yet distinct
    sessions — an elapsed-time key would swallow the second entry. The
    day-state's stored key is pinned to the ET date directly (a UTC-date key
    is what the RTH filter now makes unreachable to distinguish by behavior:
    inside RTH the two calendars always agree)."""
    bus = EventBus()
    signals = _capture(bus)
    probe = _probe(bus)

    monday_afternoon = datetime(2026, 6, 29, 19, 50, tzinfo=UTC)  # 15:50 ET Mon
    tuesday_open = datetime(2026, 6, 30, 13, 31, tzinfo=UTC)  # 09:31 ET Tue

    await probe.on_bar(make_bar("SPY", ts=monday_afternoon))
    await probe.on_bar(make_bar("SPY", ts=monday_afternoon + timedelta(minutes=5)))
    await bus.drain()
    assert len(signals) == 1  # same ET date: one entry, however many bars
    assert probe._day_state["SPY"].et_date == date(2026, 6, 29)

    await probe.on_bar(make_bar("SPY", ts=tuesday_open))  # new ET date, <18h later
    await bus.drain()
    assert len(signals) == 2
    assert probe._day_state["SPY"].et_date == date(2026, 6, 30)


async def test_probe_non_rth_bar_emits_nothing_and_keeps_the_entry_slot() -> None:
    """IEX prints extended-hours bars: a pre-market bar must be dropped BEFORE
    day-state exists — emitting would be risk-rejected, and counting it would
    burn the day's only entry slot (review finding)."""
    bus = EventBus()
    signals = _capture(bus)
    probe = _probe(bus)

    premarket = datetime(2026, 6, 26, 12, 0, tzinfo=UTC)  # 08:00 ET Friday
    await probe.on_bar(make_bar("SPY", ts=premarket))
    await bus.drain()
    assert signals == []
    assert probe._day_state == {}  # the slot was not consumed

    await probe.on_bar(make_bar("SPY", ts=DEFAULT_NOW))  # 11:00 ET, same ET date
    await bus.drain()
    assert len(signals) == 1  # the RTH bar still gets the day's entry


async def test_probe_tracks_days_per_symbol() -> None:
    bus = EventBus()
    signals = _capture(bus)
    probe = _probe(bus, symbols=("SPY", "QQQ"))

    await probe.on_bar(make_bar("SPY"))
    await probe.on_bar(make_bar("QQQ"))
    await bus.drain()

    assert sorted(s.symbol for s in signals) == ["QQQ", "SPY"]


# ── Param parsing (JSONB floats → exact Decimals) ────────────────────


async def test_probe_parses_jsonb_float_params_to_exact_decimals() -> None:
    bus = EventBus()
    signals = _capture(bus)
    # JSONB numbers arrive as Python floats; parsing must not mint artifacts.
    probe = _probe(bus, params={"stop_pct": 0.005, "take_profit_pct": 0.01})

    assert probe.stop_pct == Decimal("0.005")
    assert str(probe.stop_pct) == "0.005"  # Decimal(0.005) would fail both
    assert probe.take_profit_pct == Decimal("0.01")

    await probe.on_bar(make_bar("SPY", close=Decimal("333.33")))
    await bus.drain()

    signal = signals[0]
    assert signal.stop_price == quantize_price(Decimal("333.33") * (1 - Decimal("0.005")))
    assert signal.take_profit_price == quantize_price(Decimal("333.33") * (1 + Decimal("0.01")))


@pytest.mark.parametrize(
    ("params", "expected_tp"),
    [
        ({}, Decimal("101.00")),  # absent → default 1%
        ({"take_profit_pct": None}, None),  # explicit null → no TP leg
        ({"take_profit_pct": 0}, None),  # explicit 0 → no TP leg
    ],
)
async def test_probe_take_profit_absent_defaults_falsy_disables(
    params: dict[str, Any], expected_tp: Decimal | None
) -> None:
    bus = EventBus()
    signals = _capture(bus)
    probe = _probe(bus, params=params)

    await probe.on_bar(make_bar("SPY", close=Decimal("100")))
    await bus.drain()

    assert signals[0].take_profit_price == expected_tp


async def test_probe_max_entries_per_session_bounds_the_day() -> None:
    bus = EventBus()
    signals = _capture(bus)
    probe = _probe(bus, params={"max_entries_per_session": 2})

    for minutes in (0, 5, 10):  # third bar must not produce a third entry
        await probe.on_bar(make_bar("SPY", ts=DEFAULT_NOW + timedelta(minutes=minutes)))
    await bus.drain()

    assert len(signals) == 2


@pytest.mark.parametrize(
    "params",
    [
        {"stop_pct": 0},
        {"stop_pct": -0.01},
        {"take_profit_pct": -0.01},
        {"max_entries_per_session": 0},
    ],
)
async def test_probe_rejects_nonsense_params(params: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match="probe"):
        _probe(EventBus(), params=params)


# ── DB-derived session bound ─────────────────────────────────────────


async def test_probe_db_bound_blocks_reentry_after_restart(
    db_engine: AsyncEngine, db_session: AsyncSession, seeded_user: User
) -> None:
    """A fresh instance (= restarted engine) sees today's entry Order and stays out."""
    portfolio = await seed_portfolio(db_session, seeded_user.id)
    row = await seed_strategy(db_session, portfolio.id, kind="probe")
    await seed_order(db_session, portfolio.id, row.id, created_at=DEFAULT_NOW)
    await db_session.commit()

    bus = EventBus()
    signals = _capture(bus)
    probe = _probe(
        bus,
        strategy_id=row.id,
        portfolio_id=portfolio.id,
        session_factory=async_sessionmaker(db_engine, expire_on_commit=False),
    )
    await probe.on_bar(make_bar("SPY"))
    await bus.drain()

    assert signals == []


async def test_probe_db_bound_ignores_prior_day_and_bracket_leg_rows(
    db_engine: AsyncEngine, db_session: AsyncSession, seeded_user: User
) -> None:
    """Yesterday's parent and today's bracket LEG must not consume today's slot."""
    portfolio = await seed_portfolio(db_session, seeded_user.id)
    row = await seed_strategy(db_session, portfolio.id, kind="probe")
    parent = await seed_order(
        db_session, portfolio.id, row.id, created_at=DEFAULT_NOW - timedelta(days=1)
    )
    # Legs inherit strategy_id from the parent; only parent rows are entries.
    await seed_order(
        db_session,
        portfolio.id,
        row.id,
        side=OrderSide.sell,
        parent_order_id=parent.id,
        created_at=DEFAULT_NOW,
    )
    await db_session.commit()

    bus = EventBus()
    signals = _capture(bus)
    probe = _probe(
        bus,
        strategy_id=row.id,
        portfolio_id=portfolio.id,
        session_factory=async_sessionmaker(db_engine, expire_on_commit=False),
    )
    await probe.on_bar(make_bar("SPY"))
    await bus.drain()

    assert len(signals) == 1


class _CountingSessionmaker(async_sessionmaker[AsyncSession]):
    """Counts how many sessions the probe opens (= how many count queries run)."""

    def __init__(self, engine: AsyncEngine) -> None:
        super().__init__(engine, expire_on_commit=False)
        self.calls = 0

    def __call__(self, **local_kw: Any) -> AsyncSession:
        self.calls += 1
        return super().__call__(**local_kw)


async def test_probe_db_bound_queries_once_per_symbol_per_day(db_engine: AsyncEngine) -> None:
    factory = _CountingSessionmaker(db_engine)
    bus = EventBus()
    signals = _capture(bus)
    probe = _probe(bus, session_factory=factory)

    await probe.on_bar(make_bar("SPY"))
    await probe.on_bar(make_bar("SPY", ts=DEFAULT_NOW + timedelta(minutes=5)))
    await bus.drain()
    assert factory.calls == 1  # cached for the day, not re-read per bar
    assert len(signals) == 1

    await probe.on_bar(make_bar("SPY", ts=NEXT_ET_DAY))
    await bus.drain()
    assert factory.calls == 2  # day rollover re-seeds from the DB
    assert len(signals) == 2
