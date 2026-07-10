"""FlattenController: "flat and protected" as a level-triggered OUTCOME.

Every test drives ``_tick()`` directly with an injected broker clock/calendar —
no real sleeps, no wall-clock dependence. The properties under test are the 2c
design's confirmed criticals: the 15:55 window runs unconditionally, the
re-drive never cancels its own liquidations, kill-switch flattens verify their
outcome on the command row, next-open remediation catches whatever last session
failed to close, and overnight exposure raises an alarm exactly once.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, cast
from zoneinfo import ZoneInfo

from sqlalchemy.ext.asyncio import async_sessionmaker

from app.engine.bus import EventBus
from app.engine.events import FlattenEvent
from app.engine.flatten_controller import FlattenController
from app.engine.kill_switch import KillSwitch
from app.models.enums import EngineCommandAction, OrderClass, OrderSide, OrderStatus
from tests.engine.builders import (
    FakeEngineAdapter,
    make_broker_order,
    make_clock,
    seed_lot,
    seed_order,
    seed_portfolio,
)
from tests.engine.flatten_helpers import (
    FakeRedis,
    get_command_row,
    get_events,
    make_broker_position,
    make_calendar_day,
    seed_command,
)

if TYPE_CHECKING:
    import redis.asyncio as aioredis
    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

    from app.brokers.dto import BrokerOrder, BrokerPosition, CalendarDay, MarketClock
    from app.models.user import User

_ET = ZoneInfo("America/New_York")

# EDT normal session — Friday 2026-06-26: 9:30–16:00 ET = 13:30–20:00 UTC.
EDT_DATE = date(2026, 6, 26)
EDT_OPEN = datetime(2026, 6, 26, 13, 30, tzinfo=UTC)
EDT_CLOSE = datetime(2026, 6, 26, 20, 0, tzinfo=UTC)

# EST winter session — Thursday 2026-01-15: 9:30–16:00 ET = 14:30–21:00 UTC.
EST_DATE = date(2026, 1, 15)
EST_OPEN = datetime(2026, 1, 15, 14, 30, tzinfo=UTC)
EST_CLOSE = datetime(2026, 1, 15, 21, 0, tzinfo=UTC)

# EST half day — Friday 2026-11-27 (post-Thanksgiving): close 13:00 ET = 18:00 UTC.
HALF_DATE = date(2026, 11, 27)
HALF_OPEN = datetime(2026, 11, 27, 14, 30, tzinfo=UTC)
HALF_CLOSE = datetime(2026, 11, 27, 18, 0, tzinfo=UTC)


def _edt_day() -> CalendarDay:
    return make_calendar_day(EDT_DATE, EDT_OPEN, EDT_CLOSE)


class _ControllerAdapter(FakeEngineAdapter):
    """Configurable broker truth: clock, calendar, positions, open orders."""

    def __init__(self, *, clock: MarketClock, calendar: list[CalendarDay] | None = None) -> None:
        super().__init__(clock=clock)
        self.clock = clock
        self.calendar = list(calendar or [])
        self.positions: list[BrokerPosition] = []
        self.open_orders: list[BrokerOrder] = []
        self.calendar_calls: list[tuple[date, date]] = []

    async def get_clock(self) -> MarketClock:
        return self.clock

    async def get_calendar(self, start: date, end: date) -> list[CalendarDay]:
        self.calendar_calls.append((start, end))
        return [d for d in self.calendar if start <= d.trading_date <= end]

    async def list_positions(self) -> list[BrokerPosition]:
        return list(self.positions)

    async def list_orders(
        self,
        *,
        status: str = "open",
        after: datetime | None = None,
        until: datetime | None = None,
        limit: int = 100,
        nested: bool = True,
    ) -> list[BrokerOrder]:
        return list(self.open_orders)


@dataclass
class _Rig:
    controller: FlattenController
    adapter: _ControllerAdapter
    redis: FakeRedis
    bus: EventBus
    kill_switch: KillSwitch
    portfolio_id: uuid.UUID
    events: list[FlattenEvent] = field(default_factory=list)

    async def tick(self) -> float:
        """One controller tick, then drain the bus so captures land."""
        sleep_for = await self.controller._tick()
        await self.bus.drain()
        return sleep_for


def _rig(
    db_engine: AsyncEngine,
    adapter: _ControllerAdapter,
    *,
    portfolio_id: uuid.UUID | None = None,
    buffer_minutes: int = 5,
) -> _Rig:
    factory = async_sessionmaker(db_engine, expire_on_commit=False)
    bus = EventBus()
    events: list[FlattenEvent] = []

    async def capture(event: FlattenEvent) -> None:
        events.append(event)

    bus.subscribe(FlattenEvent, capture)
    redis = FakeRedis()
    kill_switch = KillSwitch(factory)
    pid = portfolio_id if portfolio_id is not None else uuid.uuid4()
    controller = FlattenController(
        bus=bus,
        adapter=adapter,
        session_factory=factory,
        redis=cast("aioredis.Redis", redis),
        portfolio_id=pid,
        kill_switch=kill_switch,
        boot_reconciled=asyncio.Event(),
        flatten_buffer=timedelta(minutes=buffer_minutes),
    )
    return _Rig(controller, adapter, redis, bus, kill_switch, pid, events)


# ── Session math + the watch window ──────────────────────────────────


async def test_normal_day_window_opens_at_close_minus_buffer(db_engine: AsyncEngine) -> None:
    adapter = _ControllerAdapter(
        clock=make_clock(now=EDT_CLOSE - timedelta(minutes=10), is_open=True),
        calendar=[_edt_day()],
    )
    rig = _rig(db_engine, adapter)
    adapter.positions = [make_broker_position()]
    rig.controller._next_open_snapshot = False  # exposure arose intra-session

    sleep_before = await rig.tick()  # 15:50 ET — before the window
    assert rig.events == []
    assert sleep_before > 0

    adapter.clock = make_clock(now=EDT_CLOSE - timedelta(minutes=4), is_open=True)
    sleep_inside = await rig.tick()  # 15:56 ET — inside [15:55, 16:00)
    assert [e.source for e in rig.events] == ["scheduled_close"]
    assert rig.events[0].portfolio_id == rig.portfolio_id
    assert rig.events[0].command_seq is None
    assert sleep_inside == 30.0  # the fast in-window cadence


async def test_half_day_close_moves_the_window_to_1255_et(db_engine: AsyncEngine) -> None:
    # EST winter half day: 13:00 ET close ⇒ the window opens 12:55 ET.
    adapter = _ControllerAdapter(
        clock=make_clock(now=HALF_CLOSE - timedelta(minutes=6), is_open=True),
        calendar=[make_calendar_day(HALF_DATE, HALF_OPEN, HALF_CLOSE)],
    )
    rig = _rig(db_engine, adapter)
    adapter.positions = [make_broker_position()]
    rig.controller._next_open_snapshot = False

    await rig.tick()  # 12:54 ET — one minute early
    assert rig.events == []

    adapter.clock = make_clock(now=HALF_CLOSE - timedelta(minutes=4), is_open=True)
    await rig.tick()  # 12:56 ET — inside the shortened window
    assert [e.source for e in rig.events] == ["scheduled_close"]


async def test_holiday_sleeps_toward_next_open_without_publishing(
    db_engine: AsyncEngine,
) -> None:
    next_open = datetime(2026, 7, 6, 13, 30, tzinfo=UTC)  # Monday after the 4th
    adapter = _ControllerAdapter(
        clock=make_clock(
            now=datetime(2026, 7, 3, 15, 0, tzinfo=UTC), is_open=False, next_open=next_open
        ),
        calendar=[],  # observed holiday: no session today
    )
    rig = _rig(db_engine, adapter)
    adapter.positions = [make_broker_position()]  # even exposed: nothing fires while closed

    sleep_for = await rig.tick()
    assert rig.events == []
    assert sleep_for == 900.0  # far-mode chunked sleep, re-anchored every wake
    assert adapter.calendar_calls == [(date(2026, 7, 3), date(2026, 7, 3))]


async def test_watch_window_does_not_refire_over_its_own_liquidations(
    db_engine: AsyncEngine,
) -> None:
    # Every exposed symbol already has a working roigen-flatten order: the
    # re-drive must WAIT for those fills, not cancel/re-fire (that loop would
    # prevent the flatten from ever completing).
    adapter = _ControllerAdapter(
        clock=make_clock(now=EDT_CLOSE - timedelta(minutes=4), is_open=True),
        calendar=[_edt_day()],
    )
    rig = _rig(db_engine, adapter)
    adapter.positions = [make_broker_position("AAPL")]
    adapter.open_orders = [
        make_broker_order(
            broker_order_id="bo-liq",
            client_order_id="roigen-flatten-abc123def456-aapl",
            symbol="AAPL",
            side=OrderSide.sell,
            order_class=OrderClass.simple,
        )
    ]

    sleep_for = await rig.tick()
    assert rig.events == []  # covered: no new FlattenEvent
    assert sleep_for == 30.0  # but the window keeps watching


async def test_watch_window_stays_quiet_when_flat(db_engine: AsyncEngine) -> None:
    adapter = _ControllerAdapter(
        clock=make_clock(now=EDT_CLOSE - timedelta(minutes=4), is_open=True),
        calendar=[_edt_day()],
    )
    rig = _rig(db_engine, adapter)

    await rig.tick()
    assert rig.events == []


async def test_pending_submit_rows_count_as_exposure(
    db_engine: AsyncEngine, db_session: AsyncSession, seeded_user: User
) -> None:
    # An order that may exist at the broker but has no broker id yet is
    # exposure the broker's own listing cannot show.
    portfolio = await seed_portfolio(db_session, seeded_user.id)
    await seed_order(
        db_session, portfolio.id, None, status=OrderStatus.pending_submit, symbol="AAPL"
    )
    await db_session.commit()

    adapter = _ControllerAdapter(
        clock=make_clock(now=EDT_CLOSE - timedelta(minutes=4), is_open=True),
        calendar=[_edt_day()],
    )
    rig = _rig(db_engine, adapter, portfolio_id=portfolio.id)
    rig.controller._next_open_snapshot = False

    await rig.tick()  # broker says flat; the local pending row says otherwise
    assert [e.source for e in rig.events] == ["scheduled_close"]


# ── Kill-switch flattens ─────────────────────────────────────────────


async def test_kill_flatten_drives_while_the_market_is_open(
    db_engine: AsyncEngine, db_session: AsyncSession
) -> None:
    row = await seed_command(db_session, EngineCommandAction.flatten, reason="ops kill")
    await db_session.commit()

    adapter = _ControllerAdapter(
        clock=make_clock(now=datetime(2026, 1, 15, 16, 0, tzinfo=UTC), is_open=True),
        calendar=[make_calendar_day(EST_DATE, EST_OPEN, EST_CLOSE)],
    )
    rig = _rig(db_engine, adapter)
    adapter.positions = [make_broker_position()]
    await rig.kill_switch.load()
    assert rig.kill_switch.is_flattening is True

    sleep_for = await rig.tick()  # 11:00 ET — mid-session, far from any window
    assert [e.source for e in rig.events] == ["kill_switch"]
    assert rig.events[0].command_seq == row.seq
    assert rig.events[0].reason == "ops kill"
    assert sleep_for == 30.0  # actively driving: fast cadence


async def test_kill_flatten_holds_while_the_market_is_closed(
    db_engine: AsyncEngine, db_session: AsyncSession
) -> None:
    row = await seed_command(db_session, EngineCommandAction.flatten)
    await db_session.commit()

    adapter = _ControllerAdapter(
        clock=make_clock(
            now=datetime(2026, 6, 27, 15, 0, tzinfo=UTC),  # Saturday
            is_open=False,
            next_open=datetime(2026, 6, 29, 13, 30, tzinfo=UTC),
        ),
        calendar=[],
    )
    rig = _rig(db_engine, adapter)
    adapter.positions = [make_broker_position()]
    await rig.kill_switch.load()

    sleep_for = await rig.tick()

    # No after-hours market orders — the STATE holds and drives at next open.
    assert rig.events == []
    assert rig.kill_switch.is_flattening is True
    assert (await get_command_row(db_engine, row.seq)).result is None
    assert sleep_for > 0


async def test_kill_flatten_verifies_outcome_when_broker_is_flat(
    db_engine: AsyncEngine, db_session: AsyncSession
) -> None:
    row = await seed_command(db_session, EngineCommandAction.flatten)
    await db_session.commit()

    adapter = _ControllerAdapter(
        clock=make_clock(
            now=datetime(2026, 6, 27, 15, 0, tzinfo=UTC),
            is_open=False,
            next_open=datetime(2026, 6, 29, 13, 30, tzinfo=UTC),
        ),
        calendar=[],
    )
    rig = _rig(db_engine, adapter)  # broker truth: no positions, no orders
    await rig.kill_switch.load()

    await rig.tick()

    assert rig.events == []  # nothing to drive
    assert (await get_command_row(db_engine, row.seq)).result == "flat_verified"
    assert rig.kill_switch.is_flattening is False
    completed = await get_events(db_engine, "flatten.completed")
    assert len(completed) == 1
    assert completed[0].payload["command_seq"] == row.seq

    await rig.tick()  # completion is audited once, not per tick
    assert len(await get_events(db_engine, "flatten.completed")) == 1


# ── The next-open remediation rule ───────────────────────────────────


async def test_pre_open_exposure_snapshot_fires_next_open_flatten_once(
    db_engine: AsyncEngine,
) -> None:
    adapter = _ControllerAdapter(
        clock=make_clock(
            now=datetime(2026, 6, 26, 12, 0, tzinfo=UTC), is_open=False, next_open=EDT_OPEN
        ),
        calendar=[_edt_day()],
    )
    rig = _rig(db_engine, adapter)
    adapter.positions = [make_broker_position("AAPL")]

    await rig.tick()  # 08:00 ET pre-open: snapshot the leftover exposure
    assert rig.events == []
    assert rig.controller._next_open_snapshot is True

    adapter.clock = make_clock(now=EDT_OPEN + timedelta(minutes=30), is_open=True)
    await rig.tick()  # first in-session tick remediates
    await rig.tick()  # and never again this session
    assert [e.source for e in rig.events] == ["next_open"]
    assert "predating" in rig.events[0].reason


async def test_clean_pre_open_snapshot_never_fires_next_open(db_engine: AsyncEngine) -> None:
    adapter = _ControllerAdapter(
        clock=make_clock(
            now=datetime(2026, 6, 26, 12, 0, tzinfo=UTC), is_open=False, next_open=EDT_OPEN
        ),
        calendar=[_edt_day()],
    )
    rig = _rig(db_engine, adapter)

    await rig.tick()  # pre-open: flat
    assert rig.controller._next_open_snapshot is False

    # Exposure appears IN session — that's a legitimate entry, not a leftover.
    adapter.positions = [make_broker_position("AAPL")]
    adapter.clock = make_clock(now=EDT_OPEN + timedelta(minutes=30), is_open=True)
    await rig.tick()
    await rig.tick()
    assert rig.events == []


async def test_mid_session_boot_flattens_exposure_with_stale_lot_evidence(
    db_engine: AsyncEngine, db_session: AsyncSession, seeded_user: User
) -> None:
    # Booted mid-session (no pre-open observation): a lot opened BEFORE
    # today's open proves the position predates the session — day-TIF
    # protection is dead, so it must not ride.
    portfolio = await seed_portfolio(db_session, seeded_user.id)
    await seed_lot(
        db_session,
        portfolio.id,
        None,
        symbol="AAPL",
        qty_orig=Decimal("5"),
        qty_open=Decimal("5"),
        opened_at=datetime(2026, 6, 25, 18, 0, tzinfo=UTC),  # yesterday
    )
    await db_session.commit()

    adapter = _ControllerAdapter(
        clock=make_clock(now=EDT_OPEN + timedelta(minutes=30), is_open=True),
        calendar=[_edt_day()],
    )
    rig = _rig(db_engine, adapter, portfolio_id=portfolio.id)
    adapter.positions = [make_broker_position("AAPL", qty=Decimal("5"))]

    await rig.tick()
    assert [e.source for e in rig.events] == ["next_open"]


async def test_mid_session_boot_leaves_fresh_intra_session_lots_alone(
    db_engine: AsyncEngine, db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio = await seed_portfolio(db_session, seeded_user.id)
    await seed_lot(
        db_session,
        portfolio.id,
        None,
        symbol="AAPL",
        qty_orig=Decimal("5"),
        qty_open=Decimal("5"),
        opened_at=EDT_OPEN + timedelta(minutes=15),  # after today's open
    )
    await db_session.commit()

    adapter = _ControllerAdapter(
        clock=make_clock(now=EDT_OPEN + timedelta(hours=1), is_open=True),
        calendar=[_edt_day()],
    )
    rig = _rig(db_engine, adapter, portfolio_id=portfolio.id)
    adapter.positions = [make_broker_position("AAPL", qty=Decimal("5"))]

    await rig.tick()
    assert rig.events == []


async def test_position_with_no_lot_evidence_fails_toward_flat(db_engine: AsyncEngine) -> None:
    # An unexplained position is exactly what must not ride unprotected.
    adapter = _ControllerAdapter(
        clock=make_clock(now=EDT_OPEN + timedelta(minutes=30), is_open=True),
        calendar=[_edt_day()],
    )
    rig = _rig(db_engine, adapter)
    adapter.positions = [make_broker_position("AAPL")]

    await rig.tick()
    assert [e.source for e in rig.events] == ["next_open"]


# ── Post-close check + day rollover ──────────────────────────────────


async def test_overnight_exposure_alarms_exactly_once_per_date(db_engine: AsyncEngine) -> None:
    adapter = _ControllerAdapter(
        clock=make_clock(
            now=EDT_CLOSE + timedelta(minutes=30),
            is_open=False,
            next_open=datetime(2026, 6, 29, 13, 30, tzinfo=UTC),
        ),
        calendar=[_edt_day()],
    )
    rig = _rig(db_engine, adapter)
    adapter.positions = [make_broker_position("AAPL")]

    sleep_for = await rig.tick()
    assert rig.events == []  # no after-hours orders — an ALARM, not a flatten
    assert sleep_for > 0
    alarms = await get_events(db_engine, "flatten.overnight_exposure")
    assert len(alarms) == 1
    assert alarms[0].payload["trading_date"] == str(EDT_DATE)
    assert len(rig.redis.published) == 1
    channel, payload = rig.redis.published[0]
    assert channel == "engine:alerts"
    assert json.loads(payload)["kind"] == "overnight_exposure"

    adapter.clock = make_clock(
        now=EDT_CLOSE + timedelta(minutes=35),
        is_open=False,
        next_open=datetime(2026, 6, 29, 13, 30, tzinfo=UTC),
    )
    await rig.tick()  # same date: the alarm never repeats
    assert len(await get_events(db_engine, "flatten.overnight_exposure")) == 1
    assert len(rig.redis.published) == 1


async def test_flat_post_close_is_silent(db_engine: AsyncEngine) -> None:
    adapter = _ControllerAdapter(
        clock=make_clock(
            now=EDT_CLOSE + timedelta(minutes=30),
            is_open=False,
            next_open=datetime(2026, 6, 29, 13, 30, tzinfo=UTC),
        ),
        calendar=[_edt_day()],
    )
    rig = _rig(db_engine, adapter)

    await rig.tick()
    assert rig.events == []
    assert await get_events(db_engine, "flatten.overnight_exposure") == []
    assert rig.redis.published == []


async def test_day_rollover_sleeps_without_spinning_the_calendar(
    db_engine: AsyncEngine,
) -> None:
    adapter = _ControllerAdapter(
        clock=make_clock(
            now=EDT_CLOSE + timedelta(minutes=30),
            is_open=False,
            next_open=datetime(2026, 6, 29, 13, 30, tzinfo=UTC),
        ),
        calendar=[_edt_day()],
    )
    rig = _rig(db_engine, adapter)

    first_sleep = await rig.tick()
    adapter.clock = make_clock(
        now=EDT_CLOSE + timedelta(minutes=35),
        is_open=False,
        next_open=datetime(2026, 6, 29, 13, 30, tzinfo=UTC),
    )
    second_sleep = await rig.tick()

    assert first_sleep > 0
    assert second_sleep > 0  # a rollover tick always sleeps — never spins
    assert len(adapter.calendar_calls) == 1  # session resolved once per date


# ── Broker-time anchoring ────────────────────────────────────────────


async def test_tick_follows_the_broker_clock_not_wall_time(db_engine: AsyncEngine) -> None:
    # The broker clock runs 6 minutes AHEAD of this machine. With a 5-minute
    # buffer, broker-now is inside the watch window while wall-now is still
    # before it — a publish proves the controller anchors on the CLOCK.
    wall_now = datetime.now(UTC)
    clock_now = wall_now + timedelta(minutes=6)
    trading_date = clock_now.astimezone(_ET).date()
    day = make_calendar_day(
        trading_date,
        rth_open=clock_now - timedelta(hours=2),
        rth_close=clock_now + timedelta(minutes=2),  # flatten_at = clock_now − 3m
    )
    adapter = _ControllerAdapter(clock=make_clock(now=clock_now, is_open=True), calendar=[day])
    rig = _rig(db_engine, adapter)
    adapter.positions = [make_broker_position()]
    rig.controller._next_open_snapshot = False

    await rig.tick()
    assert [e.source for e in rig.events] == ["scheduled_close"]
