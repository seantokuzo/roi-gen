"""FlattenController — owns "flat and protected" as an OUTCOME, not an event.

The 2c design review's central finding, five lenses converging: a flatten that
is fired-and-forgotten (an event on an in-memory queue, bookkept at dispatch
time) can be lost to a crash, a dropped Redis poke, a failed close, or a LULD
halt — every one of which strips protection exactly when the operator believes
they are flat. So flatten here is **level-triggered from durable state**:

- The kill switch's ``flatten`` is a *state* in ``engine_commands`` (Postgres).
  While the latest command is a flatten AND broker exposure exists, this
  controller keeps driving — a lost event costs one tick, never the intent.
- The 15:55 window (``rth_close − flatten_buffer_minutes``) runs UNCONDITIONALLY
  every session, re-driving until flat or the bell.
- The next-open rule remediates whatever last session failed to close: any
  exposure predating today's session (or with no lot evidence at all) is
  flattened at the open — a standing per-session rule, not a boot-only one,
  because bracket legs are day-TIF and an overnight position has NO stop.

Broker truth decides everything: what counts as exposure, whether we are flat,
whether a working liquidation already covers a symbol. The local ``Position``
table lags the fill stream and never picks an action (it would both miss
exposure and re-close closed symbols). The one local read is ``pending_submit``
order rows — orders that may exist at the broker but have no broker id yet are
exposure the broker listing can't show.

All time comparisons use the BROKER clock (`get_clock().timestamp`), re-anchored
on every wake: machine skew and macOS process suspension must not be able to
move the 15:55 flatten (the monotonic clock does not advance through a lid-close;
a fixed sleep schedule would).
"""

from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from sqlalchemy import select

from app.core.logging import get_logger
from app.engine.events import FlattenEvent
from app.models.enums import EventSource, OrderStatus
from app.models.telemetry import EventLog
from app.models.trading import Lot, Order

if TYPE_CHECKING:
    import redis.asyncio as aioredis
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from app.brokers.base import BrokerAdapter
    from app.brokers.dto import CalendarDay, MarketClock
    from app.engine.bus import EventBus
    from app.engine.kill_switch import KillSwitch

log = get_logger("engine.flatten")

_ET = ZoneInfo("America/New_York")

# Alerts that must outlive a log line (overnight exposure, flatten failure).
# The status endpoint / future UI subscribes; EventLog rows persist regardless.
CHANNEL_ALERTS = "engine:alerts"

# Liquidation client-id prefix — must match the execution handler's. Imported
# there from here would invert the dependency; a shared literal with a test
# asserting equality keeps the modules decoupled.
FLATTEN_CLIENT_ID_PREFIX = "roigen-flatten"

# Order rows that mean "may exist at the broker" — exposure the broker's own
# open-orders listing cannot show yet.
_PENDING_LOCAL_STATUSES = (OrderStatus.pending_submit.value,)

# Tick cadences by distance to the next deadline. Chunked short so every wake
# re-anchors on the broker clock; capped well under flatten_buffer so chunk
# arithmetic can never overshoot the window entirely.
_TICK_IN_WINDOW = 30.0
_TICK_NEAR = 120.0
_TICK_FAR = 900.0
_NEAR_THRESHOLD = timedelta(minutes=30)

# Let the opening auction print before a next-open remediation fires market
# liquidations into it.
_OPEN_GRACE = timedelta(seconds=15)


@dataclass(frozen=True, slots=True)
class _Session:
    """One trading session's instants, resolved from the broker calendar."""

    trading_date: date
    rth_open: datetime
    rth_close: datetime
    flatten_at: datetime


@dataclass(slots=True)
class _Exposure:
    """What stands between the portfolio and 'flat', per broker truth."""

    position_symbols: list[str]
    working_orders: int
    pending_submit: int
    liquidation_covered: set[str]

    @property
    def exposed(self) -> bool:
        return bool(self.position_symbols) or self.working_orders > 0 or self.pending_submit > 0

    @property
    def fully_covered(self) -> bool:
        """Every exposed symbol already has a working liquidation — wait, don't re-fire."""
        return (
            bool(self.position_symbols)
            and self.working_orders == 0
            and self.pending_submit == 0
            and all(s in self.liquidation_covered for s in self.position_symbols)
        )

    def to_payload(self) -> dict[str, object]:
        return {
            "position_symbols": self.position_symbols,
            "working_orders": self.working_orders,
            "pending_submit": self.pending_submit,
            "liquidation_covered": sorted(self.liquidation_covered),
        }


class FlattenController:
    """The single publisher of ``FlattenEvent`` and owner of flatten outcomes."""

    def __init__(
        self,
        *,
        bus: EventBus,
        adapter: BrokerAdapter,
        session_factory: async_sessionmaker[AsyncSession],
        redis: aioredis.Redis,
        portfolio_id: uuid.UUID,
        kill_switch: KillSwitch,
        boot_reconciled: asyncio.Event,
        flatten_buffer: timedelta,
    ) -> None:
        self._bus = bus
        self._adapter = adapter
        self._session_factory = session_factory
        self._redis = redis
        self._portfolio_id = portfolio_id
        self._kill_switch = kill_switch
        self._boot_reconciled = boot_reconciled
        self._flatten_buffer = flatten_buffer
        # Poked by the command sweeper so a kill-switch flatten doesn't wait a tick.
        self._wake = asyncio.Event()
        self._calendar_cache: dict[date, _Session | None] = {}
        self._post_close_checked: set[date] = set()
        # Pre-open exposure observation, keyed to ITS trading date: a process
        # suspension can wake straight from one session into the next, and a
        # stale scalar would replay yesterday's verdict against today's book
        # (review finding, three lenses independently).
        self._next_open_snapshot: tuple[date, bool] | None = None
        self._next_open_fired: set[date] = set()

    def poke(self) -> None:
        """Wake the loop now (command sweeper calls this on a new kill command)."""
        self._wake.set()

    async def run(self, shutdown: asyncio.Event) -> None:
        """Drive the standing rules forever; every wake re-anchors on broker time."""
        await self._boot_reconciled.wait()
        log.info("engine.flatten.controller_started", portfolio_id=str(self._portfolio_id))
        while not shutdown.is_set():
            # Clear BEFORE the tick, not before the sleep: a poke that lands
            # while _tick is mid-broker-I/O must survive into the wait below,
            # or a kill-flatten issued during a long pre-window tick would sit
            # out the full sleep chunk (review finding: lost-wakeup).
            self._wake.clear()
            try:
                sleep_for = await self._tick()
            except Exception:  # noqa: BLE001 — the safety loop must survive anything
                log.exception("engine.flatten.tick_error")
                sleep_for = _TICK_IN_WINDOW
            await self._sleep(shutdown, sleep_for)

    async def _tick(self) -> float:
        """Evaluate every standing rule once; return seconds until the next wake."""
        clock = await self._adapter.get_clock()
        now = clock.timestamp
        session = await self._session_for(now)

        # Rule 1 — kill-switch flatten: a durable state, driven whenever the
        # market can execute it. Checked first; it also owns writing the
        # command's verified outcome.
        if self._kill_switch.is_flattening:
            drove = await self._drive_kill_flatten(clock)
            if drove:
                return _TICK_IN_WINDOW

        if session is None:
            return self._sleep_toward(now, clock.next_open)

        # The calendar was cached (possibly just after midnight); the LIVE
        # clock's next_close wins when it is earlier — an intraday early-close
        # amendment must pull the flatten forward, never let market orders be
        # scheduled into a closed market (folded design-review finding).
        rth_close = session.rth_close
        if clock.is_open and clock.next_close is not None:
            rth_close = min(rth_close, clock.next_close)
        flatten_at = rth_close - self._flatten_buffer

        if now < session.rth_open:
            # Pre-open: snapshot whether exposure predates the session, so the
            # open-tick knows leftovers from legitimate same-session entries.
            self._next_open_snapshot = (session.trading_date, await self._has_any_exposure())
            return self._sleep_toward(now, session.rth_open)

        if now < flatten_at:
            driving = await self._maybe_next_open_flatten(session, now, clock)
            if driving:
                return _TICK_IN_WINDOW
            return self._sleep_toward(now, flatten_at)

        if now < rth_close and clock.is_open:
            # WATCH WINDOW — unconditional every session, whether or not any
            # flatten was requested: this is the 15:55 rule itself.
            await self._drive(source="scheduled_close", reason="mandatory close-window flatten")
            return _TICK_IN_WINDOW

        await self._post_close_check(session)
        return self._sleep_toward(now, clock.next_open)

    # ── Standing rules ───────────────────────────────────────────────

    async def _drive_kill_flatten(self, clock: MarketClock) -> bool:
        """Drive the durable kill-switch flatten; verify + record its outcome.

        Returns True while actively driving (caller keeps the fast cadence).
        Market closed → the state holds (entries stay frozen) and the drive
        resumes at the open; we never submit after-hours market orders — they
        queue at the broker overnight, hold qty, and fight the next-open rule.
        """
        command_seq = self._kill_switch.flatten_command_seq
        exposure = await self._exposure()
        if not exposure.exposed:
            verified = await self._kill_switch.mark_flatten_verified(command_seq)
            if verified:
                await self._audit(
                    "flatten.completed",
                    payload={
                        "command_seq": command_seq,
                        "verified_at": datetime.now(UTC).isoformat(),
                    },
                )
                log.info("engine.flatten.kill_flatten_verified", command_seq=command_seq)
            return False
        if not clock.is_open:
            log.warning(
                "engine.flatten.kill_flatten_market_closed",
                command_seq=command_seq,
                exposure=exposure.to_payload(),
            )
            return False
        if exposure.fully_covered:
            return True
        await self._publish(
            source="kill_switch",
            reason=self._kill_switch.reason or "operator flatten",
            command_seq=command_seq,
            exposure=exposure,
        )
        return True

    async def _maybe_next_open_flatten(
        self, session: _Session, now: datetime, clock: MarketClock
    ) -> bool:
        """Flatten exposure that predates the session, right after the open.

        Bracket legs are day-TIF: anything that survived last close has no stop
        working NOW. Stale exposure is identified by the pre-open snapshot for
        THIS trading date when we watched the open happen, or by lot evidence
        (min ``Lot.opened_at`` before today's open; NO lot evidence at all is
        treated as stale — fail toward flat) otherwise. LEVEL-TRIGGERED like
        every other flatten source (review finding): the date is marked done
        only once the stale exposure is confirmed gone — a transient error at
        the open re-drives on the next tick instead of silently waiting for
        15:55. Returns True while actively driving (caller keeps fast cadence).
        """
        if session.trading_date in self._next_open_fired:
            return False
        if not clock.is_open or now < session.rth_open + _OPEN_GRACE:
            # Let the opening auction print before firing market liquidations.
            return False
        exposure = await self._exposure()
        if not exposure.exposed:
            self._next_open_fired.add(session.trading_date)
            return False
        snapshot = self._next_open_snapshot
        if snapshot is not None and snapshot[0] == session.trading_date:
            stale = snapshot[1]
        else:
            # No same-date pre-open observation (mid-session boot, or a
            # suspension slept through the open): consult lot evidence.
            stale = await self._exposure_predates(session.rth_open, exposure.position_symbols)
        if not stale:
            self._next_open_fired.add(session.trading_date)
            return False
        if exposure.fully_covered:
            return True  # drive in flight — re-check next tick, don't re-fire
        log.warning(
            "engine.flatten.next_open_remediation",
            trading_date=str(session.trading_date),
            exposure=exposure.to_payload(),
        )
        await self._publish(
            source="next_open",
            reason="exposure predating session open (day-TIF protection is dead)",
            command_seq=None,
            exposure=exposure,
        )
        return True

    async def _drive(self, *, source: str, reason: str) -> None:
        """One re-drive tick: fire iff exposed and not already fully covered."""
        exposure = await self._exposure()
        if not exposure.exposed or exposure.fully_covered:
            return
        await self._publish(source=source, reason=reason, command_seq=None, exposure=exposure)

    async def _post_close_check(self, session: _Session) -> None:
        """The one mandatory after-the-bell truth check — the alarm's single owner."""
        if session.trading_date in self._post_close_checked:
            return
        exposure = await self._exposure()
        # Only a SUCCESSFUL broker-truth read counts as checked: a broker error
        # at 16:00 is correlated with whatever broke the flatten, and it must
        # not permanently silence the one overnight-exposure alarm (review
        # finding). The tick-level catch retries us on the next wake.
        self._post_close_checked.add(session.trading_date)
        # Keep the per-date structures from growing unbounded across long uptimes.
        if len(self._post_close_checked) > 30:
            cutoff = sorted(self._post_close_checked)[-30]
            self._post_close_checked = {d for d in self._post_close_checked if d >= cutoff}
            self._next_open_fired = {d for d in self._next_open_fired if d >= cutoff}
            self._calendar_cache = {d: s for d, s in self._calendar_cache.items() if d >= cutoff}
        if not exposure.exposed:
            # The session's verified-flat terminal row: scheduled/next-open
            # flattens get a completion record too, not just kill-switch ones.
            await self._audit(
                "flatten.session_flat",
                payload={
                    "trading_date": str(session.trading_date),
                    "verified_at": datetime.now(UTC).isoformat(),
                },
            )
            log.info("engine.flatten.post_close_flat", trading_date=str(session.trading_date))
            return
        payload: dict[str, object] = {
            "trading_date": str(session.trading_date),
            "exposure": exposure.to_payload(),
            "detail": (
                "exposure survived the close: day-TIF protection is dead until the "
                "next-open flatten — investigate before the open"
            ),
        }
        log.critical("engine.flatten.overnight_exposure", **payload)
        await self._audit("flatten.overnight_exposure", payload=payload)
        await self._alert("overnight_exposure", payload)

    # ── Exposure (broker truth + local pending) ──────────────────────

    async def _exposure(self) -> _Exposure:
        positions = await self._adapter.list_positions()
        # nested=False for the same reason the flatten cancel-sweep uses it: a
        # filled bracket parent's protective legs must count as working orders
        # (keep driving until they're canceled) instead of hiding under a
        # rolled-up parent and letting `fully_covered` read true prematurely.
        open_orders = await self._adapter.list_orders(status="open", nested=False)
        working = 0
        covered: set[str] = set()
        for order in open_orders:
            client_id = order.client_order_id or ""
            if client_id.startswith(FLATTEN_CLIENT_ID_PREFIX):
                covered.add(order.symbol)
            else:
                working += 1
        pending = await self._count_pending_submit()
        return _Exposure(
            position_symbols=[p.symbol for p in positions if p.qty != 0],
            working_orders=working,
            pending_submit=pending,
            liquidation_covered=covered,
        )

    async def _has_any_exposure(self) -> bool:
        return (await self._exposure()).exposed

    async def _count_pending_submit(self) -> int:
        """Local pending_submit orders that may be live but broker-invisible.

        Excludes our OWN flatten liquidations (review finding): a liquidation is
        position-REDUCING, so an ambiguous one stuck in pending_submit (until
        reconcile ages it, up to ~7 min) is not exposure to flatten — counting
        it would withhold ``flat_verified`` and refuse ``resume`` against a
        broker-flat book. If such a liquidation never actually landed, its
        target position is still open and independently counted from broker
        truth; the liquidation row itself must not double as exposure.
        """
        async with self._session_factory() as session:
            rows = await session.execute(
                select(Order.id).where(
                    Order.portfolio_id == self._portfolio_id,
                    Order.status.in_(_PENDING_LOCAL_STATUSES),
                    ~Order.client_order_id.startswith(FLATTEN_CLIENT_ID_PREFIX),
                )
            )
            return len(rows.scalars().all())

    async def _exposure_predates(self, rth_open: datetime, symbols: list[str]) -> bool:
        """Lot-evidence test for mid-session boots: does any open exposure predate
        today's open? No lot evidence for an open symbol counts as stale (fail
        toward flat — an unexplained position is exactly what must not ride)."""
        if not symbols:
            return False
        async with self._session_factory() as session:
            rows = await session.execute(
                select(Lot.symbol, Lot.opened_at).where(
                    Lot.portfolio_id == self._portfolio_id,
                    Lot.symbol.in_(symbols),
                    Lot.qty_open > 0,
                )
            )
            earliest: dict[str, datetime] = {}
            for symbol, opened_at in rows.all():
                if symbol not in earliest or opened_at < earliest[symbol]:
                    earliest[symbol] = opened_at
        for symbol in symbols:
            opened = earliest.get(symbol)
            if opened is None or opened < rth_open:
                return True
        return False

    # ── Session/calendar math ────────────────────────────────────────

    async def _session_for(self, now: datetime) -> _Session | None:
        """Resolve the session for `now`'s ET trading date (holiday → None).

        Cached per date; the flatten instant is re-derived from the LIVE clock's
        ``next_close`` each watch-window tick indirectly (the calendar and the
        clock agree in practice; the calendar's early-close awareness is what we
        need — a mid-day amendment shows up when the cache rolls to the next date).
        """
        trading_date = now.astimezone(_ET).date()
        if trading_date not in self._calendar_cache:
            days: list[CalendarDay] = await self._adapter.get_calendar(trading_date, trading_date)
            if not days:
                self._calendar_cache[trading_date] = None
            else:
                day = days[0]
                self._calendar_cache[trading_date] = _Session(
                    trading_date=trading_date,
                    rth_open=day.rth_open,
                    rth_close=day.rth_close,
                    flatten_at=day.rth_close - self._flatten_buffer,
                )
        return self._calendar_cache[trading_date]

    def _sleep_toward(self, now: datetime, target: datetime) -> float:
        remaining = (target - now).total_seconds()
        if remaining <= 0:
            return _TICK_IN_WINDOW
        if remaining > _NEAR_THRESHOLD.total_seconds():
            return min(_TICK_FAR, remaining / 2)
        return min(_TICK_NEAR, max(remaining / 2, 1.0))

    async def _sleep(self, shutdown: asyncio.Event, seconds: float) -> None:
        """Sleep until timeout, shutdown, or a poke — whichever first.

        The wake event is cleared by ``run()`` BEFORE each tick (never here):
        a poke landing mid-tick must cut this sleep short.
        """
        shutdown_task = asyncio.create_task(shutdown.wait())
        wake_task = asyncio.create_task(self._wake.wait())
        try:
            await asyncio.wait(
                {shutdown_task, wake_task}, timeout=seconds, return_when=asyncio.FIRST_COMPLETED
            )
        finally:
            shutdown_task.cancel()
            wake_task.cancel()

    # ── Plumbing ─────────────────────────────────────────────────────

    async def _publish(
        self,
        *,
        source: str,
        reason: str,
        command_seq: int | None,
        exposure: _Exposure,
    ) -> None:
        event = FlattenEvent(
            portfolio_id=self._portfolio_id,
            reason=reason,
            source=source,
            command_seq=command_seq,
        )
        log.warning(
            "engine.flatten.drive",
            flatten_id=str(event.flatten_id),
            source=source,
            reason=reason,
            exposure=exposure.to_payload(),
        )
        await self._bus.publish(event)

    async def _audit(self, event_type: str, *, payload: dict[str, object]) -> None:
        try:
            async with self._session_factory() as session:
                session.add(
                    EventLog(
                        source=EventSource.engine.value,
                        event_type=event_type,
                        portfolio_id=self._portfolio_id,
                        payload=payload,
                    )
                )
                await session.commit()
        except Exception:  # noqa: BLE001 — last-ditch; never raise out of the audit path
            log.exception("engine.flatten.audit_failed", event_type=event_type)

    async def _alert(self, kind: str, payload: dict[str, object]) -> None:
        """Publish to the alerts channel; failure is logged, never raised."""
        try:
            await self._redis.publish(
                CHANNEL_ALERTS,
                json.dumps(
                    {
                        "type": "alert",
                        "kind": kind,
                        "portfolio_id": str(self._portfolio_id),
                        **payload,
                    },
                    default=str,
                ),
            )
        except Exception:  # noqa: BLE001
            log.exception("engine.flatten.alert_publish_failed", kind=kind)
