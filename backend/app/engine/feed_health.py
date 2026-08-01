"""FeedHealth — level-based market-data health, wired into the entry gate.

Game plan principle #5 ("stale data = no trading") was documented intent until
2c; the watchdog published `feed_stale`/`feed_ok` transitions that only a
logger consumed. Transitions over pub/sub also cannot gate safety: pub/sub is
at-most-once, so a `feed_stale` published during this subscriber's reconnect
would be lost forever and entries would keep flowing on a dead feed.

So health is a LEVEL: the market-data consumer maintains a TTL'd Redis key
while data flows; this poller derives staleness from the key's current value —
and from its ABSENCE. No key means the watchdog itself is dead, Redis was
flushed, or data never arrived since boot: all of them are "stale" (fail
closed). `is_stale` joins the engine's halt composite and blocks NEW entries
only; the flatten path deliberately ignores it — the kill switch must work
during exactly the outage that trips this gate.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from app.core.logging import get_logger

if TYPE_CHECKING:
    import redis.asyncio as aioredis

log = get_logger("engine.feed_health")

_POLL_INTERVAL = 5.0


class FeedHealth:
    """Polls the feed-health key; exposes the level as a cheap attribute read."""

    def __init__(
        self,
        *,
        redis: aioredis.Redis,
        key: str,
        poll_interval: float = _POLL_INTERVAL,
    ) -> None:
        self._redis = redis
        self._key = key
        self._poll_interval = poll_interval
        # Fail closed from birth: unhealthy until a healthy key is observed.
        self._stale = True

    @property
    def is_stale(self) -> bool:
        return self._stale

    async def run(self, shutdown: asyncio.Event) -> None:
        while not shutdown.is_set():
            try:
                stale = await self._read_level()
            except Exception:  # noqa: BLE001 — a Redis error is a health unknown → stale
                log.exception("engine.feed_health.poll_error")
                stale = True
            if stale != self._stale:
                log_fn = log.warning if stale else log.info
                log_fn("engine.feed_health.transition", stale=stale, key=self._key)
            self._stale = stale
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=self._poll_interval)
            except TimeoutError:
                continue

    async def _read_level(self) -> bool:
        """True = stale. Absent, unparseable, not-ok, or simply TOO OLD.

        The age check is not redundant with the key's TTL. Redis can reject
        writes while still serving reads — a failing RDB snapshot (``MISCONF``)
        or ``maxmemory`` pressure does exactly that — and the writer swallows
        those failures by design so a telemetry hiccup can't kill the feed. In
        that state the last good ``ok`` would sit there being believed until
        its TTL lapsed. Trusting the payload's own timestamp against the window
        the writer stamped into it collapses that window to one poll interval.
        """
        raw = await self._redis.get(self._key)
        if raw is None:
            return True
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, TypeError, UnicodeDecodeError):
            return True
        if data.get("status") != "ok":
            return True
        stamped_at = data.get("at")
        window = data.get("window_seconds")
        if not isinstance(stamped_at, str) or not isinstance(window, int | float):
            # A writer too old to stamp freshness — fall back to TTL-only
            # trust rather than hard-failing a healthy feed.
            return False
        try:
            age = (datetime.now(UTC) - datetime.fromisoformat(stamped_at)).total_seconds()
        except ValueError:
            return True
        return age > float(window)
