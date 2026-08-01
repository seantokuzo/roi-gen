"""FeedHealth: level-based staleness, fail-closed from birth.

The gate derives health from the TTL'd Redis key's CURRENT state — and from
its absence. Every unknown (no key, garbage value, Redis error) must read as
stale, because "unknown feed health" and "dead feed" earn the same answer:
no new entries.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, cast

from app.engine.feed_health import FeedHealth
from tests.engine.flatten_helpers import FakeRedis, wait_until

if TYPE_CHECKING:
    import redis.asyncio as aioredis

_KEY = "engine:feed_health:test-portfolio"


def _health(redis: FakeRedis) -> FeedHealth:
    return FeedHealth(redis=cast("aioredis.Redis", redis), key=_KEY, poll_interval=0.005)


async def test_starts_stale_before_any_observation() -> None:
    # Fail closed from birth: no data has been seen, so no entries may open.
    health = _health(FakeRedis())
    assert health.is_stale is True


async def test_ok_key_reads_healthy() -> None:
    redis = FakeRedis()
    redis.value = json.dumps(
        {"status": "ok", "at": datetime.now(UTC).isoformat(), "window_seconds": 120}
    )
    health = _health(redis)

    shutdown = asyncio.Event()
    task = asyncio.create_task(health.run(shutdown))
    try:
        await wait_until(lambda: not health.is_stale)
    finally:
        shutdown.set()
        await asyncio.wait_for(task, timeout=2.0)
    assert health.is_stale is False


async def test_stale_status_reads_stale() -> None:
    redis = FakeRedis()
    redis.value = json.dumps(
        {"status": "ok", "at": datetime.now(UTC).isoformat(), "window_seconds": 120}
    )
    health = _health(redis)

    shutdown = asyncio.Event()
    task = asyncio.create_task(health.run(shutdown))
    try:
        await wait_until(lambda: not health.is_stale)
        redis.value = '{"status": "stale"}'
        await wait_until(lambda: health.is_stale)
    finally:
        shutdown.set()
        await asyncio.wait_for(task, timeout=2.0)


async def test_absent_key_reads_stale() -> None:
    # TTL lapsed / watchdog dead / Redis flushed — all the same answer.
    redis = FakeRedis()
    redis.value = json.dumps(
        {"status": "ok", "at": datetime.now(UTC).isoformat(), "window_seconds": 120}
    )
    health = _health(redis)

    shutdown = asyncio.Event()
    task = asyncio.create_task(health.run(shutdown))
    try:
        await wait_until(lambda: not health.is_stale)
        redis.value = None
        await wait_until(lambda: health.is_stale)
    finally:
        shutdown.set()
        await asyncio.wait_for(task, timeout=2.0)


async def test_unparseable_value_reads_stale() -> None:
    redis = FakeRedis()
    redis.value = json.dumps(
        {"status": "ok", "at": datetime.now(UTC).isoformat(), "window_seconds": 120}
    )
    health = _health(redis)

    shutdown = asyncio.Event()
    task = asyncio.create_task(health.run(shutdown))
    try:
        await wait_until(lambda: not health.is_stale)
        redis.value = b"{not json at all"
        await wait_until(lambda: health.is_stale)
    finally:
        shutdown.set()
        await asyncio.wait_for(task, timeout=2.0)


async def test_redis_error_reads_stale() -> None:
    # A health check that cannot run is a health check that failed.
    redis = FakeRedis()
    redis.value = json.dumps(
        {"status": "ok", "at": datetime.now(UTC).isoformat(), "window_seconds": 120}
    )
    health = _health(redis)

    shutdown = asyncio.Event()
    task = asyncio.create_task(health.run(shutdown))
    try:
        await wait_until(lambda: not health.is_stale)
        redis.get_error = ConnectionError("redis went away")
        await wait_until(lambda: health.is_stale)
    finally:
        shutdown.set()
        await asyncio.wait_for(task, timeout=2.0)


# ── Freshness: a stale "ok" must not be believed until its TTL lapses ──


def _ok_payload(*, age_seconds: float, window: float = 120.0) -> str:
    stamped = datetime.now(UTC) - timedelta(seconds=age_seconds)
    return json.dumps({"status": "ok", "at": stamped.isoformat(), "window_seconds": window})


async def test_fresh_ok_payload_reads_healthy() -> None:
    redis = FakeRedis()
    redis.value = _ok_payload(age_seconds=5)
    assert await _health(redis)._read_level() is False  # noqa: SLF001


async def test_ok_payload_older_than_its_window_reads_stale() -> None:
    """Redis can reject writes while still serving reads (MISCONF / OOM).

    The writer swallows those failures by design, so without an age check the
    last good "ok" would be trusted for the whole 240s TTL while the feed was
    already dark.
    """
    redis = FakeRedis()
    redis.value = _ok_payload(age_seconds=200, window=120.0)
    assert await _health(redis)._read_level() is True  # noqa: SLF001


async def test_payload_without_freshness_stamp_reads_stale() -> None:
    """No freshness evidence ⇒ stale: this module fails closed everywhere."""
    redis = FakeRedis()
    redis.value = '{"status": "ok"}'
    assert await _health(redis)._read_level() is True  # noqa: SLF001


async def test_quiet_but_healthy_feed_inside_its_window_reads_ok() -> None:
    """Pins the cross-file coupling to the writer's LEVEL-triggered refresh.

    `streams.py`'s watchdog re-stamps "ok" every poll (5s) even when no
    messages arrive, so `at` never approaches the 120s window on a healthy
    feed. If that write is ever made edge-triggered, this test fails — which
    is the point: the reader's age check is only safe because of it.
    """
    redis = FakeRedis()
    redis.value = _ok_payload(age_seconds=119, window=120.0)
    assert await _health(redis)._read_level() is False  # noqa: SLF001


async def test_unparseable_timestamp_reads_stale() -> None:
    redis = FakeRedis()
    redis.value = json.dumps({"status": "ok", "at": "not-a-time", "window_seconds": 120})
    assert await _health(redis)._read_level() is True  # noqa: SLF001
