"""FeedHealth: level-based staleness, fail-closed from birth.

The gate derives health from the TTL'd Redis key's CURRENT state — and from
its absence. Every unknown (no key, garbage value, Redis error) must read as
stale, because "unknown feed health" and "dead feed" earn the same answer:
no new entries.
"""

from __future__ import annotations

import asyncio
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
    redis.value = '{"status": "ok", "stamped_at": "2026-06-26T15:00:00+00:00"}'
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
    redis.value = '{"status": "ok"}'
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
    redis.value = '{"status": "ok"}'
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
    redis.value = '{"status": "ok"}'
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
    redis.value = '{"status": "ok"}'
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
