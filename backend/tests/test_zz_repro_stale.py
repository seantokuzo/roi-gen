"""Reproduction: a swallowed FAILED 'stale' health write is never retried.

The ok->stale transition writes the health key exactly once (edge-triggered on
`_feed_stale`). Post-c09c79f that write's failure is swallowed, so the key keeps
its last successful "ok" value until the TTL lapses — the fail-closed reader
sees a HEALTHY feed for the whole remaining TTL during a real blackout.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

from tests.test_alpaca_streams import (
    CREDS,
    FakeMarketStream,
    FakeRedis,
    _FakeClockConsumer,
    _join,
    _wait_for,
    fake_bar,
)

pytestmark = pytest.mark.asyncio


class StaleWriteFailsOnceRedis(FakeRedis):
    """Every setex succeeds EXCEPT the first one carrying status=stale."""

    def __init__(self) -> None:
        super().__init__()
        self.stale_attempts = 0

    async def setex(self, name: str, time: int, value: str) -> bool:
        if json.loads(value)["status"] == "stale":
            self.stale_attempts += 1
            if self.stale_attempts == 1:
                msg = "Timeout connecting to server"
                raise TimeoutError(msg)
        return await super().setex(name, time, value)


async def test_failed_stale_write_is_never_retried() -> None:
    redis = StaleWriteFailsOnceRedis()
    stream = FakeMarketStream()
    key = "engine:feed_health:pf-hk"
    consumer = _FakeClockConsumer(
        CREDS,
        redis,
        ["AAPL"],
        stream_factory=lambda creds, feed: stream,
        subscribe_trades=True,
        staleness_seconds=30,
        watchdog_poll_seconds=0.005,
        portfolio_id="pf-hk",
    )

    task = asyncio.create_task(consumer.start())
    await _wait_for(lambda: stream.bar_handler is not None)

    # Healthy: watchdog refreshes "ok" each tick (fake clock advances 1s/step so
    # the 1s throttle lets them through).
    for t in (1.0, 2.0, 3.0):
        consumer.fake_time = t
        await asyncio.sleep(0.03)
    assert redis.health_statuses(key)[-1] == "ok"

    # Blackout: cross the threshold. One stale write is attempted and fails.
    consumer.fake_time = 40.0
    await _wait_for(lambda: redis.stale_attempts >= 1)

    # Keep the watchdog ticking well past many poll intervals with time still
    # advancing (so the 1s refresh throttle can never be the reason).
    for t in (41.0, 42.0, 43.0, 44.0, 45.0, 46.0):
        consumer.fake_time = t
        await asyncio.sleep(0.05)

    await consumer.stop()
    await _join(task)

    statuses = redis.health_statuses(key)
    print(f"\nstale write attempts: {redis.stale_attempts}")
    print(f"statuses written to the key: {statuses}")
    print(f"LAST value the fail-closed reader would see: {statuses[-1]!r}")
    print(f"key TTL (seconds the bad value survives): {redis.setex_calls[-1][1]}")

    assert redis.stale_attempts == 1, "the failed stale write was retried (good)"
    assert "stale" not in statuses, "no 'stale' value ever landed in the key"
    assert statuses[-1] == "ok", "the reader keeps seeing a HEALTHY feed"


async def test_failed_ok_write_self_heals_next_tick() -> None:
    """Control: the 'ok' path DOES retry — proving the asymmetry."""

    class OkWriteFailsOnceRedis(FakeRedis):
        def __init__(self) -> None:
            super().__init__()
            self.failed = False

        async def setex(self, name: str, time: int, value: str) -> bool:
            if not self.failed:
                self.failed = True
                msg = "Timeout connecting to server"
                raise TimeoutError(msg)
            return await super().setex(name, time, value)

    redis: Any = OkWriteFailsOnceRedis()
    stream = FakeMarketStream()
    consumer = _FakeClockConsumer(
        CREDS,
        redis,
        ["AAPL"],
        stream_factory=lambda creds, feed: stream,
        subscribe_trades=True,
        staleness_seconds=30,
        watchdog_poll_seconds=0.005,
        portfolio_id="pf-hk",
    )
    task = asyncio.create_task(consumer.start())
    await _wait_for(lambda: stream.bar_handler is not None)
    consumer.fake_time = 2.0
    await _wait_for(lambda: redis.health_statuses("engine:feed_health:pf-hk") == ["ok"])
    await consumer.stop()
    await _join(task)
    assert fake_bar is not None
