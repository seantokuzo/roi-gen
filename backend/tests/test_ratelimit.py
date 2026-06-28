"""AsyncTokenBucket: deterministic rate enforcement under a fake clock.

We never sleep for real. A :class:`FakeClock` is the injected ``time_fn``, and
``asyncio.sleep`` is patched (in the bucket's module) to advance that clock by
the requested duration before yielding. That makes the bucket's wait loop both
fast and exact: the accounting math runs against the same clock the "sleep"
advances, so observed throughput equals the configured rate.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

import pytest

from app.brokers import ratelimit
from app.brokers.ratelimit import AsyncTokenBucket


class FakeClock:
    """A manually-advanced monotonic clock (seconds)."""

    def __init__(self) -> None:
        self.t = 0.0

    def now(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def fake_sleep(
    clock: FakeClock, monkeypatch: pytest.MonkeyPatch
) -> Callable[[float], Awaitable[None]]:
    """Patch ``asyncio.sleep`` (as the bucket sees it) to advance ``clock``.

    A non-zero requested sleep advances the fake clock by that duration; we
    then yield via the *real* ``asyncio.sleep(0)`` so concurrent waiters
    interleave without consuming wall-clock time.
    """
    real_sleep = asyncio.sleep  # capture the genuine builtin before patching

    async def _dispatch(seconds: float) -> None:
        if seconds > 0:
            clock.advance(seconds)
        await real_sleep(0)

    monkeypatch.setattr(ratelimit.asyncio, "sleep", _dispatch)
    return _dispatch


async def test_acquire_returns_immediately_when_tokens_available(clock: FakeClock) -> None:
    bucket = AsyncTokenBucket(rate_per_minute=60, time_fn=clock.now)
    # Bucket starts full (capacity == rate); first acquire must not advance time.
    await bucket.acquire()
    assert clock.now() == 0.0


async def test_burst_capacity_drains_then_throttles(
    clock: FakeClock, fake_sleep: Callable[[float], Awaitable[None]]
) -> None:
    # rate 60/min => 1 token/sec, capacity defaults to 60 (one minute's burst).
    bucket = AsyncTokenBucket(rate_per_minute=60, time_fn=clock.now)
    # Drain the full burst instantly.
    for _ in range(60):
        await bucket.acquire()
    assert clock.now() == 0.0
    # The 61st must wait ~1s for one token to refill.
    await bucket.acquire()
    assert clock.now() == pytest.approx(1.0, abs=1e-9)


async def test_rapid_acquires_obey_rate(
    clock: FakeClock, fake_sleep: Callable[[float], Awaitable[None]]
) -> None:
    # Small burst so throttling kicks in immediately and we can measure spacing.
    bucket = AsyncTokenBucket(rate_per_minute=120, burst=1, time_fn=clock.now)
    n = 10
    for _ in range(n):
        await bucket.acquire()
    # 120/min => 2 tokens/sec => 0.5s spacing. Starting full, the first is free
    # and the next (n-1) each wait exactly 0.5s.
    elapsed = clock.now()
    assert elapsed == pytest.approx((n - 1) * 0.5, abs=1e-9)
    # The token-bucket invariant: tokens consumed over a window never exceed
    # burst + rate*elapsed (this is the real ceiling, not a naive average that
    # double-counts the initial burst against zero elapsed time).
    refill_per_sec = 120 / 60
    assert n <= 1 + refill_per_sec * elapsed + 1e-9


async def test_concurrent_acquires_do_not_oversubscribe(
    clock: FakeClock, fake_sleep: Callable[[float], Awaitable[None]]
) -> None:
    # burst=2: at most 2 may proceed at t=0; the rest are rate-limited.
    bucket = AsyncTokenBucket(rate_per_minute=60, burst=2, time_fn=clock.now)
    completion_times: list[float] = []

    async def worker() -> None:
        await bucket.acquire()
        completion_times.append(clock.now())

    await asyncio.gather(*(worker() for _ in range(6)))

    assert len(completion_times) == 6
    # Two free at t=0; then one token/sec. With the lock serializing waiters,
    # completions land at 0, 0, 1, 2, 3, 4.
    completion_times.sort()
    assert completion_times == pytest.approx([0.0, 0.0, 1.0, 2.0, 3.0, 4.0], abs=1e-9)
    # Never more than `burst` consumed before any refill occurred.
    assert sum(1 for t in completion_times if t == 0.0) == 2


async def test_acquire_multiple_tokens_at_once(
    clock: FakeClock, fake_sleep: Callable[[float], Awaitable[None]]
) -> None:
    bucket = AsyncTokenBucket(rate_per_minute=60, burst=10, time_fn=clock.now)
    await bucket.acquire(10)  # drains the bucket
    assert clock.now() == 0.0
    await bucket.acquire(5)  # waits 5s for 5 tokens
    assert clock.now() == pytest.approx(5.0, abs=1e-9)


def test_invalid_construction_rejected() -> None:
    with pytest.raises(ValueError, match="rate_per_minute must be positive"):
        AsyncTokenBucket(rate_per_minute=0)
    with pytest.raises(ValueError, match="burst must be positive"):
        AsyncTokenBucket(rate_per_minute=60, burst=0)


async def test_acquire_rejects_bad_token_counts() -> None:
    bucket = AsyncTokenBucket(rate_per_minute=60, burst=5)
    with pytest.raises(ValueError, match="tokens must be positive"):
        await bucket.acquire(0)
    with pytest.raises(ValueError, match="exceeds bucket capacity"):
        await bucket.acquire(6)
