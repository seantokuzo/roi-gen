"""Async token-bucket rate limiter for REST throttling.

Alpaca's trading API caps at 200 requests/minute (project CLAUDE.md). Every
adapter routes its REST calls through one :class:`AsyncTokenBucket` so bursts
across concurrent coroutines never exceed the broker's hard limit.

The bucket refills *continuously* (fractional tokens accrue at
``rate_per_minute / 60`` per second) rather than in discrete windows, which
keeps throughput smooth and avoids the thundering-herd a fixed-window limiter
produces at each window boundary.

``time_fn`` is injected so tests drive a fake monotonic clock and never sleep
for real: actual waiting is gated on :func:`asyncio.sleep`, but the *duration*
to wait is always computed from ``time_fn`` — so under a fake clock the sleeps
are zero-length while the accounting stays exact.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from time import monotonic


class AsyncTokenBucket:
    """A coroutine-safe token bucket throttling work to ``rate_per_minute``.

    Parameters
    ----------
    rate_per_minute:
        Sustained rate. Tokens accrue at ``rate_per_minute / 60`` per second.
    burst:
        Bucket capacity (max tokens that can accumulate). Defaults to
        ``rate_per_minute`` — i.e. up to one minute's worth may burst at once.
    time_fn:
        Monotonic clock source (seconds). Injected for deterministic tests;
        defaults to :func:`time.monotonic`.
    """

    def __init__(
        self,
        rate_per_minute: int,
        burst: int | None = None,
        time_fn: Callable[[], float] = monotonic,
    ) -> None:
        if rate_per_minute <= 0:
            msg = "rate_per_minute must be positive"
            raise ValueError(msg)
        if burst is not None and burst <= 0:
            msg = "burst must be positive"
            raise ValueError(msg)

        self._capacity: float = float(burst if burst is not None else rate_per_minute)
        self._refill_per_sec: float = rate_per_minute / 60.0
        self._time_fn = time_fn
        self._lock = asyncio.Lock()
        # Start full so the first burst is allowed immediately.
        self._tokens: float = self._capacity
        self._updated_at: float = time_fn()

    def _refill(self) -> None:
        """Accrue tokens for elapsed wall time (caller holds the lock)."""
        now = self._time_fn()
        elapsed = now - self._updated_at
        if elapsed > 0:
            self._tokens = min(self._capacity, self._tokens + elapsed * self._refill_per_sec)
            self._updated_at = now

    async def acquire(self, tokens: int = 1) -> None:
        """Block until ``tokens`` are available, then consume them.

        Returns immediately when enough tokens have accrued. Holding the lock
        across the wait serializes waiters into FIFO order and guarantees the
        bucket is never oversubscribed by concurrent coroutines.
        """
        if tokens <= 0:
            msg = "tokens must be positive"
            raise ValueError(msg)
        if tokens > self._capacity:
            msg = f"requested {tokens} tokens exceeds bucket capacity {self._capacity}"
            raise ValueError(msg)

        async with self._lock:
            while True:
                self._refill()
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return
                deficit = tokens - self._tokens
                # Sleep just long enough for the deficit to accrue. Computed
                # from time_fn, so a fake clock yields a (near-)zero sleep.
                wait = deficit / self._refill_per_sec
                await asyncio.sleep(wait)
