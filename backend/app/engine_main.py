"""Engine daemon — the always-on trader's process shell.

Phase 0 gave it a heartbeat. Phase 1b makes it run the market-data spine: it
opens the single Alpaca market-data websocket (via
:class:`~app.brokers.alpaca.streams.AlpacaMarketDataConsumer`), fans normalized
bars/quotes/trades out to Redis, and watches for feed staleness. Phase 2 builds
the event bus / strategies / risk / execution on top of this shell.

The watchlist and credentials are intentionally simple here (env paper keys, a
constant symbol list): Phase 2 drives both from active strategies/portfolios.

Run: ``python -m app.engine_main``
"""

from __future__ import annotations

import asyncio
import json
import signal
from datetime import UTC, datetime

import redis.asyncio as aioredis
from redis.exceptions import RedisError

from app.brokers.alpaca.streams import CHANNEL_FEED_STATUS, AlpacaMarketDataConsumer
from app.brokers.credentials import BrokerCredentials
from app.core.config import Settings, get_settings
from app.core.logging import get_logger, setup_logging

HEARTBEAT_CHANNEL = "engine:heartbeat"
HEARTBEAT_INTERVAL_SECONDS = 5.0

# Phase 1b observation watchlist — two of the most liquid US ETFs, the first
# strategy targets (RESEARCH.md). Phase 2 replaces this constant with the union
# of symbols across active strategies.
DEFAULT_WATCHLIST: tuple[str, ...] = ("SPY", "QQQ")

log = get_logger("engine")


async def _heartbeat_loop(redis: aioredis.Redis, shutdown: asyncio.Event) -> None:
    """Publish a liveness heartbeat every ``HEARTBEAT_INTERVAL_SECONDS``."""
    while not shutdown.is_set():
        payload = json.dumps({"timestamp": datetime.now(UTC).isoformat(), "status": "running"})
        try:
            await redis.publish(HEARTBEAT_CHANNEL, payload)
        except (RedisError, OSError) as exc:
            # Redis down is not fatal — keep beating, it will come back.
            log.warning("engine.heartbeat_failed", error=str(exc))
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=HEARTBEAT_INTERVAL_SECONDS)
        except TimeoutError:
            continue


async def _feed_status_logger(redis: aioredis.Redis, shutdown: asyncio.Event) -> None:
    """Surface market-data feed health (stale/ok) into the engine log.

    Low-traffic by design: only watchdog transitions land on
    ``engine:feed_status``. A stale feed during RTH is the signal the risk
    layer will use to block new entries (project gotcha), so it belongs in the
    operator-visible log here.
    """
    pubsub = redis.pubsub()
    try:
        await pubsub.subscribe(CHANNEL_FEED_STATUS)
    except (RedisError, OSError) as exc:
        log.warning("engine.feed_status.subscribe_failed", error=str(exc))
        return
    try:
        while not shutdown.is_set():
            try:
                msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            except (RedisError, OSError) as exc:
                log.warning("engine.feed_status.read_failed", error=str(exc))
                await asyncio.sleep(1.0)
                continue
            if msg is None:
                continue
            data = msg.get("data")
            if isinstance(data, bytes | bytearray):
                data = bytes(data).decode()
            try:
                event = json.loads(data) if isinstance(data, str) else {}
            except json.JSONDecodeError:
                continue
            log.info(
                "engine.feed_status",
                status=event.get("status"),
                feed=event.get("feed"),
                symbols=event.get("symbols"),
            )
    finally:
        with _suppress_cleanup_errors():
            await pubsub.aclose()  # type: ignore[no-untyped-call]  # redis stub gap


def _build_market_data_consumer(
    settings: Settings, redis: aioredis.Redis
) -> AlpacaMarketDataConsumer | None:
    """Build the market-data consumer from env credentials, or ``None`` if unset."""
    if not (settings.alpaca_api_key and settings.alpaca_secret_key):
        return None
    creds = BrokerCredentials(
        api_key=settings.alpaca_api_key,
        api_secret=settings.alpaca_secret_key,
        paper=True,
    )
    return AlpacaMarketDataConsumer(
        creds,
        redis,
        DEFAULT_WATCHLIST,
        feed=settings.alpaca_data_feed,
    )


class _suppress_cleanup_errors:  # noqa: N801 - context-manager helper
    """Swallow shutdown-path errors so cleanup never masks the real exit."""

    def __enter__(self) -> None:
        return None

    def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
        return True


def _log_task_death(task: asyncio.Task[None]) -> None:
    """Surface a background task that dies UNEXPECTEDLY (not on shutdown-cancel).

    Without this, a market-data consumer that crashes mid-session is collected
    by the final ``gather(..., return_exceptions=True)`` and the engine logs a
    clean "stopped" — hiding the fact that the feed went dark.
    """
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        log.error("engine.task_died", task=task.get_name(), error=repr(exc))


async def main() -> None:
    settings = get_settings()
    setup_logging(debug=settings.debug)

    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown.set)

    redis_client: aioredis.Redis = aioredis.Redis.from_url(
        settings.redis_url,
        socket_connect_timeout=5.0,
        socket_timeout=5.0,
    )

    consumer = _build_market_data_consumer(settings, redis_client)
    tasks: list[asyncio.Task[None]] = [
        asyncio.create_task(_heartbeat_loop(redis_client, shutdown), name="heartbeat"),
        asyncio.create_task(_feed_status_logger(redis_client, shutdown), name="feed_status"),
    ]
    if consumer is not None:
        tasks.append(asyncio.create_task(consumer.start(), name="market_data"))
        log.info(
            "engine.market_data.starting",
            symbols=list(DEFAULT_WATCHLIST),
            feed=settings.alpaca_data_feed,
        )
    else:
        log.warning(
            "engine.market_data.disabled",
            reason="ALPACA_API_KEY/ALPACA_SECRET_KEY not set in env",
        )
    for task in tasks:
        task.add_done_callback(_log_task_death)

    log.info("engine.started", heartbeat_channel=HEARTBEAT_CHANNEL)

    try:
        await shutdown.wait()
    finally:
        log.info("engine.stopping")
        if consumer is not None:
            with _suppress_cleanup_errors():
                await consumer.stop()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)
        await redis_client.aclose()
        log.info("engine.stopped")


if __name__ == "__main__":
    asyncio.run(main())
