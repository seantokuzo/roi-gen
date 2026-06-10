"""Engine daemon skeleton — the always-on trader's process shell.

Phase 0: publish a heartbeat to Redis every 5s and shut down cleanly on
SIGINT/SIGTERM. Phase 2 builds the real event-driven engine inside this shell.

Run: ``python -m app.engine_main``
"""

import asyncio
import json
import signal
from datetime import UTC, datetime

import redis.asyncio as aioredis
from redis.exceptions import RedisError

from app.core.config import get_settings
from app.core.logging import get_logger, setup_logging

HEARTBEAT_CHANNEL = "engine:heartbeat"
HEARTBEAT_INTERVAL_SECONDS = 5.0


def _heartbeat_payload() -> str:
    return json.dumps(
        {
            "timestamp": datetime.now(UTC).isoformat(),
            "status": "idle",
        }
    )


async def main() -> None:
    settings = get_settings()
    setup_logging(debug=settings.debug)
    log = get_logger("engine")

    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown.set)

    redis_client: aioredis.Redis = aioredis.Redis.from_url(
        settings.redis_url,
        socket_connect_timeout=5.0,
        socket_timeout=5.0,
    )
    log.info(
        "engine.started",
        channel=HEARTBEAT_CHANNEL,
        interval_seconds=HEARTBEAT_INTERVAL_SECONDS,
    )

    try:
        while not shutdown.is_set():
            try:
                await redis_client.publish(HEARTBEAT_CHANNEL, _heartbeat_payload())
            except (RedisError, OSError) as exc:
                # Redis down is not fatal — keep beating, it will come back.
                log.warning("engine.heartbeat_failed", error=str(exc))

            try:
                await asyncio.wait_for(shutdown.wait(), timeout=HEARTBEAT_INTERVAL_SECONDS)
            except TimeoutError:
                continue
    finally:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)
        await redis_client.aclose()
        log.info("engine.stopped")


if __name__ == "__main__":
    asyncio.run(main())
