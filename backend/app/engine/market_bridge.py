"""MarketDataBridge — Redis ``md:*`` fan-out → typed events on the engine bus.

The market-data consumer publishes vendor-neutral DTOs to Redis for every
subscriber (future UI, this engine); strategies consume :class:`BarEvent` &
friends from the in-process bus (the backtest/live parity seam — the simulator
publishes the identical events directly). This bridge is the live-mode glue
between the two, new in 2c because nothing consumed bars until the probe
strategy existed.

Deliberately unbuffered and ungated: a bar that arrives before boot
reconciliation just reaches strategies whose handlers aren't registered yet
(or whose signals the halted composite rejects). Market data is a lossy
telemetry stream, not order-state truth — the trade-updates pipeline is the
one that buffers.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from typing import TYPE_CHECKING

from pydantic import ValidationError

from app.brokers.alpaca.streams import CHANNEL_BAR, CHANNEL_QUOTE, CHANNEL_TRADE
from app.brokers.dto import Bar, Quote, Trade
from app.core.logging import get_logger
from app.engine.events import BarEvent, QuoteEvent, TradeEvent

if TYPE_CHECKING:
    import redis.asyncio as aioredis

    from app.engine.bus import EventBus

log = get_logger("engine.market_bridge")


class MarketDataBridge:
    """Subscribes the ``md:*`` channels and republishes onto the bus."""

    def __init__(self, redis: aioredis.Redis, bus: EventBus) -> None:
        self._redis = redis
        self._bus = bus

    async def run(self, shutdown: asyncio.Event) -> None:
        while not shutdown.is_set():
            pubsub = self._redis.pubsub()
            try:
                await pubsub.subscribe(CHANNEL_BAR, CHANNEL_QUOTE, CHANNEL_TRADE)
                log.info("engine.market_bridge.subscribed")
                async for message in pubsub.listen():
                    if shutdown.is_set():
                        break
                    if message.get("type") != "message":
                        continue
                    await self._dispatch(message.get("data"))
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 — reconnect; md is lossy by contract
                log.exception("engine.market_bridge.error")
                await asyncio.sleep(1.0)
            finally:
                with contextlib.suppress(Exception):
                    await pubsub.aclose()  # type: ignore[no-untyped-call]  # redis stub gap

    async def _dispatch(self, raw: object) -> None:
        try:
            data = raw.decode() if isinstance(raw, bytes | bytearray) else str(raw)
            payload = json.loads(data)
            msg_type = payload.pop("type", None)
            if msg_type == "bar":
                await self._bus.publish(BarEvent(bar=Bar.model_validate(payload)))
            elif msg_type == "quote":
                await self._bus.publish(QuoteEvent(quote=Quote.model_validate(payload)))
            elif msg_type == "trade":
                await self._bus.publish(TradeEvent(trade=Trade.model_validate(payload)))
        except (json.JSONDecodeError, ValidationError, UnicodeDecodeError):
            log.warning("engine.market_bridge.malformed", payload_type=type(raw).__name__)
