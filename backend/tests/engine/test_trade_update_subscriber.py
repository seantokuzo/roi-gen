"""RedisTradeUpdateSubscriber: the buffer/release boot gate and decode hygiene.

The release gate is a money-path control: it removes the boot-window
interleavings where a live stream event and the boot synthesizer both derive
ledger rows from the same execution. It must actually hold events back.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import TYPE_CHECKING, Any

from app.brokers.dto import TradeUpdate
from app.engine.execution.trade_updates import RedisTradeUpdateSubscriber
from tests.engine.builders import make_trade_update

if TYPE_CHECKING:
    from decimal import Decimal


class _StageSpy:
    """Records what the drain loop delivers (stands in for TradeUpdateStage)."""

    def __init__(self) -> None:
        self.received: list[TradeUpdate] = []

    async def on_trade_update(self, update: TradeUpdate) -> None:
        self.received.append(update)


def _subscriber(stage: _StageSpy) -> RedisTradeUpdateSubscriber:
    # No Redis needed: listen() is transport; these tests drive the internal
    # queue + gate directly (the seam the boot ordering depends on).
    return RedisTradeUpdateSubscriber(
        redis=None,  # type: ignore[arg-type]  # transport unused in these tests
        portfolio_id=uuid.uuid4(),
        stage=stage,  # type: ignore[arg-type]  # protocol-compatible spy
    )


def _wire_message(**over: Any) -> str:
    payload = make_trade_update(**over).model_dump(mode="json")
    payload["type"] = "trade_update"
    return json.dumps(payload)


async def test_events_are_buffered_until_release() -> None:
    stage = _StageSpy()
    sub = _subscriber(stage)
    shutdown = asyncio.Event()

    sub._queue.put_nowait(_wire_message())
    task = asyncio.create_task(sub.process(shutdown))
    await asyncio.sleep(0.05)
    assert stage.received == []  # gated: boot reconcile hasn't committed yet

    sub.release()
    await asyncio.sleep(0.05)
    assert len(stage.received) == 1  # buffered event delivered after release

    shutdown.set()
    await asyncio.wait_for(task, timeout=3.0)


async def test_decode_round_trips_the_wire_format() -> None:
    # The consumer publishes model_dump(mode="json") (Decimals as strings) —
    # the subscriber must rehydrate through the DTO, not raw-parse numerics.
    raw = _wire_message()
    update = RedisTradeUpdateSubscriber._decode(raw)
    assert update is not None
    original = make_trade_update()
    qty: Decimal | None = update.qty
    assert qty == original.qty
    assert update.order.filled_qty == original.order.filled_qty
    assert update.order.broker_order_id == original.order.broker_order_id


async def test_decode_rejects_garbage_without_raising() -> None:
    assert RedisTradeUpdateSubscriber._decode("not json") is None
    assert RedisTradeUpdateSubscriber._decode(json.dumps({"type": "other"})) is None
    assert RedisTradeUpdateSubscriber._decode(json.dumps(["not", "a", "dict"])) is None
    assert (
        RedisTradeUpdateSubscriber._decode(json.dumps({"type": "trade_update", "bogus": 1})) is None
    )


async def test_process_drains_backlog_then_stops_on_shutdown() -> None:
    stage = _StageSpy()
    sub = _subscriber(stage)
    shutdown = asyncio.Event()
    for _ in range(3):
        sub._queue.put_nowait(_wire_message())
    sub.release()

    task = asyncio.create_task(sub.process(shutdown))
    await asyncio.sleep(0.05)
    shutdown.set()
    await asyncio.wait_for(task, timeout=3.0)
    assert len(stage.received) == 3  # backlog fully drained before exit
