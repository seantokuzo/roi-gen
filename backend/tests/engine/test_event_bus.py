"""EventBus: deterministic FIFO dispatch, type isolation, fault isolation."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from app.engine.bus import EventBus
from app.engine.events import Event


@dataclass(frozen=True, slots=True)
class _A(Event):
    n: int


@dataclass(frozen=True, slots=True)
class _B(Event):
    n: int


async def test_drain_dispatches_to_subscribed_handler() -> None:
    bus = EventBus()
    seen: list[int] = []

    async def handler(event: _A) -> None:
        seen.append(event.n)

    bus.subscribe(_A, handler)
    await bus.publish(_A(1))
    await bus.publish(_A(2))
    await bus.drain()

    assert seen == [1, 2]


async def test_publish_nowait_enqueues() -> None:
    bus = EventBus()
    seen: list[int] = []

    async def handler(event: _A) -> None:
        seen.append(event.n)

    bus.subscribe(_A, handler)
    bus.publish_nowait(_A(5))  # synchronous producer path (e.g. stream callbacks)
    await bus.drain()

    assert seen == [5]


async def test_dispatch_is_isolated_by_concrete_type() -> None:
    bus = EventBus()
    a_seen: list[int] = []
    b_seen: list[int] = []

    async def on_a(event: _A) -> None:
        a_seen.append(event.n)

    async def on_b(event: _B) -> None:
        b_seen.append(event.n)

    bus.subscribe(_A, on_a)
    bus.subscribe(_B, on_b)
    await bus.publish(_A(1))
    await bus.publish(_B(2))
    await bus.drain()

    assert a_seen == [1]
    assert b_seen == [2]


async def test_fifo_order_preserved() -> None:
    bus = EventBus()
    order: list[int] = []

    async def handler(event: _A) -> None:
        order.append(event.n)

    bus.subscribe(_A, handler)
    for i in range(5):
        await bus.publish(_A(i))
    await bus.drain()

    assert order == [0, 1, 2, 3, 4]


async def test_handler_publishing_downstream_event_runs_after_current() -> None:
    bus = EventBus()
    chain: list[tuple[str, int]] = []

    async def on_a(event: _A) -> None:
        chain.append(("a", event.n))
        await bus.publish(_B(event.n * 10))

    async def on_b(event: _B) -> None:
        chain.append(("b", event.n))

    bus.subscribe(_A, on_a)
    bus.subscribe(_B, on_b)
    await bus.publish(_A(1))
    await bus.drain()

    # The cascade is breadth-first: A is fully handled (enqueuing B) before B runs.
    assert chain == [("a", 1), ("b", 10)]


async def test_multiple_handlers_run_in_registration_order() -> None:
    bus = EventBus()
    calls: list[int] = []

    async def first(event: _A) -> None:
        calls.append(1)

    async def second(event: _A) -> None:
        calls.append(2)

    bus.subscribe(_A, first)
    bus.subscribe(_A, second)
    await bus.publish(_A(0))
    await bus.drain()

    assert calls == [1, 2]


async def test_handler_exception_is_isolated_and_bus_survives() -> None:
    bus = EventBus()
    good: list[int] = []

    async def boom(event: _A) -> None:
        raise RuntimeError("handler fault")

    async def ok(event: _A) -> None:
        good.append(event.n)

    bus.subscribe(_A, boom)
    bus.subscribe(_A, ok)
    await bus.publish(_A(7))
    await bus.drain()  # must not raise

    # The good handler still runs even though the prior one raised.
    assert good == [7]


async def test_run_consumes_until_shutdown() -> None:
    bus = EventBus()
    got: list[int] = []
    seen = asyncio.Event()

    async def handler(event: _A) -> None:
        got.append(event.n)
        seen.set()

    bus.subscribe(_A, handler)
    shutdown = asyncio.Event()
    task = asyncio.create_task(bus.run(shutdown))
    try:
        await bus.publish(_A(99))
        await asyncio.wait_for(seen.wait(), timeout=2.0)
    finally:
        shutdown.set()
        await asyncio.wait_for(task, timeout=2.0)

    assert got == [99]
