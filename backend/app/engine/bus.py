"""In-process FIFO event bus — the engine's deterministic backbone.

One asyncio consumer pulls events off one FIFO queue and dispatches each to the
handlers registered for its concrete type, in registration order, awaiting each
before moving on. That single-consumer, in-order discipline is what makes the
MarketEvent→Signal→Order→Fill cascade reproducible: identical events in an
identical order yield identical decisions, whether they arrived from the live
Redis feeds or the Phase-3 simulator. The bus is the backtest/live parity seam.

Handlers publish downstream events back onto the same queue (a strategy's
``on_bar`` enqueues a ``SignalEvent``), so one tick fans out breadth-first and
every event is fully processed before the next is dequeued. A handler that
raises is logged and isolated — one fault never kills the loop or the daemon.

Dispatch is by **concrete type**: subscribing to ``BarEvent`` receives bars
only, not the ``Event`` base. Handlers that want several event types subscribe
to each (the :class:`~app.engine.strategy.StrategyRunner` does exactly this).
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Awaitable, Callable
from typing import TypeVar, cast

from app.core.logging import get_logger
from app.engine.events import Event

log = get_logger("engine.bus")

E = TypeVar("E", bound=Event)
Handler = Callable[[Event], Awaitable[None]]

# Live-mode poll interval: how often run() checks the shutdown flag while the
# queue is idle. Short enough to stop promptly, long enough to not spin.
_RUN_POLL_SECONDS = 0.5


class EventBus:
    """A FIFO asyncio event queue with typed publish/subscribe."""

    def __init__(self) -> None:
        self._queue: asyncio.Queue[Event] = asyncio.Queue()
        self._handlers: dict[type[Event], list[Handler]] = defaultdict(list)

    def subscribe(self, event_type: type[E], handler: Callable[[E], Awaitable[None]]) -> None:
        """Register ``handler`` for events whose concrete type is ``event_type``."""
        # Safe narrowing: a handler is only ever invoked with an event of its
        # subscribed concrete type (see _dispatch), so the variance is sound.
        self._handlers[event_type].append(cast("Handler", handler))

    async def publish(self, event: Event) -> None:
        """Enqueue ``event`` (FIFO). Processed by drain() or run()."""
        await self._queue.put(event)

    def publish_nowait(self, event: Event) -> None:
        """Non-blocking enqueue — for synchronous producers (e.g. stream callbacks)."""
        self._queue.put_nowait(event)

    async def _dispatch(self, event: Event) -> None:
        for handler in self._handlers.get(type(event), ()):
            try:
                await handler(event)
            except Exception:  # noqa: BLE001 — the bus must survive any handler fault
                log.exception("engine.bus.handler_error", event_type=type(event).__name__)

    async def drain(self) -> None:
        """Process every queued event (and any they enqueue) until the queue empties.

        The settle-a-tick primitive: feed events, drain, observe. Used by tests
        and by the Phase-3 backtest replay to fully resolve one tick before the
        next is fed.
        """
        while not self._queue.empty():
            event = self._queue.get_nowait()
            try:
                await self._dispatch(event)
            finally:
                self._queue.task_done()

    async def run(self, shutdown: asyncio.Event) -> None:
        """Consume events until ``shutdown`` is set (the live-mode loop)."""
        while not shutdown.is_set():
            try:
                event = await asyncio.wait_for(self._queue.get(), timeout=_RUN_POLL_SECONDS)
            except TimeoutError:
                continue
            try:
                await self._dispatch(event)
            finally:
                self._queue.task_done()
