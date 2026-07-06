"""Trade-updates writer — the order-state source of truth, persisted.

The Alpaca trade-updates stream (already normalized to
:class:`~app.brokers.dto.TradeUpdate` and fanned out over Redis by
:class:`~app.brokers.alpaca.streams.AlpacaTradeUpdatesConsumer`) is
authoritative for order lifecycle — never polled (project iron rule). This
module is its consumer: every event becomes DB truth (order row transition,
Fill row, FIFO lot application, Position delta) in one transaction, then a
:class:`~app.engine.events.FillEvent` on the bus.

Discipline (each rule traces to a verified design-review finding):

- **Row locks.** The writer, the ExecutionStage's post-submit apply, and
  reconciliation all mutate order rows from separate tasks; every mutation
  here happens under ``SELECT … FOR UPDATE``.
- **Fills are recorded even on absorbed/stale status** — money truth beats
  status bookkeeping. Dedup is the execution id; effective quantity is clamped
  to the cumulative delta so no interleaving can double-apply lots.
- **Gap detection.** If the broker's cumulative shows quantity our ledger
  never saw (a dropped event during a reconnect), the gap is synthesized
  in-line at the backed-out span price — not silently skipped.
- **Park and retry, then orphan.** A leg fill can beat the commit that
  persists its row (paper fills are near-instant); unmatched events are
  retried briefly before being audited as orphans. True orphans (manual
  orders) are recorded, never adopted.
- **``failed`` is resurrectable.** Broker evidence for a client id we wrote
  off as not-placed means our guess was wrong — adopt reality, loudly.

At-most-once contract: ``FillEvent`` is published after commit and never
replayed; anything needing fill history hydrates from the DB.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from typing import TYPE_CHECKING

from pydantic import ValidationError
from sqlalchemy import func, select

from app.brokers.dto import TradeUpdate
from app.core.logging import get_logger
from app.engine.events import FillEvent
from app.engine.execution.apply import (
    apply_snapshot,
    lock_order_by_broker_id,
    lock_order_by_client_id,
    persist_bracket_legs,
)
from app.engine.execution.lots import apply_fill_to_lots
from app.engine.execution.positions import apply_fill_to_position
from app.engine.execution.state import TransitionPlan, is_absorbing
from app.engine.execution.synthesis import applied_ledger, span_price, synthesize_span
from app.models.enums import EventSource, OrderClass, OrderSide, OrderStatus
from app.models.telemetry import EventLog
from app.models.trading import Fill, Order

if TYPE_CHECKING:
    import uuid
    from collections.abc import Sequence
    from datetime import datetime
    from decimal import Decimal

    import redis.asyncio as aioredis
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from app.brokers.base import BrokerAdapter
    from app.engine.bus import EventBus

log = get_logger("engine.trade_updates")

# Unmatched events (usually a leg fill racing its row's commit) are retried on
# these delays before being declared orphans.
_DEFAULT_MATCH_RETRY_DELAYS: tuple[float, ...] = (0.25, 0.5, 1.0, 2.0)


class TradeUpdateStage:
    """Applies one :class:`TradeUpdate` at a time to DB truth, then the bus."""

    def __init__(
        self,
        *,
        bus: EventBus,
        session_factory: async_sessionmaker[AsyncSession],
        adapter: BrokerAdapter,
        match_retry_delays: Sequence[float] = _DEFAULT_MATCH_RETRY_DELAYS,
    ) -> None:
        self._bus = bus
        self._session_factory = session_factory
        self._adapter = adapter
        self._match_retry_delays = tuple(match_retry_delays)

    async def on_trade_update(self, update: TradeUpdate) -> None:
        """Session-per-event; self-audits — a failed event must not vanish."""
        try:
            await self._process(update)
        except Exception as exc:  # noqa: BLE001 — auditable, never silent
            log.exception(
                "engine.trade_updates.error",
                broker_event=update.event,
                broker_order_id=update.order.broker_order_id,
            )
            await self._audit_error(update, exc)

    async def _process(self, update: TradeUpdate) -> None:
        fill_event: FillEvent | None = None
        async with self._session_factory() as session:
            order = await self._locate(session, update)
            if order is None:
                await self._orphan(session, update)
                await session.commit()
                return

            if order.status == OrderStatus.failed.value:
                # We guessed "not placed"; the broker says otherwise. Adopt.
                log.warning(
                    "engine.trade_updates.failed_resurrected",
                    client_order_id=order.client_order_id,
                    broker_order_id=update.order.broker_order_id,
                )
                order.status = OrderStatus.pending_submit.value
                session.add(
                    EventLog(
                        source=EventSource.broker.value,
                        event_type="order.failed_resurrected",
                        portfolio_id=order.portfolio_id,
                        strategy_id=order.strategy_id,
                        order_id=order.id,
                        payload={
                            "client_order_id": order.client_order_id,
                            "broker_order_id": update.order.broker_order_id,
                            "broker_status": update.order.status.value,
                        },
                    )
                )

            local_status_before = order.status
            fill_event = await self._record_execution(session, order, update)
            plan = apply_snapshot(order, update.order)
            if plan is not TransitionPlan.apply:
                log.info(
                    "engine.trade_updates.snapshot_skipped",
                    client_order_id=order.client_order_id,
                    plan=plan.value,
                    local_status=local_status_before,
                    incoming_status=update.order.status.value,
                )
                session.add(
                    EventLog(
                        source=EventSource.broker.value,
                        event_type="trade_update.snapshot_skipped",
                        portfolio_id=order.portfolio_id,
                        strategy_id=order.strategy_id,
                        order_id=order.id,
                        payload={
                            "plan": plan.value,
                            "event": update.event,
                            "local_status": local_status_before,
                            "incoming_status": update.order.status.value,
                            "local_filled_qty": str(order.filled_qty),
                            "incoming_filled_qty": str(update.order.filled_qty),
                        },
                    )
                )

            if (
                order.order_class == OrderClass.bracket.value
                and order.broker_order_id is not None
                and order.parent_order_id is None
            ):
                await self._ensure_legs(session, order)

            await session.commit()

        if fill_event is not None:
            await self._bus.publish(fill_event)

    # ── Locate / orphan ──────────────────────────────────────────────

    async def _locate(self, session: AsyncSession, update: TradeUpdate) -> Order | None:
        """Find the local row (broker id first, then client id), with retries.

        The retry loop exists for one expected interleaving: a bracket leg's
        first event racing the commit that persists the leg row.
        """
        for attempt, delay in enumerate((0.0, *self._match_retry_delays)):
            if delay:
                await asyncio.sleep(delay)
            order = await lock_order_by_broker_id(session, update.order.broker_order_id)
            if order is None and update.order.client_order_id:
                order = await lock_order_by_client_id(session, update.order.client_order_id)
            if order is not None:
                if attempt:
                    log.info(
                        "engine.trade_updates.matched_after_retry",
                        broker_order_id=update.order.broker_order_id,
                        attempt=attempt,
                    )
                return order
            # Release the (empty) transaction before sleeping so we don't
            # hold anything across the retry delay.
            await session.rollback()
        return None

    async def _orphan(self, session: AsyncSession, update: TradeUpdate) -> None:
        """An order we never placed (manual/foreign). Record it; never adopt."""
        log.warning(
            "engine.trade_updates.orphan",
            broker_event=update.event,
            broker_order_id=update.order.broker_order_id,
            symbol=update.order.symbol,
        )
        session.add(
            EventLog(
                source=EventSource.broker.value,
                event_type="trade_update.orphan",
                payload={
                    "event": update.event,
                    "broker_order_id": update.order.broker_order_id,
                    "client_order_id": update.order.client_order_id,
                    "symbol": update.order.symbol,
                    "status": update.order.status.value,
                    "filled_qty": str(update.order.filled_qty),
                },
            )
        )

    # ── Executions → Fill / lots / position ──────────────────────────

    async def _record_execution(
        self, session: AsyncSession, order: Order, update: TradeUpdate
    ) -> FillEvent | None:
        """Record the event's execution (if any) against the fill ledger.

        Runs regardless of the status-transition plan: a fill is money truth
        even when the status snapshot is stale or the row already absorbed.
        The effective applied quantity is clamped to the cumulative delta, so
        replays and synthesis overlaps can never double-apply lots.

        NON-fill events run gap detection too: a ``canceled``/``expired`` event
        can be the FIRST place a dropped partial fill surfaces (as cumulative
        ``filled_qty`` with no execution attached). Skipping it would adopt an
        absorbing status over an under-applied ledger — quantity permanently
        lost to lots (design review C2).
        """
        if update.qty is None or update.price is None:
            await self._synthesize_gap(
                session,
                order,
                expected_prior=update.order.filled_qty,
                prior_avg=update.order.filled_avg_price,
                occurred_at=update.timestamp,
                broker_cum=update.order.filled_qty,
            )
            return None

        side = OrderSide(order.side)
        fill_key = update.execution_id
        if fill_key is None:
            cum_key = format(update.order.filled_qty.normalize(), "f")
            fill_key = f"noexec-{order.id}-{cum_key}"
            session.add(
                EventLog(
                    source=EventSource.broker.value,
                    event_type="trade_update.fill_missing_execution_id",
                    portfolio_id=order.portfolio_id,
                    strategy_id=order.strategy_id,
                    order_id=order.id,
                    payload={
                        "broker_order_id": update.order.broker_order_id,
                        "synthetic_fill_id": fill_key,
                    },
                )
            )

        already = (
            await session.execute(select(Fill.id).where(Fill.broker_fill_id == fill_key))
        ).scalar_one_or_none()
        if already is not None:
            log.info(
                "engine.trade_updates.duplicate_execution",
                fill_id=fill_key,
                client_order_id=order.client_order_id,
            )
            return None

        cum = update.order.filled_qty

        # Gap: the broker's cumulative minus this execution exceeds what the
        # ledger has seen → events were dropped. Synthesize the gap first.
        await self._synthesize_gap(
            session,
            order,
            expected_prior=cum - update.qty,
            prior_avg=self._prior_avg(update),
            occurred_at=update.timestamp,
            broker_cum=cum,
        )
        applied = await applied_ledger(session, order.id)

        effective = min(update.qty, cum - applied.qty)
        if effective <= 0:
            log.info(
                "engine.trade_updates.execution_already_applied",
                client_order_id=order.client_order_id,
                execution_qty=str(update.qty),
                cumulative=str(cum),
                applied=str(applied.qty),
            )
            return None

        fill = Fill(
            order_id=order.id,
            broker_fill_id=fill_key,
            qty=effective,
            price=update.price,
            occurred_at=update.timestamp,
            raw=update.raw or None,
        )
        session.add(fill)
        await session.flush()

        await apply_fill_to_lots(
            session,
            portfolio_id=order.portfolio_id,
            strategy_id=order.strategy_id,
            symbol=order.symbol,
            side=side,
            qty=effective,
            price=update.price,
            occurred_at=update.timestamp,
            order_id=order.id,
            fill_id=fill.id,
        )
        await apply_fill_to_position(
            session,
            portfolio_id=order.portfolio_id,
            symbol=order.symbol,
            side=side,
            qty=effective,
            price=update.price,
        )

        if is_absorbing(order.status):
            # Money truth recorded against a row status considered final —
            # anomalous (late fill after cancel?), loud, but never dropped.
            session.add(
                EventLog(
                    source=EventSource.broker.value,
                    event_type="trade_update.terminal_fill_anomaly",
                    portfolio_id=order.portfolio_id,
                    strategy_id=order.strategy_id,
                    order_id=order.id,
                    payload={
                        "local_status": order.status,
                        "fill_qty": str(effective),
                        "price": str(update.price),
                    },
                )
            )

        return FillEvent(
            portfolio_id=order.portfolio_id,
            order_id=order.id,
            symbol=order.symbol,
            side=side,
            qty=effective,
            price=update.price,
            occurred_at=update.timestamp,
            position_qty=update.position_qty,
            strategy_id=order.strategy_id,
        )

    async def _synthesize_gap(
        self,
        session: AsyncSession,
        order: Order,
        *,
        expected_prior: Decimal,
        prior_avg: Decimal | None,
        occurred_at: datetime,
        broker_cum: Decimal,
    ) -> None:
        """Back-fill any span the fill ledger never saw, up to ``expected_prior``."""
        applied = await applied_ledger(session, order.id)
        if applied.qty >= expected_prior:
            return
        gap_qty = expected_prior - applied.qty
        price = span_price(
            cum_qty=expected_prior,
            cum_avg_price=prior_avg,
            applied=applied,
            span_qty=gap_qty,
        )
        await synthesize_span(
            session,
            order,
            span_qty=gap_qty,
            price=price,
            occurred_at=occurred_at,
            tag="gap",
            apply_position=True,
        )
        session.add(
            EventLog(
                source=EventSource.engine.value,
                event_type="trade_update.gap_synthesized",
                portfolio_id=order.portfolio_id,
                strategy_id=order.strategy_id,
                order_id=order.id,
                payload={
                    "gap_qty": str(gap_qty),
                    "price": str(price),
                    "broker_cumulative": str(broker_cum),
                },
            )
        )

    @staticmethod
    def _prior_avg(update: TradeUpdate) -> Decimal | None:
        """Cumulative average BEFORE this execution, backed out of the update."""
        cum = update.order.filled_qty
        avg = update.order.filled_avg_price
        if avg is None or update.qty is None or update.price is None:
            return avg
        prior_qty = cum - update.qty
        if prior_qty <= 0:
            return avg
        return (cum * avg - update.qty * update.price) / prior_qty

    # ── Legs / audit ─────────────────────────────────────────────────

    async def _ensure_legs(self, session: AsyncSession, order: Order) -> None:
        """A bracket parent with no child rows: fetch nested and persist legs.

        Converges leg adoption from the stream side, so a parent whose submit
        response was never seen (ambiguous timeout, crash) still gets its
        protective legs tracked before their fills arrive.
        """
        children = (
            await session.execute(
                select(func.count(Order.id)).where(Order.parent_order_id == order.id)
            )
        ).scalar()
        if children:
            return
        if order.broker_order_id is None:  # pragma: no cover — guarded by caller
            return
        try:
            nested = await self._adapter.get_order(order.broker_order_id)
        except Exception as exc:  # noqa: BLE001 — a leg-fetch blip must not roll back the fill
            log.warning(
                "engine.trade_updates.leg_fetch_failed",
                client_order_id=order.client_order_id,
                error=repr(exc),
            )
            return  # the next event or reconcile retries; the fill txn survives
        if nested is None or not nested.legs:
            return
        created = await persist_bracket_legs(session, order, nested)
        if created:
            log.info(
                "engine.trade_updates.legs_adopted",
                client_order_id=order.client_order_id,
                legs=created,
            )

    async def _audit_error(self, update: TradeUpdate, exc: Exception) -> None:
        try:
            async with self._session_factory() as session:
                session.add(
                    EventLog(
                        source=EventSource.engine.value,
                        event_type="trade_update.error",
                        payload={
                            "event": update.event,
                            "broker_order_id": update.order.broker_order_id,
                            "client_order_id": update.order.client_order_id,
                            "error": repr(exc),
                        },
                    )
                )
                await session.commit()
        except Exception:  # noqa: BLE001 — last-ditch; never raise out of the audit path
            log.exception("engine.trade_updates.audit_failed")


class RedisTradeUpdateSubscriber:
    """Bridges the Redis trade-updates channel into :class:`TradeUpdateStage`.

    Subscribes immediately (so no message is missed) but BUFFERS everything
    into an internal queue; processing starts only when :meth:`release` is
    called — after boot reconciliation + synthesis have committed. That
    ordering removes the boot-window interleavings where a live event and the
    boot synthesizer both derive ledger rows from the same execution.
    """

    def __init__(
        self,
        redis: aioredis.Redis,
        portfolio_id: uuid.UUID,
        stage: TradeUpdateStage,
    ) -> None:
        self._redis = redis
        self._channel = f"broker:trade_updates:{portfolio_id}"
        self._stage = stage
        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._ready = asyncio.Event()

    def release(self) -> None:
        """Allow buffered + future events to be processed (post-boot-reconcile)."""
        self._ready.set()

    async def listen(self, shutdown: asyncio.Event) -> None:
        """Receive loop: Redis message → internal buffer. Reconnects forever."""
        while not shutdown.is_set():
            pubsub = self._redis.pubsub()
            try:
                await pubsub.subscribe(self._channel)
                log.info("engine.trade_updates.subscribed", channel=self._channel)
                while not shutdown.is_set():
                    msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                    if msg is None:
                        continue
                    data = msg.get("data")
                    if isinstance(data, bytes | bytearray):
                        data = bytes(data).decode()
                    if isinstance(data, str):
                        self._queue.put_nowait(data)
            except Exception as exc:  # noqa: BLE001 — reconnect, never die silently
                log.warning("engine.trade_updates.listen_error", error=repr(exc))
                await asyncio.sleep(1.0)
            finally:
                with contextlib.suppress(Exception):
                    await pubsub.aclose()  # type: ignore[no-untyped-call]  # redis stub gap

    async def process(self, shutdown: asyncio.Event) -> None:
        """Drain loop: buffer → decode → stage. Gated on :meth:`release`."""
        await self._ready.wait()
        while not (shutdown.is_set() and self._queue.empty()):
            try:
                raw = await asyncio.wait_for(self._queue.get(), timeout=1.0)
            except TimeoutError:
                if shutdown.is_set():
                    return
                continue
            update = self._decode(raw)
            if update is not None:
                await self._stage.on_trade_update(update)

    @staticmethod
    def _decode(raw: str) -> TradeUpdate | None:
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            log.warning("engine.trade_updates.bad_json")
            return None
        if not isinstance(payload, dict) or payload.get("type") != "trade_update":
            return None
        payload.pop("type", None)
        try:
            return TradeUpdate.model_validate(payload)
        except ValidationError as exc:
            log.warning("engine.trade_updates.bad_payload", error=str(exc))
            return None
