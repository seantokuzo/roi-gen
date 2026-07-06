"""Engine daemon — the always-on trader's process shell.

Phase 0 gave it a heartbeat. Phase 1b added the market-data spine (the single
Alpaca market-data websocket fanned out over Redis, with a staleness watchdog).
Phase 2b adds the trading spine: the event bus with the risk and execution
stages registered, the per-portfolio trade-updates pipeline
(websocket → Redis → order-state writer), and broker reconciliation — at boot
(with missed-fill synthesis, BEFORE the writer starts draining) and then
periodically.

Boot ordering matters: the trade-updates subscriber SUBSCRIBES immediately (so
nothing is missed) but buffers; it is released only after boot reconciliation
commits, which removes the interleavings where a live event and the boot
synthesizer both derive ledger rows from the same execution.

The kill switch lands in 2c; until then the risk stage's ``halted`` hook is
wired to critical-task liveness — if any trading task dies, new entries are
blocked rather than trading deaf.

The watchlist and credentials are intentionally simple here (env paper keys, a
constant symbol list, one portfolio via ``ENGINE_PORTFOLIO_ID``): later phases
drive both from active strategies/portfolios.

Run: ``python -m app.engine_main``
"""

from __future__ import annotations

import asyncio
import json
import signal
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime

import redis.asyncio as aioredis
from redis.exceptions import RedisError

from app.brokers.alpaca.factory import build_alpaca_adapter
from app.brokers.alpaca.rest import AlpacaBrokerAdapter
from app.brokers.alpaca.streams import (
    CHANNEL_FEED_STATUS,
    AlpacaMarketDataConsumer,
    AlpacaTradeUpdatesConsumer,
)
from app.brokers.credentials import BrokerCredentials
from app.core.config import Settings, get_settings
from app.core.database import async_session_factory
from app.core.logging import get_logger, setup_logging
from app.engine.bus import EventBus
from app.engine.execution import (
    ExecutionStage,
    RedisTradeUpdateSubscriber,
    TradeUpdateStage,
)
from app.engine.risk.controls import RiskLimits
from app.engine.risk.engine import RiskEngine
from app.engine.risk.state import RiskStateProvider
from app.engine.stage import RiskStage
from app.services.reconciliation import ReconciliationService

HEARTBEAT_CHANNEL = "engine:heartbeat"
HEARTBEAT_INTERVAL_SECONDS = 5.0
_BOOT_RECONCILE_RETRY_SECONDS = 30.0

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


# ── Trading spine (Phase 2b) ─────────────────────────────────────────


@dataclass
class _TradingStack:
    """Everything the trading spine owns; built once per engine process."""

    portfolio_id: uuid.UUID
    adapter: AlpacaBrokerAdapter
    bus: EventBus
    tu_consumer: AlpacaTradeUpdatesConsumer
    subscriber: RedisTradeUpdateSubscriber
    # Set once boot reconciliation commits: the same "no execution before the
    # book is reconciled" gate the fill stream gets from the subscriber's
    # buffer applies to ORDER ENTRY via RiskStage.halted.
    boot_reconciled: asyncio.Event = field(default_factory=asyncio.Event)
    # Tasks whose death must block new entries (feeds RiskStage.halted). The
    # list is populated in main() AFTER task creation; the halted closure built
    # in _build_trading reads it live.
    critical: list[asyncio.Task[None]] = field(default_factory=list)


def _build_trading(settings: Settings, redis: aioredis.Redis) -> _TradingStack | None:
    """Wire bus + risk + execution + trade-updates for one portfolio, or ``None``.

    Execution is opt-in: it needs Alpaca env keys AND ``ENGINE_PORTFOLIO_ID``.
    Without them the engine runs the Phase-1b observation shell unchanged.
    """
    if not settings.engine_portfolio_id:
        log.warning("engine.trading.disabled", reason="ENGINE_PORTFOLIO_ID not set")
        return None
    if not (settings.alpaca_api_key and settings.alpaca_secret_key):
        log.warning("engine.trading.disabled", reason="ALPACA_API_KEY/SECRET_KEY not set")
        return None
    try:
        portfolio_id = uuid.UUID(settings.engine_portfolio_id)
    except ValueError:
        log.error(
            "engine.trading.disabled",
            reason="ENGINE_PORTFOLIO_ID is not a valid UUID",
            value=settings.engine_portfolio_id,
        )
        return None

    creds = BrokerCredentials(
        api_key=settings.alpaca_api_key,
        api_secret=settings.alpaca_secret_key,
        paper=True,  # live-mode wiring is a later phase (encrypted DB creds)
    )
    adapter = build_alpaca_adapter(creds)
    bus = EventBus()
    critical: list[asyncio.Task[None]] = []
    boot_reconciled = asyncio.Event()

    risk_stage = RiskStage(
        bus=bus,
        engine=RiskEngine(RiskLimits.from_settings(settings)),
        provider=RiskStateProvider(),
        session_factory=async_session_factory,
        adapter=adapter,
        # Halted until boot reconciliation commits (an unreconciled book must
        # not source risk state), and halted again if any trading task dies
        # (block new entries instead of trading deaf). 2c layers the kill
        # switch on this same hook.
        halted=lambda: not boot_reconciled.is_set() or any(task.done() for task in critical),
    )
    risk_stage.register_handlers()

    execution_stage = ExecutionStage(
        bus=bus,
        session_factory=async_session_factory,
        adapter=adapter,
    )
    execution_stage.register_handlers()

    tu_stage = TradeUpdateStage(
        bus=bus,
        session_factory=async_session_factory,
        adapter=adapter,
    )
    subscriber = RedisTradeUpdateSubscriber(redis, portfolio_id, tu_stage)
    tu_consumer = AlpacaTradeUpdatesConsumer(creds, redis, str(portfolio_id))

    return _TradingStack(
        portfolio_id=portfolio_id,
        adapter=adapter,
        bus=bus,
        tu_consumer=tu_consumer,
        subscriber=subscriber,
        boot_reconciled=boot_reconciled,
        critical=critical,
    )


async def _reconcile_task(
    stack: _TradingStack, settings: Settings, shutdown: asyncio.Event
) -> None:
    """Boot reconcile (with missed-fill synthesis) → release the writer → periodic.

    Boot reconciliation retries until it succeeds: trading state derived from
    an unreconciled book is exactly the legacy failure mode this phase exists
    to kill. The trade-updates subscriber buffers until :meth:`release`.
    """
    service = ReconciliationService()

    async def _run_once() -> None:
        async with async_session_factory() as session:
            await service.reconcile_portfolio(
                session,
                stack.portfolio_id,
                stack.adapter,
                synthesize_fills=True,
            )
            await session.commit()

    while not shutdown.is_set():
        try:
            await _run_once()
            break
        except Exception as exc:  # noqa: BLE001 — retry loop; every failure is logged
            log.error("engine.reconcile.boot_failed", error=repr(exc))
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=_BOOT_RECONCILE_RETRY_SECONDS)
            except TimeoutError:
                continue
    if shutdown.is_set():
        return

    stack.subscriber.release()  # fill stream may drain
    stack.boot_reconciled.set()  # order entry may proceed (RiskStage un-halts)
    log.info("engine.reconcile.boot_complete", portfolio_id=str(stack.portfolio_id))

    interval = settings.reconcile_interval_seconds
    if interval <= 0:
        # Boot-only mode: park until shutdown (this task is liveness-critical;
        # returning would trip the halted() guard and block all entries).
        await shutdown.wait()
        return
    while not shutdown.is_set():
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=float(interval))
            return
        except TimeoutError:
            pass
        try:
            await _run_once()
        except Exception as exc:  # noqa: BLE001 — periodic repair; log and try next cycle
            log.error("engine.reconcile.periodic_failed", error=repr(exc))


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
    trading = _build_trading(settings, redis_client)

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

    if trading is not None:
        trading_tasks = [
            asyncio.create_task(trading.bus.run(shutdown), name="event_bus"),
            asyncio.create_task(trading.tu_consumer.start(), name="trade_updates_ws"),
            asyncio.create_task(trading.subscriber.listen(shutdown), name="trade_updates_listen"),
            asyncio.create_task(trading.subscriber.process(shutdown), name="trade_updates_write"),
            asyncio.create_task(_reconcile_task(trading, settings, shutdown), name="reconcile"),
        ]
        # Any of these dying blocks new entries (RiskStage.halted reads this list).
        trading.critical.extend(trading_tasks)
        tasks.extend(trading_tasks)
        log.info("engine.trading.starting", portfolio_id=str(trading.portfolio_id))

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
        if trading is not None:
            with _suppress_cleanup_errors():
                await trading.tu_consumer.stop()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if trading is not None:
            with _suppress_cleanup_errors():
                await trading.adapter.aclose()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)
        await redis_client.aclose()
        log.info("engine.stopped")


if __name__ == "__main__":
    asyncio.run(main())
