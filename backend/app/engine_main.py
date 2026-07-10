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

Phase 2c adds the safety spine, all composed into the one ``halted`` hook the
risk stage reads per signal and the execution stage re-checks per submit:
boot-reconcile gating, critical-task liveness, the operator kill switch
(``engine_commands`` ← API/CLI via a Redis poke), and level-based feed health.
The FlattenController owns "flat and protected" as an outcome — the 15:55
window, next-open remediation, and kill-switch flattens are its standing
rules, all driven from broker truth. A Postgres advisory lock makes the
engine a singleton per portfolio: two engines mean two sweepers, two
flatteners, and a market-data 406 fight.

The credentials are intentionally simple here (env paper keys, one portfolio
via ``ENGINE_PORTFOLIO_ID``): later phases drive them from portfolio records.

Run: ``python -m app.engine_main``
"""

from __future__ import annotations

import asyncio
import json
import signal
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import redis.asyncio as aioredis
from redis.exceptions import RedisError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from app.brokers.alpaca.factory import build_alpaca_adapter
from app.brokers.alpaca.rest import AlpacaBrokerAdapter
from app.brokers.alpaca.streams import (
    CHANNEL_FEED_STATUS,
    AlpacaMarketDataConsumer,
    AlpacaTradeUpdatesConsumer,
    feed_health_key,
)
from app.brokers.credentials import BrokerCredentials
from app.core.config import Settings, get_settings
from app.core.database import async_session_factory
from app.core.database import engine as db_engine
from app.core.logging import get_logger, setup_logging
from app.engine.bus import EventBus
from app.engine.commands import EngineCommandSweeper, heartbeat_key
from app.engine.execution import (
    ExecutionStage,
    RedisTradeUpdateSubscriber,
    TradeUpdateStage,
)
from app.engine.feed_health import FeedHealth
from app.engine.flatten_controller import FlattenController
from app.engine.kill_switch import KillSwitch
from app.engine.loader import load_active_strategies
from app.engine.market_bridge import MarketDataBridge
from app.engine.risk.controls import RiskLimits
from app.engine.risk.engine import RiskEngine
from app.engine.risk.state import RiskStateProvider
from app.engine.stage import RiskStage
from app.engine.strategy import StrategyRunner
from app.models.portfolio import Portfolio
from app.services.reconciliation import ReconciliationService

# Importing the strategies package IS the registration step: each module in it
# decorates its class into the process-wide registry the loader resolves kinds
# against.
import app.engine.strategies  # noqa: F401,E402  isort:skip

HEARTBEAT_CHANNEL = "engine:heartbeat"
HEARTBEAT_INTERVAL_SECONDS = 5.0
_BOOT_RECONCILE_RETRY_SECONDS = 30.0

# Phase 1b observation watchlist — two of the most liquid US ETFs, the first
# strategy targets (RESEARCH.md). Phase 2 replaces this constant with the union
# of symbols across active strategies.
DEFAULT_WATCHLIST: tuple[str, ...] = ("SPY", "QQQ")

log = get_logger("engine")


async def _heartbeat_loop(
    redis: aioredis.Redis,
    shutdown: asyncio.Event,
    *,
    key: str,
    task_health: Callable[[], dict[str, bool]],
) -> None:
    """Publish liveness every ``HEARTBEAT_INTERVAL_SECONDS`` — pub/sub AND a TTL key.

    The key (3× TTL headroom) is what the API status endpoint and the live E2E
    read; its payload carries PER-TASK health because the process outliving its
    trading tasks is precisely the failure the operator must be able to see —
    a beating heart with a dead scheduler would otherwise look "up" while the
    15:55 flatten silently isn't coming.
    """
    ttl = int(HEARTBEAT_INTERVAL_SECONDS * 3)
    while not shutdown.is_set():
        payload = json.dumps(
            {
                "timestamp": datetime.now(UTC).isoformat(),
                "status": "running",
                "tasks": task_health(),
            }
        )
        try:
            await redis.publish(HEARTBEAT_CHANNEL, payload)
            await redis.setex(key, ttl, payload)
        except (RedisError, OSError) as exc:
            # Redis down is not fatal — keep beating, it will come back.
            log.warning("engine.heartbeat_failed", error=str(exc))
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=HEARTBEAT_INTERVAL_SECONDS)
        except TimeoutError:
            continue


async def _acquire_engine_lock(portfolio_id: uuid.UUID) -> AsyncConnection | None:
    """Postgres advisory lock: ONE engine per portfolio, enforced, fail-fast.

    Two engines (dev-on-host + container is the realistic accident) mean two
    command sweepers, two flatten drivers, and a second market-data socket the
    broker 406s. The session-scoped lock lives exactly as long as the returned
    connection — hold it until shutdown.
    """
    conn = await db_engine.connect()
    got = await conn.scalar(
        text("SELECT pg_try_advisory_lock(hashtext(:key))"),
        {"key": f"roigen-engine-{portfolio_id}"},
    )
    if not got:
        await conn.close()
        return None
    return conn


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
    settings: Settings, redis: aioredis.Redis, watchlist: tuple[str, ...]
) -> AlpacaMarketDataConsumer | None:
    """Build the market-data consumer from env credentials, or ``None`` if unset."""
    if not (settings.alpaca_api_key and settings.alpaca_secret_key):
        return None
    creds = BrokerCredentials(
        api_key=settings.alpaca_api_key,
        api_secret=settings.alpaca_secret_key,
        paper=True,
    )
    # Normalized to the parsed-UUID string form so the writer's feed-health key
    # and the reader's (FeedHealth, built from the UUID) can never diverge on
    # env-var casing/formatting.
    portfolio_id: str | None = None
    if settings.engine_portfolio_id:
        try:
            portfolio_id = str(uuid.UUID(settings.engine_portfolio_id))
        except ValueError:
            portfolio_id = None  # _build_trading logs and disables trading
    return AlpacaMarketDataConsumer(
        creds,
        redis,
        watchlist,
        feed=settings.alpaca_data_feed,
        # Scopes the feed-health TTL key so a dev engine and a container engine
        # on shared Redis can't report each other's feed as healthy.
        portfolio_id=portfolio_id,
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
    kill_switch: KillSwitch
    feed_health: FeedHealth
    controller: FlattenController
    sweeper: EngineCommandSweeper
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
    kill_switch = KillSwitch(async_session_factory)
    feed_health = FeedHealth(
        redis=redis,
        key=feed_health_key(str(portfolio_id)),
    )

    # THE halt composite (2c complete): no entries before the book is
    # reconciled, after any trading task dies, while the operator kill switch
    # is engaged, or while the market-data feed is stale (game plan principle
    # #5). Read fresh per signal by RiskStage AND re-checked per submit by
    # ExecutionStage (a signal approved just before the flip must not slip
    # through the queue). Flatten deliberately bypasses all of it.
    def halted() -> bool:
        return (
            not boot_reconciled.is_set()
            or any(task.done() for task in critical)
            or kill_switch.is_halted
            or feed_health.is_stale
        )

    risk_stage = RiskStage(
        bus=bus,
        engine=RiskEngine(RiskLimits.from_settings(settings)),
        provider=RiskStateProvider(),
        session_factory=async_session_factory,
        adapter=adapter,
        halted=halted,
    )
    risk_stage.register_handlers()

    execution_stage = ExecutionStage(
        bus=bus,
        session_factory=async_session_factory,
        adapter=adapter,
        halted=halted,
    )
    execution_stage.register_handlers()

    tu_stage = TradeUpdateStage(
        bus=bus,
        session_factory=async_session_factory,
        adapter=adapter,
    )
    subscriber = RedisTradeUpdateSubscriber(redis, portfolio_id, tu_stage)
    tu_consumer = AlpacaTradeUpdatesConsumer(creds, redis, str(portfolio_id))

    controller = FlattenController(
        bus=bus,
        adapter=adapter,
        session_factory=async_session_factory,
        redis=redis,
        portfolio_id=portfolio_id,
        kill_switch=kill_switch,
        boot_reconciled=boot_reconciled,
        flatten_buffer=timedelta(minutes=settings.flatten_buffer_minutes),
    )
    sweeper = EngineCommandSweeper(
        redis=redis,
        session_factory=async_session_factory,
        kill_switch=kill_switch,
        controller=controller,
        boot_reconciled=boot_reconciled,
    )

    return _TradingStack(
        portfolio_id=portfolio_id,
        adapter=adapter,
        bus=bus,
        tu_consumer=tu_consumer,
        subscriber=subscriber,
        kill_switch=kill_switch,
        feed_health=feed_health,
        controller=controller,
        sweeper=sweeper,
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


async def _load_strategy_runner(
    stack: _TradingStack,
) -> tuple[StrategyRunner | None, tuple[str, ...]]:
    """Load active strategies from the DB into a runner; derive the watchlist.

    No active strategies (or a missing portfolio row) is a warning, not an
    error — the engine still runs the observation shell on the default
    watchlist, and the safety spine (kill switch, flatten controller) still
    protects whatever the account holds.
    """
    async with async_session_factory() as session:
        portfolio = await session.get(Portfolio, stack.portfolio_id)
        if portfolio is None:
            log.error("engine.strategies.portfolio_missing", portfolio_id=str(stack.portfolio_id))
            return None, DEFAULT_WATCHLIST
        strategies = await load_active_strategies(
            session, portfolio, bus=stack.bus, session_factory=async_session_factory
        )
    if not strategies:
        log.warning(
            "engine.strategies.none_active",
            portfolio_id=str(stack.portfolio_id),
            detail="no paper/live strategies for this portfolio — observation mode",
        )
        return None, DEFAULT_WATCHLIST
    runner = StrategyRunner(stack.bus)
    for strategy in strategies:
        runner.add(strategy)
    watchlist = tuple(sorted({sym for s in strategies for sym in s.symbols}))
    log.info(
        "engine.strategies.loaded",
        count=len(strategies),
        kinds=sorted({type(s).__name__ for s in strategies}),
        watchlist=list(watchlist),
    )
    return runner, watchlist


async def _strategy_lifecycle_task(
    runner: StrategyRunner, stack: _TradingStack, shutdown: asyncio.Event
) -> None:
    """Register + start strategies only after boot reconciliation.

    Handlers subscribed pre-reconcile would produce signals the halted
    composite rejects — pure audit noise. Parks until shutdown afterward so
    membership in the critical list stays meaningful (a returned task reads as
    dead to the liveness halt), then fans on_stop.
    """
    boot_task = asyncio.create_task(stack.boot_reconciled.wait())
    stop_task = asyncio.create_task(shutdown.wait())
    try:
        await asyncio.wait({boot_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        boot_task.cancel()
        stop_task.cancel()
    if shutdown.is_set():
        return
    runner.register_handlers()
    await runner.start()
    log.info("engine.strategies.started")
    try:
        await shutdown.wait()
    finally:
        with _suppress_cleanup_errors():
            await runner.stop()


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

    trading = _build_trading(settings, redis_client)

    engine_lock: AsyncConnection | None = None
    runner: StrategyRunner | None = None
    watchlist: tuple[str, ...] = DEFAULT_WATCHLIST
    if trading is not None:
        engine_lock = await _acquire_engine_lock(trading.portfolio_id)
        if engine_lock is None:
            log.error(
                "engine.singleton_lock_held",
                portfolio_id=str(trading.portfolio_id),
                detail=(
                    "another engine holds this portfolio's advisory lock — "
                    "refusing to start a second sweeper/flattener; exiting"
                ),
            )
            await redis_client.aclose()
            return
        # The operator's kill-state survives restarts: derive it before any
        # task can act on the default.
        await trading.kill_switch.load()
        runner, watchlist = await _load_strategy_runner(trading)

    consumer = _build_market_data_consumer(settings, redis_client, watchlist)

    tasks: list[asyncio.Task[None]] = [
        asyncio.create_task(_feed_status_logger(redis_client, shutdown), name="feed_status"),
    ]
    if consumer is not None:
        tasks.append(asyncio.create_task(consumer.start(), name="market_data"))
        log.info(
            "engine.market_data.starting",
            symbols=list(watchlist),
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
            asyncio.create_task(trading.sweeper.listen(shutdown), name="commands_listen"),
            asyncio.create_task(trading.sweeper.process(shutdown), name="commands_sweep"),
            asyncio.create_task(trading.controller.run(shutdown), name="flatten_controller"),
            asyncio.create_task(trading.feed_health.run(shutdown), name="feed_health"),
            asyncio.create_task(
                MarketDataBridge(redis_client, trading.bus).run(shutdown), name="market_bridge"
            ),
        ]
        if runner is not None:
            trading_tasks.append(
                asyncio.create_task(
                    _strategy_lifecycle_task(runner, trading, shutdown), name="strategies"
                )
            )
        # Any of these dying blocks new entries (RiskStage.halted reads this list).
        trading.critical.extend(trading_tasks)
        if consumer is not None:
            # The feed's death IS the outage the staleness gate exists for; a
            # dead consumer means a dead watchdog, so treat it as
            # entry-blocking like the rest of the trading spine.
            md_task = next(t for t in tasks if t.get_name() == "market_data")
            trading.critical.append(md_task)
        tasks.extend(trading_tasks)
        log.info("engine.trading.starting", portfolio_id=str(trading.portfolio_id))

    def task_health() -> dict[str, bool]:
        return {t.get_name(): not t.done() for t in (trading.critical if trading else tasks)}

    hb_key = heartbeat_key(str(trading.portfolio_id) if trading else None)
    tasks.append(
        asyncio.create_task(
            _heartbeat_loop(redis_client, shutdown, key=hb_key, task_health=task_health),
            name="heartbeat",
        )
    )

    for task in tasks:
        task.add_done_callback(_log_task_death)

    log.info("engine.started", heartbeat_channel=HEARTBEAT_CHANNEL, heartbeat_key=hb_key)

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
        if engine_lock is not None:
            with _suppress_cleanup_errors():
                await engine_lock.close()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)
        await redis_client.aclose()
        log.info("engine.stopped")


if __name__ == "__main__":
    asyncio.run(main())
