"""Alpaca streaming consumers — the real-time market-data + trade-updates spine.

Two long-lived async consumers, each owning exactly one Alpaca websocket:

* :class:`AlpacaMarketDataConsumer` — the *single* market-data socket for an
  account. Alpaca permits only ONE concurrent market-data connection per
  account key; a second connection is rejected with error code 406
  (``connection limit exceeded``). One process, one consumer, one socket — the
  fan-out to many readers happens over Redis, never by opening more sockets.
* :class:`AlpacaTradeUpdatesConsumer` — the trade-updates socket for one
  trading account (a different endpoint, paper vs. live). This stream is the
  order-state source of truth (project CLAUDE.md): order lifecycle is read from
  here and **never** polled.

Both normalize alpaca-py's vendor message objects into our broker-agnostic DTOs
(:mod:`app.brokers.dto`) and publish them as JSON to Redis pub/sub channels for
the engine and the UI fan-out. Money/prices/sizes are :class:`~decimal.Decimal`
built from ``str(...)`` (iron law #7 — never construct a Decimal from a float)
and timestamps are coerced to timezone-aware UTC (iron law #5).

Embedding note (the reason we don't call alpaca-py's ``.run()``): alpaca-py's
``StockDataStream.run()`` / ``TradingStream.run()`` are *synchronous* and call
``asyncio.run(self._run_forever())`` internally — they start and own a brand new
event loop. That is wrong inside our already-running asyncio daemon (you cannot
nest ``asyncio.run``). The async-native entry point is the coroutine
``_run_forever()`` (it calls ``asyncio.get_running_loop()`` and attaches to the
*current* loop); the async-native stop is the coroutine ``stop_ws()`` (which
signals via the stream's internal ``_stop_stream_queue``). We supervise
``_run_forever()`` ourselves so we control reconnect backoff and logging — and
so a fatal early-return inside alpaca-py (e.g. its silent return on an
"insufficient subscription" ``ValueError``) becomes a logged reconnect rather
than a dead stream.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Protocol

from app.brokers.alpaca.status import map_status
from app.brokers.dto import Bar, BrokerOrder, Quote, Trade, TradeUpdate
from app.core.logging import get_logger
from app.models.enums import (
    OrderClass,
    OrderSide,
    OrderType,
    TimeInForce,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable

    from pydantic import BaseModel

    from app.brokers.credentials import BrokerCredentials

log = get_logger("broker.alpaca.streams")

# ── Redis channel names ──────────────────────────────────────────────
# Market-data channels are account-global (one consumer per process); the
# symbol travels inside the JSON payload. Trade-updates are per-portfolio so a
# subscriber can listen to exactly the account it owns.
CHANNEL_BAR = "md:bar"
CHANNEL_QUOTE = "md:quote"
CHANNEL_TRADE = "md:trade"
CHANNEL_FEED_STATUS = "engine:feed_status"


def _trade_updates_channel(portfolio_id: str) -> str:
    """Per-portfolio trade-updates channel: ``broker:trade_updates:{id}``."""
    return f"broker:trade_updates:{portfolio_id}"


# ── Reconnect / watchdog tuning ──────────────────────────────────────
_BACKOFF_BASE_SECONDS = 1.0
_BACKOFF_CAP_SECONDS = 30.0
_DEFAULT_STALENESS_SECONDS = 30.0


# Order-status translation is shared with the REST adapter — one source of
# truth (``app.brokers.alpaca.status.map_status``) keeps the trade-updates
# stream and REST reads byte-identical.


# ── Normalization helpers ────────────────────────────────────────────


def _to_utc(value: datetime) -> datetime:
    """Coerce a datetime to timezone-aware UTC (iron law #5).

    alpaca-py yields aware UTC timestamps; a naive datetime is treated as UTC
    rather than guessed-at, and any other zone is converted.
    """
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _dec(value: Any) -> Decimal:
    """Build a :class:`Decimal` via ``str`` so a float never poisons precision."""
    return Decimal(str(value))


def _opt_dec(value: Any) -> Decimal | None:
    """``_dec`` but pass ``None`` through (for nullable price/qty fields)."""
    return None if value is None else _dec(value)


def _bar_from_alpaca(msg: Any) -> Bar:
    """Normalize an alpaca-py ``Bar`` → our :class:`~app.brokers.dto.Bar`."""
    return Bar(
        symbol=msg.symbol,
        timestamp=_to_utc(msg.timestamp),
        open=_dec(msg.open),
        high=_dec(msg.high),
        low=_dec(msg.low),
        close=_dec(msg.close),
        volume=_dec(msg.volume),
        trade_count=None if msg.trade_count is None else int(msg.trade_count),
        vwap=_opt_dec(msg.vwap),
    )


def _quote_from_alpaca(msg: Any) -> Quote:
    """Normalize an alpaca-py ``Quote`` → our :class:`~app.brokers.dto.Quote`."""
    return Quote(
        symbol=msg.symbol,
        timestamp=_to_utc(msg.timestamp),
        bid_price=_dec(msg.bid_price),
        bid_size=_dec(msg.bid_size),
        ask_price=_dec(msg.ask_price),
        ask_size=_dec(msg.ask_size),
    )


def _trade_from_alpaca(msg: Any) -> Trade:
    """Normalize an alpaca-py ``Trade`` → our :class:`~app.brokers.dto.Trade`."""
    return Trade(
        symbol=msg.symbol,
        timestamp=_to_utc(msg.timestamp),
        price=_dec(msg.price),
        size=_dec(msg.size),
    )


def _order_from_alpaca(order: Any) -> BrokerOrder:
    """Normalize an alpaca-py ``Order`` → our :class:`BrokerOrder` (recursive).

    Mirrors the REST adapter's conventions so the stream and the REST reads
    produce identical order shapes. ``type`` supersedes the deprecated
    ``order_type`` on the vendor model; bracket/OCO children arrive in ``legs``.
    """
    raw_type = order.type if order.type is not None else order.order_type
    return BrokerOrder(
        broker_order_id=str(order.id),
        client_order_id=order.client_order_id,
        symbol=order.symbol,
        side=OrderSide(str(order.side)),
        order_type=OrderType(str(raw_type)),
        order_class=OrderClass(str(order.order_class)),
        time_in_force=TimeInForce(str(order.time_in_force)),
        status=map_status(str(order.status)),
        qty=_opt_dec(order.qty),
        filled_qty=_dec(order.filled_qty) if order.filled_qty is not None else Decimal("0"),
        limit_price=_opt_dec(order.limit_price),
        stop_price=_opt_dec(order.stop_price),
        trail_percent=_opt_dec(order.trail_percent),
        filled_avg_price=_opt_dec(order.filled_avg_price),
        extended_hours=bool(order.extended_hours),
        submitted_at=None if order.submitted_at is None else _to_utc(order.submitted_at),
        filled_at=None if order.filled_at is None else _to_utc(order.filled_at),
        canceled_at=None if order.canceled_at is None else _to_utc(order.canceled_at),
        legs=[_order_from_alpaca(leg) for leg in (order.legs or [])],
    )


def _trade_update_from_alpaca(msg: Any) -> TradeUpdate:
    """Normalize an alpaca-py ``TradeUpdate`` → our :class:`TradeUpdate`."""
    return TradeUpdate(
        event=str(msg.event),
        order=_order_from_alpaca(msg.order),
        execution_id=None if msg.execution_id is None else str(msg.execution_id),
        price=_opt_dec(msg.price),
        qty=_opt_dec(msg.qty),
        position_qty=_opt_dec(msg.position_qty),
        timestamp=_to_utc(msg.timestamp),
    )


# ── Injectable surfaces (so tests never open a socket / hit Redis) ───


class _RedisLike(Protocol):
    """The slice of ``redis.asyncio.Redis`` the consumers use.

    Declared as a Protocol so tests pass a trivial capture-only fake and so the
    consumers never hard-depend on the redis package at type-check time.

    ``publish`` is typed as returning an :class:`~collections.abc.Awaitable`
    rather than as ``async def`` so BOTH the real ``redis.asyncio.Redis``
    (whose ``publish`` returns ``Awaitable[int]``) and an ``async def`` fake
    satisfy it structurally.
    """

    def publish(self, channel: str, message: str) -> Awaitable[Any]: ...


class _MarketStreamLike(Protocol):
    """The alpaca-py ``StockDataStream`` surface we drive."""

    def subscribe_bars(self, handler: Callable[[Any], Awaitable[None]], *symbols: str) -> None: ...
    def subscribe_quotes(
        self, handler: Callable[[Any], Awaitable[None]], *symbols: str
    ) -> None: ...
    def subscribe_trades(
        self, handler: Callable[[Any], Awaitable[None]], *symbols: str
    ) -> None: ...
    async def _run_forever(self) -> None: ...
    async def stop_ws(self) -> None: ...


class _TradeStreamLike(Protocol):
    """The alpaca-py ``TradingStream`` surface we drive."""

    def subscribe_trade_updates(self, handler: Callable[[Any], Awaitable[None]]) -> None: ...
    async def _run_forever(self) -> None: ...
    async def stop_ws(self) -> None: ...


# A factory builds the concrete stream lazily, so the default factory only
# imports alpaca-py when a real consumer starts — tests inject a fake factory
# and the SDK is never touched.


def _default_market_stream_factory(creds: BrokerCredentials, feed: str) -> _MarketStreamLike:
    """Construct a real alpaca-py ``StockDataStream`` (imported lazily)."""
    from alpaca.data.enums import DataFeed
    from alpaca.data.live.stock import StockDataStream

    return StockDataStream(
        api_key=creds.api_key,
        secret_key=creds.api_secret,
        feed=DataFeed(feed),
    )


def _default_trade_stream_factory(creds: BrokerCredentials) -> _TradeStreamLike:
    """Construct a real alpaca-py ``TradingStream`` (imported lazily)."""
    from alpaca.trading.stream import TradingStream

    return TradingStream(
        api_key=creds.api_key,
        secret_key=creds.api_secret,
        paper=creds.paper,
    )


# ── Shared supervisor base ───────────────────────────────────────────


class _SupervisedStreamConsumer:
    """Reconnect-with-backoff supervisor shared by both consumers.

    Subclasses build + subscribe a fresh vendor stream in :meth:`_make_stream`;
    this base owns the run/stop lifecycle and the exponential backoff. We drive
    the vendor stream's *async* entry point (``_run_forever``) directly so it
    attaches to our running loop rather than spawning its own via ``.run()``.
    """

    def __init__(
        self,
        *,
        backoff_base_seconds: float = _BACKOFF_BASE_SECONDS,
        backoff_cap_seconds: float = _BACKOFF_CAP_SECONDS,
    ) -> None:
        self._stream: Any = None
        self._stop_requested = False
        self._backoff_base = backoff_base_seconds
        self._backoff_cap = backoff_cap_seconds
        self._backoff = backoff_base_seconds

    def _make_stream(self) -> Any:  # pragma: no cover - overridden
        raise NotImplementedError

    async def start(self) -> None:
        """Run the stream until :meth:`stop` (or a non-recoverable cancel).

        Each iteration builds a fresh subscribed stream and awaits its async
        ``_run_forever``. A normal return or any exception triggers a logged,
        backed-off reconnect; cancellation propagates out cleanly.
        """
        self._stop_requested = False
        while not self._stop_requested:
            self._stream = self._make_stream()
            try:
                await self._stream._run_forever()
            except asyncio.CancelledError:
                await self._safe_stop_stream()
                raise
            except Exception as exc:  # noqa: BLE001 - supervisor must not die
                if self._stop_requested:
                    break
                await self._on_disconnect(reason=repr(exc))
                continue
            # Clean return from _run_forever: either we asked it to stop, or the
            # SDK bailed (e.g. its silent return on an insufficient-subscription
            # ValueError). If we didn't request it, treat as a disconnect.
            if self._stop_requested:
                break
            await self._on_disconnect(reason="stream returned unexpectedly")
        await self._safe_stop_stream()

    async def _on_disconnect(self, *, reason: str) -> None:
        """Log the disconnect and sleep the current backoff, then double it."""
        log.warning(
            "alpaca.stream.reconnect",
            consumer=type(self).__name__,
            reason=reason,
            backoff_seconds=self._backoff,
        )
        await self._safe_stop_stream()
        await asyncio.sleep(self._backoff)
        self._backoff = min(self._backoff * 2, self._backoff_cap)

    def _reset_backoff(self) -> None:
        """Reset backoff to base after a healthy data point."""
        self._backoff = self._backoff_base

    async def _safe_stop_stream(self) -> None:
        """Best-effort close of the current vendor stream (never raises)."""
        if self._stream is None:
            return
        with contextlib.suppress(Exception):
            await self._stream.stop_ws()
        self._stream = None

    async def stop(self) -> None:
        """Request a graceful shutdown; idempotent."""
        self._stop_requested = True
        await self._safe_stop_stream()


# ── Market-data consumer ─────────────────────────────────────────────


class AlpacaMarketDataConsumer(_SupervisedStreamConsumer):
    """Owns the single Alpaca market-data websocket for one account.

    Subscribes to minute bars (always) and optionally quotes and trades for the
    configured symbols, normalizes each message to our DTOs, and publishes them
    as JSON to Redis (``md:bar`` / ``md:quote`` / ``md:trade``).

    Alpaca allows only ONE market-data connection per account key — a second
    concurrent connection is rejected with code 406. Keep exactly one instance
    of this consumer alive per account; everything else reads from Redis.

    A staleness watchdog publishes a ``feed_stale`` status to
    ``engine:feed_status`` when no market-data message has arrived for
    ``staleness_seconds`` (default 30) while the feed is expected live — the
    signal the risk layer uses to block new entries during a feed blackout
    (project gotcha: RTH feed silence ⇒ block entries). It clears with
    ``feed_ok`` as soon as data resumes.
    """

    def __init__(
        self,
        credentials: BrokerCredentials,
        redis: _RedisLike,
        symbols: Iterable[str],
        *,
        feed: str = "iex",
        subscribe_quotes: bool = False,
        subscribe_trades: bool = False,
        staleness_seconds: float = _DEFAULT_STALENESS_SECONDS,
        watchdog_poll_seconds: float | None = None,
        backoff_base_seconds: float = _BACKOFF_BASE_SECONDS,
        backoff_cap_seconds: float = _BACKOFF_CAP_SECONDS,
        stream_factory: Callable[[BrokerCredentials, str], _MarketStreamLike] | None = None,
    ) -> None:
        super().__init__(
            backoff_base_seconds=backoff_base_seconds,
            backoff_cap_seconds=backoff_cap_seconds,
        )
        self._credentials = credentials
        self._redis = redis
        self._symbols = list(symbols)
        self._feed = feed
        self._want_quotes = subscribe_quotes
        self._want_trades = subscribe_trades
        self._staleness_seconds = staleness_seconds
        # Poll a fraction of the window so detection latency is bounded; capped
        # at 5s so a long staleness window still reacts promptly. Injectable so
        # tests can drive it fast.
        self._watchdog_poll = (
            watchdog_poll_seconds
            if watchdog_poll_seconds is not None
            else min(max(staleness_seconds / 5, 0.01), 5.0)
        )
        self._stream_factory = stream_factory or _default_market_stream_factory

        # Watchdog state.
        self._last_msg_at: float | None = None
        self._feed_stale = False
        self._watchdog_task: asyncio.Task[None] | None = None

    # -- stream construction --------------------------------------------------

    def _make_stream(self) -> _MarketStreamLike:
        stream = self._stream_factory(self._credentials, self._feed)
        stream.subscribe_bars(self._on_bar, *self._symbols)
        if self._want_quotes:
            stream.subscribe_quotes(self._on_quote, *self._symbols)
        if self._want_trades:
            stream.subscribe_trades(self._on_trade, *self._symbols)
        return stream

    # -- lifecycle (wrap base to run the watchdog alongside) ------------------

    async def start(self) -> None:
        """Start the stream supervisor and the staleness watchdog together."""
        await self._mark_alive()
        self._watchdog_task = asyncio.create_task(self._watchdog_loop())
        try:
            await super().start()
        finally:
            await self._cancel_watchdog()

    async def stop(self) -> None:
        await super().stop()
        await self._cancel_watchdog()

    async def _cancel_watchdog(self) -> None:
        task = self._watchdog_task
        self._watchdog_task = None
        if task is None:
            return
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    # -- handlers -------------------------------------------------------------

    async def _on_bar(self, msg: Any) -> None:
        await self._mark_alive()
        await self._publish(CHANNEL_BAR, "bar", _bar_from_alpaca(msg))

    async def _on_quote(self, msg: Any) -> None:
        await self._mark_alive()
        await self._publish(CHANNEL_QUOTE, "quote", _quote_from_alpaca(msg))

    async def _on_trade(self, msg: Any) -> None:
        await self._mark_alive()
        await self._publish(CHANNEL_TRADE, "trade", _trade_from_alpaca(msg))

    async def _publish(self, channel: str, msg_type: str, dto: BaseModel) -> None:
        """Publish ``dto`` as JSON to ``channel`` with a ``type`` discriminator.

        The DTOs are vendor-neutral value objects with no ``type`` field of
        their own, so we serialize via ``model_dump(mode="json")`` (which turns
        Decimal→str and datetime→ISO-8601) and add ``type`` for the engine's
        message switch — keeping the wire payload self-describing per channel.
        """
        payload = dto.model_dump(mode="json")
        payload["type"] = msg_type
        await self._redis.publish(channel, json.dumps(payload))

    # -- staleness watchdog ---------------------------------------------------

    async def _mark_alive(self) -> None:
        """Record a fresh data point; clear staleness + reset backoff.

        If the feed was flagged stale, publish a ``feed_ok`` clear so the risk
        layer can re-enable entries the moment data resumes.
        """
        self._last_msg_at = self._now()
        self._reset_backoff()
        if self._feed_stale:
            self._feed_stale = False
            await self._publish_feed_status("feed_ok")

    async def _watchdog_loop(self) -> None:
        """Emit ``feed_stale`` once no data has arrived for the timeout window.

        Re-checks on a fraction of the timeout so detection latency is bounded;
        clearing back to ``feed_ok`` is handled inline by :meth:`_mark_alive`
        the instant data resumes.
        """
        while True:
            await asyncio.sleep(self._watchdog_poll)
            last = self._last_msg_at
            if last is None:
                continue
            stale = (self._now() - last) >= self._staleness_seconds
            if stale and not self._feed_stale:
                self._feed_stale = True
                log.error(
                    "alpaca.feed.stale",
                    seconds_since_data=self._now() - last,
                    threshold=self._staleness_seconds,
                    symbols=self._symbols,
                )
                await self._publish_feed_status("feed_stale")

    async def _publish_feed_status(self, status: str) -> None:
        """Publish a feed status object to ``engine:feed_status``."""
        payload = {
            "type": "feed_status",
            "status": status,
            "feed": self._feed,
            "symbols": self._symbols,
            "timestamp": self._now_iso(),
        }
        await self._redis.publish(CHANNEL_FEED_STATUS, json.dumps(payload))

    # -- clock (overridable in tests) -----------------------------------------

    def _now(self) -> float:
        """Monotonic-ish wall clock used by the watchdog (seconds)."""
        return asyncio.get_running_loop().time()

    def _now_iso(self) -> str:
        return datetime.now(UTC).isoformat()


# ── Trade-updates consumer ───────────────────────────────────────────


class AlpacaTradeUpdatesConsumer(_SupervisedStreamConsumer):
    """Owns the trade-updates websocket for ONE trading account.

    Subscribes to the account's trade updates (paper vs. live chosen from
    ``credentials.paper``), normalizes each event to our :class:`TradeUpdate`,
    and publishes it as JSON to ``broker:trade_updates:{portfolio_id}``.

    This stream is the order-state source of truth (project CLAUDE.md): order
    lifecycle is consumed from here and must never be replaced by polling. The
    same reconnect-with-backoff supervision as the market-data consumer applies.
    """

    def __init__(
        self,
        credentials: BrokerCredentials,
        redis: _RedisLike,
        portfolio_id: str,
        *,
        backoff_base_seconds: float = _BACKOFF_BASE_SECONDS,
        backoff_cap_seconds: float = _BACKOFF_CAP_SECONDS,
        stream_factory: Callable[[BrokerCredentials], _TradeStreamLike] | None = None,
    ) -> None:
        super().__init__(
            backoff_base_seconds=backoff_base_seconds,
            backoff_cap_seconds=backoff_cap_seconds,
        )
        self._credentials = credentials
        self._redis = redis
        self._portfolio_id = portfolio_id
        self._channel = _trade_updates_channel(portfolio_id)
        self._stream_factory = stream_factory or _default_trade_stream_factory

    def _make_stream(self) -> _TradeStreamLike:
        stream = self._stream_factory(self._credentials)
        stream.subscribe_trade_updates(self._on_trade_update)
        return stream

    async def _on_trade_update(self, msg: Any) -> None:
        self._reset_backoff()
        dto = _trade_update_from_alpaca(msg)
        payload = dto.model_dump(mode="json")
        payload["type"] = "trade_update"
        await self._redis.publish(self._channel, json.dumps(payload))
