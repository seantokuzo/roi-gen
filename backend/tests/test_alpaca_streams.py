"""Alpaca streaming consumers — normalization, publishing, reconnect, watchdog.

No real sockets, no real Redis. We inject:

* a **fake stream** exposing the exact alpaca-py surface the consumers drive
  (``subscribe_bars/quotes/trades`` / ``subscribe_trade_updates`` plus the async
  ``_run_forever`` / ``stop_ws``), which feeds canned alpaca-py-shaped messages
  to the registered handler; and
* a **fake redis** that captures ``publish(channel, payload)`` calls.

The consumers normalize the vendor objects to our DTOs, so the fakes only need
to expose the attributes our normalizers read (duck typing) — they need not be
real alpaca-py models.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

from app.brokers.alpaca.status import ALPACA_STATUS_MAP, map_status
from app.brokers.alpaca.streams import (
    CHANNEL_BAR,
    CHANNEL_FEED_STATUS,
    CHANNEL_QUOTE,
    CHANNEL_TRADE,
    AlpacaMarketDataConsumer,
    AlpacaTradeUpdatesConsumer,
    feed_health_key,
)
from app.brokers.credentials import BrokerCredentials
from app.brokers.dto import Bar, Quote, Trade, TradeUpdate
from app.models.enums import (
    OrderClass,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)

CREDS = BrokerCredentials(api_key="k", api_secret="s", paper=True)
TS = datetime(2026, 6, 23, 14, 30, 5, tzinfo=UTC)


# ── Fakes ────────────────────────────────────────────────────────────


class FakeRedis:
    """Capture-only stand-in for ``redis.asyncio.Redis``."""

    def __init__(self) -> None:
        self.published: list[tuple[str, str]] = []
        self.setex_calls: list[tuple[str, int, str]] = []

    async def publish(self, channel: str, message: str) -> int:
        self.published.append((channel, message))
        return 1

    async def setex(self, name: str, time: int, value: str) -> bool:
        self.setex_calls.append((name, time, value))
        return True

    def payloads(self, channel: str) -> list[dict[str, Any]]:
        """Decoded JSON payloads published to ``channel`` (in order)."""
        return [json.loads(msg) for ch, msg in self.published if ch == channel]

    def health_statuses(self, key: str) -> list[str]:
        """The ``status`` of every JSON value SETEX'd to ``key`` (in order)."""
        return [
            json.loads(value)["status"] for name, _ttl, value in self.setex_calls if name == key
        ]


class FakeMarketStream:
    """Fake ``StockDataStream``: records subscriptions, drives canned messages.

    ``_run_forever`` replays ``self.feed`` (a list of ``(channel, msg)``) to the
    matching registered handler, then blocks until ``stop_ws`` is called so the
    supervisor sees a long-lived connection (as the real one does).
    """

    def __init__(self) -> None:
        self.bar_handler: Any = None
        self.quote_handler: Any = None
        self.trade_handler: Any = None
        self.bar_symbols: tuple[str, ...] = ()
        self.feed: list[tuple[str, Any]] = []
        self.run_count = 0
        self._stop = asyncio.Event()

    def subscribe_bars(self, handler: Any, *symbols: str) -> None:
        self.bar_handler = handler
        self.bar_symbols = symbols

    def subscribe_quotes(self, handler: Any, *symbols: str) -> None:
        self.quote_handler = handler

    def subscribe_trades(self, handler: Any, *symbols: str) -> None:
        self.trade_handler = handler

    async def _run_forever(self) -> None:
        self.run_count += 1
        handlers = {
            "bar": self.bar_handler,
            "quote": self.quote_handler,
            "trade": self.trade_handler,
        }
        for kind, msg in self.feed:
            handler = handlers[kind]
            assert handler is not None, f"no handler subscribed for {kind}"
            await handler(msg)
        await self._stop.wait()

    async def stop_ws(self) -> None:
        self._stop.set()


class FakeTradeStream:
    """Fake ``TradingStream``: records the trade-updates handler + replays msgs."""

    def __init__(self) -> None:
        self.handler: Any = None
        self.feed: list[Any] = []
        self.run_count = 0
        self._stop = asyncio.Event()

    def subscribe_trade_updates(self, handler: Any) -> None:
        self.handler = handler

    async def _run_forever(self) -> None:
        self.run_count += 1
        assert self.handler is not None
        for msg in self.feed:
            await self.handler(msg)
        await self._stop.wait()

    async def stop_ws(self) -> None:
        self._stop.set()


# ── alpaca-py-shaped canned messages (duck-typed) ────────────────────


def fake_bar(**over: Any) -> SimpleNamespace:
    base: dict[str, Any] = {
        "symbol": "AAPL",
        "timestamp": TS,
        "open": 190.12,
        "high": 191.5,
        "low": 189.9,
        "close": 190.77,
        "volume": 12345.0,
        "trade_count": 42.0,
        "vwap": 190.5012,
    }
    base.update(over)
    return SimpleNamespace(**base)


def fake_quote(**over: Any) -> SimpleNamespace:
    base: dict[str, Any] = {
        "symbol": "AAPL",
        "timestamp": TS,
        "bid_price": 190.10,
        "bid_size": 3.0,
        "ask_price": 190.20,
        "ask_size": 5.0,
    }
    base.update(over)
    return SimpleNamespace(**base)


def fake_trade(**over: Any) -> SimpleNamespace:
    base: dict[str, Any] = {
        "symbol": "AAPL",
        "timestamp": TS,
        "price": 190.15,
        "size": 100.0,
    }
    base.update(over)
    return SimpleNamespace(**base)


def fake_order(**over: Any) -> SimpleNamespace:
    base: dict[str, Any] = {
        "id": "ord-1",
        "client_order_id": "cid-1",
        "symbol": "AAPL",
        "side": OrderSide.buy,
        "type": OrderType.limit,
        "order_type": OrderType.limit,
        "order_class": OrderClass.simple,
        "time_in_force": TimeInForce.day,
        "status": "new",
        "qty": "10",
        "filled_qty": "0",
        "limit_price": "190.50",
        "stop_price": None,
        "trail_percent": None,
        "filled_avg_price": None,
        "extended_hours": False,
        "submitted_at": TS,
        "filled_at": None,
        "canceled_at": None,
        "legs": [],
    }
    base.update(over)
    return SimpleNamespace(**base)


def fake_trade_update(**over: Any) -> SimpleNamespace:
    base: dict[str, Any] = {
        "event": "fill",
        "execution_id": "exec-9",
        "order": fake_order(status="filled", filled_qty="10", filled_avg_price="190.55"),
        "timestamp": TS,
        "position_qty": 10.0,
        "price": 190.55,
        "qty": 10.0,
    }
    base.update(over)
    return SimpleNamespace(**base)


# A staleness-watchdog test needs a fake clock so we never sleep wall-seconds.
class _FakeClockConsumer(AlpacaMarketDataConsumer):
    """Market consumer with an injectable monotonic clock for the watchdog."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.fake_time = 0.0

    def _now(self) -> float:  # type: ignore[override]
        return self.fake_time


def _md_consumer(
    redis: FakeRedis, stream: FakeMarketStream, **kwargs: Any
) -> AlpacaMarketDataConsumer:
    return AlpacaMarketDataConsumer(
        CREDS,
        redis,
        ["AAPL"],
        stream_factory=lambda creds, feed: stream,
        **kwargs,
    )


# ── Normalization: bars / quotes / trades ────────────────────────────


async def test_bar_normalized_and_published() -> None:
    redis = FakeRedis()
    stream = FakeMarketStream()
    stream.feed = [("bar", fake_bar())]
    consumer = _md_consumer(redis, stream, staleness_seconds=1000)

    await _run_until_idle(consumer)

    payloads = redis.payloads(CHANNEL_BAR)
    assert len(payloads) == 1
    bar = Bar.model_validate(payloads[0])
    assert bar.symbol == "AAPL"
    assert bar.timestamp == TS
    assert bar.timestamp.tzinfo is not None
    # Decimal built via str(float) — exact, not the float's binary tail.
    assert bar.open == Decimal("190.12")
    assert bar.close == Decimal("190.77")
    assert bar.volume == Decimal("12345.0")
    assert bar.trade_count == 42  # int, not float
    assert bar.vwap == Decimal("190.5012")
    # The published JSON carries a discriminator the engine can switch on.
    assert payloads[0]["type"] == "bar"


async def test_quote_normalized_and_published() -> None:
    redis = FakeRedis()
    stream = FakeMarketStream()
    stream.feed = [("quote", fake_quote())]
    consumer = _md_consumer(redis, stream, subscribe_quotes=True, staleness_seconds=1000)

    await _run_until_idle(consumer)

    payloads = redis.payloads(CHANNEL_QUOTE)
    assert len(payloads) == 1
    quote = Quote.model_validate(payloads[0])
    assert quote.bid_price == Decimal("190.10")
    assert quote.ask_price == Decimal("190.20")
    assert quote.bid_size == Decimal("3.0")
    assert quote.ask_size == Decimal("5.0")
    assert quote.timestamp == TS
    assert payloads[0]["type"] == "quote"


async def test_trade_normalized_and_published() -> None:
    redis = FakeRedis()
    stream = FakeMarketStream()
    stream.feed = [("trade", fake_trade())]
    consumer = _md_consumer(redis, stream, subscribe_trades=True, staleness_seconds=1000)

    await _run_until_idle(consumer)

    payloads = redis.payloads(CHANNEL_TRADE)
    assert len(payloads) == 1
    trade = Trade.model_validate(payloads[0])
    assert trade.price == Decimal("190.15")
    assert trade.size == Decimal("100.0")
    assert trade.timestamp == TS
    assert payloads[0]["type"] == "trade"


async def test_quotes_and_trades_not_subscribed_by_default() -> None:
    redis = FakeRedis()
    stream = FakeMarketStream()
    stream.feed = [("bar", fake_bar())]
    consumer = _md_consumer(redis, stream, staleness_seconds=1000)

    await _run_until_idle(consumer)

    # Only bars subscribed → quote/trade handlers never registered.
    assert stream.bar_handler is not None
    assert stream.quote_handler is None
    assert stream.trade_handler is None
    assert stream.bar_symbols == ("AAPL",)


async def test_naive_timestamp_coerced_to_utc() -> None:
    redis = FakeRedis()
    stream = FakeMarketStream()
    naive = datetime(2026, 6, 23, 14, 30, 5)  # noqa: DTZ001 - deliberately naive
    stream.feed = [("bar", fake_bar(timestamp=naive))]
    consumer = _md_consumer(redis, stream, staleness_seconds=1000)

    await _run_until_idle(consumer)

    bar = Bar.model_validate(redis.payloads(CHANNEL_BAR)[0])
    assert bar.timestamp.tzinfo is not None
    assert bar.timestamp == TS  # interpreted as UTC


async def test_bar_with_null_optional_fields() -> None:
    redis = FakeRedis()
    stream = FakeMarketStream()
    stream.feed = [("bar", fake_bar(trade_count=None, vwap=None))]
    consumer = _md_consumer(redis, stream, staleness_seconds=1000)

    await _run_until_idle(consumer)

    bar = Bar.model_validate(redis.payloads(CHANNEL_BAR)[0])
    assert bar.trade_count is None
    assert bar.vwap is None


# ── Trade-updates normalization + publishing ─────────────────────────


async def test_trade_update_normalized_and_published() -> None:
    redis = FakeRedis()
    stream = FakeTradeStream()
    stream.feed = [fake_trade_update()]
    consumer = AlpacaTradeUpdatesConsumer(CREDS, redis, "pf-7", stream_factory=lambda creds: stream)

    await _run_until_idle(consumer)

    channel = "broker:trade_updates:pf-7"
    payloads = [json.loads(msg) for ch, msg in redis.published if ch == channel]
    assert len(payloads) == 1
    assert payloads[0]["type"] == "trade_update"
    tu = TradeUpdate.model_validate(payloads[0])
    assert tu.event == "fill"
    assert tu.execution_id == "exec-9"
    assert tu.price == Decimal("190.55")
    assert tu.qty == Decimal("10.0")
    assert tu.position_qty == Decimal("10.0")
    assert tu.timestamp == TS
    # Nested order normalized through the same status map (filled).
    assert tu.order.status is OrderStatus.filled
    assert tu.order.broker_order_id == "ord-1"
    assert tu.order.filled_qty == Decimal("10")
    assert tu.order.filled_avg_price == Decimal("190.55")
    assert tu.order.side is OrderSide.buy


async def test_trade_update_maps_alpaca_new_to_submitted() -> None:
    redis = FakeRedis()
    stream = FakeTradeStream()
    # event "new" carries an order whose Alpaca status is "new" (routed to the
    # exchange and working) → our ``submitted`` (canonical map_status).
    stream.feed = [
        fake_trade_update(
            event="new",
            execution_id=None,
            price=None,
            qty=None,
            position_qty=None,
            order=fake_order(status="new", filled_qty="0"),
        )
    ]
    consumer = AlpacaTradeUpdatesConsumer(CREDS, redis, "pf-1", stream_factory=lambda creds: stream)

    await _run_until_idle(consumer)

    tu = TradeUpdate.model_validate(json.loads(redis.published[0][1]))
    assert tu.event == "new"
    assert tu.order.status is OrderStatus.submitted
    assert tu.price is None
    assert tu.qty is None
    assert tu.order.filled_qty == Decimal("0")


async def test_trade_update_nested_legs_normalized() -> None:
    redis = FakeRedis()
    stream = FakeTradeStream()
    leg = fake_order(id="leg-tp", client_order_id=None, status="new", limit_price="200")
    parent = fake_order(status="new", legs=[leg])
    stream.feed = [fake_trade_update(event="new", order=parent, price=None, qty=None)]
    consumer = AlpacaTradeUpdatesConsumer(CREDS, redis, "pf-3", stream_factory=lambda creds: stream)

    await _run_until_idle(consumer)

    tu = TradeUpdate.model_validate(json.loads(redis.published[0][1]))
    assert len(tu.order.legs) == 1
    assert tu.order.legs[0].broker_order_id == "leg-tp"
    assert tu.order.legs[0].limit_price == Decimal("200")
    assert tu.order.legs[0].status is OrderStatus.submitted


# ── Status map ───────────────────────────────────────────────────────


def test_status_map_targets_are_domain_enum_members() -> None:
    # Every mapped target must be a real domain OrderStatus.
    for target in ALPACA_STATUS_MAP.values():
        assert isinstance(target, OrderStatus)


def test_status_map_covers_known_alpaca_statuses() -> None:
    # The full set Alpaca documents on its Order model + trade-update events.
    expected = {
        "new",
        "accepted",
        "accepted_for_bidding",
        "pending_new",
        "partially_filled",
        "filled",
        "done_for_day",
        "canceled",
        "expired",
        "replaced",
        "restated",
        "pending_cancel",
        "pending_replace",
        "pending_review",
        "stopped",
        "rejected",
        "suspended",
        "calculated",
        "held",
    }
    assert expected <= set(ALPACA_STATUS_MAP)


def test_status_map_unknown_falls_back_to_held() -> None:
    assert map_status("some_new_alpaca_status") is OrderStatus.held


# ── Reconnect with backoff ───────────────────────────────────────────


class _FailThenServeMarketFactory:
    """Market-stream factory: the first call's stream fails ``_run_forever``
    (simulating a dropped/refused connection), every later call serves a bar
    then idles. Each call yields a FRESH stream — exactly as the real factory
    rebuilds the alpaca-py stream on every reconnect."""

    def __init__(self, *, fail_mode: str) -> None:
        self.fail_mode = fail_mode  # "raise" | "return"
        self.calls = 0
        self.streams: list[FakeMarketStream] = []

    def __call__(self, creds: BrokerCredentials, feed: str) -> FakeMarketStream:
        self.calls += 1
        first = self.calls == 1
        fail = self.fail_mode
        outer = self

        class _S(FakeMarketStream):
            async def _run_forever(self) -> None:
                self.run_count += 1
                if first:
                    if fail == "raise":
                        raise ConnectionError("simulated drop")
                    return  # mimic alpaca-py's silent return (insufficient sub)
                await self.bar_handler(fake_bar())
                await self._stop.wait()

        stream = _S()
        outer.streams.append(stream)
        return stream


async def test_reconnect_after_disconnect() -> None:
    """A first connection that raises forces a logged, backed-off reconnect."""
    redis = FakeRedis()
    factory = _FailThenServeMarketFactory(fail_mode="raise")
    consumer = AlpacaMarketDataConsumer(
        CREDS,
        redis,
        ["AAPL"],
        staleness_seconds=1000,
        backoff_base_seconds=0.01,  # tiny: reconnect without a real wait
        stream_factory=factory,
    )

    task = asyncio.create_task(consumer.start())
    await _wait_for(lambda: len(redis.payloads(CHANNEL_BAR)) == 1)
    await consumer.stop()
    await _join(task)

    assert factory.calls == 2  # rebuilt the stream after the simulated drop
    assert len(redis.payloads(CHANNEL_BAR)) == 1


async def test_unexpected_return_triggers_reconnect() -> None:
    """A clean early-return from _run_forever (SDK silent bail) → reconnect."""
    redis = FakeRedis()
    factory = _FailThenServeMarketFactory(fail_mode="return")
    consumer = AlpacaMarketDataConsumer(
        CREDS,
        redis,
        ["AAPL"],
        staleness_seconds=1000,
        backoff_base_seconds=0.01,
        stream_factory=factory,
    )

    task = asyncio.create_task(consumer.start())
    await _wait_for(lambda: len(redis.payloads(CHANNEL_BAR)) == 1)
    await consumer.stop()
    await _join(task)

    assert factory.calls == 2


class _FailThenServeTradeFactory:
    """Trade-stream factory: first stream fails, later streams serve one update
    then idle. Fresh stream per call (mirrors the real reconnect rebuild)."""

    def __init__(self) -> None:
        self.calls = 0

    def __call__(self, creds: BrokerCredentials) -> FakeTradeStream:
        self.calls += 1
        first = self.calls == 1

        class _S(FakeTradeStream):
            async def _run_forever(self) -> None:
                self.run_count += 1
                if first:
                    raise ConnectionError("simulated drop")
                await self.handler(fake_trade_update())
                await self._stop.wait()

        return _S()


async def test_trade_updates_reconnect_after_disconnect() -> None:
    redis = FakeRedis()
    factory = _FailThenServeTradeFactory()
    consumer = AlpacaTradeUpdatesConsumer(
        CREDS, redis, "pf-9", backoff_base_seconds=0.01, stream_factory=factory
    )

    task = asyncio.create_task(consumer.start())
    channel = "broker:trade_updates:pf-9"
    await _wait_for(lambda: any(ch == channel for ch, _ in redis.published))
    await consumer.stop()
    await _join(task)

    assert factory.calls == 2


# ── Staleness watchdog ───────────────────────────────────────────────


async def test_staleness_watchdog_publishes_feed_stale_then_feed_ok() -> None:
    """With a fake clock: advance past the threshold → feed_stale; data → feed_ok."""
    redis = FakeRedis()
    stream = FakeMarketStream()

    # Build a consumer that idles (no canned feed) so the watchdog can fire,
    # with a fake clock so we never sleep wall-seconds.
    consumer = _FakeClockConsumer(
        CREDS,
        redis,
        ["AAPL"],
        stream_factory=lambda creds, feed: stream,
        subscribe_trades=True,  # continuous channel: a 30s window is honest here
        staleness_seconds=30,
        watchdog_poll_seconds=0.005,  # fast real-time poll; fake clock drives staleness
    )

    task = asyncio.create_task(consumer.start())
    # start() seeds last_msg_at at fake_time=0 via _mark_alive.
    await _wait_for(lambda: consumer._last_msg_at is not None)  # type: ignore[attr-defined]

    # Jump the clock past the staleness threshold; the watchdog polls fast
    # so it observes the gap on its next tick.
    consumer.fake_time = 31.0
    await _wait_for(lambda: _has_status(redis, "feed_stale"))

    # Now deliver a bar → _mark_alive clears staleness with feed_ok.
    consumer.fake_time = 32.0
    await stream.bar_handler(fake_bar())
    await _wait_for(lambda: _has_status(redis, "feed_ok"))

    await consumer.stop()
    await _join(task)

    statuses = [p["status"] for p in redis.payloads(CHANNEL_FEED_STATUS)]
    assert statuses[0] == "feed_stale"
    assert "feed_ok" in statuses
    stale_payload = next(
        p for p in redis.payloads(CHANNEL_FEED_STATUS) if p["status"] == "feed_stale"
    )
    assert stale_payload["type"] == "feed_status"
    assert stale_payload["symbols"] == ["AAPL"]


async def test_no_feed_stale_while_data_flows() -> None:
    """Fresh data each tick keeps the feed healthy — no feed_stale emitted."""
    redis = FakeRedis()
    stream = FakeMarketStream()
    consumer = _FakeClockConsumer(
        CREDS,
        redis,
        ["AAPL"],
        stream_factory=lambda creds, feed: stream,
        subscribe_trades=True,  # continuous channel: a 30s window is honest here
        staleness_seconds=30,
        watchdog_poll_seconds=0.005,
    )

    task = asyncio.create_task(consumer.start())
    await _wait_for(lambda: consumer._last_msg_at is not None)  # type: ignore[attr-defined]

    # Advance time but keep delivering bars before the threshold each step.
    for t in (10.0, 20.0, 29.0):
        consumer.fake_time = t
        await stream.bar_handler(fake_bar())
        await asyncio.sleep(0)

    await consumer.stop()
    await _join(task)

    assert not _has_status(redis, "feed_stale")


# ── Feed-health key (level-based, fail-closed) ───────────────────────

HEALTH_KEY = feed_health_key("pf-hk")


def _health_consumer(redis: FakeRedis, stream: FakeMarketStream) -> _FakeClockConsumer:
    """A fake-clock consumer wired with a portfolio id (health key active)."""
    return _FakeClockConsumer(
        CREDS,
        redis,
        ["AAPL"],
        stream_factory=lambda creds, feed: stream,
        subscribe_trades=True,  # continuous channel: a 30s window is honest here
        staleness_seconds=30,
        watchdog_poll_seconds=0.005,
        portfolio_id="pf-hk",
    )


def test_feed_health_key_shape() -> None:
    assert feed_health_key("pf-hk") == "engine:feed_health:pf-hk"


async def test_feed_health_key_written_on_first_data_with_ttl() -> None:
    """The first real message writes status=ok under TTL 2× the window.

    Boot deliberately writes nothing — health must be earned by data, and key
    absence already reads as stale.
    """
    redis = FakeRedis()
    stream = FakeMarketStream()
    stream.feed = [("bar", fake_bar())]
    consumer = _health_consumer(redis, stream)

    task = asyncio.create_task(consumer.start())
    await _wait_for(lambda: len(redis.setex_calls) >= 1)
    await consumer.stop()
    await _join(task)

    name, ttl, value = redis.setex_calls[0]
    assert name == HEALTH_KEY
    assert ttl == 60  # 2 × 30s staleness window
    payload = json.loads(value)
    assert payload["status"] == "ok"
    # stamped-at parses as ISO-8601 and is tz-aware (iron law #5).
    assert datetime.fromisoformat(payload["at"]).tzinfo is not None


async def test_feed_health_refreshes_are_throttled() -> None:
    """A busy feed refreshes the key at most ~1/sec, not once per message."""
    redis = FakeRedis()
    stream = FakeMarketStream()
    stream.feed = [("bar", fake_bar())]  # earns the first health write
    consumer = _health_consumer(redis, stream)

    task = asyncio.create_task(consumer.start())
    await _wait_for(lambda: len(redis.setex_calls) == 1)
    await _wait_for(lambda: stream.bar_handler is not None)

    # Three messages inside the same fake second → all throttled.
    for _ in range(3):
        await stream.bar_handler(fake_bar())
    assert len(redis.setex_calls) == 1

    # One throttle window later a single refresh lands (whoever writes first —
    # handler or watchdog — resets the throttle for the other).
    consumer.fake_time = 1.0
    await stream.bar_handler(fake_bar())
    assert len(redis.setex_calls) == 2
    assert redis.health_statuses(HEALTH_KEY) == ["ok", "ok"]

    await consumer.stop()
    await _join(task)


async def test_watchdog_refreshes_health_key_while_quiet_but_healthy() -> None:
    """No data flowing yet not stale: the WATCHDOG keeps the key alive, so
    key absence means watchdog-dead — never mere feed quiet."""
    redis = FakeRedis()
    stream = FakeMarketStream()
    stream.feed = [("bar", fake_bar())]  # one message, then silence
    consumer = _health_consumer(redis, stream)

    task = asyncio.create_task(consumer.start())
    await _wait_for(lambda: len(redis.setex_calls) >= 1)
    consumer.fake_time = 10.0  # quiet but inside the 30s window
    await _wait_for(lambda: len(redis.setex_calls) >= 2)
    await consumer.stop()
    await _join(task)

    assert redis.health_statuses(HEALTH_KEY)[-1] == "ok"


async def test_never_received_data_goes_stale_and_key_flips() -> None:
    """A boot that never receives a single message must go stale one window
    later — feed_stale published AND the level key flipped to stale."""
    redis = FakeRedis()
    stream = FakeMarketStream()  # empty feed forever
    consumer = _health_consumer(redis, stream)

    task = asyncio.create_task(consumer.start())
    await _wait_for(lambda: consumer._last_msg_at is not None)  # type: ignore[attr-defined]
    consumer.fake_time = 31.0
    await _wait_for(lambda: "stale" in redis.health_statuses(HEALTH_KEY))
    await consumer.stop()
    await _join(task)

    assert _has_status(redis, "feed_stale")
    statuses = redis.health_statuses(HEALTH_KEY)
    assert "ok" not in statuses, "advertised health without ever receiving data"
    assert statuses[-1] == "stale"  # transition forced through the throttle


async def test_feed_health_key_flips_back_to_ok_on_recovery() -> None:
    """Data resuming after stale forces an immediate ok write (no throttle lag)."""
    redis = FakeRedis()
    stream = FakeMarketStream()
    consumer = _health_consumer(redis, stream)

    task = asyncio.create_task(consumer.start())
    await _wait_for(lambda: stream.bar_handler is not None)
    consumer.fake_time = 31.0
    await _wait_for(lambda: "stale" in redis.health_statuses(HEALTH_KEY))

    # Recover INSIDE the throttle window of the stale write: the ok→stale→ok
    # transition must bypass the throttle.
    consumer.fake_time = 31.5
    await stream.bar_handler(fake_bar())
    assert redis.health_statuses(HEALTH_KEY)[-1] == "ok"

    await consumer.stop()
    await _join(task)

    assert _has_status(redis, "feed_ok")


class FlakyRedis(FakeRedis):
    """FakeRedis whose ``setex``/``publish`` fail for the first ``n`` calls.

    Models the real failure: a cold-boot ``redis.exceptions.TimeoutError`` on
    the very first health write.
    """

    def __init__(self, *, setex_failures: int = 0, publish_failures: int = 0) -> None:
        super().__init__()
        self.setex_failures = setex_failures
        self.publish_failures = publish_failures

    async def setex(self, name: str, time: int, value: str) -> bool:
        if self.setex_failures > 0:
            self.setex_failures -= 1
            msg = "Timeout connecting to server"
            raise TimeoutError(msg)
        return await super().setex(name, time, value)

    async def publish(self, channel: str, message: str) -> int:
        if self.publish_failures > 0:
            self.publish_failures -= 1
            msg = "Timeout connecting to server"
            raise TimeoutError(msg)
        return await super().publish(channel, message)


async def test_health_write_failure_neither_kills_nor_reconnects_the_consumer() -> None:
    """A Redis hiccup on a health write must not disturb the feed at all.

    Regression, observed live in the Phase-2 dress rehearsal: ``start()`` used
    to await ``_mark_alive()`` (a health SETEX) BEFORE the supervisor's
    try/except, so a raise there escaped ``start()`` and killed the
    market-data task permanently — no reconnect, and because that task is
    ``critical`` the composite ``halted()`` blocked every entry for the life
    of the process, behind a heartbeat that still looked alive.

    Boot no longer writes at all, so the write now happens on a real message
    INSIDE the supervisor — where an escape would trigger a stream teardown
    and reconnect instead. Swallowing keeps a telemetry hiccup from becoming a
    reconnect storm.
    """
    redis = FlakyRedis(setex_failures=1)
    stream = FakeMarketStream()
    consumer = _health_consumer(redis, stream)

    task = asyncio.create_task(consumer.start())
    await _wait_for(lambda: stream.bar_handler is not None)

    await stream.bar_handler(fake_bar())  # this write raises, and is swallowed
    assert not task.done(), "a health-write failure killed the market-data task"
    assert stream.run_count == 1, "a health-write failure forced a stream reconnect"

    # The next write (one throttle window later) recovers the key.
    consumer.fake_time = 1.0
    await stream.bar_handler(fake_bar())
    assert redis.health_statuses(HEALTH_KEY) == ["ok"]

    await consumer.stop()
    await _join(task)


async def test_failed_health_write_does_not_advance_the_throttle() -> None:
    """A write that never landed must be retried, not throttled out."""
    redis = FlakyRedis(setex_failures=1)
    stream = FakeMarketStream()
    consumer = _health_consumer(redis, stream)

    task = asyncio.create_task(consumer.start())
    await _wait_for(lambda: stream.bar_handler is not None)

    # Two messages in the SAME fake second: the first write fails, and were the
    # throttle advanced on failure the retry would be swallowed and the key
    # would stay absent for a full refresh window.
    assert consumer.fake_time == 0.0
    await stream.bar_handler(fake_bar())  # fails
    await stream.bar_handler(fake_bar())  # retry, same second
    assert redis.health_statuses(HEALTH_KEY) == ["ok"]

    await consumer.stop()
    await _join(task)


async def test_feed_status_publish_failure_does_not_kill_the_market_data_task() -> None:
    """The edge-triggered status channel is telemetry — its failure is survivable."""
    redis = FlakyRedis(publish_failures=99)  # every publish fails, forever
    stream = FakeMarketStream()
    consumer = _health_consumer(redis, stream)

    task = asyncio.create_task(consumer.start())
    await _wait_for(lambda: stream.bar_handler is not None)
    # Drive the watchdog into the stale transition (which publishes + writes).
    consumer.fake_time = 31.0
    await _wait_for(lambda: "stale" in redis.health_statuses(HEALTH_KEY))
    assert not task.done(), "a failed feed-status publish killed the market-data task"

    await consumer.stop()
    await _join(task)


async def test_no_feed_health_key_without_portfolio_id() -> None:
    """Default (no portfolio id, the observation shell): pub/sub only, no key."""
    redis = FakeRedis()
    stream = FakeMarketStream()
    stream.feed = [("bar", fake_bar())]
    consumer = _md_consumer(redis, stream, staleness_seconds=1000)

    await _run_until_idle(consumer)

    assert redis.setex_calls == []


# ── Lifecycle ────────────────────────────────────────────────────────


async def test_stop_is_idempotent() -> None:
    redis = FakeRedis()
    stream = FakeMarketStream()
    consumer = _md_consumer(redis, stream, staleness_seconds=1000)
    # stop before start must not raise.
    await consumer.stop()

    task = asyncio.create_task(consumer.start())
    await _wait_for(lambda: stream.run_count == 1)
    await consumer.stop()
    await consumer.stop()  # second stop is a no-op
    await _join(task)


# ── Helpers ──────────────────────────────────────────────────────────


async def _run_until_idle(consumer: Any) -> None:
    """Start a consumer, let it drain its canned feed, then stop it.

    The fake streams replay their feed then block on ``stop_ws``; we poll until
    the stream has run once (feed drained) and stop, so each normalization test
    is fast and deterministic.
    """
    task = asyncio.create_task(consumer.start())
    try:
        await _wait_for(lambda: _stream_ran(consumer))
        # Yield once more so any final handler publish settles.
        await asyncio.sleep(0)
    finally:
        await consumer.stop()
        await _join(task)


def _stream_ran(consumer: Any) -> bool:
    stream = consumer._stream
    return stream is not None and getattr(stream, "run_count", 0) >= 1


def _has_status(redis: FakeRedis, status: str) -> bool:
    return any(p.get("status") == status for p in redis.payloads(CHANNEL_FEED_STATUS))


async def _wait_for(predicate: Any, *, timeout: float = 2.0) -> None:
    """Poll ``predicate`` on the event loop until true or ``timeout`` elapses."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            msg = "condition not met before timeout"
            raise AssertionError(msg)
        await asyncio.sleep(0.005)


async def _join(task: asyncio.Task[None]) -> None:
    """Await a consumer task to completion, tolerating cancellation."""
    try:
        await asyncio.wait_for(task, timeout=2.0)
    except (TimeoutError, asyncio.CancelledError):
        task.cancel()
        with pytest.raises((asyncio.CancelledError, TimeoutError)):
            await task


# ── Staleness floor (threshold must exceed the slowest subscribed cadence) ──


def _floor_consumer(**over: Any) -> AlpacaMarketDataConsumer:
    kwargs: dict[str, Any] = {
        "stream_factory": lambda creds, feed: FakeMarketStream(),
        "portfolio_id": "pf-floor",
    }
    kwargs.update(over)
    return AlpacaMarketDataConsumer(CREDS, FakeRedis(), ["AAPL"], **kwargs)


def test_bars_only_staleness_is_floored_above_the_bar_interval() -> None:
    """The default 30s window is a guaranteed false-positive on 1/min bars.

    Bars arrive once a MINUTE, so a sub-minute threshold doesn't detect
    outages — it manufactures one every single minute, blocking entries at
    random (Phase-2 dress-rehearsal finding).
    """
    consumer = _floor_consumer(staleness_seconds=30)
    assert consumer._staleness_seconds == 120.0  # noqa: SLF001


def test_continuous_channels_leave_the_requested_window_alone() -> None:
    """Quotes/trades print far faster than a minute — 30s is honest there."""
    assert _floor_consumer(staleness_seconds=30, subscribe_trades=True)._staleness_seconds == 30  # noqa: SLF001
    assert _floor_consumer(staleness_seconds=30, subscribe_quotes=True)._staleness_seconds == 30  # noqa: SLF001


def test_a_window_already_above_the_floor_is_untouched() -> None:
    consumer = _floor_consumer(staleness_seconds=300)
    assert consumer._staleness_seconds == 300  # noqa: SLF001


def test_health_key_ttl_never_lapses_before_the_staleness_window() -> None:
    """TTL is derived from the EFFECTIVE window, not the raw request.

    A TTL shorter than the threshold would expire the health key while the
    feed is still considered healthy, and the fail-closed reader (absence ⇒
    stale) would call that an outage — the same false-stale bug, entered from
    the opposite end.
    """
    for kwargs in ({"staleness_seconds": 30}, {"staleness_seconds": 30, "subscribe_trades": True}):
        consumer = _floor_consumer(**kwargs)
        assert consumer._feed_health_ttl >= consumer._staleness_seconds  # noqa: SLF001


def test_watchdog_poll_tracks_the_effective_window() -> None:
    """The poll derives from the FLOORED window, not the raw request.

    Uses 10s deliberately: the poll formula caps at 5s, so at the 30s default
    both the old and new code land on 5.0 and an assertion there would guard
    nothing. At 10s the pre-fix code gives 2.0 (10/5) and the floored window
    gives 5.0 (120/5 capped) — so this test actually fails if the derivation
    regresses to the raw parameter.
    """
    consumer = _floor_consumer(staleness_seconds=10)
    assert consumer._staleness_seconds == 120.0  # noqa: SLF001
    assert consumer._watchdog_poll == 5.0  # noqa: SLF001


async def test_a_failed_stale_health_write_is_retried_on_the_next_tick() -> None:
    """The blackout marker must survive a dropped write.

    Health writes are swallowed best-effort so a Redis hiccup can't kill the
    feed — which means the ok→stale write cannot be edge-triggered: one
    dropped write would leave the key holding its last "ok" for the rest of
    the blackout, lying to every reader of that key. (The reader's freshness
    check is the second line of defense; this is the first.)
    """

    class StaleWriteFailsOnce(FakeRedis):
        def __init__(self) -> None:
            super().__init__()
            self.stale_attempts = 0

        async def setex(self, name: str, time: int, value: str) -> bool:
            if json.loads(value)["status"] == "stale":
                self.stale_attempts += 1
                if self.stale_attempts == 1:
                    msg = "Timeout connecting to server"
                    raise TimeoutError(msg)
            return await super().setex(name, time, value)

    redis = StaleWriteFailsOnce()
    stream = FakeMarketStream()
    consumer = _FakeClockConsumer(
        CREDS,
        redis,
        ["AAPL"],
        stream_factory=lambda creds, feed: stream,
        subscribe_trades=True,  # continuous channel: a 30s window is honest here
        staleness_seconds=30,
        watchdog_poll_seconds=0.005,
        portfolio_id="pf-hk",
    )

    task = asyncio.create_task(consumer.start())
    await _wait_for(lambda: stream.bar_handler is not None)

    # Cross the threshold; the first "stale" write raises and is swallowed.
    consumer.fake_time = 40.0
    await _wait_for(lambda: redis.stale_attempts >= 1)

    # Keep ticking with the clock advancing so the 1s refresh throttle can
    # never be the reason a retry didn't happen.
    for moment in (41.0, 42.0, 43.0):
        consumer.fake_time = moment
        await _wait_for(lambda: redis.stale_attempts >= 2, timeout=1.0)

    await consumer.stop()
    await _join(task)

    assert redis.stale_attempts >= 2, "the failed stale write was never retried"
    statuses = redis.health_statuses(HEALTH_KEY)
    assert statuses[-1] == "stale", f"key still advertises {statuses[-1]!r} during a blackout"


async def test_production_config_bars_only_healthy_feed_never_goes_stale() -> None:
    """The shipping configuration, exercised behaviourally — not just asserted
    at the constructor.

    Bars-only with the floored 120s window is what the engine actually runs.
    A healthy feed delivering one bar per 60s must NEVER publish feed_stale
    (the bug this whole branch exists to kill), and must still go stale when
    the feed genuinely dies.
    """
    redis = FakeRedis()
    stream = FakeMarketStream()
    consumer = _FakeClockConsumer(
        CREDS,
        redis,
        ["AAPL"],
        stream_factory=lambda creds, feed: stream,
        portfolio_id="pf-hk",  # bars-only: no subscribe_quotes/trades
        watchdog_poll_seconds=0.005,
    )
    assert consumer._staleness_seconds == 120.0  # noqa: SLF001 — the shipping window

    task = asyncio.create_task(consumer.start())
    await _wait_for(lambda: stream.bar_handler is not None)

    # Three healthy minutes: a bar every 60s, watchdog polling throughout.
    for minute in (60.0, 120.0, 180.0):
        consumer.fake_time = minute
        await stream.bar_handler(fake_bar())
        await asyncio.sleep(0.03)
    assert not _has_status(redis, "feed_stale"), "healthy 1/min feed was flagged stale"

    # Now the feed genuinely dies: past 120s since the last bar → stale.
    consumer.fake_time = 301.0
    await _wait_for(lambda: _has_status(redis, "feed_stale"))

    await consumer.stop()
    await _join(task)


async def test_boot_advertises_no_health_until_real_data_arrives() -> None:
    """A socket that never connects must not read healthy for a whole window.

    Key absence already means stale to the reader, so writing nothing is the
    fail-closed answer — and `start()` seeding the staleness clock must not be
    mistaken for evidence the feed works.
    """
    redis = FakeRedis()
    stream = FakeMarketStream()
    consumer = _FakeClockConsumer(
        CREDS,
        redis,
        ["AAPL"],
        stream_factory=lambda creds, feed: stream,
        portfolio_id="pf-hk",
        watchdog_poll_seconds=0.005,
    )

    task = asyncio.create_task(consumer.start())
    await _wait_for(lambda: stream.bar_handler is not None)
    await asyncio.sleep(0.05)  # several watchdog ticks, still no data
    assert redis.health_statuses(HEALTH_KEY) == [], "boot advertised unearned health"

    # First real bar → now we have evidence, and the key says so.
    consumer.fake_time = 1.0
    await stream.bar_handler(fake_bar())
    await _wait_for(lambda: redis.health_statuses(HEALTH_KEY) == ["ok"])

    await consumer.stop()
    await _join(task)


def test_continuous_channel_window_still_has_an_absolute_floor() -> None:
    """A zero window makes every tick stale and wedges entries off forever."""
    assert _floor_consumer(staleness_seconds=0, subscribe_trades=True)._staleness_seconds == 5.0  # noqa: SLF001
