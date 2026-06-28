"""AlpacaBrokerAdapter REST behaviour against a fully mocked transport.

No live network and no real Alpaca: every test drives an
``httpx.AsyncClient(transport=httpx.MockTransport(handler))`` so we control the
exact response (status, headers, body) the adapter parses. We assert three
things the rest of the system leans on:

1. Decimal/timestamp fidelity (no float, tz-aware UTC) on the way *in*.
2. The request shape on the way *out* (POST body for market & bracket).
3. The error taxonomy — and critically that an ambiguous submit failure is
   distinguishable from an unavailable read.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import httpx
import pytest

from app.brokers.alpaca.rest import AlpacaBrokerAdapter
from app.brokers.alpaca.status import ALPACA_STATUS_MAP, map_status
from app.brokers.credentials import BrokerCredentials
from app.brokers.dto import OrderRequest
from app.brokers.errors import (
    AmbiguousOrderState,
    BrokerAuthError,
    BrokerRateLimited,
    BrokerUnavailable,
    OrderRejected,
)
from app.brokers.ratelimit import AsyncTokenBucket
from app.models.enums import (
    OrderClass,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)

Handler = Callable[[httpx.Request], httpx.Response]

PAPER_CREDS = BrokerCredentials(api_key="AK-TEST", api_secret="SK-TEST", paper=True)


def _adapter(handler: Handler) -> AlpacaBrokerAdapter:
    """Build an adapter whose transport is a MockTransport over ``handler``.

    A fast bucket (huge rate) keeps the rate limiter a no-op for these tests —
    throttling has its own dedicated suite.
    """
    client = httpx.AsyncClient(
        base_url="https://paper-api.alpaca.markets",
        transport=httpx.MockTransport(handler),
    )
    bucket = AsyncTokenBucket(rate_per_minute=1_000_000)
    return AlpacaBrokerAdapter(PAPER_CREDS, client=client, bucket=bucket)


def _json(payload: Any, status_code: int = 200) -> httpx.Response:
    return httpx.Response(status_code, json=payload)


# Representative order payloads (shapes mirror Alpaca's Trading API v2). ───────


def _order_payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "id": "order-uuid-1",
        "client_order_id": "cid-abc",
        "symbol": "AAPL",
        "side": "buy",
        "type": "market",
        "order_class": "",
        "time_in_force": "day",
        "status": "filled",
        "qty": "10",
        "filled_qty": "10",
        "limit_price": None,
        "stop_price": None,
        "trail_percent": None,
        "filled_avg_price": "190.125",
        "extended_hours": False,
        "submitted_at": "2026-06-23T13:30:00.123456Z",
        "filled_at": "2026-06-23T13:30:01Z",
        "canceled_at": None,
        "legs": None,
    }
    payload.update(overrides)
    return payload


# ── Account ───────────────────────────────────────────────────────────


async def test_get_account_parses_decimals_and_no_pdt() -> None:
    account_body = {
        "id": "acct-123",
        "status": "ACTIVE",
        "currency": "USD",
        "equity": "100000.55",
        "last_equity": "99500.10",
        "cash": "25000.00",
        "buying_power": "200001.10",
        "long_market_value": "75000.55",
        "short_market_value": "-1000.00",
        # PDT fields deliberately present in the raw payload — the adapter must
        # ignore them entirely (iron law #3).
        "pattern_day_trader": True,
        "daytrade_count": 7,
        "daytrading_buying_power": "400000.00",
        "trading_blocked": False,
        "account_blocked": False,
    }

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v2/account"
        assert request.headers["APCA-API-KEY-ID"] == "AK-TEST"
        assert request.headers["APCA-API-SECRET-KEY"] == "SK-TEST"
        return _json(account_body)

    adapter = _adapter(handler)
    account = await adapter.get_account()

    assert account.account_id == "acct-123"
    assert account.equity == Decimal("100000.55")
    assert account.buying_power == Decimal("200001.10")
    # Net position market value = long + short (short is already negative).
    assert account.position_market_value == Decimal("74000.55")
    assert account.trading_blocked is False
    # The DTO must not surface PDT — neither as model fields nor leaked attrs.
    for forbidden in ("pattern_day_trader", "daytrade_count", "daytrading_buying_power"):
        assert forbidden not in type(account).model_fields
        assert not hasattr(account, forbidden)
    # raw still carries the untouched payload for forensics (that's fine).
    assert account.raw["pattern_day_trader"] is True
    await adapter.aclose()


# ── Positions ─────────────────────────────────────────────────────────


async def test_list_positions_signed_short_qty() -> None:
    positions_body = [
        {
            "symbol": "TSLA",
            "qty": "-15",
            "side": "short",
            "avg_entry_price": "250.50",
            "market_value": "-3750.00",
            "cost_basis": "-3757.50",
            "unrealized_pl": "7.50",
            "current_price": "250.00",
        },
        {
            "symbol": "AAPL",
            "qty": "100",
            "side": "long",
            "avg_entry_price": "190.00",
            "market_value": "19012.50",
            "cost_basis": "19000.00",
            "unrealized_pl": "12.50",
            "current_price": "190.125",
        },
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v2/positions"
        return _json(positions_body)

    adapter = _adapter(handler)
    positions = await adapter.list_positions()

    assert len(positions) == 2
    short = positions[0]
    assert short.symbol == "TSLA"
    assert short.qty == Decimal("-15")
    assert short.qty < 0
    assert short.side == "short"
    assert short.current_price == Decimal("250.00")
    assert positions[1].qty == Decimal("100")
    await adapter.aclose()


async def test_get_position_missing_returns_none() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v2/positions/NVDA"
        return _json({"message": "position does not exist"}, status_code=404)

    adapter = _adapter(handler)
    assert await adapter.get_position("NVDA") is None
    await adapter.aclose()


# ── Orders (reads) ────────────────────────────────────────────────────


async def test_list_orders_with_nested_legs() -> None:
    parent = _order_payload(
        id="parent-1",
        client_order_id="cid-parent",
        type="market",
        order_class="bracket",
        status="filled",
        legs=[
            _order_payload(
                id="leg-tp",
                client_order_id=None,
                side="sell",
                type="limit",
                order_class="bracket",
                status="held",
                qty="10",
                filled_qty="0",
                limit_price="200.00",
                filled_avg_price=None,
                filled_at=None,
            ),
            _order_payload(
                id="leg-sl",
                client_order_id=None,
                side="sell",
                type="stop",
                order_class="bracket",
                status="held",
                qty="10",
                filled_qty="0",
                stop_price="180.00",
                filled_avg_price=None,
                filled_at=None,
            ),
        ],
    )

    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v2/orders"
        captured["params"] = dict(request.url.params)
        return _json([parent])

    adapter = _adapter(handler)
    orders = await adapter.list_orders(status="closed", nested=True, limit=50)

    # Query params serialized as Alpaca expects.
    assert captured["params"]["status"] == "closed"
    assert captured["params"]["nested"] == "true"
    assert captured["params"]["limit"] == "50"

    assert len(orders) == 1
    order = orders[0]
    assert order.broker_order_id == "parent-1"
    assert order.order_class is OrderClass.bracket
    assert order.status is OrderStatus.filled
    assert order.filled_avg_price == Decimal("190.125")
    assert order.submitted_at == datetime(2026, 6, 23, 13, 30, 0, 123456, tzinfo=UTC)
    assert order.submitted_at is not None
    assert order.submitted_at.tzinfo is UTC
    # Legs recursed.
    assert {leg.broker_order_id for leg in order.legs} == {"leg-tp", "leg-sl"}
    tp = next(leg for leg in order.legs if leg.broker_order_id == "leg-tp")
    assert tp.limit_price == Decimal("200.00")
    assert tp.filled_qty == Decimal("0")  # never None
    sl = next(leg for leg in order.legs if leg.broker_order_id == "leg-sl")
    assert sl.stop_price == Decimal("180.00")
    await adapter.aclose()


async def test_list_orders_passes_time_bounds_as_utc() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["params"] = dict(request.url.params)
        return _json([])

    adapter = _adapter(handler)
    after = datetime(2026, 6, 23, 9, 30, tzinfo=UTC)
    until = datetime(2026, 6, 23, 16, 0, tzinfo=UTC)
    await adapter.list_orders(after=after, until=until)

    assert captured["params"]["after"].startswith("2026-06-23T09:30:00")
    assert captured["params"]["until"].startswith("2026-06-23T16:00:00")
    await adapter.aclose()


async def test_get_order_by_client_id() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        captured["params"] = dict(request.url.params)
        return _json(_order_payload(client_order_id="cid-reconcile"))

    adapter = _adapter(handler)
    order = await adapter.get_order_by_client_id("cid-reconcile")

    # The reconciliation endpoint + query key.
    assert captured["path"] == "/v2/orders:by_client_order_id"
    assert captured["params"]["client_order_id"] == "cid-reconcile"
    assert order is not None
    assert order.client_order_id == "cid-reconcile"
    await adapter.aclose()


async def test_get_order_by_client_id_missing_returns_none() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return _json({"message": "order not found"}, status_code=404)

    adapter = _adapter(handler)
    assert await adapter.get_order_by_client_id("cid-nope") is None
    await adapter.aclose()


async def test_get_order_missing_returns_none() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v2/orders/does-not-exist"
        return _json({"message": "order not found"}, status_code=404)

    adapter = _adapter(handler)
    assert await adapter.get_order("does-not-exist") is None
    await adapter.aclose()


# ── Clock / calendar ──────────────────────────────────────────────────


async def test_get_clock_parses_offset_timestamps_to_utc() -> None:
    clock_body = {
        "timestamp": "2026-06-23T09:45:00-04:00",
        "is_open": True,
        "next_open": "2026-06-24T09:30:00-04:00",
        "next_close": "2026-06-23T16:00:00-04:00",
    }

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v2/clock"
        return _json(clock_body)

    adapter = _adapter(handler)
    clock = await adapter.get_clock()

    assert clock.is_open is True
    # -04:00 (EDT) 09:45 == 13:45 UTC.
    assert clock.timestamp == datetime(2026, 6, 23, 13, 45, tzinfo=UTC)
    assert clock.next_close == datetime(2026, 6, 23, 20, 0, tzinfo=UTC)
    assert clock.timestamp.tzinfo is UTC
    await adapter.aclose()


async def test_get_calendar_localizes_eastern_walltime() -> None:
    # Alpaca returns open/close as bare HH:MM *Eastern* wall times.
    calendar_body = [
        {"date": "2026-06-23", "open": "09:30", "close": "16:00"},
        # An early-close day (e.g. day before a holiday) at 13:00 ET.
        {"date": "2026-11-27", "open": "09:30", "close": "13:00"},
    ]
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v2/calendar"
        captured["params"] = dict(request.url.params)
        return _json(calendar_body)

    adapter = _adapter(handler)
    days = await adapter.get_calendar(date(2026, 6, 23), date(2026, 11, 27))

    assert captured["params"]["start"] == "2026-06-23"
    assert captured["params"]["end"] == "2026-11-27"

    # June 23 is EDT (-04:00): 09:30 ET == 13:30 UTC, 16:00 ET == 20:00 UTC.
    summer = days[0]
    assert summer.trading_date == date(2026, 6, 23)
    assert summer.session_open == datetime(2026, 6, 23, 13, 30, tzinfo=UTC)
    assert summer.session_close == datetime(2026, 6, 23, 20, 0, tzinfo=UTC)
    # Nov 27 is EST (-05:00): 09:30 ET == 14:30 UTC, early close 13:00 == 18:00.
    autumn = days[1]
    assert autumn.session_open == datetime(2026, 11, 27, 14, 30, tzinfo=UTC)
    assert autumn.session_close == datetime(2026, 11, 27, 18, 0, tzinfo=UTC)
    await adapter.aclose()


# ── submit_order: request body shapes ─────────────────────────────────


async def test_submit_market_order_body() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == "/v2/orders"
        captured["body"] = json_body(request)
        return _json(_order_payload(status="accepted"), status_code=200)

    adapter = _adapter(handler)
    req = OrderRequest(
        client_order_id="cid-mkt",
        symbol="AAPL",
        side=OrderSide.buy,
        order_type=OrderType.market,
        time_in_force=TimeInForce.day,
        qty=Decimal("10"),
    )
    order = await adapter.submit_order(req)

    body = captured["body"]
    assert body["symbol"] == "AAPL"
    assert body["side"] == "buy"
    assert body["type"] == "market"
    assert body["time_in_force"] == "day"
    assert body["order_class"] == "simple"
    assert body["client_order_id"] == "cid-mkt"
    assert body["qty"] == "10"  # Decimal serialized as string
    assert "notional" not in body
    assert "take_profit" not in body
    assert "stop_loss" not in body
    assert order.status is OrderStatus.accepted
    await adapter.aclose()


async def test_submit_notional_order_body() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json_body(request)
        return _json(_order_payload(status="accepted"))

    adapter = _adapter(handler)
    req = OrderRequest(
        client_order_id="cid-not",
        symbol="AAPL",
        side=OrderSide.buy,
        order_type=OrderType.market,
        time_in_force=TimeInForce.day,
        notional=Decimal("2500.50"),
    )
    await adapter.submit_order(req)

    body = captured["body"]
    assert body["notional"] == "2500.50"
    assert "qty" not in body
    await adapter.aclose()


async def test_submit_bracket_order_body_shape() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json_body(request)
        return _json(_order_payload(order_class="bracket", status="accepted"))

    adapter = _adapter(handler)
    req = OrderRequest(
        client_order_id="cid-brk",
        symbol="AAPL",
        side=OrderSide.buy,
        order_type=OrderType.limit,
        time_in_force=TimeInForce.day,
        order_class=OrderClass.bracket,
        qty=Decimal("10"),
        limit_price=Decimal("190.00"),
        take_profit_limit_price=Decimal("200.00"),
        stop_loss_stop_price=Decimal("180.00"),
        stop_loss_limit_price=Decimal("179.50"),
    )
    await adapter.submit_order(req)

    body = captured["body"]
    assert body["order_class"] == "bracket"
    assert body["limit_price"] == "190.00"
    # Nested protective legs, Decimals stringified.
    assert body["take_profit"] == {"limit_price": "200.00"}
    assert body["stop_loss"] == {"stop_price": "180.00", "limit_price": "179.50"}
    await adapter.aclose()


async def test_submit_bracket_stop_loss_without_limit() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json_body(request)
        return _json(_order_payload(order_class="bracket", status="accepted"))

    adapter = _adapter(handler)
    req = OrderRequest(
        client_order_id="cid-brk2",
        symbol="AAPL",
        side=OrderSide.buy,
        order_type=OrderType.market,
        time_in_force=TimeInForce.day,
        order_class=OrderClass.bracket,
        qty=Decimal("5"),
        stop_loss_stop_price=Decimal("180.00"),
    )
    await adapter.submit_order(req)

    body = captured["body"]
    # stop_loss carries only stop_price (no limit) → plain stop exit.
    assert body["stop_loss"] == {"stop_price": "180.00"}
    assert "take_profit" not in body
    await adapter.aclose()


async def test_submit_trailing_stop_body() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json_body(request)
        return _json(_order_payload(type="trailing_stop", status="accepted"))

    adapter = _adapter(handler)
    req = OrderRequest(
        client_order_id="cid-trail",
        symbol="AAPL",
        side=OrderSide.sell,
        order_type=OrderType.trailing_stop,
        time_in_force=TimeInForce.day,
        qty=Decimal("10"),
        trail_percent=Decimal("1.5"),
    )
    await adapter.submit_order(req)

    body = captured["body"]
    assert body["type"] == "trailing_stop"
    assert body["trail_percent"] == "1.5"
    await adapter.aclose()


# ── Error taxonomy ────────────────────────────────────────────────────


async def test_submit_422_raises_order_rejected() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return _json({"message": "insufficient buying power"}, status_code=422)

    adapter = _adapter(handler)
    req = OrderRequest(
        client_order_id="cid-rej",
        symbol="AAPL",
        side=OrderSide.buy,
        order_type=OrderType.market,
        time_in_force=TimeInForce.day,
        qty=Decimal("10"),
    )
    with pytest.raises(OrderRejected) as exc_info:
        await adapter.submit_order(req)
    assert exc_info.value.status_code == 422
    assert "insufficient buying power" in str(exc_info.value)
    await adapter.aclose()


async def test_submit_400_raises_order_rejected() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return _json({"message": "malformed bracket"}, status_code=400)

    adapter = _adapter(handler)
    req = OrderRequest(
        client_order_id="cid-rej2",
        symbol="AAPL",
        side=OrderSide.buy,
        order_type=OrderType.market,
        time_in_force=TimeInForce.day,
        qty=Decimal("10"),
    )
    with pytest.raises(OrderRejected):
        await adapter.submit_order(req)
    await adapter.aclose()


async def test_auth_401_raises_broker_auth_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return _json({"message": "unauthorized"}, status_code=401)

    adapter = _adapter(handler)
    with pytest.raises(BrokerAuthError) as exc_info:
        await adapter.get_account()
    assert exc_info.value.status_code == 401
    await adapter.aclose()


async def test_auth_403_raises_broker_auth_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return _json({"message": "forbidden"}, status_code=403)

    adapter = _adapter(handler)
    with pytest.raises(BrokerAuthError):
        await adapter.get_account()
    await adapter.aclose()


async def test_429_raises_rate_limited_with_retry_after() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            429,
            headers={"Retry-After": "30"},
            json={"message": "too many requests"},
        )

    adapter = _adapter(handler)
    with pytest.raises(BrokerRateLimited) as exc_info:
        await adapter.get_account()
    assert exc_info.value.status_code == 429
    assert exc_info.value.retry_after == 30.0
    await adapter.aclose()


async def test_429_without_retry_after_header() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return _json({"message": "too many requests"}, status_code=429)

    adapter = _adapter(handler)
    with pytest.raises(BrokerRateLimited) as exc_info:
        await adapter.list_positions()
    assert exc_info.value.retry_after is None
    await adapter.aclose()


async def test_read_5xx_raises_broker_unavailable() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return _json({"message": "internal error"}, status_code=503)

    adapter = _adapter(handler)
    with pytest.raises(BrokerUnavailable) as exc_info:
        await adapter.get_account()
    assert exc_info.value.status_code == 503
    await adapter.aclose()


async def test_submit_timeout_raises_ambiguous_order_state() -> None:
    """The single most important error mapping: a submit that may have landed."""

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectTimeout("timed out", request=request)

    adapter = _adapter(handler)
    req = OrderRequest(
        client_order_id="cid-ambig",
        symbol="AAPL",
        side=OrderSide.buy,
        order_type=OrderType.market,
        time_in_force=TimeInForce.day,
        qty=Decimal("10"),
    )
    with pytest.raises(AmbiguousOrderState):
        await adapter.submit_order(req)
    await adapter.aclose()


async def test_submit_transport_error_raises_ambiguous_order_state() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection reset", request=request)

    adapter = _adapter(handler)
    req = OrderRequest(
        client_order_id="cid-ambig2",
        symbol="AAPL",
        side=OrderSide.buy,
        order_type=OrderType.market,
        time_in_force=TimeInForce.day,
        qty=Decimal("10"),
    )
    with pytest.raises(AmbiguousOrderState):
        await adapter.submit_order(req)
    await adapter.aclose()


async def test_read_timeout_raises_broker_unavailable_not_ambiguous() -> None:
    """The SAME transport failure on a read is merely BrokerUnavailable."""

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("timed out", request=request)

    adapter = _adapter(handler)
    with pytest.raises(BrokerUnavailable):
        await adapter.list_positions()
    # And it is NOT the ambiguous type (proves reads don't poison reconciliation).
    with pytest.raises(BrokerUnavailable) as exc_info:
        await adapter.get_clock()
    assert not isinstance(exc_info.value, AmbiguousOrderState)
    await adapter.aclose()


# ── Cancels / closes ──────────────────────────────────────────────────


async def test_cancel_order_issues_delete() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["path"] = request.url.path
        return httpx.Response(204)

    adapter = _adapter(handler)
    await adapter.cancel_order("order-uuid-1")
    assert captured["method"] == "DELETE"
    assert captured["path"] == "/v2/orders/order-uuid-1"
    await adapter.aclose()


async def test_cancel_order_already_gone_is_noop() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        # 422 = not cancelable (already filled/canceled). Idempotent no-op.
        return _json({"message": "order is not cancelable"}, status_code=422)

    adapter = _adapter(handler)
    # Must NOT raise — cancel is idempotent best-effort.
    await adapter.cancel_order("order-uuid-gone")
    await adapter.aclose()


async def test_cancel_all_orders_issues_delete() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["path"] = request.url.path
        return _json([], status_code=207)

    adapter = _adapter(handler)
    await adapter.cancel_all_orders()
    assert captured["method"] == "DELETE"
    assert captured["path"] == "/v2/orders"
    await adapter.aclose()


async def test_close_position_with_qty() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["path"] = request.url.path
        captured["params"] = dict(request.url.params)
        return _json(_order_payload(id="close-order", side="sell", status="accepted"))

    adapter = _adapter(handler)
    order = await adapter.close_position("AAPL", qty=Decimal("5"))
    assert captured["method"] == "DELETE"
    assert captured["path"] == "/v2/positions/AAPL"
    assert captured["params"]["qty"] == "5"
    assert "percentage" not in captured["params"]
    assert order.broker_order_id == "close-order"
    await adapter.aclose()


async def test_close_position_with_percentage() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["params"] = dict(request.url.params)
        return _json(_order_payload(id="close-pct", status="accepted"))

    adapter = _adapter(handler)
    await adapter.close_position("AAPL", percentage=Decimal("50"))
    assert captured["params"]["percentage"] == "50"
    assert "qty" not in captured["params"]
    await adapter.aclose()


async def test_close_position_rejects_qty_and_percentage_together() -> None:
    adapter = _adapter(lambda request: _json({}))
    with pytest.raises(ValueError, match="at most one of qty or percentage"):
        await adapter.close_position("AAPL", qty=Decimal("5"), percentage=Decimal("50"))
    await adapter.aclose()


async def test_close_all_positions_cancels_orders_flag() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["path"] = request.url.path
        captured["params"] = dict(request.url.params)
        return _json([], status_code=207)

    adapter = _adapter(handler)
    await adapter.close_all_positions(cancel_orders=True)
    assert captured["method"] == "DELETE"
    assert captured["path"] == "/v2/positions"
    assert captured["params"]["cancel_orders"] == "true"
    await adapter.aclose()


# ── Rate limiting + lifecycle ─────────────────────────────────────────


async def test_every_request_acquires_a_token() -> None:
    """The bucket gates the wire: each call consumes exactly one token."""

    class CountingBucket(AsyncTokenBucket):
        def __init__(self) -> None:
            super().__init__(rate_per_minute=1_000_000)
            self.acquired = 0

        async def acquire(self, tokens: int = 1) -> None:
            self.acquired += tokens
            await super().acquire(tokens)

    bucket = CountingBucket()
    client = httpx.AsyncClient(
        base_url="https://paper-api.alpaca.markets",
        transport=httpx.MockTransport(lambda request: _json(_clock_min())),
    )
    adapter = AlpacaBrokerAdapter(PAPER_CREDS, client=client, bucket=bucket)
    await adapter.get_clock()
    await adapter.get_clock()
    assert bucket.acquired == 2
    await adapter.aclose()


async def test_aclose_is_idempotent() -> None:
    adapter = _adapter(lambda request: _json({}))
    await adapter.aclose()
    await adapter.aclose()  # second call must not raise
    assert adapter._closed is True


async def test_context_manager_closes_transport() -> None:
    closed: dict[str, bool] = {"value": False}

    def handler(request: httpx.Request) -> httpx.Response:
        return _json(_clock_min())

    client = httpx.AsyncClient(
        base_url="https://paper-api.alpaca.markets",
        transport=httpx.MockTransport(handler),
    )
    adapter = AlpacaBrokerAdapter(PAPER_CREDS, client=client)
    async with adapter:
        await adapter.get_clock()
    closed["value"] = client.is_closed
    assert closed["value"] is True


# ── Status map table test ─────────────────────────────────────────────


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("new", OrderStatus.submitted),
        ("accepted", OrderStatus.accepted),
        ("pending_new", OrderStatus.submitted),
        ("accepted_for_bidding", OrderStatus.accepted),
        ("partially_filled", OrderStatus.partially_filled),
        ("filled", OrderStatus.filled),
        ("done_for_day", OrderStatus.done_for_day),
        ("canceled", OrderStatus.canceled),
        ("expired", OrderStatus.expired),
        ("replaced", OrderStatus.replaced),
        ("restated", OrderStatus.replaced),
        ("pending_cancel", OrderStatus.pending_cancel),
        ("pending_replace", OrderStatus.pending_replace),
        ("pending_review", OrderStatus.held),
        ("stopped", OrderStatus.stopped),
        ("rejected", OrderStatus.rejected),
        ("suspended", OrderStatus.suspended),
        ("calculated", OrderStatus.calculated),
        ("held", OrderStatus.held),
    ],
)
def test_status_map_covers_every_alpaca_status(raw: str, expected: OrderStatus) -> None:
    assert ALPACA_STATUS_MAP[raw] is expected
    assert map_status(raw) is expected


def test_status_map_unknown_falls_back_without_raising() -> None:
    # A status we've never seen must degrade, not crash (forward-compat).
    assert map_status("some_future_status") is OrderStatus.held


def test_status_map_has_no_extra_keys() -> None:
    # Guards against a typo'd duplicate or stray key creeping into the table.
    assert len(ALPACA_STATUS_MAP) == 19


# ── Helpers ───────────────────────────────────────────────────────────


def json_body(request: httpx.Request) -> dict[str, Any]:
    """Decode a request's JSON body (MockTransport gives us the raw content)."""
    import json as _json_mod

    return dict(_json_mod.loads(request.content.decode()))


def _clock_min() -> dict[str, Any]:
    """Minimal valid clock payload for tests that only exercise plumbing."""
    return {
        "timestamp": "2026-06-23T13:30:00Z",
        "is_open": True,
        "next_open": "2026-06-24T13:30:00Z",
        "next_close": "2026-06-23T20:00:00Z",
    }
