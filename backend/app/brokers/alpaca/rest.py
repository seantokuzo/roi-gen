"""Alpaca Trading API v2 REST adapter.

Concrete :class:`~app.brokers.base.BrokerAdapter` over Alpaca's REST surface.
Everything the engine needs that is *request/response* (not streaming) lives
here; the trade-updates websocket is a sibling module.

Design rules baked in:

- **Every** REST call goes through the injected :class:`AsyncTokenBucket`
  (``acquire`` before the wire) so concurrent coroutines can never blow past
  Alpaca's 200 req/min trading cap (project CLAUDE.md).
- **Money/qty parse through ``Decimal(str(value))``** — never float. Alpaca
  returns numbers as JSON strings already, but we coerce defensively so a bare
  number can't sneak a binary-float through (iron law #7).
- **Timestamps land tz-aware UTC** (iron law #5). Clock/order timestamps are
  RFC-3339 with an offset; calendar open/close are ``HH:MM`` *Eastern* wall
  times that we localize to ``America/New_York`` then convert to UTC.
- **No PDT fields are ever read** (iron law #3): ``pattern_day_trader`` /
  ``daytrade_count`` / ``daytrading_buying_power`` are deleted from the API and
  must not reappear here.
- **Error mapping disambiguates intent** for the never-resubmit discipline: a
  timeout/transport drop *on submit* is :class:`AmbiguousOrderState` (the order
  may have landed → reconcile, never blind-resubmit); the same failure on a
  read is merely :class:`BrokerUnavailable`.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

import httpx

from app.brokers.alpaca.status import map_status
from app.brokers.base import BrokerAdapter
from app.brokers.dto import (
    BrokerAccount,
    BrokerOrder,
    BrokerPosition,
    CalendarDay,
    MarketClock,
    OrderRequest,
)
from app.brokers.errors import (
    AmbiguousOrderState,
    BrokerAuthError,
    BrokerError,
    BrokerRateLimited,
    BrokerUnavailable,
    OrderRejected,
)
from app.brokers.ratelimit import AsyncTokenBucket
from app.core.logging import get_logger
from app.models.enums import (
    OrderClass,
    OrderSide,
    OrderType,
    TimeInForce,
)

if TYPE_CHECKING:
    from app.brokers.credentials import BrokerCredentials

logger = get_logger(__name__)

_PAPER_BASE_URL = "https://paper-api.alpaca.markets"
_LIVE_BASE_URL = "https://api.alpaca.markets"

#: Alpaca's trading API ceiling (project CLAUDE.md). One bucket per adapter.
_DEFAULT_RATE_PER_MIN = 200

#: Alpaca clock/calendar speak Eastern; calendar open/close are bare HH:MM ET.
_MARKET_TZ = ZoneInfo("America/New_York")

_DEFAULT_TIMEOUT = httpx.Timeout(10.0, connect=5.0)


# ── Parse helpers ─────────────────────────────────────────────────────


def _dec(value: Any) -> Decimal | None:
    """Parse an Alpaca JSON number/string to ``Decimal`` (``None`` passes through).

    Always routes through ``str`` so a JSON float (should Alpaca ever emit one)
    can't introduce binary-float error before it reaches ``Decimal``.
    """
    if value is None:
        return None
    return Decimal(str(value))


def _dec0(value: Any) -> Decimal:
    """Like :func:`_dec` but ``None``/absent collapses to ``Decimal("0")``."""
    parsed = _dec(value)
    return parsed if parsed is not None else Decimal("0")


def _dt(value: Any) -> datetime | None:
    """Parse an RFC-3339 timestamp to tz-aware UTC (``None`` passes through).

    Alpaca emits offset-bearing timestamps (e.g. ``...-04:00`` or ``...Z``).
    ``Z`` is normalized for :meth:`datetime.fromisoformat`, and any value that
    somehow arrives naive is assumed UTC, then everything is converted to UTC.
    """
    if value is None:
        return None
    text = str(value)
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _et_walltime_to_utc(day: date, hhmm: str) -> datetime:
    """Combine a calendar date + ``HH:MM`` Eastern wall time → tz-aware UTC.

    Alpaca's calendar ``open``/``close`` are local Eastern strings with no date
    and no offset; the offset depends on whether that date is in EDT or EST, so
    we localize against ``America/New_York`` (which resolves DST for the date)
    and then convert to UTC.
    """
    hour_str, minute_str = hhmm.split(":")
    local = datetime(
        day.year,
        day.month,
        day.day,
        int(hour_str),
        int(minute_str),
        tzinfo=_MARKET_TZ,
    )
    return local.astimezone(UTC)


def _enum_or(default: Any, enum_cls: Any, raw: Any) -> Any:
    """Coerce a raw broker string into ``enum_cls``; fall back to ``default``.

    Alpaca uses ``''`` for a simple order class (and could add a value before we
    ship a mapping), so an unrecognized member degrades to ``default`` with a
    warning rather than raising mid-parse.
    """
    if raw is None or raw == "":
        return default
    try:
        return enum_cls(raw)
    except ValueError:
        logger.warning(
            "alpaca.unknown_enum_value",
            enum=enum_cls.__name__,
            raw_value=raw,
            fallback=str(default),
        )
        return default


# ── Adapter ───────────────────────────────────────────────────────────


class AlpacaBrokerAdapter(BrokerAdapter):
    """Alpaca Trading API v2 adapter bound to one (paper or live) account."""

    def __init__(
        self,
        credentials: BrokerCredentials,
        *,
        client: httpx.AsyncClient | None = None,
        bucket: AsyncTokenBucket | None = None,
    ) -> None:
        self._base_url = _PAPER_BASE_URL if credentials.paper else _LIVE_BASE_URL
        self._paper = credentials.paper
        headers = {
            "APCA-API-KEY-ID": credentials.api_key,
            "APCA-API-SECRET-KEY": credentials.api_secret,
        }
        # Track whether WE created the client: an injected client is the test's
        # to own, but aclose() closing either is harmless and documented.
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            base_url=self._base_url,
            headers=headers,
            timeout=_DEFAULT_TIMEOUT,
        )
        if not self._owns_client:
            # Injected clients (tests) may lack our auth/base — set them so the
            # adapter behaves identically regardless of who built the transport.
            self._client.headers.update(headers)
        self._bucket = bucket or AsyncTokenBucket(rate_per_minute=_DEFAULT_RATE_PER_MIN)
        self._closed = False

    # ── Transport core ───────────────────────────────────────────────

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
        is_submit: bool = False,
    ) -> httpx.Response:
        """Rate-limited HTTP request with the broker error taxonomy applied.

        ``is_submit`` flips the ambiguous-failure behaviour: only order
        submission treats a timeout/transport drop as
        :class:`AmbiguousOrderState`. Reads and cancels surface those as
        :class:`BrokerUnavailable` (safe to retry).
        """
        await self._bucket.acquire()
        try:
            response = await self._client.request(method, path, params=params, json=json)
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            if is_submit:
                msg = (
                    f"submit to {path} failed mid-flight ({type(exc).__name__}); "
                    "order state is UNKNOWN — reconcile by client_order_id"
                )
                raise AmbiguousOrderState(msg) from exc
            msg = f"{method} {path} transport failure: {type(exc).__name__}"
            raise BrokerUnavailable(msg) from exc

        self._raise_for_status(response, is_submit=is_submit)
        return response

    @staticmethod
    def _retry_after(response: httpx.Response) -> float | None:
        """Parse a ``Retry-After`` header (seconds) when present and numeric."""
        raw = response.headers.get("Retry-After")
        if raw is None:
            return None
        try:
            return float(raw)
        except ValueError:
            return None

    def _raise_for_status(self, response: httpx.Response, *, is_submit: bool) -> None:
        """Translate an HTTP error response into the broker exception taxonomy."""
        status = response.status_code
        if status < 400:
            return

        body = self._safe_body(response)
        if status in (401, 403):
            raise BrokerAuthError(f"alpaca auth rejected ({status}): {body}", status_code=status)
        if status == 429:
            raise BrokerRateLimited(
                f"alpaca rate limited: {body}",
                status_code=status,
                retry_after=self._retry_after(response),
            )
        if status in (400, 422) and is_submit:
            # Definitive order-level refusal: the broker received and rejected
            # it, so it was NOT placed — surface, never reconcile.
            raise OrderRejected(f"alpaca rejected order ({status}): {body}", status_code=status)
        if status >= 500:
            raise BrokerUnavailable(f"alpaca server error ({status}): {body}", status_code=status)
        # Other 4xx on reads/cancels (404 handled by callers before here for the
        # "missing → None" cases; anything else is a genuine client error).
        raise BrokerError(f"alpaca request failed ({status}): {body}", status_code=status)

    @staticmethod
    def _safe_body(response: httpx.Response) -> str:
        """Best-effort response text for error messages (never raises)."""
        try:
            return response.text
        except (UnicodeDecodeError, httpx.HTTPError):  # pragma: no cover
            return "<unreadable response body>"

    # ── Market calendar / clock ──────────────────────────────────────

    async def get_clock(self) -> MarketClock:
        response = await self._request("GET", "/v2/clock")
        data: dict[str, Any] = response.json()
        return MarketClock(
            timestamp=_require_dt(_dt(data.get("timestamp")), "clock.timestamp"),
            is_open=bool(data["is_open"]),
            next_open=_require_dt(_dt(data.get("next_open")), "clock.next_open"),
            next_close=_require_dt(_dt(data.get("next_close")), "clock.next_close"),
        )

    async def get_calendar(self, start: date, end: date) -> list[CalendarDay]:
        params = {"start": start.isoformat(), "end": end.isoformat()}
        response = await self._request("GET", "/v2/calendar", params=params)
        rows: list[dict[str, Any]] = response.json()
        days: list[CalendarDay] = []
        for row in rows:
            trading_date = date.fromisoformat(str(row["date"]))
            days.append(
                CalendarDay(
                    trading_date=trading_date,
                    session_open=_et_walltime_to_utc(trading_date, str(row["open"])),
                    session_close=_et_walltime_to_utc(trading_date, str(row["close"])),
                )
            )
        return days

    # ── Account / positions ──────────────────────────────────────────

    async def get_account(self) -> BrokerAccount:
        response = await self._request("GET", "/v2/account")
        data: dict[str, Any] = response.json()
        # Alpaca has no single position_market_value; it splits long/short.
        # Net = long + short (short market value is already negative).
        long_mv = _dec0(data.get("long_market_value"))
        short_mv = _dec0(data.get("short_market_value"))
        return BrokerAccount(
            account_id=str(data["id"]),
            status=str(data["status"]),
            currency=str(data["currency"]),
            equity=_dec0(data.get("equity")),
            last_equity=_dec0(data.get("last_equity")),
            cash=_dec0(data.get("cash")),
            buying_power=_dec0(data.get("buying_power")),
            position_market_value=long_mv + short_mv,
            trading_blocked=bool(data.get("trading_blocked", False)),
            account_blocked=bool(data.get("account_blocked", False)),
            raw=data,
        )

    async def list_positions(self) -> list[BrokerPosition]:
        response = await self._request("GET", "/v2/positions")
        rows: list[dict[str, Any]] = response.json()
        return [self._parse_position(row) for row in rows]

    async def get_position(self, symbol: str) -> BrokerPosition | None:
        try:
            response = await self._request("GET", f"/v2/positions/{symbol}")
        except BrokerError as exc:
            if exc.status_code == 404:
                return None  # flat in this symbol
            raise
        return self._parse_position(response.json())

    @staticmethod
    def _parse_position(data: dict[str, Any]) -> BrokerPosition:
        # qty is signed by Alpaca for shorts; keep the sign (DTO contract).
        return BrokerPosition(
            symbol=str(data["symbol"]),
            qty=_dec0(data.get("qty")),
            side=str(data.get("side", "")),
            avg_entry_price=_dec0(data.get("avg_entry_price")),
            market_value=_dec0(data.get("market_value")),
            cost_basis=_dec0(data.get("cost_basis")),
            unrealized_pl=_dec0(data.get("unrealized_pl")),
            current_price=_dec(data.get("current_price")),
            raw=data,
        )

    # ── Orders (reads) ───────────────────────────────────────────────

    async def list_orders(
        self,
        *,
        status: str = "open",
        after: datetime | None = None,
        until: datetime | None = None,
        limit: int = 100,
        nested: bool = True,
    ) -> list[BrokerOrder]:
        params: dict[str, Any] = {
            "status": status,
            "limit": limit,
            "nested": str(nested).lower(),
            "direction": "desc",
        }
        if after is not None:
            params["after"] = _to_rfc3339(after)
        if until is not None:
            params["until"] = _to_rfc3339(until)
        response = await self._request("GET", "/v2/orders", params=params)
        rows: list[dict[str, Any]] = response.json()
        return [self._parse_order(row) for row in rows]

    async def get_order_by_client_id(self, client_order_id: str) -> BrokerOrder | None:
        try:
            response = await self._request(
                "GET",
                "/v2/orders:by_client_order_id",
                params={"client_order_id": client_order_id},
            )
        except BrokerError as exc:
            if exc.status_code == 404:
                return None
            raise
        return self._parse_order(response.json())

    async def get_order(self, broker_order_id: str) -> BrokerOrder | None:
        try:
            response = await self._request(
                "GET",
                f"/v2/orders/{broker_order_id}",
                params={"nested": "true"},
            )
        except BrokerError as exc:
            if exc.status_code == 404:
                return None
            raise
        return self._parse_order(response.json())

    @classmethod
    def _parse_order(cls, data: dict[str, Any]) -> BrokerOrder:
        """Map a raw Alpaca order dict to :class:`BrokerOrder` (recursing legs)."""
        legs_raw = data.get("legs") or []
        legs = [cls._parse_order(leg) for leg in legs_raw]
        return BrokerOrder(
            broker_order_id=str(data["id"]),
            client_order_id=_opt_str(data.get("client_order_id")),
            symbol=str(data["symbol"]),
            side=_enum_or(OrderSide.buy, OrderSide, data.get("side")),
            order_type=_enum_or(
                OrderType.market, OrderType, data.get("type") or data.get("order_type")
            ),
            order_class=_enum_or(OrderClass.simple, OrderClass, data.get("order_class")),
            time_in_force=_enum_or(TimeInForce.day, TimeInForce, data.get("time_in_force")),
            status=map_status(str(data.get("status", ""))),
            qty=_dec(data.get("qty")),
            filled_qty=_dec0(data.get("filled_qty")),
            limit_price=_dec(data.get("limit_price")),
            stop_price=_dec(data.get("stop_price")),
            trail_percent=_dec(data.get("trail_percent")),
            filled_avg_price=_dec(data.get("filled_avg_price")),
            extended_hours=bool(data.get("extended_hours", False)),
            submitted_at=_dt(data.get("submitted_at")),
            filled_at=_dt(data.get("filled_at")),
            canceled_at=_dt(data.get("canceled_at")),
            legs=legs,
            raw=data,
        )

    # ── Orders (mutations — execution-handler only, iron law #1) ─────

    async def submit_order(self, req: OrderRequest) -> BrokerOrder:
        body = _build_order_body(req)
        response = await self._request("POST", "/v2/orders", json=body, is_submit=True)
        return self._parse_order(response.json())

    async def cancel_order(self, broker_order_id: str) -> None:
        try:
            await self._request("DELETE", f"/v2/orders/{broker_order_id}")
        except BrokerError as exc:
            # Idempotent best-effort: already-gone or non-cancelable is a no-op.
            if exc.status_code in (404, 422):
                logger.info(
                    "alpaca.cancel_order_noop",
                    broker_order_id=broker_order_id,
                    status_code=exc.status_code,
                )
                return
            raise

    async def cancel_all_orders(self) -> None:
        # 207 multi-status is the success shape; _raise_for_status passes it.
        await self._request("DELETE", "/v2/orders")

    async def close_position(
        self,
        symbol: str,
        *,
        qty: Decimal | None = None,
        percentage: Decimal | None = None,
    ) -> BrokerOrder:
        if qty is not None and percentage is not None:
            msg = "pass at most one of qty or percentage to close_position"
            raise ValueError(msg)
        params: dict[str, Any] = {}
        if qty is not None:
            params["qty"] = str(qty)
        elif percentage is not None:
            params["percentage"] = str(percentage)
        response = await self._request("DELETE", f"/v2/positions/{symbol}", params=params or None)
        return self._parse_order(response.json())

    async def close_all_positions(self, *, cancel_orders: bool = True) -> None:
        await self._request(
            "DELETE",
            "/v2/positions",
            params={"cancel_orders": str(cancel_orders).lower()},
        )

    # ── Lifecycle ────────────────────────────────────────────────────

    async def aclose(self) -> None:
        """Close the httpx client. Idempotent (safe to call repeatedly).

        Closes the transport whether or not this adapter created it: an injected
        client is conceptually the caller's, but closing a closed/shared client
        is harmless and guarantees the context-manager exit always releases it.
        """
        if self._closed:
            return
        self._closed = True
        await self._client.aclose()


# ── Module-level builders (kept out of the class for unit testability) ─


def _require_dt(value: datetime | None, field: str) -> datetime:
    """Assert a required timestamp parsed (clock fields are never null)."""
    if value is None:  # pragma: no cover — Alpaca always sends these
        msg = f"alpaca response missing required timestamp: {field}"
        raise BrokerError(msg)
    return value


def _opt_str(value: Any) -> str | None:
    """Stringify a present value, preserving ``None``."""
    return None if value is None else str(value)


def _to_rfc3339(value: datetime) -> str:
    """Serialize a tz-aware datetime to RFC-3339 for an Alpaca query param.

    Naive datetimes are rejected: the engine is UTC-aware by iron law #5, and a
    naive bound would silently mean "server local" to Alpaca.
    """
    if value.tzinfo is None:
        msg = "datetime filters must be timezone-aware (iron law #5)"
        raise ValueError(msg)
    return value.astimezone(UTC).isoformat()


def _build_order_body(req: OrderRequest) -> dict[str, Any]:
    """Translate an :class:`OrderRequest` into Alpaca's POST /v2/orders body.

    All Decimals are serialized as strings (Alpaca expects string-encoded
    numbers and we must not let a float in). ``qty`` XOR ``notional`` and the
    price-by-type invariants are already guaranteed by ``OrderRequest``'s
    validator, so we don't re-check them here.
    """
    body: dict[str, Any] = {
        "symbol": req.symbol,
        "side": req.side.value,
        "type": req.order_type.value,
        "time_in_force": req.time_in_force.value,
        "order_class": req.order_class.value,
        "client_order_id": req.client_order_id,
        "extended_hours": req.extended_hours,
    }
    if req.qty is not None:
        body["qty"] = str(req.qty)
    if req.notional is not None:
        body["notional"] = str(req.notional)
    if req.limit_price is not None:
        body["limit_price"] = str(req.limit_price)
    if req.stop_price is not None:
        body["stop_price"] = str(req.stop_price)
    if req.trail_percent is not None:
        body["trail_percent"] = str(req.trail_percent)

    # Protective legs for bracket/OTO/OCO. take_profit needs a limit_price;
    # stop_loss needs a stop_price and optionally a limit_price (stop-limit exit).
    if req.take_profit_limit_price is not None:
        body["take_profit"] = {"limit_price": str(req.take_profit_limit_price)}
    if req.stop_loss_stop_price is not None or req.stop_loss_limit_price is not None:
        stop_loss: dict[str, str] = {}
        if req.stop_loss_stop_price is not None:
            stop_loss["stop_price"] = str(req.stop_loss_stop_price)
        if req.stop_loss_limit_price is not None:
            stop_loss["limit_price"] = str(req.stop_loss_limit_price)
        body["stop_loss"] = stop_loss

    return body
