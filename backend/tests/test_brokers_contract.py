"""Broker contract: DTO validation, Decimal fidelity, and credential loading."""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from app.brokers import (
    Bar,
    BrokerAccount,
    BrokerOrder,
    BrokerPosition,
)
from app.brokers.credentials import BrokerCredentials, load_credentials
from app.brokers.dto import CalendarDay, MarketClock, OrderRequest
from app.brokers.errors import CredentialsNotFound
from app.models import BrokerCredential, Portfolio, PortfolioMode, User
from app.models.enums import (
    OrderClass,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from app.services.crypto import encrypt_str

NOW = datetime(2026, 6, 23, 14, 30, tzinfo=UTC)


def _base_request(**overrides: object) -> dict[str, object]:
    """A valid simple-market OrderRequest payload, with field overrides."""
    payload: dict[str, object] = {
        "client_order_id": "cid-123",
        "symbol": "AAPL",
        "side": OrderSide.buy,
        "order_type": OrderType.market,
        "time_in_force": TimeInForce.day,
        "qty": Decimal("10"),
    }
    payload.update(overrides)
    return payload


# ── OrderRequest validation ──────────────────────────────────────────


def test_order_request_happy_path_market() -> None:
    req = OrderRequest(**_base_request())  # type: ignore[arg-type]
    assert req.order_class is OrderClass.simple
    assert req.qty == Decimal("10")
    assert req.notional is None
    assert req.extended_hours is False


def test_order_request_happy_path_notional() -> None:
    req = OrderRequest(**_base_request(qty=None, notional=Decimal("2500.50")))  # type: ignore[arg-type]
    assert req.qty is None
    assert req.notional == Decimal("2500.50")


def test_order_request_rejects_both_qty_and_notional() -> None:
    with pytest.raises(ValidationError, match="exactly one of qty or notional"):
        OrderRequest(**_base_request(notional=Decimal("100")))  # type: ignore[arg-type]


def test_order_request_rejects_neither_qty_nor_notional() -> None:
    with pytest.raises(ValidationError, match="exactly one of qty or notional"):
        OrderRequest(**_base_request(qty=None))  # type: ignore[arg-type]


def test_order_request_rejects_nonpositive_qty() -> None:
    with pytest.raises(ValidationError, match="qty must be positive"):
        OrderRequest(**_base_request(qty=Decimal("0")))  # type: ignore[arg-type]


def test_order_request_rejects_nonpositive_notional() -> None:
    with pytest.raises(ValidationError, match="notional must be positive"):
        OrderRequest(**_base_request(qty=None, notional=Decimal("-5")))  # type: ignore[arg-type]


def test_order_request_limit_requires_limit_price() -> None:
    with pytest.raises(ValidationError, match="requires limit_price"):
        OrderRequest(**_base_request(order_type=OrderType.limit))  # type: ignore[arg-type]
    # With the price it constructs.
    req = OrderRequest(
        **_base_request(order_type=OrderType.limit, limit_price=Decimal("190.25"))  # type: ignore[arg-type]
    )
    assert req.limit_price == Decimal("190.25")


def test_order_request_stop_requires_stop_price() -> None:
    with pytest.raises(ValidationError, match="requires stop_price"):
        OrderRequest(**_base_request(order_type=OrderType.stop))  # type: ignore[arg-type]


def test_order_request_stop_limit_requires_both_prices() -> None:
    # Missing limit_price.
    with pytest.raises(ValidationError, match="requires limit_price"):
        OrderRequest(
            **_base_request(order_type=OrderType.stop_limit, stop_price=Decimal("99"))  # type: ignore[arg-type]
        )
    # Missing stop_price.
    with pytest.raises(ValidationError, match="requires stop_price"):
        OrderRequest(
            **_base_request(order_type=OrderType.stop_limit, limit_price=Decimal("99"))  # type: ignore[arg-type]
        )
    # Both present → valid.
    req = OrderRequest(
        **_base_request(  # type: ignore[arg-type]
            order_type=OrderType.stop_limit,
            limit_price=Decimal("99"),
            stop_price=Decimal("98"),
        )
    )
    assert req.limit_price == Decimal("99")
    assert req.stop_price == Decimal("98")


def test_order_request_trailing_stop_requires_trail_percent() -> None:
    with pytest.raises(ValidationError, match="trailing_stop order requires trail_percent"):
        OrderRequest(**_base_request(order_type=OrderType.trailing_stop))  # type: ignore[arg-type]
    req = OrderRequest(
        **_base_request(order_type=OrderType.trailing_stop, trail_percent=Decimal("1.5"))  # type: ignore[arg-type]
    )
    assert req.trail_percent == Decimal("1.5")


def test_order_request_bracket_requires_protective_leg() -> None:
    with pytest.raises(ValidationError, match="take-profit or stop-loss"):
        OrderRequest(**_base_request(order_class=OrderClass.bracket))  # type: ignore[arg-type]
    # Take-profit alone is enough.
    tp = OrderRequest(
        **_base_request(  # type: ignore[arg-type]
            order_class=OrderClass.bracket, take_profit_limit_price=Decimal("200")
        )
    )
    assert tp.take_profit_limit_price == Decimal("200")
    # Stop-loss alone is enough.
    sl = OrderRequest(
        **_base_request(  # type: ignore[arg-type]
            order_class=OrderClass.bracket, stop_loss_stop_price=Decimal("180")
        )
    )
    assert sl.stop_loss_stop_price == Decimal("180")


def test_order_request_bracket_rejects_extended_hours() -> None:
    # Iron law #4: bracket is RTH-only.
    with pytest.raises(ValidationError, match="iron law #4"):
        OrderRequest(
            **_base_request(  # type: ignore[arg-type]
                order_class=OrderClass.bracket,
                take_profit_limit_price=Decimal("200"),
                extended_hours=True,
            )
        )


def test_order_request_simple_extended_hours_allowed() -> None:
    # A non-bracket extended-hours order is permitted at this layer (the broker
    # enforces the limit-type rule against live session state).
    req = OrderRequest(
        **_base_request(  # type: ignore[arg-type]
            order_type=OrderType.limit,
            limit_price=Decimal("190"),
            extended_hours=True,
        )
    )
    assert req.extended_hours is True


# ── Decimal fidelity ─────────────────────────────────────────────────


def test_decimal_precision_round_trips() -> None:
    # A price with more precision than a float can represent must survive intact.
    precise = Decimal("190.123456789")
    bar = Bar(
        symbol="AAPL",
        timestamp=NOW,
        open=precise,
        high=Decimal("190.2"),
        low=Decimal("189.9"),
        close=Decimal("190.05"),
        volume=Decimal("1234567.000000001"),
        trade_count=42,
        vwap=Decimal("190.0987654321"),
    )
    assert bar.open == precise
    assert bar.volume == Decimal("1234567.000000001")
    # JSON round-trip must not lose precision (Decimal serialized as string).
    restored = Bar.model_validate_json(bar.model_dump_json())
    assert restored.open == precise
    assert restored.vwap == Decimal("190.0987654321")
    assert restored.timestamp == NOW


def test_position_signed_qty_and_decimal() -> None:
    pos = BrokerPosition(
        symbol="TSLA",
        qty=Decimal("-15"),  # short
        side="short",
        avg_entry_price=Decimal("250.50"),
        market_value=Decimal("-3750.00"),
        cost_basis=Decimal("-3757.50"),
        unrealized_pl=Decimal("7.50"),
    )
    assert pos.qty == Decimal("-15")
    assert pos.qty < 0
    assert pos.current_price is None


def test_account_has_no_pdt_fields() -> None:
    # Iron law #3: PDT fields must not exist on the model.
    fields = set(BrokerAccount.model_fields)
    for forbidden in (
        "pattern_day_trader",
        "daytrade_count",
        "daytrading_buying_power",
    ):
        assert forbidden not in fields


# ── BrokerOrder leg nesting ──────────────────────────────────────────


def test_broker_order_legs_nesting() -> None:
    child_tp = BrokerOrder(
        broker_order_id="leg-tp",
        client_order_id=None,
        symbol="AAPL",
        side=OrderSide.sell,
        order_type=OrderType.limit,
        order_class=OrderClass.bracket,
        time_in_force=TimeInForce.day,
        status=OrderStatus.held,
        qty=Decimal("10"),
        limit_price=Decimal("200"),
    )
    child_sl = BrokerOrder(
        broker_order_id="leg-sl",
        symbol="AAPL",
        side=OrderSide.sell,
        order_type=OrderType.stop,
        order_class=OrderClass.bracket,
        time_in_force=TimeInForce.day,
        status=OrderStatus.held,
        qty=Decimal("10"),
        stop_price=Decimal("180"),
    )
    parent = BrokerOrder(
        broker_order_id="parent-1",
        client_order_id="cid-parent",
        symbol="AAPL",
        side=OrderSide.buy,
        order_type=OrderType.market,
        order_class=OrderClass.bracket,
        time_in_force=TimeInForce.day,
        status=OrderStatus.filled,
        qty=Decimal("10"),
        filled_qty=Decimal("10"),
        filled_avg_price=Decimal("190.10"),
        submitted_at=NOW,
        filled_at=NOW,
        legs=[child_tp, child_sl],
    )
    assert len(parent.legs) == 2
    assert {leg.broker_order_id for leg in parent.legs} == {"leg-tp", "leg-sl"}
    assert parent.legs[0].limit_price == Decimal("200")
    # filled_qty defaults to 0 (never None) on a leg that didn't set it.
    assert parent.legs[1].filled_qty == Decimal("0")
    # Deep round-trip preserves nesting.
    restored = BrokerOrder.model_validate_json(parent.model_dump_json())
    assert [leg.broker_order_id for leg in restored.legs] == ["leg-tp", "leg-sl"]


def test_broker_order_filled_qty_defaults_zero() -> None:
    order = BrokerOrder(
        broker_order_id="o-1",
        symbol="AAPL",
        side=OrderSide.buy,
        order_type=OrderType.market,
        order_class=OrderClass.simple,
        time_in_force=TimeInForce.day,
        status=OrderStatus.accepted,
        qty=Decimal("5"),
    )
    assert order.filled_qty == Decimal("0")
    assert order.legs == []
    assert order.raw == {}


def test_market_clock_is_frozen() -> None:
    clock = MarketClock(timestamp=NOW, is_open=True, next_open=NOW, next_close=NOW)
    with pytest.raises(ValidationError):
        clock.is_open = False  # type: ignore[misc]


# ── load_credentials ─────────────────────────────────────────────────


async def _seed_portfolio_with_credentials(
    session: AsyncSession,
    user: User,
    *,
    api_key: str,
    api_secret: str,
    paper: bool,
    broker: str = "alpaca",
) -> Portfolio:
    mode = PortfolioMode.paper if paper else PortfolioMode.live
    portfolio = Portfolio(user_id=user.id, name="creds-pf", mode=mode)
    session.add(portfolio)
    await session.flush()
    session.add(
        BrokerCredential(
            portfolio_id=portfolio.id,
            broker=broker,
            api_key_encrypted=encrypt_str(api_key),
            api_secret_encrypted=encrypt_str(api_secret),
            paper=paper,
        )
    )
    await session.commit()
    await session.refresh(portfolio)
    return portfolio


async def test_load_credentials_round_trips(db_session: AsyncSession, seeded_user: User) -> None:
    portfolio = await _seed_portfolio_with_credentials(
        db_session,
        seeded_user,
        api_key="AK-LIVE-PLAIN",
        api_secret="SK-LIVE-PLAIN",
        paper=True,
    )
    creds = await load_credentials(db_session, portfolio.id)
    assert isinstance(creds, BrokerCredentials)
    assert creds.api_key == "AK-LIVE-PLAIN"
    assert creds.api_secret == "SK-LIVE-PLAIN"
    assert creds.paper is True
    assert creds.broker == "alpaca"


async def test_load_credentials_carries_live_paper_flag(
    db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio = await _seed_portfolio_with_credentials(
        db_session,
        seeded_user,
        api_key="AK",
        api_secret="SK",
        paper=False,
    )
    creds = await load_credentials(db_session, portfolio.id)
    assert creds.paper is False


async def test_load_credentials_missing_raises(db_session: AsyncSession, seeded_user: User) -> None:
    # A portfolio with no credential row.
    portfolio = Portfolio(user_id=seeded_user.id, name="no-creds", mode=PortfolioMode.paper)
    db_session.add(portfolio)
    await db_session.commit()
    await db_session.refresh(portfolio)

    with pytest.raises(CredentialsNotFound, match=str(portfolio.id)):
        await load_credentials(db_session, portfolio.id)


async def test_load_credentials_unknown_portfolio_raises(db_session: AsyncSession) -> None:
    with pytest.raises(CredentialsNotFound):
        await load_credentials(db_session, uuid.uuid4())


def test_broker_credentials_is_frozen() -> None:
    creds = BrokerCredentials(api_key="AK", api_secret="SK", paper=True)
    with pytest.raises(ValidationError):
        creds.api_key = "mutated"  # type: ignore[misc]


def test_calendar_day_constructs() -> None:
    day = CalendarDay(
        trading_date=date(2026, 6, 23),
        session_open=datetime(2026, 6, 23, 13, 30, tzinfo=UTC),
        session_close=datetime(2026, 6, 23, 20, 0, tzinfo=UTC),
    )
    assert day.trading_date == date(2026, 6, 23)
    assert day.session_open.tzinfo is UTC
