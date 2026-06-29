"""RiskEngine.evaluate — the full sizing + gate sweep, and the mint guard."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from app.engine.risk.approval import RiskApproval
from app.engine.risk.engine import RiskEngine
from app.models.enums import OrderClass, OrderSide, OrderType
from tests.engine.builders import DEFAULT_NOW, make_limits, make_signal, make_state

# Every evaluation records one ControlCheck per control — the full audit trail.
_EXPECTED_CHECK_COUNT = 13


def _engine() -> RiskEngine:
    return RiskEngine(make_limits())


def test_clean_signal_is_approved_with_a_protected_bracket() -> None:
    decision = _engine().evaluate(make_signal(), make_state())

    assert decision.approved is True
    assert decision.approval is not None
    assert decision.order_request is not None
    assert len(decision.checks) == _EXPECTED_CHECK_COUNT
    assert all(c.passed for c in decision.checks)

    order = decision.order_request
    assert order.order_class is OrderClass.bracket
    assert order.qty == Decimal("250")
    assert order.side is OrderSide.buy
    assert order.symbol == "SPY"
    assert order.stop_loss_stop_price == Decimal("99")  # mandatory protection (iron law #4)
    assert order.limit_price is None  # market entry
    assert order.client_order_id.startswith("roigen-")

    approval = decision.approval
    assert approval.qty == Decimal("250")
    assert approval.client_order_id == order.client_order_id
    assert approval.risk_pct == Decimal("0.25")


def test_take_profit_propagates_to_bracket() -> None:
    signal = make_signal(take_profit_price=Decimal("103"))
    order = _engine().evaluate(signal, make_state()).order_request
    assert order is not None
    assert order.take_profit_limit_price == Decimal("103")


def test_limit_entry_carries_limit_price() -> None:
    signal = make_signal(order_type=OrderType.limit)
    order = _engine().evaluate(signal, make_state()).order_request
    assert order is not None
    assert order.order_type is OrderType.limit
    assert order.limit_price == Decimal("100")


def test_blocked_account_is_rejected_with_no_approval() -> None:
    decision = _engine().evaluate(make_signal(), make_state(trading_halted=True))
    assert decision.approved is False
    assert decision.approval is None
    assert decision.order_request is None
    assert decision.reason is not None
    assert "account_tradeable" in decision.reason


def test_closed_market_is_rejected() -> None:
    decision = _engine().evaluate(make_signal(), make_state(market_open=False))
    assert decision.approved is False
    assert "session_open" in (decision.reason or "")


def test_degenerate_stop_is_rejected_via_sizing() -> None:
    signal = make_signal(entry_price=Decimal("100"), stop_price=Decimal("100"))
    decision = _engine().evaluate(signal, make_state())
    assert decision.approved is False
    assert "sizing" in (decision.reason or "")


def test_unsupported_entry_order_type_is_rejected_not_dropped() -> None:
    # A stop-limit entry carries no trigger price, so it would fail OrderRequest
    # validation and vanish on the bus. The engine rejects it explicitly instead.
    decision = _engine().evaluate(make_signal(order_type=OrderType.stop_limit), make_state())
    assert decision.approved is False
    assert decision.order_request is None
    assert "entry_order_type" in (decision.reason or "")


def test_all_checks_recorded_even_when_rejected() -> None:
    # Two independent gates trip; the audit still records every control's verdict.
    state = make_state(trading_halted=True, market_open=False)
    decision = _engine().evaluate(make_signal(), state)
    assert decision.approved is False
    assert len(decision.checks) == _EXPECTED_CHECK_COUNT
    assert decision.reason is not None
    assert "account_tradeable" in decision.reason
    assert "session_open" in decision.reason


def test_near_close_entry_blocked_by_flatten_buffer() -> None:
    state = make_state(now=DEFAULT_NOW, next_close=DEFAULT_NOW + timedelta(minutes=2))
    decision = _engine().evaluate(make_signal(), state)
    assert decision.approved is False
    assert "session_open" in (decision.reason or "")


def test_approval_audit_payload_is_serializable() -> None:
    decision = _engine().evaluate(make_signal(), make_state())
    assert decision.approval is not None
    payload = decision.approval.audit_payload()
    assert payload["client_order_id"] == decision.approval.client_order_id
    assert payload["qty"] == "250"
    assert len(payload["checks"]) == _EXPECTED_CHECK_COUNT
    assert all(check["passed"] for check in payload["checks"])


def test_risk_approval_cannot_be_minted_outside_the_engine() -> None:
    # The capability key gates construction — no forging an approval (iron law #1).
    with pytest.raises(RuntimeError, match="minted by the Risk Engine"):
        RiskApproval(
            approval_id=uuid.uuid4(),
            signal_id=uuid.uuid4(),
            portfolio_id=uuid.uuid4(),
            strategy_id=uuid.uuid4(),
            symbol="SPY",
            side=OrderSide.buy,
            qty=Decimal("1"),
            entry_price=Decimal("100"),
            stop_price=Decimal("99"),
            take_profit_price=None,
            risk_pct=Decimal("0.25"),
            risk_amount=Decimal("250"),
            equity=Decimal("100000"),
            client_order_id="forged",
            approved_at=datetime.now(UTC),
            checks=(),
            _mint=object(),
        )
