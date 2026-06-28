"""Each risk gate, fired and cleared in isolation."""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

from app.engine.risk.controls import (
    check_account_tradeable,
    check_consecutive_losses,
    check_cooldown,
    check_daily_loss,
    check_drawdown_halt,
    check_extended_hours,
    check_margin_headroom,
    check_max_positions,
    check_no_pyramiding,
    check_session_open,
    check_volatility_halt,
)
from tests.engine.builders import DEFAULT_NOW, make_limits, make_signal, make_state


def test_account_tradeable_blocks() -> None:
    assert check_account_tradeable(make_state()).passed is True
    assert check_account_tradeable(make_state(trading_blocked=True)).passed is False
    assert check_account_tradeable(make_state(account_blocked=True)).passed is False
    halted = check_account_tradeable(make_state(trading_halted=True))
    assert halted.passed is False
    assert "kill_switch" in halted.detail


def test_extended_hours_blocked() -> None:
    assert check_extended_hours(make_signal()).passed is True
    assert check_extended_hours(make_signal(extended_hours=True)).passed is False


def test_session_open_requires_rth_outside_buffer() -> None:
    limits = make_limits()
    assert check_session_open(make_state(), limits).passed is True
    assert check_session_open(make_state(market_open=False), limits).passed is False
    # 3 minutes from close is inside the 5-minute flatten buffer.
    near_close = make_state(now=DEFAULT_NOW, next_close=DEFAULT_NOW + timedelta(minutes=3))
    assert check_session_open(near_close, limits).passed is False


def test_daily_loss_strategy_scope() -> None:
    limits = make_limits()
    risk_amount = Decimal("100")  # strategy limit = 2.5 × 100 = 250
    assert check_daily_loss(make_state(), limits, risk_amount).passed is True
    breached = make_state(day_realized_pnl_strategy=Decimal("-300"))
    assert check_daily_loss(breached, limits, risk_amount).passed is False


def test_daily_loss_portfolio_scope() -> None:
    limits = make_limits()  # 2% of 100k = $2000 portfolio limit
    # equity below prior close by $3000 → portfolio day P&L −3000 ≤ −2000.
    breached = make_state(equity=Decimal("100000"), last_equity=Decimal("103000"))
    result = check_daily_loss(breached, limits, Decimal("250"))
    assert result.passed is False
    assert "portfolio" in result.detail


def test_consecutive_losses_limit() -> None:
    limits = make_limits()  # max 4
    assert check_consecutive_losses(make_state(consecutive_losses=3), limits).passed is True
    assert check_consecutive_losses(make_state(consecutive_losses=4), limits).passed is False


def test_drawdown_halt() -> None:
    limits = make_limits()  # halt at 15%
    ok = make_state(peak_equity=Decimal("100000"), equity=Decimal("90000"))  # 10%
    assert check_drawdown_halt(ok, limits).passed is True
    halt = make_state(peak_equity=Decimal("100000"), equity=Decimal("84000"))  # 16%
    assert check_drawdown_halt(halt, limits).passed is False


def test_margin_headroom() -> None:
    limits = make_limits()  # cap = buying_power × 0.85 = 340k
    ok = check_margin_headroom(make_state(), limits, qty=Decimal("250"), entry=Decimal("100"))
    assert ok.passed is True
    over = check_margin_headroom(make_state(), limits, qty=Decimal("5000"), entry=Decimal("100"))
    assert over.passed is False


def test_margin_headroom_counts_existing_exposure() -> None:
    limits = make_limits()
    state = make_state(position_market_value=Decimal("330000"))
    # 330k held + 25k new = 355k > 340k cap.
    result = check_margin_headroom(state, limits, qty=Decimal("250"), entry=Decimal("100"))
    assert result.passed is False


def test_max_positions() -> None:
    assert check_max_positions(make_state(strategy_max_positions=None)).passed is True
    at_cap = make_state(strategy_max_positions=2, open_positions_count=2)
    assert check_max_positions(at_cap).passed is False
    below = make_state(strategy_max_positions=2, open_positions_count=1)
    assert check_max_positions(below).passed is True


def test_no_pyramiding() -> None:
    assert check_no_pyramiding(make_state(strategy_open_qty=Decimal("0"))).passed is True
    assert check_no_pyramiding(make_state(strategy_open_qty=Decimal("5"))).passed is False


def test_cooldown() -> None:
    limits = make_limits()  # 60s cooldown
    assert check_cooldown(make_state(last_entry_at=None), limits).passed is True
    recent = make_state(now=DEFAULT_NOW, last_entry_at=DEFAULT_NOW - timedelta(seconds=30))
    assert check_cooldown(recent, limits).passed is False
    elapsed = make_state(now=DEFAULT_NOW, last_entry_at=DEFAULT_NOW - timedelta(seconds=120))
    assert check_cooldown(elapsed, limits).passed is True


def test_volatility_halt_is_a_passing_hook() -> None:
    result = check_volatility_halt()
    assert result.passed is True
    assert "Phase 4" in result.detail
