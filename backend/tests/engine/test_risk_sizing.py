"""Fixed-fractional position sizing and the risk-% clamps."""

from __future__ import annotations

from decimal import Decimal

from app.engine.risk.controls import (
    drawdown_size_factor,
    effective_risk_pct,
    size_position,
)
from tests.engine.builders import make_limits, make_signal, make_state


def test_unproven_strategy_sizes_at_quarter_percent() -> None:
    # equity 100k × 0.25% = $250 risk; entry 100 / stop 99 = $1/sh → 250 shares.
    sizing = size_position(make_signal(), make_state(strategy_proven=False), make_limits())
    assert sizing.risk_pct == Decimal("0.25")
    assert sizing.risk_amount == Decimal("250.0000")
    assert sizing.qty == Decimal("250")
    assert sizing.check.passed is True


def test_proven_strategy_sizes_at_default_percent() -> None:
    # 0.75% × 100k = $750 → 750 shares.
    sizing = size_position(make_signal(), make_state(strategy_proven=True), make_limits())
    assert sizing.risk_pct == Decimal("0.75")
    assert sizing.qty == Decimal("750")


def test_strategy_override_respected_when_proven() -> None:
    state = make_state(strategy_proven=True, strategy_risk_pct=Decimal("1.0"))
    sizing = size_position(make_signal(), state, make_limits())
    assert sizing.risk_pct == Decimal("1.0")
    assert sizing.qty == Decimal("1000")


def test_override_clamped_to_unproven_ceiling() -> None:
    # An unproven strategy asking for 1% is held to the 0.25% unproven cap.
    state = make_state(strategy_proven=False, strategy_risk_pct=Decimal("1.0"))
    assert effective_risk_pct(
        make_limits(), strategy_risk_pct=Decimal("1.0"), strategy_proven=False
    ) == Decimal("0.25")
    sizing = size_position(make_signal(), state, make_limits())
    assert sizing.qty == Decimal("250")


def test_override_clamped_to_hard_ceiling() -> None:
    # 5% requested, proven, but the 2% hard ceiling binds → $2000 → 2000 shares.
    state = make_state(strategy_proven=True, strategy_risk_pct=Decimal("5.0"))
    sizing = size_position(make_signal(), state, make_limits())
    assert sizing.risk_pct == Decimal("2.0")
    assert sizing.qty == Decimal("2000")


def test_quantity_floored_to_whole_shares() -> None:
    # $250 risk over $3/sh = 83.33 → 83 whole shares (brackets cannot be fractional).
    signal = make_signal(entry_price=Decimal("100"), stop_price=Decimal("97"))
    sizing = size_position(signal, make_state(), make_limits())
    assert sizing.qty == Decimal("83")


def test_entry_equal_to_stop_is_rejected() -> None:
    signal = make_signal(entry_price=Decimal("100"), stop_price=Decimal("100"))
    sizing = size_position(signal, make_state(), make_limits())
    assert sizing.qty == Decimal("0")
    assert sizing.check.passed is False
    assert "no risk distance" in sizing.check.detail


def test_sub_one_share_is_rejected() -> None:
    # Tiny equity: $100 × 0.25% = $0.25 over $1/sh → 0.25 → floors below one share.
    sizing = size_position(make_signal(), make_state(equity=Decimal("100")), make_limits())
    assert sizing.qty == Decimal("0")
    assert sizing.check.passed is False
    assert "below one whole share" in sizing.check.detail


def test_drawdown_halve_rung_halves_size() -> None:
    # 10% peak-to-trough drawdown halves position size.
    state = make_state(strategy_proven=True, peak_equity=Decimal("100000"), equity=Decimal("90000"))
    assert drawdown_size_factor(make_limits(), state) == Decimal("0.5")
    sizing = size_position(make_signal(), state, make_limits())
    # 0.75% × 90k = $675, halved = $337.5 over $1/sh → 337 shares.
    assert sizing.drawdown_factor == Decimal("0.5")
    assert sizing.qty == Decimal("337")


def test_no_drawdown_keeps_full_size() -> None:
    state = make_state(peak_equity=Decimal("100000"), equity=Decimal("100000"))
    assert drawdown_size_factor(make_limits(), state) == Decimal("1")
