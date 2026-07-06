"""Sub-penny price quantization (SEC Rule 612 — Alpaca rejects violations)."""

from decimal import Decimal

from app.brokers.pricing import quantize_price


def test_at_or_above_a_dollar_rounds_to_pennies() -> None:
    assert quantize_price(Decimal("123.456")) == Decimal("123.46")
    assert quantize_price(Decimal("123.454")) == Decimal("123.45")
    assert quantize_price(Decimal("1.005")) == Decimal("1.01")  # half-up
    assert quantize_price(Decimal("100")) == Decimal("100.00")


def test_below_a_dollar_rounds_to_hundredths_of_a_cent() -> None:
    assert quantize_price(Decimal("0.123456")) == Decimal("0.1235")
    assert quantize_price(Decimal("0.99995")) == Decimal("1.0000")  # half-up across regime
    assert quantize_price(Decimal("0.0001")) == Decimal("0.0001")


def test_boundary_exactly_one_dollar_uses_penny_grid() -> None:
    assert quantize_price(Decimal("1.0000")) == Decimal("1.00")
    assert quantize_price(Decimal("0.9999")) == Decimal("0.9999")


def test_already_quantized_prices_are_unchanged() -> None:
    assert quantize_price(Decimal("99.00")) == Decimal("99.00")
    assert quantize_price(Decimal("0.5000")) == Decimal("0.5000")
