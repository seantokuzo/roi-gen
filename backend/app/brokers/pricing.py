"""Price quantization for broker submission (US equity sub-penny rules).

SEC Rule 612 (and Alpaca's enforcement of it): limit/stop prices at or above
$1.00 may carry at most 2 decimal places; below $1.00 at most 4. Alpaca rejects
orders that violate this, so every price the risk engine puts on an
:class:`~app.brokers.dto.OrderRequest` is quantized here first.

ROUND_HALF_UP keeps the rounding rule direction-agnostic and predictable; the
sub-penny delta is noise relative to spread/slippage, and the approval audit
records the quantized prices actually sent. Derived analytic prices (e.g.
reconciliation's back-computed fill-span price) must NOT be quantized with this
— broker-reported fill prices are legitimately sub-penny.
"""

from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal

_PENNY = Decimal("0.01")
_SUB_PENNY = Decimal("0.0001")
_ONE_DOLLAR = Decimal("1")


def quantize_price(price: Decimal) -> Decimal:
    """Quantize ``price`` to the max precision Alpaca accepts for orders."""
    tick = _PENNY if price >= _ONE_DOLLAR else _SUB_PENNY
    return price.quantize(tick, rounding=ROUND_HALF_UP)
