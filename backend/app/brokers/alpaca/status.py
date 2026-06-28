"""Alpaca order-status → :class:`~app.models.enums.OrderStatus` mapping.

Alpaca's order lifecycle uses a handful of status names that don't all line up
1:1 with our domain enum (e.g. Alpaca ``new`` is our ``submitted``, and both
``accepted`` *and* ``accepted_for_bidding`` collapse to ``accepted``). The map
is the single source of truth for that translation, and :func:`map_status`
fails *soft* on an unrecognized value: a broker can add a status faster than we
can ship a release, and a single unknown status must never crash the adapter
mid-session — it degrades to a safe, conservative default and logs a warning so
we notice and add the mapping.
"""

from __future__ import annotations

from app.core.logging import get_logger
from app.models.enums import OrderStatus

logger = get_logger(__name__)

# Every documented Alpaca order status → our normalized enum. Keep exhaustive;
# add a row (don't widen the fallback) when Alpaca introduces a new status.
ALPACA_STATUS_MAP: dict[str, OrderStatus] = {
    "new": OrderStatus.submitted,
    "accepted": OrderStatus.accepted,
    "pending_new": OrderStatus.submitted,
    "accepted_for_bidding": OrderStatus.accepted,
    "partially_filled": OrderStatus.partially_filled,
    "filled": OrderStatus.filled,
    "done_for_day": OrderStatus.done_for_day,
    "canceled": OrderStatus.canceled,
    "expired": OrderStatus.expired,
    "replaced": OrderStatus.replaced,
    "restated": OrderStatus.replaced,
    "pending_cancel": OrderStatus.pending_cancel,
    "pending_replace": OrderStatus.pending_replace,
    "pending_review": OrderStatus.held,
    "stopped": OrderStatus.stopped,
    "rejected": OrderStatus.rejected,
    "suspended": OrderStatus.suspended,
    "calculated": OrderStatus.calculated,
    "held": OrderStatus.held,
}

# Conservative landing spot for an unrecognized status: ``held`` parks the order
# in a non-terminal, non-tradeable state rather than implying it filled or died.
_UNKNOWN_STATUS_DEFAULT = OrderStatus.held


def map_status(raw: str) -> OrderStatus:
    """Translate an Alpaca status string to an :class:`OrderStatus`.

    Unknown values do not raise: they log a warning and fall back to
    :data:`_UNKNOWN_STATUS_DEFAULT` so a freshly-introduced broker status can't
    take the adapter down.
    """
    mapped = ALPACA_STATUS_MAP.get(raw)
    if mapped is None:
        logger.warning(
            "alpaca.unknown_order_status",
            raw_status=raw,
            fallback=_UNKNOWN_STATUS_DEFAULT.value,
        )
        return _UNKNOWN_STATUS_DEFAULT
    return mapped
