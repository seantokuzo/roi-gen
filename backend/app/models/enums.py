"""Domain enums.

Stored as ``String(32)`` columns with :class:`enum.StrEnum` values in code —
intentionally NOT native Postgres enums (adding a member to a pg enum is a
migration tax we refuse to pay).
"""

from enum import StrEnum


class PortfolioMode(StrEnum):
    """Whether a portfolio trades against the paper or live Alpaca API."""

    paper = "paper"
    live = "live"


class OrderSide(StrEnum):
    """Buy or sell."""

    buy = "buy"
    sell = "sell"


class OrderType(StrEnum):
    """Alpaca order types."""

    market = "market"
    limit = "limit"
    stop = "stop"
    stop_limit = "stop_limit"
    trailing_stop = "trailing_stop"


class OrderClass(StrEnum):
    """Alpaca order classes (bracket legs reference parent_order_id)."""

    simple = "simple"
    bracket = "bracket"
    oco = "oco"
    oto = "oto"


class TimeInForce(StrEnum):
    """Alpaca time-in-force values."""

    day = "day"
    gtc = "gtc"
    opg = "opg"
    cls = "cls"
    ioc = "ioc"
    fok = "fok"


class OrderStatus(StrEnum):
    """Order lifecycle states (superset of Alpaca's, plus pending_submit)."""

    pending_submit = "pending_submit"
    submitted = "submitted"
    accepted = "accepted"
    partially_filled = "partially_filled"
    filled = "filled"
    canceled = "canceled"
    expired = "expired"
    rejected = "rejected"
    replaced = "replaced"
    pending_cancel = "pending_cancel"
    pending_replace = "pending_replace"
    stopped = "stopped"
    suspended = "suspended"
    calculated = "calculated"
    done_for_day = "done_for_day"
    held = "held"


class StrategyStatus(StrEnum):
    """Strategy lifecycle: draft → backtesting → paper → live (iron law #8)."""

    draft = "draft"
    backtesting = "backtesting"
    paper = "paper"
    live = "live"
    paused = "paused"
    stopped = "stopped"


class EventSource(StrEnum):
    """Origin of an event_log row."""

    api = "api"
    engine = "engine"
    broker = "broker"
    system = "system"
