"""The order state machine — pure transition rules for every order-state writer.

Three writers mutate persisted order state (the ExecutionStage's post-submit
apply, the trade-updates writer, and reconciliation), and all three run as
separate tasks/processes against the same rows. The broker is the source of
truth for order state, so this machine does not police Alpaca's (messy,
unordered) non-terminal vocabulary — it exists to stop the two failure modes
that corrupt money state:

- **Regression from terminal**: a stale snapshot or late stream event must not
  resurrect a ``filled`` order back to ``accepted`` (that would re-open the
  missed-fill synthesis window and double-count lots).
- **Stale snapshots**: cumulative ``filled_qty`` only ever increases; an event
  carrying a lower cumulative than what we've recorded is out-of-date and its
  whole order snapshot is untrustworthy.

``done_for_day`` and ``stopped`` are deliberately NOT absorbing: Alpaca's
``done_for_day`` is dormancy (a GTC order can fill next session) and
``stopped`` means a fill is guaranteed but has not yet printed. Treating either
as terminal would silently drop the fills that follow. ``failed`` (local,
provably-not-placed) is absorbing here; the trade-updates writer alone may
override it via its explicit resurrection path when broker evidence arrives —
broker reality trumps a local guess.
"""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

from app.models.enums import OrderStatus

if TYPE_CHECKING:
    from decimal import Decimal

# Absorbing states: no transition out (except the writer's explicit
# failed-resurrection). Deliberately excludes done_for_day and stopped.
ABSORBING_STATUSES: frozenset[OrderStatus] = frozenset(
    {
        OrderStatus.filled,
        OrderStatus.canceled,
        OrderStatus.expired,
        OrderStatus.rejected,
        OrderStatus.replaced,
        OrderStatus.failed,
    }
)


class TransitionPlan(Enum):
    """Verdict on applying an incoming order snapshot over the local row."""

    apply = "apply"
    stale = "stale"
    terminal_conflict = "terminal_conflict"


def is_absorbing(status: str) -> bool:
    """Whether a stored status string is absorbing (no transitions out)."""
    try:
        return OrderStatus(status) in ABSORBING_STATUSES
    except ValueError:
        # Unknown/garbage status: treat as non-absorbing so broker truth can
        # overwrite it rather than wedging the row.
        return False


def plan_transition(
    local_status: str,
    local_filled_qty: Decimal,
    incoming_status: OrderStatus,
    incoming_filled_qty: Decimal,
) -> TransitionPlan:
    """Decide whether an incoming broker order snapshot may be applied.

    Rules, in order:
    1. Absorbing local state → ``terminal_conflict`` (even for an identical
       incoming status: there is nothing left to apply, and late snapshots may
       carry regressed ``filled_qty``).
    2. Incoming cumulative ``filled_qty`` below local → ``stale``: the snapshot
       predates state we already recorded; nothing in it can be trusted.
    3. Otherwise ``apply`` — adopt the broker-reported status verbatim. No
       ordering is enforced between non-terminal statuses (Alpaca's ``accepted``
       and ``new`` can arrive in either order).

    Fills are NOT governed by this: fill *executions* are recorded and applied
    to lots whenever their ``execution_id`` is unseen, even against an absorbing
    row (money truth beats status bookkeeping) — see the trade-updates writer.
    """
    if is_absorbing(local_status):
        return TransitionPlan.terminal_conflict
    if incoming_filled_qty < local_filled_qty:
        return TransitionPlan.stale
    return TransitionPlan.apply
