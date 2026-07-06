"""Order state machine: terminal absorb, stale-snapshot guard, broker adoption."""

from __future__ import annotations

from decimal import Decimal

import pytest

from app.engine.execution.state import (
    ABSORBING_STATUSES,
    TransitionPlan,
    is_absorbing,
    plan_transition,
)
from app.models.enums import OrderStatus

_ZERO = Decimal("0")


@pytest.mark.parametrize("terminal", sorted(ABSORBING_STATUSES))
def test_absorbing_states_reject_every_transition(terminal: OrderStatus) -> None:
    for incoming in OrderStatus:
        plan = plan_transition(terminal.value, Decimal("10"), incoming, Decimal("10"))
        assert plan is TransitionPlan.terminal_conflict


def test_done_for_day_and_stopped_are_dormant_not_terminal() -> None:
    # A GTC order marked done_for_day fills next session; stopped guarantees a
    # coming fill. Absorbing either would drop those fills (design review C13).
    assert not is_absorbing(OrderStatus.done_for_day.value)
    assert not is_absorbing(OrderStatus.stopped.value)
    plan = plan_transition(OrderStatus.done_for_day.value, _ZERO, OrderStatus.filled, Decimal("10"))
    assert plan is TransitionPlan.apply


def test_stale_cumulative_fill_skips_the_snapshot() -> None:
    plan = plan_transition(
        OrderStatus.partially_filled.value, Decimal("100"), OrderStatus.accepted, Decimal("40")
    )
    assert plan is TransitionPlan.stale


def test_equal_cumulative_is_not_stale() -> None:
    plan = plan_transition(
        OrderStatus.partially_filled.value,
        Decimal("100"),
        OrderStatus.pending_cancel,
        Decimal("100"),
    )
    assert plan is TransitionPlan.apply


def test_nonterminal_statuses_adopt_broker_truth_in_any_order() -> None:
    # Alpaca's accepted/new ordering is not deterministic — no rank between
    # non-terminals; the broker is the source of truth.
    assert (
        plan_transition(OrderStatus.accepted.value, _ZERO, OrderStatus.submitted, _ZERO)
        is TransitionPlan.apply
    )
    assert (
        plan_transition(OrderStatus.submitted.value, _ZERO, OrderStatus.accepted, _ZERO)
        is TransitionPlan.apply
    )


def test_pending_submit_adopts_first_broker_state() -> None:
    plan = plan_transition(
        OrderStatus.pending_submit.value, _ZERO, OrderStatus.filled, Decimal("5")
    )
    assert plan is TransitionPlan.apply


def test_unknown_local_status_is_overwritable() -> None:
    # Garbage in the DB must not wedge the row — broker truth wins.
    assert not is_absorbing("garbage-status")
    plan = plan_transition("garbage-status", _ZERO, OrderStatus.canceled, _ZERO)
    assert plan is TransitionPlan.apply


def test_failed_is_absorbing_for_snapshots() -> None:
    # Only the writer's explicit resurrection path may revive a failed order.
    plan = plan_transition(OrderStatus.failed.value, _ZERO, OrderStatus.accepted, _ZERO)
    assert plan is TransitionPlan.terminal_conflict
