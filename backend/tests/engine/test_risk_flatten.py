"""Flatten through the risk choke point: authorize, mint, audit, publish.

``authorize_flatten`` is near-unconditional BY DESIGN — the choke point exists
for the audit row and the mint-guarded capability type, not for market gating.
The one property that must never regress: the flatten path authorizes while
the engine is HALTED, because flatten is the mechanism the halt relies on.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.engine.bus import EventBus
from app.engine.events import FlattenEvent, FlattenOrderEvent
from app.engine.risk.approval import FlattenApproval
from app.engine.risk.engine import RiskEngine
from app.engine.risk.state import RiskStateProvider
from app.engine.stage import RiskStage
from tests.engine.builders import FakeEngineAdapter, make_limits
from tests.engine.flatten_helpers import get_events

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession


def _flatten_event(**over: Any) -> FlattenEvent:
    fields: dict[str, Any] = {
        "portfolio_id": uuid.uuid4(),
        "reason": "operator flatten",
        "source": "kill_switch",
        "command_seq": None,
    }
    fields.update(over)
    return FlattenEvent(**fields)


# ── authorize_flatten (pure engine) ──────────────────────────────────


def test_authorize_flatten_mints_a_pair_bound_approval() -> None:
    event = _flatten_event(command_seq=7)
    decision = RiskEngine(make_limits()).authorize_flatten(event)

    assert decision.approved is True
    assert decision.approval is not None
    approval = decision.approval
    assert approval.flatten_id == event.flatten_id  # pair-bound to THIS intent
    assert approval.portfolio_id == event.portfolio_id
    assert approval.source == "kill_switch"
    assert approval.reason == "operator flatten"
    assert approval.command_seq == 7
    assert all(c.passed for c in approval.checks)
    assert approval.audit_payload()["flatten_id"] == str(event.flatten_id)


def test_every_known_source_authorizes() -> None:
    engine = RiskEngine(make_limits())
    for source in ("kill_switch", "scheduled_close", "next_open"):
        decision = engine.authorize_flatten(_flatten_event(source=source))
        assert decision.approved is True, source


def test_unknown_source_is_rejected_without_an_approval() -> None:
    decision = RiskEngine(make_limits()).authorize_flatten(_flatten_event(source="manual_button"))

    assert decision.approved is False
    assert decision.approval is None
    assert "flatten_provenance" in (decision.reason or "")


def test_blank_reason_is_rejected() -> None:
    decision = RiskEngine(make_limits()).authorize_flatten(_flatten_event(reason=""))

    assert decision.approved is False
    assert decision.approval is None


def test_flatten_approval_cannot_be_constructed_without_the_mint_key() -> None:
    with pytest.raises(RuntimeError, match="minted by the Risk Engine"):
        FlattenApproval(
            approval_id=uuid.uuid4(),
            flatten_id=uuid.uuid4(),
            portfolio_id=uuid.uuid4(),
            reason="forged",
            source="kill_switch",
            command_seq=None,
            approved_at=datetime.now(UTC),
            checks=(),
            _mint=object(),
        )


# ── RiskStage._on_flatten (bus + DB integration) ─────────────────────


def _flatten_stage(
    db_engine: AsyncEngine,
    captured: list[FlattenOrderEvent],
    *,
    halted: bool = False,
    session_factory: async_sessionmaker[AsyncSession] | None = None,
) -> EventBus:
    factory = (
        session_factory
        if session_factory is not None
        else async_sessionmaker(db_engine, expire_on_commit=False)
    )
    bus = EventBus()

    async def capture(event: FlattenOrderEvent) -> None:
        captured.append(event)

    bus.subscribe(FlattenOrderEvent, capture)
    stage = RiskStage(
        bus=bus,
        engine=RiskEngine(make_limits()),
        provider=RiskStateProvider(),
        session_factory=factory,
        adapter=FakeEngineAdapter(),
        halted=(lambda: True) if halted else None,
    )
    stage.register_handlers()
    return bus


async def test_approved_flatten_writes_audit_and_publishes_order(db_engine: AsyncEngine) -> None:
    captured: list[FlattenOrderEvent] = []
    bus = _flatten_stage(db_engine, captured)
    event = _flatten_event(command_seq=3)

    await bus.publish(event)
    await bus.drain()

    assert len(captured) == 1
    assert captured[0].approval.flatten_id == event.flatten_id
    assert captured[0].approval.command_seq == 3

    rows = await get_events(db_engine, "flatten.approved")
    assert len(rows) == 1
    assert rows[0].payload["flatten_id"] == str(event.flatten_id)
    assert rows[0].payload["source"] == "kill_switch"
    assert await get_events(db_engine, "flatten.rejected") == []


async def test_flatten_authorizes_while_halted(db_engine: AsyncEngine) -> None:
    # THE critical property: the kill switch freezes entries, and flatten is
    # the very mechanism that makes the frozen state safe — it must run.
    captured: list[FlattenOrderEvent] = []
    bus = _flatten_stage(db_engine, captured, halted=True)
    event = _flatten_event()

    await bus.publish(event)
    await bus.drain()

    assert len(captured) == 1
    assert captured[0].approval.flatten_id == event.flatten_id
    assert len(await get_events(db_engine, "flatten.approved")) == 1


async def test_rejected_flatten_audits_and_publishes_nothing(db_engine: AsyncEngine) -> None:
    captured: list[FlattenOrderEvent] = []
    bus = _flatten_stage(db_engine, captured)
    event = _flatten_event(source="rogue_component")

    await bus.publish(event)
    await bus.drain()

    assert captured == []
    rows = await get_events(db_engine, "flatten.rejected")
    assert len(rows) == 1
    assert rows[0].payload["flatten_id"] == str(event.flatten_id)
    assert "flatten_provenance" in rows[0].payload["reason"]
    assert await get_events(db_engine, "flatten.approved") == []


class _PoisonOnceFactory:
    """Fails the first session open (mid-flatten DB outage), then recovers —
    so the error-path audit write, which uses a fresh session, can land."""

    def __init__(self, real: async_sessionmaker[AsyncSession]) -> None:
        self._real = real
        self._failed = False

    def __call__(self) -> AsyncSession:
        if not self._failed:
            self._failed = True
            msg = "db connection pool down"
            raise RuntimeError(msg)
        return self._real()


async def test_db_failure_during_flatten_is_audited_as_flatten_error(
    db_engine: AsyncEngine,
) -> None:
    real = async_sessionmaker(db_engine, expire_on_commit=False)
    poisoned = cast("async_sessionmaker[AsyncSession]", _PoisonOnceFactory(real))
    captured: list[FlattenOrderEvent] = []
    bus = _flatten_stage(db_engine, captured, session_factory=poisoned)
    event = _flatten_event()

    await bus.publish(event)
    await bus.drain()  # the failure must not escape the bus

    assert captured == []
    rows = await get_events(db_engine, "flatten.error")
    assert len(rows) == 1
    assert rows[0].payload["flatten_id"] == str(event.flatten_id)
    assert "db connection pool down" in rows[0].payload["error"]
