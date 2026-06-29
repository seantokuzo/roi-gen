"""RiskStage: where a signal becomes an order — or an audit-logged rejection.

Every order path converges on this handler, and none can produce an OrderEvent
without a RiskApproval. These tests exercise the approve path (audit row +
emitted order), the reject path (audit row, nothing emitted), and the
kill-switch halt hook.
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.engine.bus import EventBus
from app.engine.events import OrderEvent
from app.engine.risk.engine import RiskEngine
from app.engine.risk.state import RiskState, RiskStateProvider
from app.engine.stage import RiskStage
from app.models.telemetry import EventLog
from tests.engine.builders import FakeEngineAdapter, make_limits, make_signal

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine


def _build_stage(
    db_engine: AsyncEngine, captured: list[OrderEvent], *, halted: bool = False
) -> tuple[EventBus, RiskStage]:
    factory = async_sessionmaker(db_engine, expire_on_commit=False)
    bus = EventBus()

    async def capture(event: OrderEvent) -> None:
        captured.append(event)

    bus.subscribe(OrderEvent, capture)
    stage = RiskStage(
        bus=bus,
        engine=RiskEngine(make_limits()),
        provider=RiskStateProvider(),
        session_factory=factory,
        adapter=FakeEngineAdapter(),
        halted=(lambda: True) if halted else None,
    )
    stage.register_handlers()
    return bus, stage


async def _event_log(db_engine: AsyncEngine, event_type: str) -> list[EventLog]:
    factory = async_sessionmaker(db_engine, expire_on_commit=False)
    async with factory() as session:
        result = await session.execute(select(EventLog).where(EventLog.event_type == event_type))
        return list(result.scalars().all())


async def test_approved_signal_writes_audit_and_emits_order(db_engine: AsyncEngine) -> None:
    captured: list[OrderEvent] = []
    bus, _stage = _build_stage(db_engine, captured)

    await bus.publish(make_signal())
    await bus.drain()

    assert len(captured) == 1
    order_event = captured[0]
    assert order_event.order_request.qty == Decimal("250")
    assert order_event.approval.client_order_id == order_event.order_request.client_order_id

    rows = await _event_log(db_engine, "order.approved")
    assert len(rows) == 1
    assert rows[0].payload is not None
    assert rows[0].payload["qty"] == "250"
    # No rejection was logged.
    assert await _event_log(db_engine, "order.rejected") == []


async def test_rejected_signal_writes_audit_and_emits_nothing(db_engine: AsyncEngine) -> None:
    captured: list[OrderEvent] = []
    bus, _stage = _build_stage(db_engine, captured)

    # entry == stop → the sizing control rejects before any order is built.
    await bus.publish(make_signal(entry_price=Decimal("100"), stop_price=Decimal("100")))
    await bus.drain()

    assert captured == []
    rows = await _event_log(db_engine, "order.rejected")
    assert len(rows) == 1
    assert rows[0].payload is not None
    assert "sizing" in rows[0].payload["reason"]


async def test_halt_flag_blocks_approval(db_engine: AsyncEngine) -> None:
    captured: list[OrderEvent] = []
    bus, _stage = _build_stage(db_engine, captured, halted=True)

    await bus.publish(make_signal())
    await bus.drain()

    assert captured == []
    rows = await _event_log(db_engine, "order.rejected")
    assert len(rows) == 1
    assert rows[0].payload is not None
    assert "account_tradeable" in rows[0].payload["reason"]


class _BoomProvider(RiskStateProvider):
    """Simulates a broker/DB failure during state load (e.g. an Alpaca timeout)."""

    async def load(self, *args: object, **kwargs: object) -> RiskState:  # type: ignore[override]
        raise RuntimeError("alpaca timeout")


async def test_signal_error_is_audited_not_silent(db_engine: AsyncEngine) -> None:
    factory = async_sessionmaker(db_engine, expire_on_commit=False)
    bus = EventBus()
    captured: list[OrderEvent] = []

    async def capture(event: OrderEvent) -> None:
        captured.append(event)

    bus.subscribe(OrderEvent, capture)
    stage = RiskStage(
        bus=bus,
        engine=RiskEngine(make_limits()),
        provider=_BoomProvider(),
        session_factory=factory,
        adapter=FakeEngineAdapter(),
    )
    stage.register_handlers()

    await bus.publish(make_signal())
    await bus.drain()  # the failure must not escape the bus

    assert captured == []
    rows = await _event_log(db_engine, "order.error")
    assert len(rows) == 1
    assert rows[0].payload is not None
    assert "alpaca timeout" in rows[0].payload["error"]
