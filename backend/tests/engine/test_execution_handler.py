"""ExecutionStage: persist-before-submit, failure taxonomy, ambiguous resolution.

Approvals are NEVER forged here — every test runs a real
``RiskEngine.evaluate`` and uses the approval it mints (iron law #1 applies to
tests too). The adapter is the only fake.
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.brokers.errors import AmbiguousOrderState, BrokerRateLimited, OrderRejected
from app.engine.bus import EventBus
from app.engine.events import OrderEvent
from app.engine.execution.handler import ExecutionStage
from app.engine.risk.engine import RiskEngine
from app.models.enums import OrderSide, OrderStatus, OrderType, TimeInForce
from app.models.telemetry import EventLog
from app.models.trading import Order
from tests.engine.builders import (
    RecordingAdapter,
    make_broker_order,
    make_limits,
    make_signal,
    make_state,
    seed_portfolio,
    seed_strategy,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

    from app.brokers.dto import BrokerOrder, OrderRequest
    from app.engine.risk.approval import RiskApproval
    from app.models.portfolio import Portfolio
    from app.models.strategy import Strategy as StrategyModel
    from app.models.user import User


async def _seeded_scope(
    db_session: AsyncSession, seeded_user: User
) -> tuple[Portfolio, StrategyModel]:
    portfolio = await seed_portfolio(db_session, seeded_user.id)
    strategy = await seed_strategy(db_session, portfolio.id)
    await db_session.commit()  # the stage runs on its own sessions
    return portfolio, strategy


def _approved_event(portfolio: Portfolio, strategy: StrategyModel) -> OrderEvent:
    """Run the real risk engine; return the OrderEvent the stage would receive."""
    signal = make_signal(portfolio_id=portfolio.id, strategy_id=strategy.id)
    decision = RiskEngine(make_limits()).evaluate(signal, make_state())
    assert decision.approval is not None and decision.order_request is not None
    return OrderEvent(order_request=decision.order_request, approval=decision.approval)


def _stage(db_engine: AsyncEngine, adapter: RecordingAdapter) -> ExecutionStage:
    return ExecutionStage(
        bus=EventBus(),
        session_factory=async_sessionmaker(db_engine, expire_on_commit=False),
        adapter=adapter,
        resolve_delays=(0.0, 0.0),  # no real sleeps in tests
    )


async def _order_row(db_engine: AsyncEngine, client_order_id: str) -> Order:
    factory = async_sessionmaker(db_engine, expire_on_commit=False)
    async with factory() as session:
        return (
            await session.execute(select(Order).where(Order.client_order_id == client_order_id))
        ).scalar_one()


async def _events(db_engine: AsyncEngine, event_type: str) -> list[EventLog]:
    factory = async_sessionmaker(db_engine, expire_on_commit=False)
    async with factory() as session:
        return list(
            (await session.execute(select(EventLog).where(EventLog.event_type == event_type)))
            .scalars()
            .all()
        )


class _RowCheckingAdapter(RecordingAdapter):
    """Proves persist-before-submit: at submit time the row must already exist."""

    def __init__(self, db_engine: AsyncEngine, **kwargs: object) -> None:
        super().__init__(**kwargs)  # type: ignore[arg-type]
        self._db_engine = db_engine
        self.row_existed_at_submit: bool | None = None

    async def submit_order(self, req: OrderRequest) -> BrokerOrder:
        factory = async_sessionmaker(self._db_engine, expire_on_commit=False)
        async with factory() as session:
            row = (
                await session.execute(
                    select(Order).where(Order.client_order_id == req.client_order_id)
                )
            ).scalar_one_or_none()
        self.row_existed_at_submit = (
            row is not None and row.status == OrderStatus.pending_submit.value
        )
        return await super().submit_order(req)


async def test_happy_path_persists_before_submit_and_adopts_response(
    db_engine: AsyncEngine, db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio, strategy = await _seeded_scope(db_session, seeded_user)
    event = _approved_event(portfolio, strategy)
    adapter = _RowCheckingAdapter(db_engine)
    await _stage(db_engine, adapter)._on_order(event)

    assert adapter.row_existed_at_submit is True  # committed BEFORE the broker call
    assert len(adapter.submitted) == 1

    row = await _order_row(db_engine, event.approval.client_order_id)
    assert row.broker_order_id == "bo-1"
    assert row.status == OrderStatus.accepted.value
    assert row.risk_approval is not None
    assert row.risk_approval["client_order_id"] == event.approval.client_order_id
    assert len(await _events(db_engine, "order.submitted")) == 1


async def test_bracket_legs_from_submit_response_become_child_rows(
    db_engine: AsyncEngine, db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio, strategy = await _seeded_scope(db_session, seeded_user)
    event = _approved_event(portfolio, strategy)
    legs = [
        make_broker_order(
            broker_order_id="leg-tp",
            client_order_id="alpaca-leg-tp",
            side=OrderSide.sell,
            order_type=OrderType.limit,
            status=OrderStatus.held,
            qty=event.approval.qty,
        ),
        make_broker_order(
            broker_order_id="leg-sl",
            client_order_id="alpaca-leg-sl",
            side=OrderSide.sell,
            order_type=OrderType.stop,
            status=OrderStatus.held,
            qty=event.approval.qty,
        ),
    ]
    parent = make_broker_order(client_order_id=event.approval.client_order_id, legs=legs)
    adapter = RecordingAdapter(submit_result=parent)
    await _stage(db_engine, adapter)._on_order(event)

    row = await _order_row(db_engine, event.approval.client_order_id)
    factory = async_sessionmaker(db_engine, expire_on_commit=False)
    async with factory() as session:
        children = (
            (await session.execute(select(Order).where(Order.parent_order_id == row.id)))
            .scalars()
            .all()
        )
    assert {c.broker_order_id for c in children} == {"leg-tp", "leg-sl"}
    assert all(c.strategy_id == strategy.id for c in children)  # attribution inherited
    assert all(c.status == OrderStatus.held.value for c in children)


async def test_broker_rejection_is_terminal_rejected(
    db_engine: AsyncEngine, db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio, strategy = await _seeded_scope(db_session, seeded_user)
    event = _approved_event(portfolio, strategy)
    adapter = RecordingAdapter(submit_result=OrderRejected("insufficient buying power"))
    await _stage(db_engine, adapter)._on_order(event)

    row = await _order_row(db_engine, event.approval.client_order_id)
    assert row.status == OrderStatus.rejected.value
    events = await _events(db_engine, "order.rejected_by_broker")
    assert len(events) == 1
    assert "buying power" in events[0].payload["error"]


async def test_rate_limited_submit_is_failed_not_rejected(
    db_engine: AsyncEngine, db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio, strategy = await _seeded_scope(db_session, seeded_user)
    event = _approved_event(portfolio, strategy)
    adapter = RecordingAdapter(submit_result=BrokerRateLimited("slow down"))
    await _stage(db_engine, adapter)._on_order(event)

    row = await _order_row(db_engine, event.approval.client_order_id)
    assert row.status == OrderStatus.failed.value  # provably not placed
    assert len(await _events(db_engine, "order.submit_failed")) == 1


async def test_ambiguous_submit_resolved_by_client_id_lookup(
    db_engine: AsyncEngine, db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio, strategy = await _seeded_scope(db_session, seeded_user)
    event = _approved_event(portfolio, strategy)
    found = make_broker_order(
        broker_order_id="bo-found",
        client_order_id=event.approval.client_order_id,
        status=OrderStatus.accepted,
    )
    adapter = RecordingAdapter(
        submit_result=AmbiguousOrderState("timeout mid-submit"),
        lookup_results=[None, found],  # first probe misses, second finds it
    )
    await _stage(db_engine, adapter)._on_order(event)

    assert adapter.client_id_lookups == [event.approval.client_order_id] * 2
    row = await _order_row(db_engine, event.approval.client_order_id)
    assert row.broker_order_id == "bo-found"  # adopted, never resubmitted
    assert row.status == OrderStatus.accepted.value
    assert len(adapter.submitted) == 1  # THE critical assertion: exactly one submit
    assert len(await _events(db_engine, "order.submit_ambiguous_resolved")) == 1


async def test_ambiguous_unresolved_stays_pending_for_reconciliation(
    db_engine: AsyncEngine, db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio, strategy = await _seeded_scope(db_session, seeded_user)
    event = _approved_event(portfolio, strategy)
    adapter = RecordingAdapter(submit_result=AmbiguousOrderState("gateway 504"))
    await _stage(db_engine, adapter)._on_order(event)

    row = await _order_row(db_engine, event.approval.client_order_id)
    assert row.status == OrderStatus.pending_submit.value  # NOT failed — fate unknown
    assert len(adapter.submitted) == 1
    assert len(await _events(db_engine, "order.submit_ambiguous")) == 1


async def test_unexpected_submit_exception_takes_the_ambiguous_path(
    db_engine: AsyncEngine, db_session: AsyncSession, seeded_user: User
) -> None:
    # Unknown ≠ not-placed: guessing 'failed' about a live order orphans its
    # fills (design review C21). The stage must probe, not guess.
    portfolio, strategy = await _seeded_scope(db_session, seeded_user)
    event = _approved_event(portfolio, strategy)
    adapter = RecordingAdapter(submit_result=RuntimeError("connection pool exploded"))
    await _stage(db_engine, adapter)._on_order(event)

    assert adapter.client_id_lookups  # it probed
    row = await _order_row(db_engine, event.approval.client_order_id)
    assert row.status == OrderStatus.pending_submit.value


async def test_pairing_mismatch_never_reaches_the_broker(
    db_engine: AsyncEngine, db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio, strategy = await _seeded_scope(db_session, seeded_user)
    event = _approved_event(portfolio, strategy)
    tampered = event.order_request.model_copy(update={"qty": Decimal("999999")})
    adapter = RecordingAdapter()
    await _stage(db_engine, adapter)._on_order(
        OrderEvent(order_request=tampered, approval=event.approval)
    )

    assert adapter.submitted == []  # blocked before the broker
    events = await _events(db_engine, "order.error")
    assert len(events) == 1
    assert "pairing mismatch" in events[0].payload["error"]


class _StreamWinsAdapter(RecordingAdapter):
    """Simulates the ws fill beating the REST submit response: by the time the
    response is applied, the writer has already marked the row filled."""

    def __init__(self, db_engine: AsyncEngine, approval: RiskApproval) -> None:
        super().__init__()
        self._db_engine = db_engine
        self._approval = approval

    async def submit_order(self, req: OrderRequest) -> BrokerOrder:
        factory = async_sessionmaker(self._db_engine, expire_on_commit=False)
        async with factory() as session:
            row = (
                await session.execute(
                    select(Order).where(Order.client_order_id == req.client_order_id)
                )
            ).scalar_one()
            row.status = OrderStatus.filled.value
            row.filled_qty = self._approval.qty
            row.broker_order_id = "bo-1"
            await session.commit()
        return await super().submit_order(req)


async def test_submit_response_never_regresses_a_filled_row(
    db_engine: AsyncEngine, db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio, strategy = await _seeded_scope(db_session, seeded_user)
    event = _approved_event(portfolio, strategy)
    adapter = _StreamWinsAdapter(db_engine, event.approval)
    await _stage(db_engine, adapter)._on_order(event)

    row = await _order_row(db_engine, event.approval.client_order_id)
    assert row.status == OrderStatus.filled.value  # the stale 'accepted' lost
    assert row.filled_qty == event.approval.qty


async def test_time_in_force_control_rejects_gtc_signals(
    db_engine: AsyncEngine, db_session: AsyncSession, seeded_user: User
) -> None:
    # Belt-and-suspenders for the intraday assumption: GTC never reaches
    # execution because the risk engine refuses to approve it.
    portfolio, strategy = await _seeded_scope(db_session, seeded_user)
    signal = make_signal(
        portfolio_id=portfolio.id,
        strategy_id=strategy.id,
        time_in_force=TimeInForce.gtc,
    )
    decision = RiskEngine(make_limits()).evaluate(signal, make_state())
    assert decision.approved is False
    assert "time_in_force" in (decision.reason or "")
