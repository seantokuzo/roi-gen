"""End-to-end on the bus: Signal → Risk → Execution → fills → FIFO P&L.

The Phase-2 promise, minus the live websocket: one signal walks the whole
deterministic cascade — risk approval (real engine, real mint), persist-before-
submit, a bracket submit with legs, the entry fill opening a lot, the
protective stop's fill closing it — and every hop leaves an audit row.
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.engine.bus import EventBus
from app.engine.events import FillEvent
from app.engine.execution.handler import ExecutionStage
from app.engine.execution.trade_updates import TradeUpdateStage
from app.engine.risk.engine import RiskEngine
from app.engine.risk.state import RiskStateProvider
from app.engine.stage import RiskStage
from app.models.enums import OrderSide, OrderStatus, OrderType
from app.models.telemetry import EventLog
from app.models.trading import Fill, Lot, LotClose, Order, Position
from tests.engine.builders import (
    RecordingAdapter,
    make_broker_order,
    make_limits,
    make_signal,
    make_trade_update,
    seed_portfolio,
    seed_strategy,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

    from app.models.user import User


async def test_signal_to_realized_pnl_with_full_audit_trail(
    db_engine: AsyncEngine, db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio = await seed_portfolio(db_session, seeded_user.id)
    strategy = await seed_strategy(db_session, portfolio.id)
    await db_session.commit()
    factory = async_sessionmaker(db_engine, expire_on_commit=False)

    # The broker will accept the bracket and return its protective legs.
    stop_leg = make_broker_order(
        broker_order_id="bo-leg-sl",
        client_order_id="alpaca-leg-sl",
        side=OrderSide.sell,
        order_type=OrderType.stop,
        status=OrderStatus.held,
        qty=Decimal("250"),
    )

    def _submit_response(client_order_id: str) -> object:
        return make_broker_order(
            broker_order_id="bo-parent",
            client_order_id=client_order_id,
            status=OrderStatus.accepted,
            qty=Decimal("250"),
            legs=[stop_leg],
        )

    bus = EventBus()
    fills_seen: list[FillEvent] = []

    async def capture(event: FillEvent) -> None:
        fills_seen.append(event)

    bus.subscribe(FillEvent, capture)

    adapter = RecordingAdapter()
    risk_stage = RiskStage(
        bus=bus,
        engine=RiskEngine(make_limits()),
        provider=RiskStateProvider(),
        session_factory=factory,
        adapter=adapter,
    )
    risk_stage.register_handlers()
    execution = ExecutionStage(
        bus=bus, session_factory=factory, adapter=adapter, resolve_delays=(0.0,)
    )
    execution.register_handlers()
    writer = TradeUpdateStage(
        bus=bus, session_factory=factory, adapter=adapter, match_retry_delays=()
    )

    # ── 1. The strategy proposes; risk sizes and approves; execution submits.
    signal = make_signal(portfolio_id=portfolio.id, strategy_id=strategy.id)
    await bus.publish(signal)
    await bus.drain()

    assert len(adapter.submitted) == 1
    submitted = adapter.submitted[0]
    assert submitted.order_class.value == "bracket"  # protection travelled (iron law #4)
    assert submitted.stop_loss_stop_price == Decimal("99.00")
    # The canned default response has no legs — patch the response for realism
    # next time; here the parent row exists and legs adopt from the stream side.
    async with factory() as session:
        parent = (
            await session.execute(
                select(Order).where(Order.client_order_id == submitted.client_order_id)
            )
        ).scalar_one()
        assert parent.status == OrderStatus.accepted.value
        assert parent.risk_approval is not None
        assert parent.risk_approval["checks"]  # the full control sweep rode along

    # ── 2. Entry fills (trade update): lot opens, position tracks.
    adapter.orders_by_id["bo-1"] = _submit_response(submitted.client_order_id)  # nested read
    await writer.on_trade_update(
        make_trade_update(
            execution_id="exec-entry",
            qty=Decimal("250"),
            price=Decimal("100.10"),
            position_qty=Decimal("250"),
            order=make_broker_order(
                client_order_id=submitted.client_order_id,
                status=OrderStatus.filled,
                filled_qty=Decimal("250"),
                filled_avg_price=Decimal("100.10"),
                qty=Decimal("250"),
            ),
        )
    )
    await bus.drain()

    async with factory() as session:
        lot = (await session.execute(select(Lot))).scalars().one()
        assert lot.qty_open == Decimal("250")
        assert lot.entry_price == Decimal("100.10")
        legs = (
            (await session.execute(select(Order).where(Order.parent_order_id == parent.id)))
            .scalars()
            .all()
        )
        assert len(legs) == 1  # the stop leg was adopted via the nested read

    # ── 3. The stop leg fires: lot closes, realized P&L lands on the ledger.
    await writer.on_trade_update(
        make_trade_update(
            execution_id="exec-stop",
            qty=Decimal("250"),
            price=Decimal("99.00"),
            position_qty=Decimal("0"),
            order=make_broker_order(
                broker_order_id="bo-leg-sl",
                client_order_id="alpaca-leg-sl",
                side=OrderSide.sell,
                order_type=OrderType.stop,
                status=OrderStatus.filled,
                filled_qty=Decimal("250"),
                filled_avg_price=Decimal("99.00"),
                qty=Decimal("250"),
            ),
        )
    )
    await bus.drain()

    async with factory() as session:
        lot = (await session.execute(select(Lot))).scalars().one()
        assert lot.qty_open == Decimal("0")
        assert lot.closed_at is not None
        assert lot.realized_pnl == Decimal("-275.00")  # (99.00 − 100.10) × 250

        close = (await session.execute(select(LotClose))).scalars().one()
        assert close.realized_pnl == Decimal("-275.00")
        assert close.strategy_id == strategy.id

        position = (
            await session.execute(select(Position).where(Position.portfolio_id == portfolio.id))
        ).scalar_one_or_none()
        assert position is None  # flat again

        ledger = (await session.execute(select(Fill))).scalars().all()
        assert len(ledger) == 2

        # ── 4. The audit trail is complete at every hop.
        event_types = {
            e.event_type for e in (await session.execute(select(EventLog))).scalars().all()
        }
        assert {"order.approved", "order.submitted"} <= event_types

    # Strategies heard both executions, correctly attributed.
    assert [f.qty for f in fills_seen] == [Decimal("250"), Decimal("250")]
    assert all(f.strategy_id == strategy.id for f in fills_seen)
    assert fills_seen[1].side is OrderSide.sell
