"""Integrated safety E2E — the offline mirror of the live-paper proof.

One test walks the entire 2c story on real components over the test DB (only
the broker and Redis are fakes): a signal trades through risk into a protected
bracket; the operator's ``flatten`` lands in the command log via the REAL
service; the sweeper derives kill-state; entries are refused while halted; the
controller drives the flatten (cancel legs → confirm → liquidation with the 2b
submit discipline); the liquidation fill closes the FIFO lot with per-strategy
attribution; the controller verifies broker-flat and stamps ``flat_verified``
on the command row; ``resume`` re-arms and trading resumes. Every hop is then
asserted from audit rows alone — if this chain can't be walked from the DB,
the live E2E's "full audit trail" claim is fiction.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.engine.bus import EventBus
from app.engine.execution.handler import ExecutionStage
from app.engine.execution.trade_updates import TradeUpdateStage
from app.engine.flatten_controller import FlattenController
from app.engine.kill_switch import KillSwitch
from app.engine.risk.engine import RiskEngine
from app.engine.risk.state import RiskStateProvider
from app.engine.stage import RiskStage
from app.models.engine_command import EngineCommand
from app.models.enums import EngineCommandAction, OrderSide, OrderStatus, OrderType
from app.models.telemetry import EventLog
from app.models.trading import Lot, LotClose, Order, Position
from app.services.engine_commands import issue_command
from tests.engine.builders import (
    DEFAULT_NOW,
    RecordingAdapter,
    make_broker_order,
    make_limits,
    make_signal,
    make_trade_update,
    seed_portfolio,
    seed_strategy,
)
from tests.engine.flatten_helpers import (
    FakeRedis,
    make_broker_position,
    make_calendar_day,
    wait_until,
)

if TYPE_CHECKING:
    from datetime import date

    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

    from app.brokers.dto import BrokerOrder, CalendarDay
    from app.models.user import User


class _SafetyAdapter(RecordingAdapter):
    """RecordingAdapter + the flatten path's mutations + a real calendar.

    Broker state (``positions`` / ``open_orders`` / ``orders_by_id``) is
    mutated by the test at each stage to mirror what the fills imply — the
    controller and the flatten execution read ONLY this "broker truth".
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.canceled: list[str] = []
        self.calendar: list[CalendarDay] = []

    async def submit_order(self, req: Any) -> BrokerOrder:
        self.submitted.append(req)
        return make_broker_order(
            broker_order_id=f"bo-{len(self.submitted)}",
            client_order_id=req.client_order_id,
            symbol=req.symbol,
            side=req.side,
            order_type=req.order_type,
            order_class=req.order_class,
            time_in_force=req.time_in_force,
            qty=req.qty,
        )

    async def cancel_order(self, broker_order_id: str) -> None:
        self.canceled.append(broker_order_id)

    async def get_calendar(self, start: date, end: date) -> list[CalendarDay]:
        return [d for d in self.calendar if start <= d.trading_date <= end]


async def test_kill_switch_flatten_full_stack_with_audit_chain(
    db_engine: AsyncEngine, db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio = await seed_portfolio(db_session, seeded_user.id)
    strategy = await seed_strategy(db_session, portfolio.id)
    await db_session.commit()
    factory = async_sessionmaker(db_engine, expire_on_commit=False)

    # DEFAULT_NOW is Fri 2026-06-26 15:00Z (11:00 ET) — build its RTH session.
    session_date = DEFAULT_NOW.astimezone().date()
    adapter = _SafetyAdapter()
    adapter.calendar = [
        make_calendar_day(
            DEFAULT_NOW.date(),
            DEFAULT_NOW - timedelta(hours=1, minutes=30),  # 09:30 ET
            DEFAULT_NOW + timedelta(hours=5),  # 16:00 ET
        )
    ]
    del session_date

    bus = EventBus()
    boot_reconciled = asyncio.Event()
    kill_switch = KillSwitch(factory)

    def halted() -> bool:
        return not boot_reconciled.is_set() or kill_switch.is_halted

    RiskStage(
        bus=bus,
        # Cooldown zeroed: step 7's post-resume re-entry happens seconds after
        # the flatten — the 60s per-symbol cooldown would (correctly) block it.
        engine=RiskEngine(make_limits(symbol_cooldown_seconds=0)),
        provider=RiskStateProvider(),
        session_factory=factory,
        adapter=adapter,
        halted=halted,
    ).register_handlers()
    execution = ExecutionStage(
        bus=bus,
        session_factory=factory,
        adapter=adapter,
        resolve_delays=(0.0,),
        cancel_confirm_delays=(0.0,),
        halted=halted,
    )
    execution.register_handlers()
    writer = TradeUpdateStage(
        bus=bus, session_factory=factory, adapter=adapter, match_retry_delays=()
    )
    redis = FakeRedis()
    controller = FlattenController(
        bus=bus,
        adapter=adapter,
        session_factory=factory,
        redis=redis,  # type: ignore[arg-type]  # duck-typed fake
        portfolio_id=portfolio.id,
        kill_switch=kill_switch,
        boot_reconciled=boot_reconciled,
        flatten_buffer=timedelta(minutes=5),
    )
    from app.engine.commands import EngineCommandSweeper

    sweeper = EngineCommandSweeper(
        redis=redis,  # type: ignore[arg-type]
        session_factory=factory,
        kill_switch=kill_switch,
        controller=controller,
        boot_reconciled=boot_reconciled,
    )
    boot_reconciled.set()

    # ── 1. Entry: signal → risk approval → protected bracket submitted.
    await bus.publish(make_signal(portfolio_id=portfolio.id, strategy_id=strategy.id))
    await bus.drain()
    assert len(adapter.submitted) == 1
    entry_req = adapter.submitted[0]
    assert entry_req.order_class.value == "bracket"

    stop_leg = make_broker_order(
        broker_order_id="bo-leg-sl",
        client_order_id="alpaca-leg-sl",
        side=OrderSide.sell,
        order_type=OrderType.stop,
        status=OrderStatus.held,
        qty=Decimal("250"),
    )
    adapter.orders_by_id["bo-1"] = make_broker_order(
        broker_order_id="bo-1",
        client_order_id=entry_req.client_order_id,
        status=OrderStatus.accepted,
        qty=Decimal("250"),
        legs=[stop_leg],
    )
    await writer.on_trade_update(
        make_trade_update(
            execution_id="exec-entry",
            qty=Decimal("250"),
            price=Decimal("100.10"),
            position_qty=Decimal("250"),
            order=make_broker_order(
                broker_order_id="bo-1",
                client_order_id=entry_req.client_order_id,
                status=OrderStatus.filled,
                filled_qty=Decimal("250"),
                filled_avg_price=Decimal("100.10"),
                qty=Decimal("250"),
            ),
        )
    )
    await bus.drain()
    # Broker truth now: a long position protected by its live stop leg.
    adapter.positions = [
        make_broker_position("SPY", qty=Decimal("250"), avg_entry_price=Decimal("100.10"))
    ]
    adapter.open_orders = [stop_leg]

    # ── 2. Operator flatten via the REAL service; sweeper derives kill-state.
    async with factory() as session:
        cmd = await issue_command(
            session,
            redis,  # type: ignore[arg-type]
            action=EngineCommandAction.flatten,
            reason="integrated e2e",
            actor="cli:test",
        )
    await sweeper.sweep()
    assert kill_switch.is_halted and kill_switch.is_flattening

    # ── 3. Entries are refused while halted — at the risk gate, with audit.
    await bus.publish(make_signal(portfolio_id=portfolio.id, strategy_id=strategy.id))
    await bus.drain()
    assert len(adapter.submitted) == 1  # nothing new reached the broker
    async with factory() as session:
        rejected = (
            (await session.execute(select(EventLog).where(EventLog.event_type == "order.rejected")))
            .scalars()
            .all()
        )
    assert rejected and "kill_switch" in str(rejected[-1].payload)

    # ── 4. The controller drives: cancel the leg → confirm → liquidate.
    adapter.orders_by_id["bo-leg-sl"] = make_broker_order(
        broker_order_id="bo-leg-sl",
        client_order_id="alpaca-leg-sl",
        side=OrderSide.sell,
        order_type=OrderType.stop,
        status=OrderStatus.canceled,
        qty=Decimal("250"),
    )
    await controller._tick()  # noqa: SLF001 — drive one tick deterministically
    await bus.drain()
    await wait_until(lambda: len(adapter.submitted) == 2)
    await wait_until(lambda: not execution._flatten_tasks)  # noqa: SLF001
    liq_req = adapter.submitted[1]
    assert liq_req.client_order_id.startswith("roigen-flatten-")
    assert liq_req.side is OrderSide.sell and liq_req.qty == Decimal("250")
    assert liq_req.position_intent == "sell_to_close"
    assert adapter.canceled == ["bo-leg-sl"]  # protection canceled exactly once

    # ── 5. The liquidation fills; lots close with strategy attribution intact.
    await writer.on_trade_update(
        make_trade_update(
            execution_id="exec-liq",
            qty=Decimal("250"),
            price=Decimal("99.50"),
            position_qty=Decimal("0"),
            order=make_broker_order(
                broker_order_id="bo-2",
                client_order_id=liq_req.client_order_id,
                side=OrderSide.sell,
                order_type=OrderType.market,
                status=OrderStatus.filled,
                filled_qty=Decimal("250"),
                filled_avg_price=Decimal("99.50"),
                qty=Decimal("250"),
            ),
        )
    )
    await bus.drain()
    adapter.positions = []
    adapter.open_orders = []

    # ── 6. Next tick verifies broker-flat and stamps the command's outcome.
    await controller._tick()  # noqa: SLF001
    async with factory() as session:
        cmd_row = (
            await session.execute(select(EngineCommand).where(EngineCommand.seq == cmd.seq))
        ).scalar_one()
        assert cmd_row.applied_at is not None
        assert cmd_row.result == "flat_verified"
    assert kill_switch.is_halted  # flatten verified but NOT re-armed

    # ── 7. Resume re-arms; trading flows again.
    async with factory() as session:
        await issue_command(
            session,
            redis,  # type: ignore[arg-type]
            action=EngineCommandAction.resume,
            reason="resume",
            actor="cli:test",
        )
    await sweeper.sweep()
    assert not kill_switch.is_halted
    await bus.publish(make_signal(portfolio_id=portfolio.id, strategy_id=strategy.id))
    await bus.drain()
    assert len(adapter.submitted) == 3  # a fresh entry reached the broker

    # ── 8. The whole story, re-read from rows alone.
    async with factory() as session:
        events = {e.event_type for e in (await session.execute(select(EventLog))).scalars().all()}
        liq_order = (
            await session.execute(
                select(Order).where(Order.client_order_id == liq_req.client_order_id)
            )
        ).scalar_one()
        lot = (
            await session.execute(select(Lot).where(Lot.strategy_id == strategy.id))
        ).scalar_one()
        close = (await session.execute(select(LotClose))).scalars().one()
        position = (
            await session.execute(select(Position).where(Position.symbol == "SPY"))
        ).scalar_one_or_none()

    assert {
        "order.approved",  # entry through the 14-control sweep
        "order.rejected",  # the halted refusal
        "flatten.approved",  # risk authorized the flatten (while halted)
        "flatten.executed",  # execution's per-symbol outcomes
        "flatten.completed",  # controller's broker-verified completion
    } <= events
    assert liq_order.strategy_id is None
    assert liq_order.status == OrderStatus.filled.value
    assert liq_order.risk_approval is not None
    assert liq_order.risk_approval["command_seq"] == cmd.seq  # command → order linkage
    assert lot.qty_open == Decimal("0")
    assert lot.realized_pnl == Decimal("-150.00")  # (99.50 − 100.10) × 250
    assert close.strategy_id == strategy.id  # the day breaker sees the flatten loss
    assert position is None or position.qty == Decimal("0")  # tracker deletes at flat
