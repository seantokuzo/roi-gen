"""TradeUpdateStage: the stream → DB writer that owns order state and the fill ledger."""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.engine.bus import EventBus
from app.engine.events import FillEvent
from app.engine.execution.trade_updates import TradeUpdateStage
from app.models.enums import OrderClass, OrderSide, OrderStatus, OrderType
from app.models.telemetry import EventLog
from app.models.trading import Fill, Lot, LotClose, Order, Position
from tests.engine.builders import (
    RecordingAdapter,
    make_broker_order,
    make_trade_update,
    seed_lot,
    seed_order,
    seed_portfolio,
    seed_strategy,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

    from app.models.portfolio import Portfolio
    from app.models.strategy import Strategy as StrategyModel
    from app.models.user import User


def _wired(
    db_engine: AsyncEngine, adapter: RecordingAdapter | None = None
) -> tuple[TradeUpdateStage, EventBus, list[FillEvent]]:
    bus = EventBus()
    fills: list[FillEvent] = []

    async def capture(event: FillEvent) -> None:
        fills.append(event)

    bus.subscribe(FillEvent, capture)
    stage = TradeUpdateStage(
        bus=bus,
        session_factory=async_sessionmaker(db_engine, expire_on_commit=False),
        adapter=adapter if adapter is not None else RecordingAdapter(),
        match_retry_delays=(),  # no waiting in tests unless a test opts in
    )
    return stage, bus, fills


async def _scope(db_session: AsyncSession, seeded_user: User) -> tuple[Portfolio, StrategyModel]:
    portfolio = await seed_portfolio(db_session, seeded_user.id)
    strategy = await seed_strategy(db_session, portfolio.id)
    return portfolio, strategy


async def _one(db_engine: AsyncEngine, model: type, **filters: object) -> object:
    factory = async_sessionmaker(db_engine, expire_on_commit=False)
    async with factory() as session:
        stmt = select(model)
        for col, val in filters.items():
            stmt = stmt.where(getattr(model, col) == val)
        return (await session.execute(stmt)).scalars().one()


async def _all(db_engine: AsyncEngine, model: type, **filters: object) -> list[object]:
    factory = async_sessionmaker(db_engine, expire_on_commit=False)
    async with factory() as session:
        stmt = select(model)
        for col, val in filters.items():
            stmt = stmt.where(getattr(model, col) == val)
        return list((await session.execute(stmt)).scalars().all())


async def test_fill_becomes_ledger_row_lot_position_and_fill_event(
    db_engine: AsyncEngine, db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio, strategy = await _scope(db_session, seeded_user)
    order = await seed_order(
        db_session, portfolio.id, strategy.id, broker_order_id="bo-1", qty=Decimal("250")
    )
    await db_session.commit()

    stage, bus, fills = _wired(db_engine)
    await stage.on_trade_update(make_trade_update())
    await bus.drain()

    fill = await _one(db_engine, Fill, order_id=order.id)
    assert fill.broker_fill_id == "exec-1"
    assert fill.qty == Decimal("250")

    lot = await _one(db_engine, Lot, portfolio_id=portfolio.id)
    assert lot.qty_open == Decimal("250")
    assert lot.entry_price == Decimal("100")
    assert lot.strategy_id == strategy.id

    position = await _one(db_engine, Position, portfolio_id=portfolio.id)
    assert position.qty == Decimal("250")

    updated = await _one(db_engine, Order, id=order.id)
    assert updated.status == OrderStatus.filled.value
    assert updated.filled_qty == Decimal("250")

    assert len(fills) == 1
    assert fills[0].strategy_id == strategy.id  # attribution recovered from the row
    assert fills[0].qty == Decimal("250")
    assert fills[0].position_qty == Decimal("250")


async def test_partial_fill_sequence_accumulates(
    db_engine: AsyncEngine, db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio, strategy = await _scope(db_session, seeded_user)
    order = await seed_order(
        db_session, portfolio.id, strategy.id, broker_order_id="bo-1", qty=Decimal("250")
    )
    await db_session.commit()
    stage, bus, fills = _wired(db_engine)

    await stage.on_trade_update(
        make_trade_update(
            event="partial_fill",
            execution_id="exec-a",
            qty=Decimal("100"),
            price=Decimal("99"),
            order=make_broker_order(
                status=OrderStatus.partially_filled,
                filled_qty=Decimal("100"),
                filled_avg_price=Decimal("99"),
            ),
        )
    )
    await stage.on_trade_update(
        make_trade_update(
            event="fill",
            execution_id="exec-b",
            qty=Decimal("150"),
            price=Decimal("101"),
            order=make_broker_order(
                status=OrderStatus.filled,
                filled_qty=Decimal("250"),
                filled_avg_price=Decimal("99.80"),
            ),
        )
    )
    await bus.drain()

    ledger = await _all(db_engine, Fill, order_id=order.id)
    assert sorted(f.qty for f in ledger) == [Decimal("100"), Decimal("150")]
    # Each partial opens its own lot at its own price; the open total is whole.
    lots = await _all(db_engine, Lot, portfolio_id=portfolio.id)
    assert sum(lot.qty_open for lot in lots) == Decimal("250")
    updated = await _one(db_engine, Order, id=order.id)
    assert updated.status == OrderStatus.filled.value
    assert len(fills) == 2


async def test_duplicate_execution_id_is_ignored(
    db_engine: AsyncEngine, db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio, strategy = await _scope(db_session, seeded_user)
    await seed_order(
        db_session, portfolio.id, strategy.id, broker_order_id="bo-1", qty=Decimal("250")
    )
    await db_session.commit()
    stage, bus, fills = _wired(db_engine)

    update = make_trade_update()
    await stage.on_trade_update(update)
    await stage.on_trade_update(update)  # stream redelivery after reconnect
    await bus.drain()

    assert len(await _all(db_engine, Fill)) == 1
    lots = await _all(db_engine, Lot)
    assert len(lots) == 1
    assert lots[0].qty_open == Decimal("250")  # applied exactly once
    assert len(fills) == 1


async def test_stale_snapshot_is_skipped_but_audited(
    db_engine: AsyncEngine, db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio, strategy = await _scope(db_session, seeded_user)
    order = await seed_order(
        db_session,
        portfolio.id,
        strategy.id,
        broker_order_id="bo-1",
        status=OrderStatus.partially_filled,
        filled_qty=Decimal("200"),
    )
    await db_session.commit()
    stage, bus, _ = _wired(db_engine)

    # A late 'new' event with cumulative 0 — predates everything we know.
    await stage.on_trade_update(
        make_trade_update(
            event="new",
            execution_id=None,
            qty=None,
            price=None,
            position_qty=None,
            order=make_broker_order(status=OrderStatus.submitted, filled_qty=Decimal("0")),
        )
    )

    updated = await _one(db_engine, Order, id=order.id)
    assert updated.status == OrderStatus.partially_filled.value  # unchanged
    assert updated.filled_qty == Decimal("200")
    events = await _all(db_engine, EventLog, event_type="trade_update.snapshot_skipped")
    assert len(events) == 1
    assert events[0].payload["plan"] == "stale"


async def test_orphan_event_is_audited_never_adopted(
    db_engine: AsyncEngine, db_session: AsyncSession, seeded_user: User
) -> None:
    await _scope(db_session, seeded_user)
    await db_session.commit()
    stage, _, fills = _wired(db_engine)

    await stage.on_trade_update(
        make_trade_update(order=make_broker_order(broker_order_id="foreign-1"))
    )

    assert await _all(db_engine, Order) == []
    assert await _all(db_engine, Fill) == []
    events = await _all(db_engine, EventLog, event_type="trade_update.orphan")
    assert len(events) == 1
    assert events[0].payload["broker_order_id"] == "foreign-1"
    assert fills == []


async def test_leg_fill_closes_the_parents_lot_with_attribution(
    db_engine: AsyncEngine, db_session: AsyncSession, seeded_user: User
) -> None:
    """The protective exit path: a stop-loss leg fill must consume the entry's
    lot, book realized P&L on the ledger, and attribute to the strategy."""
    portfolio, strategy = await _scope(db_session, seeded_user)
    parent = await seed_order(
        db_session,
        portfolio.id,
        strategy.id,
        broker_order_id="bo-parent",
        status=OrderStatus.filled,
        filled_qty=Decimal("250"),
    )
    leg = await seed_order(
        db_session,
        portfolio.id,
        strategy.id,
        broker_order_id="bo-leg-sl",
        side=OrderSide.sell,
        order_type=OrderType.stop,
        status=OrderStatus.accepted,
        parent_order_id=parent.id,
    )
    await seed_lot(
        db_session,
        portfolio.id,
        strategy.id,
        qty_orig=Decimal("250"),
        qty_open=Decimal("250"),
        entry_price=Decimal("100"),
    )
    await db_session.commit()
    stage, bus, fills = _wired(db_engine)

    await stage.on_trade_update(
        make_trade_update(
            execution_id="exec-sl",
            qty=Decimal("250"),
            price=Decimal("99"),
            position_qty=Decimal("0"),
            order=make_broker_order(
                broker_order_id="bo-leg-sl",
                client_order_id=leg.client_order_id,
                side=OrderSide.sell,
                order_type=OrderType.stop,
                status=OrderStatus.filled,
                filled_qty=Decimal("250"),
                filled_avg_price=Decimal("99"),
            ),
        )
    )
    await bus.drain()

    close = await _one(db_engine, LotClose, portfolio_id=portfolio.id)
    assert close.realized_pnl == Decimal("-250")  # (99 − 100) × 250: the stop did its job
    assert close.strategy_id == strategy.id
    lot = await _one(db_engine, Lot, portfolio_id=portfolio.id)
    assert lot.qty_open == Decimal("0")
    assert lot.closed_at is not None
    assert len(fills) == 1
    assert fills[0].side is OrderSide.sell
    assert fills[0].strategy_id == strategy.id


async def test_gap_in_cumulative_synthesizes_the_missed_span(
    db_engine: AsyncEngine, db_session: AsyncSession, seeded_user: User
) -> None:
    """A dropped event during a reconnect: the next event's cumulative exposes
    the hole and the writer back-fills it at the backed-out span price."""
    portfolio, strategy = await _scope(db_session, seeded_user)
    order = await seed_order(
        db_session, portfolio.id, strategy.id, broker_order_id="bo-1", qty=Decimal("250")
    )
    await db_session.commit()
    stage, bus, fills = _wired(db_engine)

    # First 100 @ 10.00 was NEVER delivered. This event: 150 @ 11.00, cum 250,
    # cumulative avg = (100×10 + 150×11)/250 = 10.60.
    await stage.on_trade_update(
        make_trade_update(
            execution_id="exec-late",
            qty=Decimal("150"),
            price=Decimal("11.00"),
            order=make_broker_order(
                status=OrderStatus.filled,
                filled_qty=Decimal("250"),
                filled_avg_price=Decimal("10.60"),
            ),
        )
    )
    await bus.drain()

    ledger = await _all(db_engine, Fill, order_id=order.id)
    assert len(ledger) == 2
    by_qty = {f.qty: f for f in ledger}
    gap = by_qty[Decimal("100")]
    assert gap.broker_fill_id is not None and gap.broker_fill_id.startswith("gap-")
    assert gap.price == Decimal("10.00")  # backed out of the notional difference
    assert by_qty[Decimal("150")].price == Decimal("11.00")

    lots = await _all(db_engine, Lot, portfolio_id=portfolio.id)
    assert sum(lot.qty_open for lot in lots) == Decimal("250")  # nothing lost
    events = await _all(db_engine, EventLog, event_type="trade_update.gap_synthesized")
    assert len(events) == 1


async def test_cancel_event_carrying_unseen_cumulative_synthesizes_before_absorbing(
    db_engine: AsyncEngine, db_session: AsyncSession, seeded_user: User
) -> None:
    """C2: the partial-fill event was dropped; the CANCELED event is the first
    place the 50 filled shares surface (cumulative only, no execution). The
    writer must synthesize the span BEFORE adopting the absorbing status, or
    the quantity never reaches lots."""
    portfolio, strategy = await _scope(db_session, seeded_user)
    order = await seed_order(
        db_session, portfolio.id, strategy.id, broker_order_id="bo-1", qty=Decimal("250")
    )
    await db_session.commit()
    stage, bus, fills = _wired(db_engine)

    await stage.on_trade_update(
        make_trade_update(
            event="canceled",
            execution_id=None,
            qty=None,
            price=None,
            position_qty=None,
            order=make_broker_order(
                status=OrderStatus.canceled,
                filled_qty=Decimal("50"),
                filled_avg_price=Decimal("101"),
            ),
        )
    )
    await bus.drain()

    ledger = await _all(db_engine, Fill, order_id=order.id)
    assert len(ledger) == 1
    assert ledger[0].qty == Decimal("50")
    assert ledger[0].price == Decimal("101")
    assert ledger[0].broker_fill_id is not None
    assert ledger[0].broker_fill_id.startswith("gap-")

    lots = await _all(db_engine, Lot, portfolio_id=portfolio.id)
    assert sum(lot.qty_open for lot in lots) == Decimal("50")  # nothing lost
    updated = await _one(db_engine, Order, id=order.id)
    assert updated.status == OrderStatus.canceled.value  # then absorbed
    assert fills == []  # gap synthesis is catch-up accounting, not a FillEvent


async def test_failed_order_is_resurrected_by_broker_evidence(
    db_engine: AsyncEngine, db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio, strategy = await _scope(db_session, seeded_user)
    order = await seed_order(
        db_session,
        portfolio.id,
        strategy.id,
        broker_order_id=None,
        status=OrderStatus.failed,  # we guessed "not placed"…
        client_order_id="roigen-resurrect",
    )
    await db_session.commit()
    stage, bus, fills = _wired(db_engine)

    await stage.on_trade_update(
        make_trade_update(
            order=make_broker_order(
                broker_order_id="bo-back-from-dead",
                client_order_id="roigen-resurrect",
                status=OrderStatus.filled,
                filled_qty=Decimal("250"),
                filled_avg_price=Decimal("100"),
            ),
        )
    )
    await bus.drain()

    updated = await _one(db_engine, Order, id=order.id)
    assert updated.status == OrderStatus.filled.value  # broker reality won
    assert updated.broker_order_id == "bo-back-from-dead"  # adopted via client id
    assert len(await _all(db_engine, EventLog, event_type="order.failed_resurrected")) == 1
    assert len(await _all(db_engine, Fill)) == 1  # and its fill hit the ledger
    assert len(fills) == 1


async def test_bracket_parent_without_children_fetches_and_adopts_legs(
    db_engine: AsyncEngine, db_session: AsyncSession, seeded_user: User
) -> None:
    """Ambiguous-exhausted recovery: the submit response was never seen, so no
    leg rows exist — the first stream event must converge them."""
    portfolio, strategy = await _scope(db_session, seeded_user)
    order = await seed_order(
        db_session,
        portfolio.id,
        strategy.id,
        broker_order_id=None,  # never learned it
        order_class=OrderClass.bracket,
        status=OrderStatus.pending_submit,
        client_order_id="roigen-ambig",
    )
    await db_session.commit()

    nested = make_broker_order(
        broker_order_id="bo-parent",
        client_order_id="roigen-ambig",
        legs=[
            make_broker_order(
                broker_order_id="bo-leg-sl",
                client_order_id="alpaca-leg-sl",
                side=OrderSide.sell,
                order_type=OrderType.stop,
                status=OrderStatus.held,
            )
        ],
    )
    adapter = RecordingAdapter(orders_by_id={"bo-parent": nested})
    stage, bus, _ = _wired(db_engine, adapter)

    await stage.on_trade_update(
        make_trade_update(
            event="new",
            execution_id=None,
            qty=None,
            price=None,
            position_qty=None,
            order=make_broker_order(
                broker_order_id="bo-parent",
                client_order_id="roigen-ambig",
                status=OrderStatus.accepted,
                filled_qty=Decimal("0"),
            ),
        )
    )

    updated = await _one(db_engine, Order, id=order.id)
    assert updated.broker_order_id == "bo-parent"
    children = await _all(db_engine, Order, parent_order_id=order.id)
    assert len(children) == 1
    assert children[0].broker_order_id == "bo-leg-sl"
    assert children[0].strategy_id == strategy.id


async def test_late_fill_on_terminal_order_is_recorded_and_flagged(
    db_engine: AsyncEngine, db_session: AsyncSession, seeded_user: User
) -> None:
    # Money truth beats status bookkeeping: the fill lands on the ledger, the
    # anomaly is loud, and the absorbed status does not regress.
    portfolio, strategy = await _scope(db_session, seeded_user)
    order = await seed_order(
        db_session,
        portfolio.id,
        strategy.id,
        broker_order_id="bo-1",
        status=OrderStatus.canceled,
        filled_qty=Decimal("0"),
    )
    await db_session.commit()
    stage, bus, fills = _wired(db_engine)

    await stage.on_trade_update(
        make_trade_update(
            execution_id="exec-late-cancel-race",
            qty=Decimal("50"),
            price=Decimal("100"),
            order=make_broker_order(
                status=OrderStatus.canceled,
                filled_qty=Decimal("50"),
                filled_avg_price=Decimal("100"),
            ),
        )
    )
    await bus.drain()

    assert len(await _all(db_engine, Fill, order_id=order.id)) == 1
    updated = await _one(db_engine, Order, id=order.id)
    assert updated.status == OrderStatus.canceled.value  # absorbed, not regressed
    anomalies = await _all(db_engine, EventLog, event_type="trade_update.terminal_fill_anomaly")
    assert len(anomalies) == 1
    assert len(fills) == 1  # strategies still hear about real money
