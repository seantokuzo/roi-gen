"""Reconciliation ordering rework + missed-fill synthesis (Phase 2b).

Covers the design-review findings that reshaped the service: the fill ledger
(SUM of Fill rows) as the synthesis cursor, atomic synthesize-with-adopt,
positions never double-applied at boot, dormant done_for_day, the
never-submitted grace window, and boot leg adoption.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

import pytest_asyncio
from sqlalchemy import select

from app.engine.execution.lots import UNAPPLIED_REMAINDER_EVENT, apply_fill_to_lots
from app.models.enums import OrderClass, OrderSide, OrderStatus, OrderType, PortfolioMode
from app.models.portfolio import Portfolio
from app.models.telemetry import EventLog
from app.models.trading import Fill, Lot, LotClose, Order, Position
from app.services.reconciliation import ReconciliationService
from tests.engine.builders import (
    RecordingAdapter,
    make_account,
    make_broker_order,
    seed_lot,
    seed_order,
    seed_strategy,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from app.brokers.dto import BrokerPosition
    from app.models.user import User


@pytest_asyncio.fixture
async def portfolio(db_session: AsyncSession, seeded_user: User) -> Portfolio:
    p = Portfolio(user_id=seeded_user.id, name="recon-synth", mode=PortfolioMode.paper)
    db_session.add(p)
    await db_session.commit()
    await db_session.refresh(p)
    return p


def _broker_position(symbol: str, qty: str) -> BrokerPosition:
    from app.brokers.dto import BrokerPosition

    return BrokerPosition(
        symbol=symbol,
        qty=Decimal(qty),
        side="long",
        avg_entry_price=Decimal("10"),
        market_value=Decimal("1000"),
        cost_basis=Decimal("1000"),
        unrealized_pl=Decimal("0"),
    )


async def test_missed_fill_synthesized_into_ledger_and_lots(
    db_session: AsyncSession, portfolio: Portfolio, seeded_user: User
) -> None:
    strategy = await seed_strategy(db_session, portfolio.id)
    order = await seed_order(
        db_session, portfolio.id, strategy.id, broker_order_id="bo-1", qty=Decimal("100")
    )
    await db_session.commit()

    broker = make_broker_order(
        broker_order_id="bo-1",
        client_order_id=order.client_order_id,
        status=OrderStatus.partially_filled,
        filled_qty=Decimal("100"),
        filled_avg_price=Decimal("10"),
    )
    adapter = RecordingAdapter(
        open_orders=[broker],
        positions=[_broker_position("SPY", "100")],
        account=make_account(position_market_value=Decimal("1000")),
    )
    result = await ReconciliationService().reconcile_portfolio(
        db_session, portfolio.id, adapter, synthesize_fills=True
    )
    await db_session.commit()

    assert result.fills_synthesized == 1
    fill = (await db_session.execute(select(Fill))).scalars().one()
    assert fill.broker_fill_id is not None and fill.broker_fill_id.startswith("recon-")
    assert fill.qty == Decimal("100")
    assert fill.price == Decimal("10")

    lot = (await db_session.execute(select(Lot))).scalars().one()
    assert lot.qty_open == Decimal("100")
    assert lot.strategy_id == strategy.id

    # C3/C16: position comes from the broker overwrite ONLY — the synthetic
    # fill must not be applied on top (that would read 200).
    position = (await db_session.execute(select(Position))).scalars().one()
    assert position.qty == Decimal("100")


async def test_synthesis_is_idempotent_across_reruns(
    db_session: AsyncSession, portfolio: Portfolio, seeded_user: User
) -> None:
    strategy = await seed_strategy(db_session, portfolio.id)
    order = await seed_order(
        db_session, portfolio.id, strategy.id, broker_order_id="bo-1", qty=Decimal("100")
    )
    await db_session.commit()
    broker = make_broker_order(
        broker_order_id="bo-1",
        client_order_id=order.client_order_id,
        status=OrderStatus.partially_filled,
        filled_qty=Decimal("100"),
        filled_avg_price=Decimal("10"),
    )
    adapter = RecordingAdapter(open_orders=[broker])

    service = ReconciliationService()
    await service.reconcile_portfolio(db_session, portfolio.id, adapter, synthesize_fills=True)
    await db_session.commit()
    second = await service.reconcile_portfolio(
        db_session, portfolio.id, adapter, synthesize_fills=True
    )
    await db_session.commit()

    assert second.fills_synthesized == 0  # cursor says nothing is missing
    fills = (await db_session.execute(select(Fill))).scalars().all()
    assert len(fills) == 1
    lots = (await db_session.execute(select(Lot))).scalars().all()
    assert len(lots) == 1


async def test_span_price_backed_out_of_notional_difference(
    db_session: AsyncSession, portfolio: Portfolio, seeded_user: User
) -> None:
    """C4/C12: with 50 recorded @ $10 and broker cum 100 @ avg $11, the missed
    50 must be priced $12 — never the $11 cumulative average."""
    strategy = await seed_strategy(db_session, portfolio.id)
    order = await seed_order(
        db_session,
        portfolio.id,
        strategy.id,
        broker_order_id="bo-1",
        qty=Decimal("100"),
        filled_qty=Decimal("50"),
        status=OrderStatus.partially_filled,
    )
    db_session.add(
        Fill(
            order_id=order.id,
            broker_fill_id="exec-live",
            qty=Decimal("50"),
            price=Decimal("10"),
            occurred_at=datetime(2026, 7, 2, 15, 0, tzinfo=UTC),
        )
    )
    await db_session.commit()

    broker = make_broker_order(
        broker_order_id="bo-1",
        client_order_id=order.client_order_id,
        status=OrderStatus.filled,
        filled_qty=Decimal("100"),
        filled_avg_price=Decimal("11"),
    )
    adapter = RecordingAdapter(open_orders=[broker])
    await ReconciliationService().reconcile_portfolio(
        db_session, portfolio.id, adapter, synthesize_fills=True
    )
    await db_session.commit()

    synthetic = (
        (await db_session.execute(select(Fill).where(Fill.broker_fill_id.like("recon-%"))))
        .scalars()
        .one()
    )
    assert synthetic.qty == Decimal("50")
    assert synthetic.price == Decimal("12")  # (100×11 − 50×10) / 50


async def test_api_sync_without_synthesis_cannot_hide_fills_from_the_engine(
    db_session: AsyncSession, portfolio: Portfolio, seeded_user: User
) -> None:
    """C8: the API endpoint advances Order.filled_qty without lot work; the
    engine's next reconcile must STILL synthesize — the cursor is the fill
    ledger, not the order column."""
    strategy = await seed_strategy(db_session, portfolio.id)
    order = await seed_order(
        db_session, portfolio.id, strategy.id, broker_order_id="bo-1", qty=Decimal("100")
    )
    await db_session.commit()
    broker = make_broker_order(
        broker_order_id="bo-1",
        client_order_id=order.client_order_id,
        status=OrderStatus.partially_filled,
        filled_qty=Decimal("100"),
        filled_avg_price=Decimal("10"),
    )
    adapter = RecordingAdapter(open_orders=[broker])
    service = ReconciliationService()

    # The API's mode: no synthesis. filled_qty advances, ledger stays empty.
    await service.reconcile_portfolio(db_session, portfolio.id, adapter, synthesize_fills=False)
    await db_session.commit()
    await db_session.refresh(order)
    assert order.filled_qty == Decimal("100")
    assert (await db_session.execute(select(Fill))).scalars().all() == []

    # The engine's mode afterwards: the hole is still visible and gets filled.
    result = await service.reconcile_portfolio(
        db_session, portfolio.id, adapter, synthesize_fills=True
    )
    await db_session.commit()
    assert result.fills_synthesized == 1
    assert len((await db_session.execute(select(Fill))).scalars().all()) == 1


async def test_never_submitted_ages_into_failed_after_grace(
    db_session: AsyncSession, portfolio: Portfolio, seeded_user: User
) -> None:
    strategy = await seed_strategy(db_session, portfolio.id)
    stale = await seed_order(
        db_session,
        portfolio.id,
        strategy.id,
        broker_order_id=None,
        status=OrderStatus.pending_submit,
    )
    fresh = await seed_order(
        db_session,
        portfolio.id,
        strategy.id,
        broker_order_id=None,
        status=OrderStatus.pending_submit,
    )
    stale.created_at = datetime.now(UTC) - timedelta(minutes=10)  # past the grace window
    await db_session.commit()

    adapter = RecordingAdapter()  # broker knows nothing about either
    result = await ReconciliationService().reconcile_portfolio(
        db_session, portfolio.id, adapter, synthesize_fills=True
    )
    await db_session.commit()
    await db_session.refresh(stale)
    await db_session.refresh(fresh)

    assert stale.status == OrderStatus.failed.value  # aged out
    assert fresh.status == OrderStatus.pending_submit.value  # in-flight grace
    assert result.missing == 1  # only the fresh one remains unresolved
    events = (
        (
            await db_session.execute(
                select(EventLog).where(EventLog.event_type == "reconcile.never_submitted")
            )
        )
        .scalars()
        .all()
    )
    assert len(events) == 1
    assert events[0].order_id == stale.id


async def test_done_for_day_is_dormant_and_gets_recovered(
    db_session: AsyncSession, portfolio: Portfolio, seeded_user: User
) -> None:
    """C13: done_for_day must NOT be skipped as terminal — the recovery lookup
    runs and finds the overnight fill."""
    strategy = await seed_strategy(db_session, portfolio.id)
    order = await seed_order(
        db_session,
        portfolio.id,
        strategy.id,
        broker_order_id="bo-dfd",
        status=OrderStatus.done_for_day,
        qty=Decimal("10"),
    )
    await db_session.commit()

    adapter = RecordingAdapter(
        orders_by_id={
            "bo-dfd": make_broker_order(
                broker_order_id="bo-dfd",
                client_order_id=order.client_order_id,
                order_class=OrderClass.simple,
                status=OrderStatus.filled,
                filled_qty=Decimal("10"),
                filled_avg_price=Decimal("50"),
                qty=Decimal("10"),
            )
        }
    )
    result = await ReconciliationService().reconcile_portfolio(
        db_session, portfolio.id, adapter, synthesize_fills=True
    )
    await db_session.commit()
    await db_session.refresh(order)

    assert order.status == OrderStatus.filled.value
    assert result.fills_synthesized == 1  # the overnight fill reached the ledger


async def test_terminal_order_with_ledger_deficit_is_swept(
    db_session: AsyncSession, portfolio: Portfolio, seeded_user: User
) -> None:
    """A locally-TERMINAL order can carry a ledger deficit (the API sync
    adopted 'filled' with no synthesis while the stream was deaf). Steps 1-2
    skip terminal orders, so the deficit sweep must catch it — otherwise those
    fills never reach the lot ledger."""
    strategy = await seed_strategy(db_session, portfolio.id)
    order = await seed_order(
        db_session,
        portfolio.id,
        strategy.id,
        broker_order_id="bo-1",
        qty=Decimal("100"),
        filled_qty=Decimal("100"),
        status=OrderStatus.filled,  # terminal locally, zero Fill rows
    )
    await db_session.commit()

    broker = make_broker_order(
        broker_order_id="bo-1",
        client_order_id=order.client_order_id,
        status=OrderStatus.filled,
        filled_qty=Decimal("100"),
        filled_avg_price=Decimal("10"),
    )
    adapter = RecordingAdapter(open_orders=[], orders_by_id={"bo-1": broker})
    result = await ReconciliationService().reconcile_portfolio(
        db_session, portfolio.id, adapter, synthesize_fills=True
    )
    await db_session.commit()

    assert result.fills_synthesized == 1
    fill = (await db_session.execute(select(Fill))).scalars().one()
    assert fill.qty == Decimal("100")
    assert fill.price == Decimal("10")
    lot = (await db_session.execute(select(Lot))).scalars().one()
    assert lot.qty_open == Decimal("100")


async def test_deficit_sweep_falls_back_to_local_columns(
    db_session: AsyncSession, portfolio: Portfolio, seeded_user: User
) -> None:
    # The broker no longer returns the order at all; the order's own columns
    # (which came from a broker snapshot) price the span.
    strategy = await seed_strategy(db_session, portfolio.id)
    order = await seed_order(
        db_session,
        portfolio.id,
        strategy.id,
        broker_order_id="bo-gone",
        qty=Decimal("40"),
        filled_qty=Decimal("40"),
        status=OrderStatus.filled,
    )
    order.filled_avg_price = Decimal("25")
    await db_session.commit()

    adapter = RecordingAdapter()  # every lookup returns None
    result = await ReconciliationService().reconcile_portfolio(
        db_session, portfolio.id, adapter, synthesize_fills=True
    )
    await db_session.commit()

    assert result.fills_synthesized == 1
    fill = (await db_session.execute(select(Fill))).scalars().one()
    assert fill.qty == Decimal("40")
    assert fill.price == Decimal("25")


async def test_boot_adopts_missing_leg_rows_from_nested_parents(
    db_session: AsyncSession, portfolio: Portfolio, seeded_user: User
) -> None:
    """C9: a bracket parent whose legs were never persisted (crash after
    submit) gets its children created from the broker's nested read."""
    strategy = await seed_strategy(db_session, portfolio.id)
    parent = await seed_order(
        db_session,
        portfolio.id,
        strategy.id,
        broker_order_id="bo-parent",
        order_class=OrderClass.bracket,
        status=OrderStatus.filled,
        filled_qty=Decimal("250"),
    )
    # Give the parent its fill so synthesis doesn't fire here (not the point).
    db_session.add(
        Fill(
            order_id=parent.id,
            broker_fill_id="exec-parent",
            qty=Decimal("250"),
            price=Decimal("100"),
            occurred_at=datetime(2026, 7, 2, 15, 0, tzinfo=UTC),
        )
    )
    await db_session.commit()

    leg = make_broker_order(
        broker_order_id="bo-leg-sl",
        client_order_id="alpaca-leg-sl",
        side=OrderSide.sell,
        order_type=OrderType.stop,
        status=OrderStatus.submitted,
        qty=Decimal("250"),
    )
    broker_parent = make_broker_order(
        broker_order_id="bo-parent",
        client_order_id=parent.client_order_id,
        status=OrderStatus.filled,
        filled_qty=Decimal("250"),
        filled_avg_price=Decimal("100"),
        legs=[leg],
    )
    adapter = RecordingAdapter(open_orders=[broker_parent])
    await ReconciliationService().reconcile_portfolio(
        db_session, portfolio.id, adapter, synthesize_fills=True
    )
    await db_session.commit()

    children = (
        (await db_session.execute(select(Order).where(Order.parent_order_id == parent.id)))
        .scalars()
        .all()
    )
    assert len(children) == 1
    assert children[0].broker_order_id == "bo-leg-sl"
    assert children[0].client_order_id == "alpaca-leg-sl"
    assert children[0].strategy_id == strategy.id


# ── Parked-remainder retry (strategy-less liquidation overshoot) ─────

_PARKED_AT = datetime(2026, 7, 2, 15, 30, tzinfo=UTC)


async def _events(db_session: AsyncSession, event_type: str) -> list[EventLog]:
    return list(
        (
            await db_session.execute(
                select(EventLog).where(EventLog.event_type == event_type).order_by(EventLog.id)
            )
        )
        .scalars()
        .all()
    )


async def _park_remainder(
    db_session: AsyncSession, portfolio: Portfolio, *, qty: str, price: str = "110"
) -> None:
    """A strategy-less (flatten) fill with nothing to close: parks its full qty."""
    application = await apply_fill_to_lots(
        db_session,
        portfolio_id=portfolio.id,
        strategy_id=None,
        symbol="SPY",
        side=OrderSide.sell,
        qty=Decimal(qty),
        price=Decimal(price),
        occurred_at=_PARKED_AT,
    )
    assert application.qty_closed == Decimal("0")
    assert application.unapplied_qty == Decimal(qty)
    await db_session.commit()


async def test_parked_remainder_retry_applies_when_lots_appear(
    db_session: AsyncSession, portfolio: Portfolio, seeded_user: User
) -> None:
    strategy = await seed_strategy(db_session, portfolio.id)
    await _park_remainder(db_session, portfolio, qty="50")

    # Cycle 1: still nothing to close — retry fails, no resolution, no lots.
    service = ReconciliationService()
    await service.reconcile_portfolio(
        db_session, portfolio.id, RecordingAdapter(), synthesize_fills=True
    )
    await db_session.commit()
    assert len(await _events(db_session, "lots.unapplied_remainder_retry_failed")) == 1
    assert await _events(db_session, "lots.unapplied_remainder_resolved") == []
    assert (await db_session.execute(select(Lot))).scalars().all() == []

    # The missing entry materializes (a later synthesis, here seeded directly).
    await seed_lot(
        db_session,
        portfolio.id,
        strategy.id,
        qty_orig=Decimal("50"),
        qty_open=Decimal("50"),
        entry_price=Decimal("100"),
        opened_at=datetime(2026, 7, 2, 15, 0, tzinfo=UTC),
    )
    await db_session.commit()

    await service.reconcile_portfolio(
        db_session, portfolio.id, RecordingAdapter(), synthesize_fills=True
    )
    await db_session.commit()

    lot = (await db_session.execute(select(Lot))).scalars().one()  # no phantom short
    assert lot.qty_open == Decimal("0")
    assert lot.closed_at == _PARKED_AT
    close = (await db_session.execute(select(LotClose))).scalars().one()
    assert close.strategy_id == strategy.id  # the LOT's strategy, not NULL
    assert close.qty == Decimal("50")
    assert close.realized_pnl == Decimal("500")  # (110 − 100) × 50

    anomaly = (await _events(db_session, UNAPPLIED_REMAINDER_EVENT))[0]
    resolved = await _events(db_session, "lots.unapplied_remainder_resolved")
    assert len(resolved) == 1
    assert resolved[0].payload["anomaly_event_id"] == anomaly.id
    assert Decimal(resolved[0].payload["applied_qty"]) == Decimal("50")
    assert resolved[0].payload["residual_qty"] == "0"

    # Resolved anomalies are not retried again: another cycle adds nothing.
    await service.reconcile_portfolio(
        db_session, portfolio.id, RecordingAdapter(), synthesize_fills=True
    )
    await db_session.commit()
    assert len(await _events(db_session, "lots.unapplied_remainder_resolved")) == 1
    assert len(await _events(db_session, "lots.unapplied_remainder_retry_failed")) == 1


async def test_unapplied_remainder_alerts_after_three_cycles(
    db_session: AsyncSession, portfolio: Portfolio, seeded_user: User
) -> None:
    await _park_remainder(db_session, portfolio, qty="25")
    service = ReconciliationService()

    for cycle in range(1, 5):
        await service.reconcile_portfolio(
            db_session, portfolio.id, RecordingAdapter(), synthesize_fills=True
        )
        await db_session.commit()
        failed = await _events(db_session, "lots.unapplied_remainder_retry_failed")
        alerts = await _events(db_session, "lots.unapplied_remainder_alert")
        assert len(failed) == cycle  # every failed cycle stays on the audit trail
        if cycle < 3:
            assert alerts == []
        else:
            assert len(alerts) == 1  # fires at cycle 3, once — never re-alerts

    alerts = await _events(db_session, "lots.unapplied_remainder_alert")
    assert alerts[0].payload["cycles"] == 3
    assert Decimal(alerts[0].payload["qty"]) == Decimal("25")


async def test_partial_remainder_retry_resolves_and_supersedes_with_residual(
    db_session: AsyncSession, portfolio: Portfolio, seeded_user: User
) -> None:
    """Partial application closes the original anomaly and parks a fresh, smaller
    one — an open anomaly's payload qty is always exactly what remains."""
    strategy = await seed_strategy(db_session, portfolio.id)
    await _park_remainder(db_session, portfolio, qty="50")
    await seed_lot(
        db_session,
        portfolio.id,
        strategy.id,
        qty_orig=Decimal("30"),
        qty_open=Decimal("30"),
        entry_price=Decimal("100"),
        opened_at=datetime(2026, 7, 2, 15, 0, tzinfo=UTC),
    )
    await db_session.commit()

    service = ReconciliationService()
    await service.reconcile_portfolio(
        db_session, portfolio.id, RecordingAdapter(), synthesize_fills=True
    )
    await db_session.commit()

    anomalies = await _events(db_session, UNAPPLIED_REMAINDER_EVENT)
    assert len(anomalies) == 2
    original, successor = anomalies
    assert Decimal(successor.payload["qty"]) == Decimal("20")
    assert successor.payload["superseded_anomaly_event_id"] == original.id
    resolved = await _events(db_session, "lots.unapplied_remainder_resolved")
    assert len(resolved) == 1
    assert resolved[0].payload["anomaly_event_id"] == original.id
    assert Decimal(resolved[0].payload["applied_qty"]) == Decimal("30")
    assert Decimal(resolved[0].payload["realized_pnl"]) == Decimal("300")
    assert Decimal(resolved[0].payload["residual_qty"]) == Decimal("20")

    # The rest of the entry appears; the residual heals on the next cycle.
    await seed_lot(
        db_session,
        portfolio.id,
        strategy.id,
        qty_orig=Decimal("20"),
        qty_open=Decimal("20"),
        entry_price=Decimal("105"),
        opened_at=datetime(2026, 7, 2, 15, 1, tzinfo=UTC),
    )
    await db_session.commit()
    await service.reconcile_portfolio(
        db_session, portfolio.id, RecordingAdapter(), synthesize_fills=True
    )
    await db_session.commit()

    resolved = await _events(db_session, "lots.unapplied_remainder_resolved")
    assert len(resolved) == 2
    assert resolved[1].payload["anomaly_event_id"] == successor.id
    assert resolved[1].payload["residual_qty"] == "0"
    closes = (
        (await db_session.execute(select(LotClose).order_by(LotClose.qty.desc()))).scalars().all()
    )
    assert [(c.qty, c.realized_pnl) for c in closes] == [
        (Decimal("30"), Decimal("300")),  # (110 − 100) × 30
        (Decimal("20"), Decimal("100")),  # (110 − 105) × 20
    ]
    assert all(c.strategy_id == strategy.id for c in closes)


async def test_interleaved_entry_after_liquidation_heals_end_to_end(
    db_session: AsyncSession, portfolio: Portfolio, seeded_user: User
) -> None:
    """The 2c race: the flatten liquidation's fill lands BEFORE the entry it is
    flattening is known locally. Park → the entry surfaces → one reconcile both
    synthesizes the entry lot (deficit sweep) and applies the parked remainder
    (retry pass). End state: lots closed, P&L attributed, no phantom lots."""
    strategy = await seed_strategy(db_session, portfolio.id)

    # 1) The liquidation order + its streamed fill (the writer's work): the
    # Fill row lands with real qty/price, but the lot application overshoots
    # everything known and parks in full.
    liq_order = await seed_order(
        db_session,
        portfolio.id,
        None,
        client_order_id="roigen-flatten-abcd1234-SPY",
        broker_order_id="bo-liq",
        side=OrderSide.sell,
        order_class=OrderClass.simple,
        status=OrderStatus.filled,
        qty=Decimal("100"),
        filled_qty=Decimal("100"),
    )
    liq_fill = Fill(
        order_id=liq_order.id,
        broker_fill_id="exec-liq",
        qty=Decimal("100"),
        price=Decimal("105"),
        occurred_at=_PARKED_AT,
    )
    db_session.add(liq_fill)
    await db_session.flush()
    application = await apply_fill_to_lots(
        db_session,
        portfolio_id=portfolio.id,
        strategy_id=None,
        symbol="SPY",
        side=OrderSide.sell,
        qty=Decimal("100"),
        price=Decimal("105"),
        occurred_at=_PARKED_AT,
        order_id=liq_order.id,
        fill_id=liq_fill.id,
    )
    assert application.unapplied_qty == Decimal("100")
    assert application.opened_lot_id is None  # no phantom short minted

    # 2) The entry order surfaces AFTERWARDS: filled at the broker, zero Fill
    # rows locally — a ledger deficit the sweep repairs from local columns.
    entry = await seed_order(
        db_session,
        portfolio.id,
        strategy.id,
        broker_order_id="bo-entry",
        side=OrderSide.buy,
        order_class=OrderClass.simple,
        status=OrderStatus.filled,
        qty=Decimal("100"),
        filled_qty=Decimal("100"),
    )
    entry.filled_avg_price = Decimal("100")
    await db_session.commit()

    result = await ReconciliationService().reconcile_portfolio(
        db_session, portfolio.id, RecordingAdapter(), synthesize_fills=True
    )
    await db_session.commit()

    assert result.fills_synthesized == 1  # the entry
    lot = (await db_session.execute(select(Lot))).scalars().one()  # ONLY the entry lot
    assert lot.strategy_id == strategy.id
    assert lot.side == OrderSide.buy.value
    assert lot.qty_open == Decimal("0")
    assert lot.closed_at == _PARKED_AT
    assert lot.realized_pnl == Decimal("500")  # (105 − 100) × 100

    close = (await db_session.execute(select(LotClose))).scalars().one()
    assert close.strategy_id == strategy.id  # breaker sees it on the strategy
    assert close.order_id == liq_order.id
    assert close.fill_id == liq_fill.id
    assert close.qty == Decimal("100")
    assert close.realized_pnl == Decimal("500")

    resolved = await _events(db_session, "lots.unapplied_remainder_resolved")
    assert len(resolved) == 1
    assert Decimal(resolved[0].payload["applied_qty"]) == Decimal("100")
    assert resolved[0].payload["residual_qty"] == "0"
    assert await _events(db_session, "lots.unapplied_remainder_retry_failed") == []
