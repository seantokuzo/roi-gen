"""ExecutionStage flatten path: cancel → confirm → liquidate, from broker truth.

Every liquidation reuses the 2b submit discipline wholesale (persist-before-
submit, resolve-by-client-id, never blind-resubmit), the halt gate suppresses
queued entries at submit time, and the flatten itself is single-flight and
replay-proof. Approvals are minted by the REAL risk engine — never forged.
"""

from __future__ import annotations

import asyncio
import uuid
from decimal import Decimal
from typing import TYPE_CHECKING, cast

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.brokers.errors import AmbiguousOrderState, OrderRejected
from app.engine.bus import EventBus
from app.engine.events import FlattenOrderEvent, OrderEvent
from app.engine.execution.handler import ExecutionStage
from app.engine.risk.engine import RiskEngine
from app.models.enums import OrderClass, OrderSide, OrderStatus, OrderType, TimeInForce
from app.models.telemetry import EventLog
from app.models.trading import Order
from tests.engine.builders import (
    FakeEngineAdapter,
    make_broker_order,
    make_limits,
    make_signal,
    make_state,
    seed_portfolio,
)
from tests.engine.flatten_helpers import get_events, make_broker_position, mint_flatten_approval

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from datetime import datetime

    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

    from app.brokers.dto import BrokerOrder, BrokerPosition, OrderRequest
    from app.engine.risk.approval import FlattenApproval
    from app.models.portfolio import Portfolio
    from app.models.user import User


class _FlattenAdapter(FakeEngineAdapter):
    """Allows + records the flatten path's broker calls, in order.

    The base class asserts on any mutation (risk tests must never reach the
    broker); the flatten suite is exactly the place mutations become legal, so
    this subclass opens them up while recording the full call choreography.
    ``submit_order`` also probes the DB so persist-before-submit is PROVEN,
    not assumed.
    """

    def __init__(
        self,
        db_engine: AsyncEngine,
        *,
        open_orders: Sequence[BrokerOrder] = (),
        positions: Sequence[BrokerPosition] = (),
        get_order_results: dict[str, BrokerOrder] | None = None,
        submit_errors: dict[str, Exception] | None = None,
        lookup_results: Sequence[BrokerOrder | None] = (),
        lookup_error: Exception | None = None,
    ) -> None:
        super().__init__()
        self._db_engine = db_engine
        self.open_orders = list(open_orders)
        self.positions = list(positions)
        self.get_order_results = dict(get_order_results or {})
        self.submit_errors = dict(submit_errors or {})  # keyed by symbol
        self.lookup_results = list(lookup_results)
        self.lookup_error = lookup_error
        self.calls: list[tuple[str, str]] = []
        self.canceled: list[str] = []
        self.submitted: list[OrderRequest] = []
        self.lookups: list[str] = []
        self.row_status_at_submit: dict[str, str | None] = {}
        self.list_orders_started: asyncio.Event | None = None
        self.list_orders_release: asyncio.Event | None = None

    async def list_orders(
        self,
        *,
        status: str = "open",
        after: datetime | None = None,
        until: datetime | None = None,
        limit: int = 100,
        nested: bool = True,
    ) -> list[BrokerOrder]:
        self.calls.append(("list_orders", status))
        if self.list_orders_started is not None:
            self.list_orders_started.set()
        if self.list_orders_release is not None:
            await self.list_orders_release.wait()
        return list(self.open_orders)

    async def cancel_order(self, broker_order_id: str) -> None:
        self.calls.append(("cancel_order", broker_order_id))
        self.canceled.append(broker_order_id)

    async def get_order(self, broker_order_id: str) -> BrokerOrder | None:
        self.calls.append(("get_order", broker_order_id))
        return self.get_order_results.get(broker_order_id)

    async def list_positions(self) -> list[BrokerPosition]:
        self.calls.append(("list_positions", ""))
        return list(self.positions)

    async def get_order_by_client_id(self, client_order_id: str) -> BrokerOrder | None:
        self.calls.append(("lookup", client_order_id))
        self.lookups.append(client_order_id)
        if self.lookup_error is not None:
            raise self.lookup_error
        if self.lookup_results:
            return self.lookup_results.pop(0)
        return None

    async def submit_order(self, req: OrderRequest) -> BrokerOrder:
        self.calls.append(("submit_order", req.client_order_id))
        self.submitted.append(req)
        # Persist-before-submit probe: the recovery-anchor row must already be
        # committed by the time the broker hears about the order.
        factory = async_sessionmaker(self._db_engine, expire_on_commit=False)
        async with factory() as session:
            row = (
                await session.execute(
                    select(Order).where(Order.client_order_id == req.client_order_id)
                )
            ).scalar_one_or_none()
        self.row_status_at_submit[req.client_order_id] = None if row is None else row.status
        error = self.submit_errors.get(req.symbol)
        if error is not None:
            raise error
        return make_broker_order(
            broker_order_id=f"bo-{req.symbol.lower()}",
            client_order_id=req.client_order_id,
            symbol=req.symbol,
            side=req.side,
            order_type=req.order_type,
            order_class=req.order_class,
            time_in_force=req.time_in_force,
            qty=req.qty,
        )


def _stage(
    db_engine: AsyncEngine,
    adapter: _FlattenAdapter,
    *,
    halted: Callable[[], bool] | None = None,
) -> ExecutionStage:
    return ExecutionStage(
        bus=EventBus(),
        session_factory=async_sessionmaker(db_engine, expire_on_commit=False),
        adapter=adapter,
        resolve_delays=(0.0, 0.0),  # no real sleeps in tests
        cancel_confirm_delays=(0.0,) * 3,
        halted=halted,
    )


async def _drive(stage: ExecutionStage, approval: FlattenApproval) -> None:
    """Deliver a FlattenOrderEvent and wait for the spawned drive to finish."""
    await stage._on_flatten(FlattenOrderEvent(approval=approval))
    tasks = list(stage._flatten_tasks)
    if tasks:
        await asyncio.gather(*tasks)


async def _seeded_portfolio(db_session: AsyncSession, seeded_user: User) -> Portfolio:
    portfolio = await seed_portfolio(db_session, seeded_user.id)
    await db_session.commit()  # the stage runs on its own sessions
    return portfolio


async def _order_row(db_engine: AsyncEngine, client_order_id: str) -> Order:
    factory = async_sessionmaker(db_engine, expire_on_commit=False)
    async with factory() as session:
        return (
            await session.execute(select(Order).where(Order.client_order_id == client_order_id))
        ).scalar_one()


async def _all_order_rows(db_engine: AsyncEngine) -> list[Order]:
    factory = async_sessionmaker(db_engine, expire_on_commit=False)
    async with factory() as session:
        return list((await session.execute(select(Order))).scalars().all())


# ── The halt gate at the submit boundary ─────────────────────────────


async def test_halted_engine_suppresses_an_approved_entry_at_submit_time(
    db_engine: AsyncEngine,
) -> None:
    # A signal approved a heartbeat before the kill switch flipped queues an
    # OrderEvent behind the flatten — it must die HERE, not at the broker.
    signal = make_signal()
    decision = RiskEngine(make_limits()).evaluate(signal, make_state())
    assert decision.approval is not None and decision.order_request is not None
    event = OrderEvent(order_request=decision.order_request, approval=decision.approval)

    adapter = _FlattenAdapter(db_engine)
    stage = _stage(db_engine, adapter, halted=lambda: True)
    await stage._on_order(event)

    assert adapter.calls == []  # the broker never heard about it
    assert await _all_order_rows(db_engine) == []  # no pending_submit row either
    rows = await get_events(db_engine, "order.suppressed_halted")
    assert len(rows) == 1
    assert rows[0].payload["client_order_id"] == decision.approval.client_order_id
    assert rows[0].payload["symbol"] == signal.symbol


# ── FlattenOrderEvent verification + dedup ───────────────────────────


class _ImpostorApproval:
    """Duck-typed forgery: right attributes, never minted."""

    def __init__(self) -> None:
        self.approval_id = uuid.uuid4()
        self.flatten_id = uuid.uuid4()
        self.portfolio_id = uuid.uuid4()
        self.reason = "forged"
        self.source = "kill_switch"
        self.command_seq: int | None = None


async def test_impostor_flatten_approval_is_ignored(db_engine: AsyncEngine) -> None:
    adapter = _FlattenAdapter(db_engine, positions=[make_broker_position()])
    stage = _stage(db_engine, adapter)

    impostor = cast("FlattenApproval", _ImpostorApproval())
    await stage._on_flatten(FlattenOrderEvent(approval=impostor))

    assert stage._flatten_tasks == set()  # nothing spawned
    assert stage._seen_flatten_ids == set()  # not even remembered
    assert adapter.calls == []
    factory = async_sessionmaker(db_engine, expire_on_commit=False)
    async with factory() as session:
        assert (await session.execute(select(EventLog))).scalars().all() == []


async def test_replayed_flatten_id_is_an_audited_no_op(
    db_engine: AsyncEngine, db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio = await _seeded_portfolio(db_session, seeded_user)
    _event, approval = mint_flatten_approval(portfolio.id)
    adapter = _FlattenAdapter(
        db_engine, positions=[make_broker_position("AAPL", qty=Decimal("10"))]
    )
    stage = _stage(db_engine, adapter)

    await _drive(stage, approval)
    await stage._on_flatten(FlattenOrderEvent(approval=approval))  # replay

    assert len(adapter.submitted) == 1  # executed exactly once
    duplicates = await get_events(db_engine, "flatten.duplicate_ignored")
    assert len(duplicates) == 1
    assert duplicates[0].payload["flatten_id"] == str(approval.flatten_id)


# ── The flatten pass itself ──────────────────────────────────────────


async def test_flatten_cancels_confirms_then_liquidates_from_broker_truth(
    db_engine: AsyncEngine, db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio = await _seeded_portfolio(db_session, seeded_user)
    _event, approval = mint_flatten_approval(portfolio.id, command_seq=42)

    parent = make_broker_order(broker_order_id="bo-parent", client_order_id="roigen-entry-1")
    leg = make_broker_order(
        broker_order_id="bo-leg",
        client_order_id="alpaca-leg-sl",
        side=OrderSide.sell,
        order_type=OrderType.stop,
    )
    prior_liquidation = make_broker_order(
        broker_order_id="bo-old-liq",
        client_order_id="roigen-flatten-deadbeefcafe-aapl",
        side=OrderSide.sell,
        order_class=OrderClass.simple,
    )
    adapter = _FlattenAdapter(
        db_engine,
        open_orders=[parent, leg, prior_liquidation],
        positions=[
            make_broker_position("AAPL", qty=Decimal("10")),
            make_broker_position("TSLA", qty=Decimal("-5")),
        ],
        get_order_results={
            "bo-parent": make_broker_order(
                broker_order_id="bo-parent", status=OrderStatus.canceled
            ),
            "bo-leg": make_broker_order(broker_order_id="bo-leg", status=OrderStatus.canceled),
        },
    )
    stage = _stage(db_engine, adapter)
    await _drive(stage, approval)

    # Cancels every working order EXCEPT a prior drive's own liquidation —
    # canceling that would restart its fill clock and prevent completion.
    assert adapter.canceled == ["bo-parent", "bo-leg"]
    assert "bo-old-liq" not in adapter.canceled

    # Positions are read fresh AFTER the cancels settled (a stop leg may have
    # shrunk the position during the confirmation window).
    names = [name for name, _ in adapter.calls]
    last_cancel = max(i for i, name in enumerate(names) if name == "cancel_order")
    assert names.index("list_positions") > last_cancel

    # One reduce-direction market liquidation per symbol, sized off broker qty.
    assert [(r.symbol, r.side, r.position_intent, r.qty) for r in adapter.submitted] == [
        ("AAPL", OrderSide.sell, "sell_to_close", Decimal("10")),
        ("TSLA", OrderSide.buy, "buy_to_close", Decimal("5")),
    ]
    assert all(r.order_type is OrderType.market for r in adapter.submitted)
    assert all(r.time_in_force is TimeInForce.day for r in adapter.submitted)
    prefix = f"roigen-flatten-{approval.flatten_id.hex[:12]}"
    client_ids = [r.client_order_id for r in adapter.submitted]
    assert client_ids == [f"{prefix}-aapl", f"{prefix}-tsla"]

    # Persist-before-submit held for BOTH liquidations (probed at submit time).
    assert adapter.row_status_at_submit == {
        client_ids[0]: OrderStatus.pending_submit.value,
        client_ids[1]: OrderStatus.pending_submit.value,
    }

    # The rows are strategy-less and carry the flatten approval as audit.
    for client_id in client_ids:
        row = await _order_row(db_engine, client_id)
        assert row.strategy_id is None
        assert row.portfolio_id == portfolio.id
        assert row.status == OrderStatus.accepted.value
        assert row.risk_approval is not None
        assert row.risk_approval["flatten_id"] == str(approval.flatten_id)
        assert row.risk_approval["command_seq"] == 42

    executed = await get_events(db_engine, "flatten.executed")
    assert len(executed) == 1
    assert executed[0].payload["command_seq"] == 42
    outcomes = executed[0].payload["outcomes"]
    assert [(o["symbol"], o["outcome"]) for o in outcomes] == [
        ("AAPL", "submitted"),
        ("TSLA", "submitted"),
    ]
    assert await get_events(db_engine, "flatten.partial") == []


async def test_a_filled_leg_counts_as_cancel_settled(
    db_engine: AsyncEngine, db_session: AsyncSession, seeded_user: User
) -> None:
    # The stop leg WON the race and filled during cancellation: that is a
    # success (the position shrank) and must settle the wait immediately.
    portfolio = await _seeded_portfolio(db_session, seeded_user)
    _event, approval = mint_flatten_approval(portfolio.id)
    adapter = _FlattenAdapter(
        db_engine,
        open_orders=[
            make_broker_order(broker_order_id="bo-a", client_order_id="roigen-entry-a"),
            make_broker_order(broker_order_id="bo-b", client_order_id="roigen-entry-b"),
        ],
        positions=[make_broker_position("SPY", qty=Decimal("10"))],
        get_order_results={
            "bo-a": make_broker_order(broker_order_id="bo-a", status=OrderStatus.filled),
            "bo-b": make_broker_order(broker_order_id="bo-b", status=OrderStatus.canceled),
        },
    )
    stage = _stage(db_engine, adapter)
    await _drive(stage, approval)

    # Both settled on the FIRST confirmation pass — one poll each, no re-polls.
    polls = [target for name, target in adapter.calls if name == "get_order"]
    assert sorted(polls) == ["bo-a", "bo-b"]
    assert len(adapter.submitted) == 1  # and the liquidation went out


async def test_an_unsettled_cancel_does_not_block_the_liquidation(
    db_engine: AsyncEngine, db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio = await _seeded_portfolio(db_session, seeded_user)
    _event, approval = mint_flatten_approval(portfolio.id)
    adapter = _FlattenAdapter(
        db_engine,
        open_orders=[
            make_broker_order(broker_order_id="bo-stuck", client_order_id="roigen-entry-stuck")
        ],
        positions=[make_broker_position("AAPL", qty=Decimal("10"))],
        get_order_results={
            # Never reaches a terminal state within the poll budget.
            "bo-stuck": make_broker_order(broker_order_id="bo-stuck", status=OrderStatus.accepted)
        },
    )
    stage = _stage(db_engine, adapter)
    await _drive(stage, approval)

    polls = [target for name, target in adapter.calls if name == "get_order"]
    assert polls == ["bo-stuck"] * 3  # the full (bounded) budget was spent
    assert len(adapter.submitted) == 1  # then the flatten proceeded anyway
    assert adapter.submitted[0].symbol == "AAPL"
    assert len(await get_events(db_engine, "flatten.executed")) == 1


async def test_one_symbols_broker_rejection_never_blocks_the_rest(
    db_engine: AsyncEngine, db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio = await _seeded_portfolio(db_session, seeded_user)
    _event, approval = mint_flatten_approval(portfolio.id)
    adapter = _FlattenAdapter(
        db_engine,
        positions=[
            make_broker_position("AAPL", qty=Decimal("10")),
            make_broker_position("MSFT", qty=Decimal("3")),
        ],
        submit_errors={"AAPL": OrderRejected("insufficient qty available")},
    )
    stage = _stage(db_engine, adapter)
    await _drive(stage, approval)

    # Both were attempted; the rejection was isolated to its symbol.
    assert [r.symbol for r in adapter.submitted] == ["AAPL", "MSFT"]
    prefix = f"roigen-flatten-{approval.flatten_id.hex[:12]}"
    assert (await _order_row(db_engine, f"{prefix}-aapl")).status == OrderStatus.rejected.value
    assert (await _order_row(db_engine, f"{prefix}-msft")).status == OrderStatus.accepted.value

    rejected = await get_events(db_engine, "order.rejected_by_broker")
    assert len(rejected) == 1
    assert "insufficient qty" in rejected[0].payload["error"]

    # The summary reports each ROW's truth, not "we tried": the rejected
    # liquidation makes this pass flatten.partial, and the controller re-drives
    # the leftover exposure on its next tick.
    partial = await get_events(db_engine, "flatten.partial")
    assert len(partial) == 1
    outcomes = {o["symbol"]: o["outcome"] for o in partial[0].payload["outcomes"]}
    assert outcomes == {"AAPL": "rejected", "MSFT": "submitted"}
    assert await get_events(db_engine, "flatten.executed") == []


async def test_a_total_broker_failure_for_one_symbol_reports_flatten_partial(
    db_engine: AsyncEngine, db_session: AsyncSession, seeded_user: User
) -> None:
    # AAPL's submit times out AND the resolve lookup errors too (broker fully
    # unreachable for that symbol): outcome failed, MSFT still closes, and the
    # summary is flatten.partial with per-symbol outcomes.
    portfolio = await _seeded_portfolio(db_session, seeded_user)
    _event, approval = mint_flatten_approval(portfolio.id)
    adapter = _FlattenAdapter(
        db_engine,
        positions=[
            make_broker_position("AAPL", qty=Decimal("10")),
            make_broker_position("MSFT", qty=Decimal("3")),
        ],
        submit_errors={"AAPL": AmbiguousOrderState("gateway 504 mid-submit")},
        lookup_error=RuntimeError("broker unreachable"),
    )
    stage = _stage(db_engine, adapter)
    await _drive(stage, approval)

    assert [r.symbol for r in adapter.submitted] == ["AAPL", "MSFT"]
    prefix = f"roigen-flatten-{approval.flatten_id.hex[:12]}"
    assert (await _order_row(db_engine, f"{prefix}-msft")).status == OrderStatus.accepted.value

    partial = await get_events(db_engine, "flatten.partial")
    assert len(partial) == 1
    outcomes = {o["symbol"]: o["outcome"] for o in partial[0].payload["outcomes"]}
    assert outcomes == {"AAPL": "failed", "MSFT": "submitted"}
    assert await get_events(db_engine, "flatten.executed") == []


async def test_ambiguous_liquidation_submit_resolves_by_client_id_never_resubmits(
    db_engine: AsyncEngine, db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio = await _seeded_portfolio(db_session, seeded_user)
    _event, approval = mint_flatten_approval(portfolio.id)
    client_id = f"roigen-flatten-{approval.flatten_id.hex[:12]}-aapl"
    found = make_broker_order(
        broker_order_id="bo-liq-found",
        client_order_id=client_id,
        symbol="AAPL",
        side=OrderSide.sell,
        order_class=OrderClass.simple,
        status=OrderStatus.accepted,
        qty=Decimal("10"),
    )
    adapter = _FlattenAdapter(
        db_engine,
        positions=[make_broker_position("AAPL", qty=Decimal("10"))],
        submit_errors={"AAPL": AmbiguousOrderState("timeout mid-submit")},
        lookup_results=[found],
    )
    stage = _stage(db_engine, adapter)
    await _drive(stage, approval)

    assert len(adapter.submitted) == 1  # THE critical assertion: never a second submit
    assert adapter.lookups == [client_id]
    row = await _order_row(db_engine, client_id)
    assert row.broker_order_id == "bo-liq-found"  # adopted broker truth
    assert row.status == OrderStatus.accepted.value
    assert len(await get_events(db_engine, "order.submit_ambiguous_resolved")) == 1

    executed = await get_events(db_engine, "flatten.executed")
    assert len(executed) == 1  # resolved ≠ failed


async def test_unresolved_ambiguous_liquidation_reports_outcome_ambiguous(
    db_engine: AsyncEngine, db_session: AsyncSession, seeded_user: User
) -> None:
    # The submit dies mid-flight and every resolve lookup comes back empty: the
    # order's fate is genuinely unknown. The row stays pending_submit for
    # reconciliation to age out or adopt, and the summary says so — "ambiguous"
    # is neither a success nor a proven failure.
    portfolio = await _seeded_portfolio(db_session, seeded_user)
    _event, approval = mint_flatten_approval(portfolio.id)
    adapter = _FlattenAdapter(
        db_engine,
        positions=[make_broker_position("AAPL", qty=Decimal("10"))],
        submit_errors={"AAPL": AmbiguousOrderState("gateway timeout mid-submit")},
        lookup_results=[None, None],  # both resolve attempts find nothing
    )
    stage = _stage(db_engine, adapter)
    await _drive(stage, approval)

    assert len(adapter.submitted) == 1  # never blind-resubmitted
    client_id = f"roigen-flatten-{approval.flatten_id.hex[:12]}-aapl"
    assert (await _order_row(db_engine, client_id)).status == OrderStatus.pending_submit.value

    partial = await get_events(db_engine, "flatten.partial")
    assert len(partial) == 1
    outcomes = {o["symbol"]: o["outcome"] for o in partial[0].payload["outcomes"]}
    assert outcomes == {"AAPL": "ambiguous"}
    assert await get_events(db_engine, "flatten.executed") == []
    assert len(await get_events(db_engine, "order.submit_ambiguous")) == 1


async def test_covered_symbol_is_not_liquidated_twice(
    db_engine: AsyncEngine, db_session: AsyncSession, seeded_user: User
) -> None:
    # SPY already has a working liquidation from a prior drive: this pass must
    # not cancel it, must not submit a second SPY liquidation (each re-drive
    # tick would otherwise mint a held-qty rejection until the first fills),
    # and must still close QQQ.
    portfolio = await _seeded_portfolio(db_session, seeded_user)
    _event, approval = mint_flatten_approval(portfolio.id)
    prior_liq = make_broker_order(
        broker_order_id="bo-prior-liq",
        client_order_id="roigen-flatten-deadbeefcafe-spy",
        symbol="SPY",
        side=OrderSide.sell,
        order_class=OrderClass.simple,
    )
    adapter = _FlattenAdapter(
        db_engine,
        open_orders=[prior_liq],
        positions=[
            make_broker_position("SPY", qty=Decimal("10")),
            make_broker_position("QQQ", qty=Decimal("5")),
        ],
    )
    stage = _stage(db_engine, adapter)
    await _drive(stage, approval)

    assert adapter.canceled == []  # its own kind is never canceled
    assert [r.symbol for r in adapter.submitted] == ["QQQ"]  # no second SPY order

    executed = await get_events(db_engine, "flatten.executed")
    assert len(executed) == 1
    outcomes = {o["symbol"]: (o["outcome"], o["detail"]) for o in executed[0].payload["outcomes"]}
    assert outcomes["QQQ"][0] == "submitted"
    assert outcomes["SPY"][0] == "submitted"  # in flight counts as submitted…
    assert "covered" in outcomes["SPY"][1]  # …with the covered provenance
    assert await get_events(db_engine, "flatten.partial") == []


async def test_zero_qty_position_is_skipped_entirely(
    db_engine: AsyncEngine, db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio = await _seeded_portfolio(db_session, seeded_user)
    _event, approval = mint_flatten_approval(portfolio.id)
    adapter = _FlattenAdapter(
        db_engine,
        positions=[
            make_broker_position("AAPL", qty=Decimal("0")),
            make_broker_position("MSFT", qty=Decimal("3")),
        ],
    )
    stage = _stage(db_engine, adapter)
    await _drive(stage, approval)

    assert [r.symbol for r in adapter.submitted] == ["MSFT"]
    executed = await get_events(db_engine, "flatten.executed")
    assert len(executed) == 1
    assert [o["symbol"] for o in executed[0].payload["outcomes"]] == ["MSFT"]


async def test_racing_flattens_coalesce_into_a_single_drive(
    db_engine: AsyncEngine, db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio = await _seeded_portfolio(db_session, seeded_user)
    _event1, first = mint_flatten_approval(portfolio.id, source="kill_switch")
    _event2, second = mint_flatten_approval(portfolio.id, source="scheduled_close")

    adapter = _FlattenAdapter(
        db_engine, positions=[make_broker_position("AAPL", qty=Decimal("10"))]
    )
    started = asyncio.Event()
    release = asyncio.Event()
    adapter.list_orders_started = started
    adapter.list_orders_release = release
    stage = _stage(db_engine, adapter)

    task_first = asyncio.create_task(stage._run_flatten(first))
    await asyncio.wait_for(started.wait(), timeout=2.0)  # first drive holds the lock

    task_second = asyncio.create_task(stage._run_flatten(second))
    await asyncio.wait_for(task_second, timeout=2.0)  # coalesces without waiting

    coalesced = await get_events(db_engine, "flatten.coalesced")
    assert len(coalesced) == 1
    assert coalesced[0].payload["flatten_id"] == str(second.flatten_id)
    assert adapter.submitted == []  # the second drive touched nothing

    release.set()
    await asyncio.wait_for(task_first, timeout=2.0)

    executed = await get_events(db_engine, "flatten.executed")
    assert len(executed) == 1
    assert executed[0].payload["flatten_id"] == str(first.flatten_id)
    assert len(adapter.submitted) == 1  # exactly one drive liquidated
