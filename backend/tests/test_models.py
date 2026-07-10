"""Model round-trips, uniqueness constraints, cascade, and Decimal precision."""

import uuid
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy import delete, select
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    BrokerCredential,
    EngineCommand,
    EngineCommandAction,
    EquitySnapshot,
    EventLog,
    EventSource,
    Fill,
    KillState,
    Lot,
    Order,
    OrderClass,
    OrderSide,
    OrderStatus,
    OrderType,
    Portfolio,
    PortfolioMode,
    Position,
    Strategy,
    StrategyStatus,
    TimeInForce,
    User,
    derive_kill_state,
)
from app.models.engine_command import RESULT_FLAT_VERIFIED
from tests.conftest import TEST_EMAIL


async def _make_portfolio(session: AsyncSession, user: User, name: str = "alpha") -> Portfolio:
    portfolio = Portfolio(user_id=user.id, name=name, mode=PortfolioMode.paper)
    session.add(portfolio)
    await session.commit()
    await session.refresh(portfolio)
    return portfolio


def _make_order(portfolio: Portfolio, **overrides: object) -> Order:
    fields: dict[str, object] = {
        "client_order_id": f"coid-{uuid.uuid4()}",
        "portfolio_id": portfolio.id,
        "symbol": "AAPL",
        "side": OrderSide.buy,
        "order_type": OrderType.limit,
        "order_class": OrderClass.simple,
        "time_in_force": TimeInForce.day,
        "qty": Decimal("10"),
        "limit_price": Decimal("190.25"),
    }
    fields.update(overrides)
    return Order(**fields)


# ── Round trips ──────────────────────────────────────────────────


async def test_user_round_trip(db_session: AsyncSession, seeded_user: User) -> None:
    row = (await db_session.execute(select(User).where(User.email == TEST_EMAIL))).scalar_one()
    assert row.id == seeded_user.id
    assert row.display_name == "Test User"
    assert row.created_at.tzinfo is not None
    assert row.updated_at.tzinfo is not None


async def test_portfolio_round_trip(db_session: AsyncSession, seeded_user: User) -> None:
    portfolio = await _make_portfolio(db_session, seeded_user)
    row = (
        await db_session.execute(select(Portfolio).where(Portfolio.id == portfolio.id))
    ).scalar_one()
    assert row.user_id == seeded_user.id
    assert row.mode == PortfolioMode.paper
    assert row.is_default is False
    assert row.description is None


async def test_broker_credential_round_trip(db_session: AsyncSession, seeded_user: User) -> None:
    portfolio = await _make_portfolio(db_session, seeded_user)
    cred = BrokerCredential(
        portfolio_id=portfolio.id,
        api_key_encrypted="enc-key",
        api_secret_encrypted="enc-secret",
        paper=True,
    )
    db_session.add(cred)
    await db_session.commit()

    row = (
        await db_session.execute(
            select(BrokerCredential).where(BrokerCredential.portfolio_id == portfolio.id)
        )
    ).scalar_one()
    assert row.broker == "alpaca"
    assert row.paper is True
    assert row.api_key_encrypted == "enc-key"


async def test_strategy_round_trip(db_session: AsyncSession, seeded_user: User) -> None:
    portfolio = await _make_portfolio(db_session, seeded_user)
    strategy = Strategy(
        portfolio_id=portfolio.id,
        name="orb-5m",
        kind="opening_range_breakout",
        params={"window_minutes": 5, "rvol_min": 1.5},
        symbols=["AAPL", "TSLA"],
        risk_per_trade_pct=Decimal("0.500"),
        max_positions=3,
    )
    db_session.add(strategy)
    await db_session.commit()

    row = (
        await db_session.execute(select(Strategy).where(Strategy.id == strategy.id))
    ).scalar_one()
    assert row.status == StrategyStatus.draft
    assert row.params == {"window_minutes": 5, "rvol_min": 1.5}
    assert row.symbols == ["AAPL", "TSLA"]
    assert row.risk_per_trade_pct == Decimal("0.500")
    assert row.max_positions == 3


async def test_order_round_trip(db_session: AsyncSession, seeded_user: User) -> None:
    portfolio = await _make_portfolio(db_session, seeded_user)
    order = _make_order(portfolio, risk_approval={"approved": True, "token": "rk-1"})
    db_session.add(order)
    await db_session.commit()

    row = (await db_session.execute(select(Order).where(Order.id == order.id))).scalar_one()
    assert row.status == OrderStatus.pending_submit
    assert row.filled_qty == Decimal("0")
    assert row.extended_hours is False
    assert row.risk_approval == {"approved": True, "token": "rk-1"}
    assert row.broker_order_id is None
    assert row.parent_order_id is None


async def test_fill_round_trip(db_session: AsyncSession, seeded_user: User) -> None:
    portfolio = await _make_portfolio(db_session, seeded_user)
    order = _make_order(portfolio)
    db_session.add(order)
    await db_session.commit()

    occurred = datetime(2026, 6, 10, 14, 30, 0, tzinfo=UTC)
    fill = Fill(
        order_id=order.id,
        broker_fill_id="bf-1",
        qty=Decimal("10"),
        price=Decimal("190.20"),
        occurred_at=occurred,
        raw={"event": "fill"},
    )
    db_session.add(fill)
    await db_session.commit()

    row = (await db_session.execute(select(Fill).where(Fill.order_id == order.id))).scalar_one()
    assert row.price == Decimal("190.20")
    assert row.occurred_at == occurred
    assert row.occurred_at.tzinfo is not None


async def test_position_round_trip(db_session: AsyncSession, seeded_user: User) -> None:
    portfolio = await _make_portfolio(db_session, seeded_user)
    position = Position(
        portfolio_id=portfolio.id,
        symbol="AAPL",
        qty=Decimal("10"),
        avg_entry_price=Decimal("190.20"),
    )
    db_session.add(position)
    await db_session.commit()

    row = (
        await db_session.execute(select(Position).where(Position.portfolio_id == portfolio.id))
    ).scalar_one()
    assert row.symbol == "AAPL"
    assert row.qty == Decimal("10")


async def test_lot_round_trip(db_session: AsyncSession, seeded_user: User) -> None:
    portfolio = await _make_portfolio(db_session, seeded_user)
    opened = datetime(2026, 6, 10, 14, 31, 0, tzinfo=UTC)
    lot = Lot(
        portfolio_id=portfolio.id,
        symbol="TSLA",
        side=OrderSide.buy,
        qty_orig=Decimal("5"),
        qty_open=Decimal("5"),
        entry_price=Decimal("250.10"),
        opened_at=opened,
    )
    db_session.add(lot)
    await db_session.commit()

    row = (
        await db_session.execute(select(Lot).where(Lot.portfolio_id == portfolio.id))
    ).scalar_one()
    assert row.side == OrderSide.buy
    assert row.realized_pnl == Decimal("0")
    assert row.closed_at is None
    assert row.opened_at == opened


async def test_equity_snapshot_round_trip(db_session: AsyncSession, seeded_user: User) -> None:
    portfolio = await _make_portfolio(db_session, seeded_user)
    ts = datetime(2026, 6, 10, 20, 0, 0, tzinfo=UTC)
    snapshot = EquitySnapshot(
        portfolio_id=portfolio.id,
        equity=Decimal("100000.50"),
        cash=Decimal("25000.25"),
        buying_power=Decimal("200001.00"),
        ts=ts,
    )
    db_session.add(snapshot)
    await db_session.commit()

    row = (
        await db_session.execute(
            select(EquitySnapshot).where(EquitySnapshot.portfolio_id == portfolio.id)
        )
    ).scalar_one()
    assert row.equity == Decimal("100000.50")
    assert row.ts == ts


async def test_event_log_round_trip(db_session: AsyncSession) -> None:
    event = EventLog(
        source=EventSource.engine,
        event_type="order.submitted",
        portfolio_id=uuid.uuid4(),  # plain UUID — no FK on event_log by design
        payload={"detail": "test"},
    )
    db_session.add(event)
    await db_session.commit()
    await db_session.refresh(event)

    assert isinstance(event.id, int)  # BigInteger autoincrement pk
    assert event.ts.tzinfo is not None  # server-defaulted timestamptz
    row = (
        await db_session.execute(select(EventLog).where(EventLog.event_type == "order.submitted"))
    ).scalar_one()
    assert row.source == EventSource.engine
    assert row.payload == {"detail": "test"}


# ── Uniqueness constraints ───────────────────────────────────────


async def test_client_order_id_unique(db_session: AsyncSession, seeded_user: User) -> None:
    portfolio = await _make_portfolio(db_session, seeded_user)
    order = _make_order(portfolio, client_order_id="dup-coid")
    db_session.add(order)
    await db_session.commit()

    db_session.add(_make_order(portfolio, client_order_id="dup-coid"))
    with pytest.raises(IntegrityError):
        await db_session.flush()
    await db_session.rollback()


async def test_position_unique_per_portfolio_symbol(
    db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio = await _make_portfolio(db_session, seeded_user)
    db_session.add(
        Position(
            portfolio_id=portfolio.id,
            symbol="AAPL",
            qty=Decimal("1"),
            avg_entry_price=Decimal("190"),
        )
    )
    await db_session.commit()

    db_session.add(
        Position(
            portfolio_id=portfolio.id,
            symbol="AAPL",
            qty=Decimal("2"),
            avg_entry_price=Decimal("191"),
        )
    )
    with pytest.raises(IntegrityError):
        await db_session.flush()
    await db_session.rollback()


async def test_portfolio_name_unique_per_user(db_session: AsyncSession, seeded_user: User) -> None:
    await _make_portfolio(db_session, seeded_user, name="alpha")

    db_session.add(Portfolio(user_id=seeded_user.id, name="alpha", mode=PortfolioMode.live))
    with pytest.raises(IntegrityError):
        await db_session.flush()
    await db_session.rollback()


async def test_only_one_default_portfolio_per_user(
    db_session: AsyncSession, seeded_user: User
) -> None:
    db_session.add(
        Portfolio(user_id=seeded_user.id, name="alpha", mode=PortfolioMode.paper, is_default=True)
    )
    await db_session.commit()

    db_session.add(
        Portfolio(user_id=seeded_user.id, name="beta", mode=PortfolioMode.paper, is_default=True)
    )
    with pytest.raises(IntegrityError):
        await db_session.flush()
    await db_session.rollback()


async def test_broker_fill_id_unique_when_present(
    db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio = await _make_portfolio(db_session, seeded_user)
    order = _make_order(portfolio)
    db_session.add(order)
    await db_session.commit()
    occurred = datetime(2026, 6, 10, 14, 30, 0, tzinfo=UTC)

    def _fill(broker_fill_id: str | None) -> Fill:
        return Fill(
            order_id=order.id,
            broker_fill_id=broker_fill_id,
            qty=Decimal("1"),
            price=Decimal("190.20"),
            occurred_at=occurred,
        )

    # NULL broker_fill_ids are exempt from the partial unique index.
    db_session.add_all([_fill(None), _fill(None), _fill("bf-dup")])
    await db_session.commit()

    db_session.add(_fill("bf-dup"))
    with pytest.raises(IntegrityError):
        await db_session.flush()
    await db_session.rollback()


@pytest.mark.parametrize(
    ("qty_orig", "qty_open"),
    [
        (Decimal("0"), Decimal("0")),  # ck_lots_qty_orig_positive
        (Decimal("5"), Decimal("-1")),  # ck_lots_qty_open_nonneg
        (Decimal("5"), Decimal("6")),  # ck_lots_qty_open_le_orig
    ],
)
async def test_lot_quantity_bounds_enforced(
    db_session: AsyncSession, seeded_user: User, qty_orig: Decimal, qty_open: Decimal
) -> None:
    portfolio = await _make_portfolio(db_session, seeded_user)
    db_session.add(
        Lot(
            portfolio_id=portfolio.id,
            symbol="TSLA",
            side=OrderSide.buy,
            qty_orig=qty_orig,
            qty_open=qty_open,
            entry_price=Decimal("250.10"),
            opened_at=datetime(2026, 6, 10, 14, 31, 0, tzinfo=UTC),
        )
    )
    with pytest.raises(IntegrityError):
        await db_session.flush()
    await db_session.rollback()


# ── Engine command log ───────────────────────────────────────────


def _make_command(**overrides: object) -> EngineCommand:
    fields: dict[str, object] = {
        "action": EngineCommandAction.halt,
        "reason": "unit test",
        "actor": "cli:test",
    }
    fields.update(overrides)
    return EngineCommand(**fields)


async def test_engine_command_round_trip(db_session: AsyncSession) -> None:
    command = _make_command(action=EngineCommandAction.flatten, reason="drawdown breach")
    db_session.add(command)
    await db_session.commit()
    await db_session.refresh(command)

    row = (
        await db_session.execute(select(EngineCommand).where(EngineCommand.id == command.id))
    ).scalar_one()
    assert row.action == EngineCommandAction.flatten
    assert row.scope == "global"
    assert row.reason == "drawdown breach"
    assert row.actor == "cli:test"
    assert isinstance(row.seq, int)  # DB-assigned identity
    assert row.issued_at.tzinfo is not None  # server-defaulted timestamptz
    assert row.applied_at is None  # not yet picked up by the sweep
    assert row.result is None  # outcome written on verified completion only


async def test_engine_command_seq_monotonic_across_inserts(db_session: AsyncSession) -> None:
    seqs: list[int] = []
    actions = (EngineCommandAction.halt, EngineCommandAction.flatten, EngineCommandAction.resume)
    for action in actions:
        command = _make_command(action=action)
        db_session.add(command)
        await db_session.commit()
        await db_session.refresh(command)
        seqs.append(command.seq)

    assert seqs == sorted(seqs)
    assert len(set(seqs)) == 3  # strictly increasing — no ties

    # Latest-state query shape: the newest command decides the kill-state.
    latest = (
        await db_session.execute(select(EngineCommand).order_by(EngineCommand.seq.desc()).limit(1))
    ).scalar_one()
    assert latest.action == EngineCommandAction.resume


async def test_engine_command_seq_is_db_assigned(db_session: AsyncSession) -> None:
    # GENERATED ALWAYS: a writer-supplied seq is rejected by the DB — writer
    # clocks/values must never order kill-switch state.
    db_session.add(_make_command(seq=999))
    with pytest.raises(DBAPIError):
        await db_session.flush()
    await db_session.rollback()


async def test_engine_command_sweep_query_returns_unapplied_only(
    db_session: AsyncSession,
) -> None:
    commands = [
        _make_command(action=EngineCommandAction.halt),
        _make_command(action=EngineCommandAction.flatten),
        _make_command(action=EngineCommandAction.resume),
    ]
    for command in commands:
        db_session.add(command)
        await db_session.commit()
    # The engine sweep stamped the first command already.
    commands[0].applied_at = datetime(2026, 7, 9, 14, 0, 0, tzinfo=UTC)
    await db_session.commit()

    # Sweep query shape (served by the partial index on seq WHERE applied_at IS NULL).
    unapplied = (
        (
            await db_session.execute(
                select(EngineCommand)
                .where(EngineCommand.applied_at.is_(None))
                .order_by(EngineCommand.seq)
            )
        )
        .scalars()
        .all()
    )
    assert [c.id for c in unapplied] == [commands[1].id, commands[2].id]


@pytest.mark.parametrize(
    ("latest_action", "latest_result", "expected"),
    [
        (None, None, KillState(halted=False, flattening=False)),  # empty log → armed
        (EngineCommandAction.halt, None, KillState(halted=True, flattening=False)),
        (EngineCommandAction.flatten, None, KillState(halted=True, flattening=True)),
        # A broker-verified flatten is DONE flattening: halted-only until resume
        # (a result-blind derivation reported "flattening" forever — review).
        (
            EngineCommandAction.flatten,
            RESULT_FLAT_VERIFIED,
            KillState(halted=True, flattening=False),
        ),
        # Any non-verified outcome keeps the drive owed.
        (
            EngineCommandAction.flatten,
            "failed: broker 503 during liquidation",
            KillState(halted=True, flattening=True),
        ),
        (EngineCommandAction.resume, None, KillState(halted=False, flattening=False)),
        # Unknown action fails closed: halt entries, never flatten on garbage.
        ("garbage", None, KillState(halted=True, flattening=False)),
    ],
)
def test_derive_kill_state(
    latest_action: str | None, latest_result: str | None, expected: KillState
) -> None:
    assert derive_kill_state(latest_action, latest_result) == expected


# ── Cascade ──────────────────────────────────────────────────────


async def test_broker_credential_cascades_on_portfolio_delete(
    db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio = await _make_portfolio(db_session, seeded_user)
    cred = BrokerCredential(
        portfolio_id=portfolio.id,
        api_key_encrypted="enc-key",
        api_secret_encrypted="enc-secret",
        paper=True,
    )
    db_session.add(cred)
    await db_session.commit()
    cred_id = cred.id

    # Core DELETE so the DB-level ON DELETE CASCADE does the work.
    await db_session.execute(delete(Portfolio).where(Portfolio.id == portfolio.id))
    await db_session.commit()

    remaining = (
        await db_session.execute(select(BrokerCredential).where(BrokerCredential.id == cred_id))
    ).scalar_one_or_none()
    assert remaining is None


# ── Decimal precision ────────────────────────────────────────────


async def test_decimal_precision_survives_round_trip(
    db_session: AsyncSession, seeded_user: User
) -> None:
    portfolio = await _make_portfolio(db_session, seeded_user)
    order = _make_order(
        portfolio,
        qty=Decimal("0.000000001"),  # Numeric(18, 9) — smallest representable qty
        limit_price=Decimal("123.456789"),  # Numeric(18, 6) — full price scale
    )
    db_session.add(order)
    await db_session.commit()
    order_id = order.id

    # Fresh SELECT (expire first so values come from the DB, not the identity map).
    db_session.expire_all()
    row = (await db_session.execute(select(Order).where(Order.id == order_id))).scalar_one()
    assert row.qty == Decimal("0.000000001")  # exact — no float drift
    assert row.limit_price == Decimal("123.456789")
    assert isinstance(row.qty, Decimal)
    assert isinstance(row.limit_price, Decimal)
