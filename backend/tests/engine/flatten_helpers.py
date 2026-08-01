"""Shared fakes + builders for the Phase 2c safety-core tests.

Local to the flatten/kill-switch/feed-health suite so ``builders.py`` (owned by
the execution-core tests) stays untouched. Approvals are never forged here
either: :func:`mint_flatten_approval` runs the REAL risk engine and returns
what it minted (iron law #1 applies to tests too).
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.brokers.dto import BrokerPosition, CalendarDay
from app.engine.events import FlattenEvent
from app.engine.risk.engine import RiskEngine
from app.models.engine_command import EngineCommand
from app.models.telemetry import EventLog
from tests.engine.builders import make_limits

if TYPE_CHECKING:
    import uuid
    from collections.abc import Callable
    from datetime import date, datetime

    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

    from app.engine.risk.approval import FlattenApproval
    from app.models.enums import EngineCommandAction


class FakeRedis:
    """Duck-typed redis: records publishes; ``get`` returns ``value`` or raises."""

    def __init__(self) -> None:
        self.published: list[tuple[str, str]] = []
        self.value: str | bytes | None = None
        self.get_error: Exception | None = None

    async def publish(self, channel: str, message: str) -> int:
        self.published.append((channel, message))
        return 1

    async def get(self, key: str) -> str | bytes | None:
        if self.get_error is not None:
            raise self.get_error
        return self.value


def make_broker_position(
    symbol: str = "AAPL",
    *,
    qty: Decimal = Decimal("10"),
    avg_entry_price: Decimal = Decimal("100"),
) -> BrokerPosition:
    """A broker-truth position snapshot; ``qty`` is signed (negative = short)."""
    return BrokerPosition(
        symbol=symbol,
        qty=qty,
        side="long" if qty >= 0 else "short",
        avg_entry_price=avg_entry_price,
        market_value=qty * avg_entry_price,
        cost_basis=abs(qty) * avg_entry_price,
        unrealized_pl=Decimal("0"),
    )


def make_calendar_day(trading_date: date, rth_open: datetime, rth_close: datetime) -> CalendarDay:
    return CalendarDay(trading_date=trading_date, rth_open=rth_open, rth_close=rth_close)


def mint_flatten_approval(
    portfolio_id: uuid.UUID,
    *,
    source: str = "kill_switch",
    reason: str = "operator flatten",
    command_seq: int | None = None,
) -> tuple[FlattenEvent, FlattenApproval]:
    """Mint a real approval via the real risk engine — never forged."""
    event = FlattenEvent(
        portfolio_id=portfolio_id, reason=reason, source=source, command_seq=command_seq
    )
    decision = RiskEngine(make_limits()).authorize_flatten(event)
    assert decision.approval is not None
    return event, decision.approval


async def seed_command(
    session: AsyncSession,
    action: EngineCommandAction,
    *,
    reason: str = "test command",
    actor: str = "cli:test",
    result: str | None = None,
) -> EngineCommand:
    """Insert one operator command row (``seq`` is DB-assigned on flush)."""
    row = EngineCommand(action=action.value, reason=reason, actor=actor, result=result)
    session.add(row)
    await session.flush()
    return row


async def get_command_row(db_engine: AsyncEngine, seq: int) -> EngineCommand:
    """Re-read a command row on a fresh session (the code under test commits its own)."""
    factory = async_sessionmaker(db_engine, expire_on_commit=False)
    async with factory() as session:
        return (
            await session.execute(select(EngineCommand).where(EngineCommand.seq == seq))
        ).scalar_one()


async def get_events(db_engine: AsyncEngine, event_type: str) -> list[EventLog]:
    """All EventLog rows of ``event_type``, read on a fresh session."""
    factory = async_sessionmaker(db_engine, expire_on_commit=False)
    async with factory() as session:
        return list(
            (await session.execute(select(EventLog).where(EventLog.event_type == event_type)))
            .scalars()
            .all()
        )


async def wait_until(predicate: Callable[[], bool], *, timeout: float = 2.0) -> None:
    """Poll ``predicate`` until true or ``timeout`` — the no-sleep-guessing wait."""

    async def _poll() -> None:
        while not predicate():
            await asyncio.sleep(0.005)

    await asyncio.wait_for(_poll(), timeout)
