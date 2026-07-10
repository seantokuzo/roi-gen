"""KillSwitch: the derived kill-state and the verified-outcome write.

The truth is the ``engine_commands`` table; the KillSwitch is just the
derivation of its latest row, so every test seeds rows and asserts what
``load()`` derives — including the restart semantics that fall out of it
(an engine that reboots is exactly as halted as the operator left it).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy.ext.asyncio import async_sessionmaker

from app.engine.kill_switch import RESULT_FLAT_VERIFIED, KillSwitch
from app.models.enums import EngineCommandAction
from tests.engine.flatten_helpers import get_command_row, seed_command

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession


def _kill_switch(db_engine: AsyncEngine) -> KillSwitch:
    return KillSwitch(async_sessionmaker(db_engine, expire_on_commit=False))


async def test_empty_command_log_derives_armed(db_engine: AsyncEngine) -> None:
    ks = _kill_switch(db_engine)
    await ks.load()

    assert ks.is_halted is False
    assert ks.is_flattening is False
    assert ks.reason is None
    assert ks.flatten_command_seq is None


async def test_halt_derives_halted_but_not_flattening(
    db_engine: AsyncEngine, db_session: AsyncSession
) -> None:
    await seed_command(db_session, EngineCommandAction.halt, reason="ops freeze")
    await db_session.commit()

    ks = _kill_switch(db_engine)
    await ks.load()

    assert ks.is_halted is True
    assert ks.is_flattening is False
    assert ks.reason == "ops freeze"
    assert ks.flatten_command_seq is None  # nothing to drive


async def test_unfinished_flatten_derives_halted_and_flattening(
    db_engine: AsyncEngine, db_session: AsyncSession
) -> None:
    # An earlier halt exists — the LATEST row (the flatten) decides.
    await seed_command(db_session, EngineCommandAction.halt)
    flatten = await seed_command(db_session, EngineCommandAction.flatten, reason="get flat")
    await db_session.commit()

    ks = _kill_switch(db_engine)
    await ks.load()

    assert ks.is_halted is True
    assert ks.is_flattening is True
    assert ks.flatten_command_seq == flatten.seq
    assert ks.reason == "get flat"


async def test_verified_flatten_stays_halted_but_stops_driving(
    db_engine: AsyncEngine, db_session: AsyncSession
) -> None:
    await seed_command(db_session, EngineCommandAction.flatten, result=RESULT_FLAT_VERIFIED)
    await db_session.commit()

    ks = _kill_switch(db_engine)
    await ks.load()

    assert ks.is_halted is True  # still halted until an explicit resume
    assert ks.is_flattening is False  # but the drive is complete
    assert ks.flatten_command_seq is None


async def test_resume_derives_armed(db_engine: AsyncEngine, db_session: AsyncSession) -> None:
    await seed_command(db_session, EngineCommandAction.flatten)
    await seed_command(db_session, EngineCommandAction.resume, reason="all clear")
    await db_session.commit()

    ks = _kill_switch(db_engine)
    await ks.load()

    assert ks.is_halted is False
    assert ks.is_flattening is False


async def test_mark_flatten_verified_writes_the_result_exactly_once(
    db_engine: AsyncEngine, db_session: AsyncSession
) -> None:
    flatten = await seed_command(db_session, EngineCommandAction.flatten)
    await db_session.commit()

    ks = _kill_switch(db_engine)
    await ks.load()
    assert ks.is_flattening is True

    assert await ks.mark_flatten_verified(flatten.seq) is True
    row = await get_command_row(db_engine, flatten.seq)
    assert row.result == RESULT_FLAT_VERIFIED
    assert ks.is_flattening is False
    assert ks.is_halted is True  # verification completes the drive, not the halt

    # Second call: the row already carries its outcome — never re-marked.
    assert await ks.mark_flatten_verified(flatten.seq) is False


async def test_mark_flatten_verified_with_no_seq_is_a_no_op(db_engine: AsyncEngine) -> None:
    ks = _kill_switch(db_engine)
    assert await ks.mark_flatten_verified(None) is False


async def test_mark_flatten_verified_never_overwrites_an_existing_result(
    db_engine: AsyncEngine, db_session: AsyncSession
) -> None:
    superseded = await seed_command(db_session, EngineCommandAction.flatten, result="superseded")
    await db_session.commit()

    ks = _kill_switch(db_engine)
    assert await ks.mark_flatten_verified(superseded.seq) is False
    row = await get_command_row(db_engine, superseded.seq)
    assert row.result == "superseded"  # untouched
