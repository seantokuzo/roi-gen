"""EngineCommandSweeper: Postgres is the truth, Redis is only a wake-up poke.

Sweep semantics under test: latest row wins, older unfinished flattens are
superseded, ``applied_at`` records pickup exactly once, the controller is poked
whenever a flatten needs driving (including the boot case where the row was
already applied by a previous process), and a malformed/spoofed poke can wake
the sweep but can never inject state. ``process()`` is gated on boot
reconciliation — no flatten drives off an unreconciled book.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, cast

from sqlalchemy.ext.asyncio import async_sessionmaker

from app.engine.commands import CommandPoke, EngineCommandSweeper
from app.engine.kill_switch import KillSwitch
from app.models.enums import EngineCommandAction
from tests.engine.flatten_helpers import get_command_row, seed_command, wait_until

if TYPE_CHECKING:
    import redis.asyncio as aioredis
    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

    from app.engine.flatten_controller import FlattenController


class _PokeRecorder:
    """Stands in for the FlattenController; records every poke."""

    def __init__(self) -> None:
        self.pokes = 0

    def poke(self) -> None:
        self.pokes += 1


def _sweeper(
    db_engine: AsyncEngine,
    *,
    kill_switch: KillSwitch | None = None,
    recorder: _PokeRecorder | None = None,
) -> tuple[EngineCommandSweeper, KillSwitch, _PokeRecorder]:
    factory = async_sessionmaker(db_engine, expire_on_commit=False)
    ks = kill_switch if kill_switch is not None else KillSwitch(factory)
    rec = recorder if recorder is not None else _PokeRecorder()
    boot = asyncio.Event()
    boot.set()
    sweeper = EngineCommandSweeper(
        redis=cast("aioredis.Redis", object()),  # sweep() never touches Redis
        session_factory=factory,
        kill_switch=ks,
        controller=cast("FlattenController", rec),
        boot_reconciled=boot,
    )
    return sweeper, ks, rec


async def test_sweep_on_an_empty_table_derives_armed(db_engine: AsyncEngine) -> None:
    sweeper, ks, rec = _sweeper(db_engine)
    await sweeper.sweep()

    assert ks.is_halted is False
    assert ks.is_flattening is False
    assert rec.pokes == 0


async def test_latest_wins_and_older_unfinished_flatten_is_superseded(
    db_engine: AsyncEngine, db_session: AsyncSession
) -> None:
    halt = await seed_command(db_session, EngineCommandAction.halt)
    flatten = await seed_command(db_session, EngineCommandAction.flatten)
    resume = await seed_command(db_session, EngineCommandAction.resume)
    await db_session.commit()

    sweeper, ks, rec = _sweeper(db_engine)
    await sweeper.sweep()

    # The newest row (resume) IS the state — the stale flatten never drives.
    assert ks.is_halted is False
    assert ks.is_flattening is False
    assert rec.pokes == 0

    assert (await get_command_row(db_engine, flatten.seq)).result == "superseded"
    assert (await get_command_row(db_engine, halt.seq)).result is None
    assert (await get_command_row(db_engine, resume.seq)).result is None
    for seq in (halt.seq, flatten.seq, resume.seq):
        assert (await get_command_row(db_engine, seq)).applied_at is not None


async def test_applied_at_is_stamped_once_and_never_restamped(
    db_engine: AsyncEngine, db_session: AsyncSession
) -> None:
    halt = await seed_command(db_session, EngineCommandAction.halt)
    await db_session.commit()

    sweeper, ks, _rec = _sweeper(db_engine)
    await sweeper.sweep()
    first = (await get_command_row(db_engine, halt.seq)).applied_at
    assert first is not None
    assert ks.is_halted is True

    await sweeper.sweep()
    assert (await get_command_row(db_engine, halt.seq)).applied_at == first


async def test_new_flatten_pokes_the_controller_exactly_once(
    db_engine: AsyncEngine, db_session: AsyncSession
) -> None:
    flatten = await seed_command(db_session, EngineCommandAction.flatten)
    await db_session.commit()

    sweeper, ks, rec = _sweeper(db_engine)
    await sweeper.sweep()

    assert ks.is_halted is True
    assert ks.is_flattening is True
    assert ks.flatten_command_seq == flatten.seq
    assert rec.pokes == 1

    # Same state, nothing new: the timer sweep must not re-poke every 5s.
    await sweeper.sweep()
    assert rec.pokes == 1


async def test_boot_sweep_repokes_an_already_applied_flatten(
    db_engine: AsyncEngine, db_session: AsyncSession
) -> None:
    # Process 1 picked the flatten up (applied_at stamped), then died.
    await seed_command(db_session, EngineCommandAction.flatten)
    await db_session.commit()
    sweeper1, _ks1, rec1 = _sweeper(db_engine)
    await sweeper1.sweep()
    assert rec1.pokes == 1

    # Process 2 boots: the row is not newly seen, but its own kill switch was
    # not flattening before this sweep — the drive must be re-kicked, or a
    # crash mid-flatten would strand the intent.
    sweeper2, ks2, rec2 = _sweeper(db_engine)
    await sweeper2.sweep()
    assert ks2.is_flattening is True
    assert rec2.pokes == 1


async def test_resume_after_flatten_arms_without_poking(
    db_engine: AsyncEngine, db_session: AsyncSession
) -> None:
    flatten = await seed_command(db_session, EngineCommandAction.flatten)
    await db_session.commit()
    sweeper, ks, rec = _sweeper(db_engine)
    await sweeper.sweep()
    assert rec.pokes == 1

    await seed_command(db_session, EngineCommandAction.resume)
    await db_session.commit()
    await sweeper.sweep()

    assert ks.is_halted is False
    assert ks.is_flattening is False
    assert rec.pokes == 1  # armed states never wake the flatten drive
    assert (await get_command_row(db_engine, flatten.seq)).result == "superseded"


async def test_malformed_or_spoofed_poke_wakes_but_injects_nothing(
    db_engine: AsyncEngine,
) -> None:
    sweeper, ks, rec = _sweeper(db_engine)

    # Undecodable bytes: still a wake-up (waking early is harmless).
    sweeper._poke.clear()
    sweeper._note_poke(b"\x80\x81 not utf-8")
    assert sweeper._poke.is_set()

    # A spoofed payload claiming an action: also just a wake-up — state is
    # only ever derived from table rows, never from the message.
    sweeper._poke.clear()
    sweeper._note_poke('{"type": "engine_command", "action": "flatten"}')
    assert sweeper._poke.is_set()

    # And the genuine envelope, for completeness.
    sweeper._poke.clear()
    sweeper._note_poke(CommandPoke().model_dump_json())
    assert sweeper._poke.is_set()

    assert ks.is_halted is False
    assert ks.is_flattening is False
    assert rec.pokes == 0


class _CountingSweeper(EngineCommandSweeper):
    """Counts sweeps — process() gating/looping is what's under test here."""

    sweeps = 0

    async def sweep(self) -> None:
        self.sweeps += 1


async def test_process_waits_for_boot_reconciliation_then_sweeps_on_a_timer(
    db_engine: AsyncEngine,
) -> None:
    factory = async_sessionmaker(db_engine, expire_on_commit=False)
    boot = asyncio.Event()
    shutdown = asyncio.Event()
    sweeper = _CountingSweeper(
        redis=cast("aioredis.Redis", object()),
        session_factory=factory,
        kill_switch=KillSwitch(factory),
        controller=cast("FlattenController", _PokeRecorder()),
        boot_reconciled=boot,
        sweep_interval=0.01,
    )
    task = asyncio.create_task(sweeper.process(shutdown))
    try:
        await asyncio.sleep(0.05)
        assert sweeper.sweeps == 0  # gated: no sweep before the book is reconciled

        boot.set()
        await wait_until(lambda: sweeper.sweeps >= 2)  # first pass + timer fallback
    finally:
        shutdown.set()
        await asyncio.wait_for(task, timeout=2.0)
