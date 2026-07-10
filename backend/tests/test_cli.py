"""Kill-switch CLI tests — parse_args → run(), in-process.

No subprocesses: ``run()`` accepts an injected session factory (test DB) and
Redis client (fake), which is exactly the seam the real CLI fills from
settings. Output lines and exit codes are the contract asserted here.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.cli import EXIT_ERROR, EXIT_OK, EXIT_REFUSED, FLATTEN_NOTE, build_parser, run
from app.core.config import get_settings
from app.engine.commands import CHANNEL_ENGINE_COMMANDS, heartbeat_key
from app.engine.kill_switch import RESULT_FLAT_VERIFIED
from app.models.engine_command import EngineCommand
from app.services.engine_commands import heartbeat_portfolio_id


class FakeRedis:
    """Capture-only stand-in for ``redis.asyncio.Redis`` (publish + get)."""

    def __init__(self) -> None:
        self.published: list[tuple[str, str]] = []
        self.values: dict[str, str] = {}

    async def publish(self, channel: str, message: str) -> int:
        self.published.append((channel, message))
        return 1

    async def get(self, name: str) -> str | None:
        return self.values.get(name)


@pytest.fixture
def session_factory(db_engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(db_engine, expire_on_commit=False)


async def _cli(
    argv: list[str],
    factory: async_sessionmaker[AsyncSession],
    redis: FakeRedis,
) -> int:
    args = build_parser().parse_args(argv)
    return await run(args, session_factory=factory, redis=redis)  # type: ignore[arg-type]


def _status_heartbeat_key() -> str:
    """The heartbeat key the status command will read (env-dependent)."""
    return heartbeat_key(heartbeat_portfolio_id(get_settings().engine_portfolio_id))


# ── halt / flatten / resume ──────────────────────────────────────────


async def test_halt_records_row_and_prints_state(
    session_factory: async_sessionmaker[AsyncSession],
    capsys: pytest.CaptureFixture[str],
) -> None:
    redis = FakeRedis()
    code = await _cli(["halt", "--reason", "fat finger"], session_factory, redis)
    assert code == EXIT_OK

    out = capsys.readouterr().out
    assert "seq=1" in out
    assert "action=halt" in out
    assert "kill-state: halted" in out

    async with session_factory() as session:
        row = await session.scalar(select(EngineCommand).where(EngineCommand.seq == 1))
    assert row is not None
    assert row.reason == "fat finger"
    assert row.actor.startswith("cli:")
    assert [channel for channel, _ in redis.published] == [CHANNEL_ENGINE_COMMANDS]


async def test_flatten_prints_honest_note(
    session_factory: async_sessionmaker[AsyncSession],
    capsys: pytest.CaptureFixture[str],
) -> None:
    code = await _cli(["flatten", "--reason", "get me out"], session_factory, FakeRedis())
    assert code == EXIT_OK
    out = capsys.readouterr().out
    assert "kill-state: halted+flattening" in out
    assert FLATTEN_NOTE in out


async def test_resume_refused_exits_2(
    session_factory: async_sessionmaker[AsyncSession],
    capsys: pytest.CaptureFixture[str],
) -> None:
    redis = FakeRedis()
    assert await _cli(["flatten", "--reason", "get me out"], session_factory, redis) == EXIT_OK
    code = await _cli(["resume"], session_factory, redis)
    assert code == EXIT_REFUSED

    err = capsys.readouterr().err
    assert "REFUSED" in err
    assert "flat_verified" in err

    # The refused resume left no row behind.
    async with session_factory() as session:
        actions = (await session.scalars(select(EngineCommand.action))).all()
    assert actions == ["flatten"]


async def test_resume_allowed_after_flat_verified(
    session_factory: async_sessionmaker[AsyncSession],
    capsys: pytest.CaptureFixture[str],
) -> None:
    redis = FakeRedis()
    assert await _cli(["flatten", "--reason", "get me out"], session_factory, redis) == EXIT_OK
    async with session_factory() as session:
        row = await session.scalar(select(EngineCommand).where(EngineCommand.seq == 1))
        assert row is not None
        row.result = RESULT_FLAT_VERIFIED
        await session.commit()

    code = await _cli(["resume"], session_factory, redis)
    assert code == EXIT_OK
    out = capsys.readouterr().out
    assert "kill-state: armed" in out

    # Omitted --reason defaulted to "resume".
    async with session_factory() as session:
        resume = await session.scalar(select(EngineCommand).where(EngineCommand.seq == 2))
    assert resume is not None
    assert resume.reason == "resume"


# ── status ───────────────────────────────────────────────────────────


async def test_status_engine_down_no_commands(
    session_factory: async_sessionmaker[AsyncSession],
    capsys: pytest.CaptureFixture[str],
) -> None:
    code = await _cli(["status"], session_factory, FakeRedis())
    assert code == EXIT_OK
    out = capsys.readouterr().out
    assert "kill-state: armed" in out
    assert "latest command: none" in out
    assert "not running / not reachable" in out


async def test_status_flattening_with_dead_task_called_out(
    session_factory: async_sessionmaker[AsyncSession],
    capsys: pytest.CaptureFixture[str],
) -> None:
    redis = FakeRedis()
    assert await _cli(["flatten", "--reason", "get me out"], session_factory, redis) == EXIT_OK
    redis.values[_status_heartbeat_key()] = json.dumps(
        {
            "timestamp": "2026-07-09T14:00:00+00:00",
            "status": "running",
            "tasks": {"event_bus": True, "flatten_controller": False},
        }
    )
    capsys.readouterr()  # drop the flatten command's output

    code = await _cli(["status"], session_factory, redis)
    assert code == EXIT_OK
    out = capsys.readouterr().out
    assert "kill-state: halted+flattening" in out
    assert "action=flatten" in out
    assert "result=in flight" in out  # unfinished flatten, honestly reported
    assert "engine: alive (heartbeat at 2026-07-09T14:00:00+00:00)" in out
    assert "event_bus=alive" in out
    assert "flatten_controller=DEAD" in out
    assert "DEAD TASKS: flatten_controller" in out


# ── error paths / parser ─────────────────────────────────────────────


async def test_blank_reason_is_an_error_exit_1(
    session_factory: async_sessionmaker[AsyncSession],
    capsys: pytest.CaptureFixture[str],
) -> None:
    code = await _cli(["halt", "--reason", "   "], session_factory, FakeRedis())
    assert code == EXIT_ERROR
    assert "error:" in capsys.readouterr().err


async def test_unexpected_error_exits_1(capsys: pytest.CaptureFixture[str]) -> None:
    class _BrokenFactory:
        def __call__(self) -> Any:
            raise RuntimeError("boom")

    args = build_parser().parse_args(["status"])
    code = await run(args, session_factory=_BrokenFactory(), redis=FakeRedis())  # type: ignore[arg-type]
    assert code == EXIT_ERROR
    assert "boom" in capsys.readouterr().err


def test_halt_requires_reason() -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(["halt"])


def test_flatten_requires_reason() -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(["flatten"])


def test_subcommand_required() -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args([])
