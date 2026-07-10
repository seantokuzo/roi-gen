"""Service tests for the shared kill-switch writer/reader.

These hit the real test database (``db_session``) with a capture-only fake
Redis: the row is the product, the poke is best-effort — both properties are
asserted here once, because the API and CLI both delegate to this module.
"""

from __future__ import annotations

import json

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.engine.commands import CHANNEL_ENGINE_COMMANDS, CommandPoke, heartbeat_key
from app.engine.kill_switch import RESULT_FLAT_VERIFIED, RESULT_SUPERSEDED
from app.models.engine_command import EngineCommand
from app.models.enums import EngineCommandAction
from app.services.engine_commands import (
    DEFAULT_RESUME_REASON,
    ResumeRefusedError,
    heartbeat_portfolio_id,
    issue_command,
    read_status,
)

PORTFOLIO_ID = "11111111-2222-3333-4444-555555555555"


class FakeRedis:
    """Capture-only stand-in for ``redis.asyncio.Redis`` (publish + get)."""

    def __init__(
        self,
        *,
        values: dict[str, str] | None = None,
        raise_on: set[str] | None = None,
    ) -> None:
        self.published: list[tuple[str, str]] = []
        self.values = values or {}
        self.raise_on = raise_on or set()

    async def publish(self, channel: str, message: str) -> int:
        if "publish" in self.raise_on:
            raise ConnectionError("redis down")
        self.published.append((channel, message))
        return 1

    async def get(self, name: str) -> str | None:
        if "get" in self.raise_on:
            raise ConnectionError("redis down")
        return self.values.get(name)


def heartbeat_payload(tasks: dict[str, bool]) -> str:
    return json.dumps(
        {"timestamp": "2026-07-09T14:00:00+00:00", "status": "running", "tasks": tasks}
    )


async def _count(session: AsyncSession) -> int:
    count = await session.scalar(select(func.count()).select_from(EngineCommand))
    assert count is not None
    return count


async def _issue(
    session: AsyncSession,
    redis: FakeRedis,
    action: EngineCommandAction,
    reason: str = "test reason",
) -> EngineCommand:
    return await issue_command(session, redis, action=action, reason=reason, actor="cli:test")


async def _mark_result(session: AsyncSession, row: EngineCommand, result: str) -> None:
    row.result = result
    await session.commit()


# ── issue_command: happy paths ───────────────────────────────────────


async def test_issue_halt_persists_row_and_pokes(db_session: AsyncSession) -> None:
    redis = FakeRedis()
    row = await issue_command(
        db_session,
        redis,
        action=EngineCommandAction.halt,
        reason="  fat finger  ",
        actor="api:sean@example.com",
    )
    assert row.seq >= 1  # DB-assigned, refreshed onto the returned row
    assert row.action == "halt"
    assert row.scope == "global"
    assert row.reason == "fat finger"  # stripped
    assert row.actor == "api:sean@example.com"
    assert row.issued_at is not None
    assert row.applied_at is None  # pickup is the engine sweep's to stamp
    assert row.result is None  # outcome is written on verification, never dispatch

    # Exactly one poke, on the engine channel, parseable as the envelope.
    assert len(redis.published) == 1
    channel, message = redis.published[0]
    assert channel == CHANNEL_ENGINE_COMMANDS
    CommandPoke.model_validate_json(message)


async def test_seq_orders_successive_commands(db_session: AsyncSession) -> None:
    redis = FakeRedis()
    halt = await _issue(db_session, redis, EngineCommandAction.halt)
    flatten = await _issue(db_session, redis, EngineCommandAction.flatten)
    assert flatten.seq > halt.seq


async def test_poke_failure_tolerated_command_still_recorded(db_session: AsyncSession) -> None:
    redis = FakeRedis(raise_on={"publish"})
    row = await _issue(db_session, redis, EngineCommandAction.halt)
    assert row.seq >= 1
    # The commit happened BEFORE the poke: a fresh query sees the row.
    persisted = await db_session.scalar(select(EngineCommand).where(EngineCommand.seq == row.seq))
    assert persisted is not None
    assert persisted.action == "halt"


# ── issue_command: reason validation ─────────────────────────────────


@pytest.mark.parametrize("action", [EngineCommandAction.halt, EngineCommandAction.flatten])
@pytest.mark.parametrize("reason", ["", "   "])
async def test_blank_reason_rejected_for_halt_and_flatten(
    db_session: AsyncSession, action: EngineCommandAction, reason: str
) -> None:
    redis = FakeRedis()
    with pytest.raises(ValueError, match="reason"):
        await _issue(db_session, redis, action, reason=reason)
    assert await _count(db_session) == 0
    assert redis.published == []


async def test_resume_blank_reason_defaults(db_session: AsyncSession) -> None:
    row = await _issue(db_session, FakeRedis(), EngineCommandAction.resume, reason="   ")
    assert row.reason == DEFAULT_RESUME_REASON


# ── issue_command: the resume guard matrix ───────────────────────────


async def test_resume_refused_after_unverified_flatten(db_session: AsyncSession) -> None:
    redis = FakeRedis()
    flatten = await _issue(db_session, redis, EngineCommandAction.flatten)
    with pytest.raises(ResumeRefusedError, match="flat"):
        await _issue(db_session, redis, EngineCommandAction.resume)
    # Nothing was inserted and nothing was poked for the refused resume.
    assert await _count(db_session) == 1
    assert len(redis.published) == 1  # only the flatten's poke
    assert flatten.result is None


async def test_resume_refused_after_failed_flatten(db_session: AsyncSession) -> None:
    redis = FakeRedis()
    flatten = await _issue(db_session, redis, EngineCommandAction.flatten)
    await _mark_result(db_session, flatten, "failed: broker 503 during liquidation")
    with pytest.raises(ResumeRefusedError):
        await _issue(db_session, redis, EngineCommandAction.resume)


async def test_resume_allowed_after_flat_verified(db_session: AsyncSession) -> None:
    redis = FakeRedis()
    flatten = await _issue(db_session, redis, EngineCommandAction.flatten)
    await _mark_result(db_session, flatten, RESULT_FLAT_VERIFIED)
    resume = await _issue(db_session, redis, EngineCommandAction.resume)
    assert resume.action == "resume"
    assert resume.seq > flatten.seq


async def test_resume_allowed_when_halt_follows_flatten(db_session: AsyncSession) -> None:
    """flatten → halt → resume: the LATEST command is what the guard reads.

    This is the sequence the engine itself produces (the sweep marks the
    older flatten superseded); the guard must allow resume even before the
    sweep has stamped it, because latest=halt already means "not flattening".
    """
    redis = FakeRedis()
    await _issue(db_session, redis, EngineCommandAction.flatten)
    await _issue(db_session, redis, EngineCommandAction.halt)
    resume = await _issue(db_session, redis, EngineCommandAction.resume)
    assert resume.action == "resume"


async def test_resume_allowed_after_superseded_flatten_with_sweep_stamp(
    db_session: AsyncSession,
) -> None:
    """Same sequence, but with the sweeper's superseded stamp actually applied."""
    redis = FakeRedis()
    flatten = await _issue(db_session, redis, EngineCommandAction.flatten)
    await _issue(db_session, redis, EngineCommandAction.halt)
    await _mark_result(db_session, flatten, RESULT_SUPERSEDED)
    resume = await _issue(db_session, redis, EngineCommandAction.resume)
    assert resume.action == "resume"


async def test_resume_allowed_when_latest_flatten_marked_superseded(
    db_session: AsyncSession,
) -> None:
    """A superseded flatten does not block resume even if it is the latest row."""
    redis = FakeRedis()
    flatten = await _issue(db_session, redis, EngineCommandAction.flatten)
    await _mark_result(db_session, flatten, RESULT_SUPERSEDED)
    resume = await _issue(db_session, redis, EngineCommandAction.resume)
    assert resume.action == "resume"


async def test_resume_allowed_on_empty_log(db_session: AsyncSession) -> None:
    resume = await _issue(db_session, FakeRedis(), EngineCommandAction.resume)
    assert resume.action == "resume"


# ── read_status ──────────────────────────────────────────────────────


async def test_read_status_empty_log_engine_down(db_session: AsyncSession) -> None:
    status = await read_status(db_session, FakeRedis(), portfolio_id=PORTFOLIO_ID)
    assert status.halted is False
    assert status.flattening is False
    assert status.latest_command is None
    assert status.engine_alive is False
    assert status.engine_heartbeat_at is None
    assert status.engine_tasks is None


async def test_read_status_flattening_with_heartbeat(db_session: AsyncSession) -> None:
    redis = FakeRedis(
        values={
            heartbeat_key(PORTFOLIO_ID): heartbeat_payload(
                {"event_bus": True, "flatten_controller": False}
            )
        }
    )
    flatten = await _issue(db_session, redis, EngineCommandAction.flatten)
    status = await read_status(db_session, redis, portfolio_id=PORTFOLIO_ID)
    assert status.halted is True
    assert status.flattening is True
    assert status.latest_command is not None
    assert status.latest_command.seq == flatten.seq
    assert status.engine_alive is True
    assert status.engine_heartbeat_at == "2026-07-09T14:00:00+00:00"
    assert status.engine_tasks == {"event_bus": True, "flatten_controller": False}


async def test_read_status_halt_state(db_session: AsyncSession) -> None:
    redis = FakeRedis()
    await _issue(db_session, redis, EngineCommandAction.halt)
    status = await read_status(db_session, redis, portfolio_id=None)
    assert status.halted is True
    assert status.flattening is False


async def test_read_status_unparseable_heartbeat_is_alive_without_detail(
    db_session: AsyncSession,
) -> None:
    redis = FakeRedis(values={heartbeat_key(PORTFOLIO_ID): "not json {{"})
    status = await read_status(db_session, redis, portfolio_id=PORTFOLIO_ID)
    assert status.engine_alive is True  # the key exists — the process is up
    assert status.engine_heartbeat_at is None
    assert status.engine_tasks is None


async def test_read_status_redis_down_reports_not_reachable(db_session: AsyncSession) -> None:
    status = await read_status(db_session, FakeRedis(raise_on={"get"}), portfolio_id=PORTFOLIO_ID)
    assert status.engine_alive is False


# ── heartbeat_portfolio_id normalization ─────────────────────────────


def test_heartbeat_portfolio_id_empty_is_none() -> None:
    assert heartbeat_portfolio_id("") is None


def test_heartbeat_portfolio_id_invalid_is_none() -> None:
    assert heartbeat_portfolio_id("not-a-uuid") is None


def test_heartbeat_portfolio_id_normalizes_casing() -> None:
    # Must match engine_main's str(uuid.UUID(...)) normalization exactly.
    assert heartbeat_portfolio_id(PORTFOLIO_ID.upper()) == PORTFOLIO_ID
