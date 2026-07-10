"""Kill-switch command writer + status reader — the ONE shared implementation.

Both the API (``/engine``) and the CLI (``python -m app.cli``) call these
functions, so the resume guard can never drift between operator surfaces.
Postgres is the source of truth: the row is committed FIRST; the Redis poke is
a best-effort wake-up — a lost or failed poke costs at most one engine timer
sweep (~5s), never the command itself.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import select

from app.core.logging import get_logger
from app.engine.commands import CHANNEL_ENGINE_COMMANDS, CommandPoke, heartbeat_key
from app.engine.kill_switch import RESULT_FLAT_VERIFIED, RESULT_SUPERSEDED
from app.models.engine_command import EngineCommand, derive_kill_state
from app.models.enums import EngineCommandAction

if TYPE_CHECKING:
    import redis.asyncio as aioredis
    from sqlalchemy.ext.asyncio import AsyncSession

log = get_logger("services.engine_commands")

DEFAULT_RESUME_REASON = "resume"


class ResumeRefusedError(Exception):
    """Resume refused: the latest command is a flatten without a verified outcome.

    The flatten already canceled protective legs; re-arming with positions
    that may still be open and now unprotected is the one thing an operator
    must not do casually. Verify flat first (watch ``status`` for
    ``flat_verified``) or re-issue the flatten.
    """


@dataclass(frozen=True)
class EngineStatus:
    """Operator-facing status: derived kill-state + latest command + liveness."""

    halted: bool
    flattening: bool
    latest_command: EngineCommand | None
    engine_alive: bool
    engine_heartbeat_at: str | None
    engine_tasks: dict[str, bool] | None


def heartbeat_portfolio_id(raw: str) -> str | None:
    """Normalize ``ENGINE_PORTFOLIO_ID`` to the engine's heartbeat-key form.

    The engine writes its heartbeat under the parsed-UUID string form
    (:mod:`app.engine_main` normalizes the same way), so env-var casing or
    formatting can never make the reader look at a different key. Empty or
    unparseable → ``None`` (the ``engine:heartbeat:default`` key).
    """
    if not raw:
        return None
    try:
        return str(uuid.UUID(raw))
    except ValueError:
        return None


async def _latest_command(session: AsyncSession) -> EngineCommand | None:
    latest: EngineCommand | None = await session.scalar(
        select(EngineCommand).order_by(EngineCommand.seq.desc()).limit(1)
    )
    return latest


async def issue_command(
    session: AsyncSession,
    redis: aioredis.Redis,
    *,
    action: EngineCommandAction,
    reason: str,
    actor: str,
) -> EngineCommand:
    """Insert one operator command, commit, then poke the engine (best-effort).

    Raises :class:`ValueError` on a blank reason for halt/flatten (resume
    defaults to ``"resume"``) and :class:`ResumeRefusedError` when resume is
    asked for while the latest command is a flatten whose ``result`` is not
    ``flat_verified`` (and not ``superseded``). Note the guard reads the
    LATEST row only: a halt issued after an unfinished flatten supersedes it
    (the engine sweep marks it so), and resume is then allowed.
    """
    clean_reason = reason.strip()
    if not clean_reason:
        if action is not EngineCommandAction.resume:
            msg = f"a non-empty reason is required for {action.value}"
            raise ValueError(msg)
        clean_reason = DEFAULT_RESUME_REASON

    if action is EngineCommandAction.resume:
        latest = await _latest_command(session)
        if (
            latest is not None
            and latest.action == EngineCommandAction.flatten
            and latest.result not in (RESULT_FLAT_VERIFIED, RESULT_SUPERSEDED)
        ):
            result = latest.result if latest.result is not None else "in flight"
            msg = (
                f"resume refused: the latest command (seq={latest.seq}) is a flatten whose "
                f"result is {result!r}, not {RESULT_FLAT_VERIFIED!r}. That flatten already "
                "canceled protective legs — re-arming while positions may still be open and "
                "unprotected is the one thing an operator must not do casually. Verify flat "
                "first (watch status for 'flat_verified') or re-issue flatten."
            )
            raise ResumeRefusedError(msg)

    row = EngineCommand(action=action.value, scope="global", reason=clean_reason, actor=actor)
    session.add(row)
    await session.commit()
    # seq + issued_at are DB-assigned; reload them onto the returned row.
    await session.refresh(row)

    try:
        await redis.publish(CHANNEL_ENGINE_COMMANDS, CommandPoke().model_dump_json())
    except Exception as exc:  # noqa: BLE001 — poke is best-effort; the row is already durable
        log.warning(
            "engine_commands.poke_failed",
            seq=row.seq,
            action=row.action,
            error=repr(exc),
            note="engine timer sweep (~5s) will pick the command up",
        )
    return row


async def read_status(
    session: AsyncSession,
    redis: aioredis.Redis,
    *,
    portfolio_id: str | None,
) -> EngineStatus:
    """Derived kill-state + latest command + heartbeat-key engine liveness.

    Liveness reads the SETEX'd heartbeat key (15s TTL): present → the engine
    process is up and the payload carries per-critical-task health; absent (or
    Redis unreachable) → not running / not reachable. The kill-state itself
    comes from Postgres and is trustworthy either way.
    """
    latest = await _latest_command(session)
    state = derive_kill_state(latest.action if latest is not None else None)

    key = heartbeat_key(portfolio_id)
    raw: object = None
    try:
        raw = await redis.get(key)
    except Exception as exc:  # noqa: BLE001 — Redis down must not break status
        log.warning("engine_commands.heartbeat_unreachable", key=key, error=repr(exc))

    alive = raw is not None
    heartbeat_at: str | None = None
    tasks: dict[str, bool] | None = None
    if raw is not None:
        try:
            data = json.loads(raw.decode() if isinstance(raw, bytes | bytearray) else str(raw))
        except (json.JSONDecodeError, UnicodeDecodeError):
            log.warning("engine_commands.heartbeat_unparseable", key=key)
            data = {}
        ts = data.get("timestamp") if isinstance(data, dict) else None
        heartbeat_at = ts if isinstance(ts, str) else None
        raw_tasks = data.get("tasks") if isinstance(data, dict) else None
        if isinstance(raw_tasks, dict):
            tasks = {str(name): bool(ok) for name, ok in raw_tasks.items()}

    return EngineStatus(
        halted=state.halted,
        flattening=state.flattening,
        latest_command=latest,
        engine_alive=alive,
        engine_heartbeat_at=heartbeat_at,
        engine_tasks=tasks,
    )
