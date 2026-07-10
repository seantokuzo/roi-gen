"""Kill-switch API: operator commands + engine status (Phase 2c).

``POST /engine/commands`` writes the command row to Postgres (the source of
truth) via the shared writer in :mod:`app.services.engine_commands` — the same
code path the CLI uses, so the resume guard cannot drift — then pokes the
engine over Redis. The poke is best-effort: a failed publish never fails the
request, because the engine's timer sweep (~5s) re-reads the table anyway.

No endpoint here touches the broker; the engine drives everything (iron law
#1 stays intact — halt/flatten/resume are table rows, not order mutations).
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, status
from sqlalchemy import select

from app.api.v1.deps import CurrentUser, DbSession, RedisClient
from app.core.config import get_settings
from app.models.engine_command import EngineCommand
from app.schemas.engine import EngineCommandIn, EngineCommandOut, EngineStatusOut
from app.services.engine_commands import (
    ResumeRefusedError,
    heartbeat_portfolio_id,
    issue_command,
    read_status,
)

router = APIRouter()


@router.post("/commands", status_code=status.HTTP_201_CREATED)
async def create_engine_command(
    payload: EngineCommandIn, user: CurrentUser, db: DbSession, redis: RedisClient
) -> EngineCommandOut:
    """Issue halt | flatten | resume. Resume is refused (409) while the latest
    flatten's result is not ``flat_verified`` — re-arming with positions whose
    protective legs the flatten already canceled needs a verified-flat book."""
    try:
        row = await issue_command(
            db,
            redis,
            action=payload.action,
            reason=payload.reason,
            actor=f"api:{user.email}",
        )
    except ResumeRefusedError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(exc)
        ) from exc
    return EngineCommandOut.from_row(row)


@router.get("/commands")
async def list_engine_commands(
    user: CurrentUser,
    db: DbSession,
    limit: int = Query(20, ge=1, le=100),
) -> list[EngineCommandOut]:
    """Recent commands, newest first (``limit`` capped at 100)."""
    rows = (
        await db.scalars(select(EngineCommand).order_by(EngineCommand.seq.desc()).limit(limit))
    ).all()
    return [EngineCommandOut.from_row(row) for row in rows]


@router.get("/status")
async def get_engine_status(
    user: CurrentUser, db: DbSession, redis: RedisClient
) -> EngineStatusOut:
    """Derived kill-state + latest command + heartbeat-key engine liveness
    (including per-critical-task health — "engine up but scheduler dead" must
    be visible to the operator)."""
    portfolio_id = heartbeat_portfolio_id(get_settings().engine_portfolio_id)
    engine_status = await read_status(db, redis, portfolio_id=portfolio_id)
    return EngineStatusOut.from_status(engine_status)
