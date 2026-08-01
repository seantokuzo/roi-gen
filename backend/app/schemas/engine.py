"""Pydantic schemas for the kill-switch API (``/engine``).

``EngineCommandOut`` mirrors an ``engine_commands`` row verbatim — the audit
trail IS the product here, so nothing is projected away. ``EngineStatusOut``
projects the service-level :class:`~app.services.engine_commands.EngineStatus`
(same ``from_*`` convention as :mod:`app.schemas.account`).
"""

from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel

from app.models.engine_command import EngineCommand
from app.models.enums import EngineCommandAction
from app.services.engine_commands import EngineStatus


class EngineCommandIn(BaseModel):
    """Operator command: ``halt`` | ``flatten`` | ``resume`` plus an audit reason.

    ``reason`` may be omitted only for resume (the service defaults it to
    ``"resume"``); blank reasons for halt/flatten are rejected with a 422.
    """

    action: EngineCommandAction
    reason: str = ""


class EngineCommandOut(BaseModel):
    """One ``engine_commands`` row, as stored.

    ``action`` stays a plain string on the way out: rows are read back
    verbatim, and a hypothetical unrecognized action must render (the engine
    fails it closed) rather than 500 the audit listing.
    """

    id: uuid.UUID
    seq: int
    action: str
    scope: str
    reason: str
    actor: str
    issued_at: datetime
    applied_at: datetime | None
    result: str | None

    @classmethod
    def from_row(cls, row: EngineCommand) -> EngineCommandOut:
        """Project an ORM row."""
        return cls(
            id=row.id,
            seq=row.seq,
            action=row.action,
            scope=row.scope,
            reason=row.reason,
            actor=row.actor,
            issued_at=row.issued_at,
            applied_at=row.applied_at,
            result=row.result,
        )


class EngineStatusOut(BaseModel):
    """Derived kill-state + latest command + engine heartbeat liveness.

    ``engine_alive`` means the heartbeat key exists; ``engine_tasks`` is the
    per-critical-task health from its payload — the operator must be able to
    see "engine up but scheduler dead". ``engine_heartbeat_at`` is the
    payload's own timestamp (ISO string, passed through unparsed so a odd
    payload can't 500 the status read).
    """

    halted: bool
    flattening: bool
    latest_command: EngineCommandOut | None
    engine_alive: bool
    engine_heartbeat_at: str | None
    engine_tasks: dict[str, bool] | None

    @classmethod
    def from_status(cls, status: EngineStatus) -> EngineStatusOut:
        """Project the service-level status."""
        latest = status.latest_command
        return cls(
            halted=status.halted,
            flattening=status.flattening,
            latest_command=EngineCommandOut.from_row(latest) if latest is not None else None,
            engine_alive=status.engine_alive,
            engine_heartbeat_at=status.engine_heartbeat_at,
            engine_tasks=status.engine_tasks,
        )
