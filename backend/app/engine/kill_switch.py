"""KillSwitch — the engine's in-memory view of the durable operator command state.

The truth lives in Postgres (``engine_commands``): the latest row by ``seq``
IS the state — ``halt``/``flatten`` → halted (flatten also drives-to-flat),
``resume`` → armed, no rows → armed. This object is just that derivation,
cached in-process and refreshed only by the command sweeper, so the hot path
(`RiskStage.halted` reads per signal, the execution boundary re-checks per
submit) costs an attribute read, not a query.

Restart semantics fall out of the derivation: an engine that reboots re-reads
the latest command and is exactly as halted as the operator left it. There is
no "clear on restart" bug to have.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select

from app.core.logging import get_logger
from app.models.engine_command import (
    RESULT_FLAT_VERIFIED,
    RESULT_SUPERSEDED,
    EngineCommand,
    derive_kill_state,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

log = get_logger("engine.kill_switch")

__all__ = ["RESULT_FLAT_VERIFIED", "RESULT_SUPERSEDED", "KillSwitch"]


class KillSwitch:
    """Derived kill-state; refreshed by the sweeper, read by everything else."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory
        self._halted = False
        self._flattening = False
        self._reason: str | None = None
        self._latest_seq: int | None = None

    @property
    def is_halted(self) -> bool:
        return self._halted

    @property
    def is_flattening(self) -> bool:
        return self._flattening

    @property
    def reason(self) -> str | None:
        return self._reason

    @property
    def flatten_command_seq(self) -> int | None:
        """The seq of the flatten command being driven (None when not flattening)."""
        return self._latest_seq if self._flattening else None

    async def load(self) -> None:
        """Boot-time derivation from the latest command row."""
        async with self._session_factory() as session:
            latest = await _latest_command(session)
        self.apply_latest(latest)
        log.info(
            "engine.kill_switch.loaded",
            halted=self._halted,
            flattening=self._flattening,
            seq=self._latest_seq,
        )

    def apply_latest(self, latest: EngineCommand | None) -> None:
        """Derive state from the latest command (sweeper calls this per sweep).

        Delegates to the model's :func:`derive_kill_state` — the SAME function
        the API/CLI status reader uses, so the two operator surfaces cannot
        drift from the engine's own view (review finding).
        """
        if latest is None:
            self._halted = False
            self._flattening = False
            self._reason = None
            self._latest_seq = None
            return
        state = derive_kill_state(latest.action, latest.result)
        self._latest_seq = latest.seq
        self._reason = latest.reason
        self._halted = state.halted
        self._flattening = state.flattening

    async def mark_flatten_verified(self, command_seq: int | None) -> bool:
        """Record broker-verified flatness on the driving command row.

        Written by the FlattenController ONLY after a broker-truth exposure
        check came back clean — `result` means outcome, never dispatch. Returns
        True iff this call newly marked the row (so completion is audited once).
        """
        if command_seq is None:
            return False
        async with self._session_factory() as session:
            row = await session.scalar(
                select(EngineCommand).where(EngineCommand.seq == command_seq).with_for_update()
            )
            if row is None or row.result is not None:
                return False
            row.result = RESULT_FLAT_VERIFIED
            await session.commit()
        self._flattening = False
        return True


async def _latest_command(session: AsyncSession) -> EngineCommand | None:
    latest: EngineCommand | None = await session.scalar(
        select(EngineCommand).order_by(EngineCommand.seq.desc()).limit(1)
    )
    return latest
