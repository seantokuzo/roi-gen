"""Engine command channel + sweeper — how operator commands reach the engine.

Postgres is the source of truth (`engine_commands`); Redis is a WAKE-UP POKE
and nothing more. The API and CLI write the row first, then publish here; the
sweeper re-reads the table on every poke AND on a timer, so a dropped poke
(pub/sub is at-most-once; the subscriber reconnect window drops silently) costs
seconds of latency, never the command. A spoofed or malformed Redis message
can wake the sweeper early; it cannot inject state, because state is only ever
derived from rows.

Sweep semantics are latest-wins (level-triggered, matching the kill switch's
derivation): the newest row by ``seq`` defines the state; older unfinished
``flatten`` rows are marked superseded — the operator's LATEST ask is the only
one honored, including across engine downtime. ``applied_at`` records pickup
for the audit trail; completion is the FlattenController's to write.

The sweeper is gated on boot reconciliation like the fill stream: no flatten
drives off an unreconciled book. It belongs in the engine's ``critical`` set —
if command processing dies, entries halt (fail-safe), at the documented cost
that ``resume`` also needs a process restart in that state.
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from pydantic import BaseModel, ValidationError
from sqlalchemy import select

from app.core.logging import get_logger
from app.engine.kill_switch import RESULT_SUPERSEDED
from app.models.engine_command import EngineCommand
from app.models.enums import EngineCommandAction

if TYPE_CHECKING:
    import redis.asyncio as aioredis
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from app.engine.flatten_controller import FlattenController
    from app.engine.kill_switch import KillSwitch

log = get_logger("engine.commands")

CHANNEL_ENGINE_COMMANDS = "engine:commands"

# Timer fallback between sweeps; the poke only shortens latency.
_SWEEP_INTERVAL = 5.0


def heartbeat_key(portfolio_id: str | None) -> str:
    """The SETEX'd liveness key (15s TTL, task-health payload) — per portfolio so
    a dev engine and a container engine on shared Redis can't clobber each other's
    status. The API's /engine/status and the live E2E read this."""
    return f"engine:heartbeat:{portfolio_id}" if portfolio_id else "engine:heartbeat:default"


class CommandPoke(BaseModel):
    """The (content-free) wake-up envelope the API/CLI publish after the row."""

    type: str = "engine_command"


class EngineCommandSweeper:
    """Applies the command table to the in-memory kill switch, forever."""

    def __init__(
        self,
        *,
        redis: aioredis.Redis,
        session_factory: async_sessionmaker[AsyncSession],
        kill_switch: KillSwitch,
        controller: FlattenController,
        boot_reconciled: asyncio.Event,
        sweep_interval: float = _SWEEP_INTERVAL,
    ) -> None:
        self._redis = redis
        self._session_factory = session_factory
        self._kill_switch = kill_switch
        self._controller = controller
        self._boot_reconciled = boot_reconciled
        self._sweep_interval = sweep_interval
        self._poke = asyncio.Event()

    async def listen(self, shutdown: asyncio.Event) -> None:
        """Subscribe to the poke channel; reconnect forever; drops cost ≤ one timer tick."""
        while not shutdown.is_set():
            pubsub = self._redis.pubsub()
            try:
                await pubsub.subscribe(CHANNEL_ENGINE_COMMANDS)
                log.info("engine.commands.subscribed", channel=CHANNEL_ENGINE_COMMANDS)
                async for message in pubsub.listen():
                    if shutdown.is_set():
                        break
                    if message.get("type") != "message":
                        continue
                    self._note_poke(message.get("data"))
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 — reconnect; the timer sweep covers the gap
                log.exception("engine.commands.listen_error")
                await asyncio.sleep(1.0)
            finally:
                with contextlib.suppress(Exception):
                    await pubsub.aclose()  # type: ignore[no-untyped-call]  # redis stub gap

    def _note_poke(self, raw: object) -> None:
        try:
            data = raw.decode() if isinstance(raw, bytes | bytearray) else str(raw)
            CommandPoke.model_validate_json(data)
        except (ValidationError, UnicodeDecodeError):
            # Unparseable pokes still wake the sweep — waking early is harmless,
            # and the table is the only thing that can change state anyway.
            log.warning("engine.commands.malformed_poke")
        self._poke.set()

    async def process(self, shutdown: asyncio.Event) -> None:
        """Sweep on poke and on the timer, after the book is reconciled."""
        await self._boot_reconciled.wait()
        log.info("engine.commands.sweeper_started")
        while not shutdown.is_set():
            try:
                await self.sweep()
            except Exception:  # noqa: BLE001 — the safety loop must survive anything
                log.exception("engine.commands.sweep_error")
            self._poke.clear()
            poke_task = asyncio.create_task(self._poke.wait())
            shutdown_task = asyncio.create_task(shutdown.wait())
            try:
                await asyncio.wait(
                    {poke_task, shutdown_task},
                    timeout=self._sweep_interval,
                    return_when=asyncio.FIRST_COMPLETED,
                )
            finally:
                poke_task.cancel()
                shutdown_task.cancel()

    async def sweep(self) -> None:
        """One latest-wins pass over the command table."""
        async with self._session_factory() as session:
            latest = await session.scalar(
                select(EngineCommand).order_by(EngineCommand.seq.desc()).limit(1)
            )
            if latest is None:
                self._kill_switch.apply_latest(None)
                return
            was_flattening = self._kill_switch.is_flattening
            newly_seen = await self._stamp_applied(session, latest.seq)
            await self._supersede_stale_flattens(session, latest)
            await session.commit()
            # Re-read post-commit state through the ORM row we already hold —
            # apply_latest only reads scalar columns.
            self._kill_switch.apply_latest(latest)
        if newly_seen:
            log.info(
                "engine.commands.applied",
                seq=latest.seq,
                action=latest.action,
                actor=latest.actor,
                reason=latest.reason,
            )
        if self._kill_switch.is_flattening and (newly_seen or not was_flattening):
            self._controller.poke()

    async def _stamp_applied(self, session: AsyncSession, latest_seq: int) -> bool:
        """Record pickup on every not-yet-seen row; True if the latest was among them."""
        rows = (
            await session.scalars(
                select(EngineCommand)
                .where(EngineCommand.applied_at.is_(None))
                .order_by(EngineCommand.seq)
                .with_for_update()
            )
        ).all()
        now = datetime.now(UTC)
        latest_was_new = False
        for row in rows:
            row.applied_at = now
            if row.seq == latest_seq:
                latest_was_new = True
        return latest_was_new

    async def _supersede_stale_flattens(self, session: AsyncSession, latest: EngineCommand) -> None:
        """Older unfinished flattens are void — only the latest ask is honored."""
        stale = (
            await session.scalars(
                select(EngineCommand)
                .where(
                    EngineCommand.action == EngineCommandAction.flatten.value,
                    EngineCommand.result.is_(None),
                    EngineCommand.seq < latest.seq,
                )
                .with_for_update()
            )
        ).all()
        for row in stale:
            row.result = RESULT_SUPERSEDED
            log.info("engine.commands.superseded", seq=row.seq)
