"""Engine command log — the kill-switch source of truth (Phase 2c).

Append-only: operators (API/CLI) insert rows; the engine sweep stamps
``applied_at`` and later writes ``result``. Nothing updates ``action``.
"""

from __future__ import annotations

from datetime import datetime
from typing import NamedTuple

from sqlalchemy import BigInteger, DateTime, Identity, Index, String, Text, func, text
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base
from app.models.base import TimestampMixin, UUIDPrimaryKeyMixin
from app.models.enums import EngineCommandAction


class EngineCommand(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """One operator command: ``halt`` | ``flatten`` | ``resume``.

    The kill-switch state is DERIVED, never stored: latest row by ``seq``
    decides — ``halt``/``flatten`` → halted (``flatten`` → also flattening),
    ``resume`` → armed, no rows at all → armed. ``seq`` is a DB-assigned
    ``GENERATED ALWAYS`` identity because it is the ordering key: writer
    clocks (laptop CLI vs server API) can skew and must not order state.

    ``action`` stores an :class:`app.models.enums.EngineCommandAction` value.
    ``issued_at`` is informational/audit only. ``applied_at`` means "picked up
    by the engine sweep" — NOT completion; the verified outcome lands in
    ``result`` (``applied`` | ``flat_verified`` | ``superseded`` |
    ``failed: <detail>``), written on verified completion, never at dispatch.
    """

    __tablename__ = "engine_commands"
    __table_args__ = (
        # THE ordering key; the unique index also serves the latest-state
        # query (ORDER BY seq DESC LIMIT 1 — btree scans both directions).
        Index("uq_engine_commands_seq", "seq", unique=True),
        # Boot/periodic sweep: "commands not yet picked up", in order.
        Index(
            "ix_engine_commands_unapplied_seq",
            "seq",
            postgresql_where=text("applied_at IS NULL"),
        ),
    )

    seq: Mapped[int] = mapped_column(BigInteger, Identity(always=True))
    action: Mapped[str] = mapped_column(String(16))
    scope: Mapped[str] = mapped_column(String(32), default="global")
    reason: Mapped[str] = mapped_column(Text)
    actor: Mapped[str] = mapped_column(String(120))
    issued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    applied_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    result: Mapped[str | None] = mapped_column(Text)


# Command-row ``result`` vocabulary. ``flat_verified`` is the only completion
# an operator may trust (written by the FlattenController after a broker-truth
# flat check); ``superseded`` means a newer command voided the row. Defined
# with the model so every derivation of kill-state shares one source.
RESULT_FLAT_VERIFIED = "flat_verified"
RESULT_SUPERSEDED = "superseded"


class KillState(NamedTuple):
    """Kill-switch state derived from the latest :class:`EngineCommand`."""

    halted: bool
    flattening: bool


def derive_kill_state(latest_action: str | None, latest_result: str | None = None) -> KillState:
    """Map the latest command's ``action`` + ``result`` to kill-state.

    ``halt`` → halted; ``flatten`` → halted + flattening — UNLESS its result
    is already ``flat_verified``, in which case the drive is done and the
    state is halted-only. The engine-side and API/CLI-side derivations MUST
    agree here (review finding: a result-blind reader reported "flattening"
    forever after verification). ``resume`` or an empty log → armed. An
    UNRECOGNIZED action fails closed to halted (block new entries) but never
    flattening — flatten mutates broker state and must not fire on garbage.
    """
    if latest_action is None or latest_action == EngineCommandAction.resume:
        return KillState(halted=False, flattening=False)
    if latest_action == EngineCommandAction.halt:
        return KillState(halted=True, flattening=False)
    if latest_action == EngineCommandAction.flatten:
        return KillState(halted=True, flattening=latest_result != RESULT_FLAT_VERIFIED)
    return KillState(halted=True, flattening=False)
