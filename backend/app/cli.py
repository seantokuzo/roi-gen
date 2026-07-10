"""Operator kill-switch CLI — direct DB write + Redis poke, no API required.

The whole point of this tool is that it works when the API is down: it builds
its OWN short-lived async engine and Redis client from :class:`Settings` and
calls the same :mod:`app.services.engine_commands` writer the API uses, so
the resume guard can never drift between operator surfaces.

Usage::

    python -m app.cli halt --reason "why"
    python -m app.cli flatten --reason "why"
    python -m app.cli resume [--reason "why"]
    python -m app.cli status

Exit codes: 0 ok, 1 unexpected error, 2 refused (resume guard).
"""

from __future__ import annotations

import argparse
import asyncio
import getpass
import sys
from typing import TYPE_CHECKING

import redis.asyncio as aioredis
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.config import get_settings
from app.models.engine_command import KillState, derive_kill_state
from app.models.enums import EngineCommandAction
from app.services.engine_commands import (
    EngineStatus,
    ResumeRefusedError,
    heartbeat_portfolio_id,
    issue_command,
    read_status,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_REFUSED = 2

FLATTEN_NOTE = "flatten recorded — engine drives it; watch `status` for flat_verified"

_REDIS_TIMEOUT_SECONDS = 5.0


def build_parser() -> argparse.ArgumentParser:
    """The argparse surface: halt | flatten | resume | status."""
    parser = argparse.ArgumentParser(
        prog="python -m app.cli",
        description=(
            "ROI-GEN operator kill switch. Writes engine_commands directly and pokes the "
            "engine over Redis — works with the API down."
        ),
    )
    sub = parser.add_subparsers(dest="command", required=True)

    halt = sub.add_parser(
        "halt", help="freeze NEW entries (no broker mutations; protective legs keep working)"
    )
    halt.add_argument("--reason", required=True, help="why (audit trail; required)")

    flatten = sub.add_parser(
        "flatten",
        help="halt + drive-to-flat (engine-driven; while the market is closed it executes at "
        "next open)",
    )
    flatten.add_argument("--reason", required=True, help="why (audit trail; required)")

    resume = sub.add_parser(
        "resume", help="re-arm (refused while the latest flatten is not flat_verified)"
    )
    resume.add_argument("--reason", default="", help="optional (defaults to 'resume')")

    sub.add_parser("status", help="derived kill-state + latest command + engine heartbeat")
    return parser


def _state_name(state: KillState) -> str:
    if state.flattening:
        return "halted+flattening"
    if state.halted:
        return "halted"
    return "armed"


async def _cmd_issue(session: AsyncSession, redis: aioredis.Redis, args: argparse.Namespace) -> int:
    action = EngineCommandAction(args.command)
    row = await issue_command(
        session,
        redis,
        action=action,
        reason=args.reason or "",
        actor=f"cli:{getpass.getuser()}",
    )
    print(f"recorded seq={row.seq} action={row.action} reason={row.reason!r} actor={row.actor}")
    # This row is now the latest by construction — the state IS its derivation.
    print(f"kill-state: {_state_name(derive_kill_state(row.action))}")
    if action is EngineCommandAction.flatten:
        print(FLATTEN_NOTE)
    return EXIT_OK


def _print_status(status: EngineStatus) -> None:
    print(f"kill-state: {_state_name(KillState(status.halted, status.flattening))}")

    latest = status.latest_command
    if latest is None:
        print("latest command: none")
    else:
        if latest.result is not None:
            result = latest.result
        elif latest.action == EngineCommandAction.flatten:
            result = "in flight"
        else:
            result = "-"
        print(
            f"latest command: seq={latest.seq} action={latest.action} reason={latest.reason!r} "
            f"actor={latest.actor} issued_at={latest.issued_at} applied_at={latest.applied_at} "
            f"result={result}"
        )

    if not status.engine_alive:
        print("engine: not running / not reachable (no heartbeat key)")
        return
    print(f"engine: alive (heartbeat at {status.engine_heartbeat_at})")
    if status.engine_tasks:
        parts = ", ".join(
            f"{name}={'alive' if ok else 'DEAD'}"
            for name, ok in sorted(status.engine_tasks.items())
        )
        print(f"  tasks: {parts}")
        dead = [name for name, ok in sorted(status.engine_tasks.items()) if not ok]
        if dead:
            print(f"  DEAD TASKS: {', '.join(dead)}")


async def _cmd_status(
    session: AsyncSession, redis: aioredis.Redis, *, portfolio_id: str | None
) -> int:
    _print_status(await read_status(session, redis, portfolio_id=portfolio_id))
    return EXIT_OK


async def run(
    args: argparse.Namespace,
    *,
    session_factory: async_sessionmaker[AsyncSession] | None = None,
    redis: aioredis.Redis | None = None,
) -> int:
    """Execute one parsed command; returns the process exit code.

    ``session_factory`` / ``redis`` are injectable so tests drive this
    in-process against the test DB and a fake Redis; when omitted (real CLI
    use) short-lived resources are built from settings and disposed here —
    deliberately NOT the module-level engine in :mod:`app.core.database`,
    which nothing would dispose.
    """
    settings = get_settings()
    own_engine = None
    own_redis: aioredis.Redis | None = None
    if session_factory is None:
        own_engine = create_async_engine(settings.database_url, pool_pre_ping=True)
        session_factory = async_sessionmaker(own_engine, expire_on_commit=False)
    if redis is None:
        own_redis = aioredis.Redis.from_url(
            settings.redis_url,
            socket_connect_timeout=_REDIS_TIMEOUT_SECONDS,
            socket_timeout=_REDIS_TIMEOUT_SECONDS,
        )
        redis = own_redis
    try:
        async with session_factory() as session:
            if args.command == "status":
                portfolio_id = heartbeat_portfolio_id(settings.engine_portfolio_id)
                return await _cmd_status(session, redis, portfolio_id=portfolio_id)
            return await _cmd_issue(session, redis, args)
    except ResumeRefusedError as exc:
        print(f"REFUSED: {exc}", file=sys.stderr)
        return EXIT_REFUSED
    except Exception as exc:  # noqa: BLE001 — CLI boundary: report honestly, exit 1
        print(f"error: {exc!r}", file=sys.stderr)
        return EXIT_ERROR
    finally:
        if own_redis is not None:
            await own_redis.aclose()
        if own_engine is not None:
            await own_engine.dispose()


def main(argv: list[str] | None = None) -> int:
    """Parse argv and run one command (mirrors engine_main's asyncio.run shape)."""
    args = build_parser().parse_args(argv)
    return asyncio.run(run(args))


if __name__ == "__main__":
    sys.exit(main())
