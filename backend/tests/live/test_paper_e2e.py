"""Live-paper E2E: a probe strategy paper-trades a bracket end-to-end, then a
kill-switch flatten drives the book flat — with the full audit trail asserted
from rows.

This is the Phase 2 deliverable ("a trivial test strategy paper-trades bracket
orders end-to-end with full audit trail"), plus the 2c safety spine exercised
on the REAL paper API: the engine runs as a subprocess (`python -m
app.engine_main`) because that is the only honest way to run it — settings are
lru-cached and the DB session factory binds at import time, so an in-process
"patched settings" engine would silently trade against the dev database.

What paper does and does not prove (project gotcha, documented in the design):
fills are optimistic NBBO and cancels are near-instant, so this run proves
PLUMBING + AUDIT (order flow, stream writer, lots, commands, flatten
completion contract), not market microstructure. The held-qty cancel/close
race IS deliberately exercised: the flatten fires while the entry's bracket
legs are live.

Runbook (see MANUAL-SETUP.md):

    ROIGEN_LIVE_E2E=1 uv run pytest -m live_paper tests/live/ -q

Gates: env flag + paper keys + local Postgres/Redis + market open with enough
runway before the mandatory close-window flatten.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import subprocess
import sys
import tempfile
import uuid
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
import redis.asyncio as aioredis
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from app.brokers.alpaca.factory import build_alpaca_adapter
from app.brokers.credentials import BrokerCredentials
from app.models.engine_command import EngineCommand
from app.models.enums import StrategyStatus
from app.models.portfolio import Portfolio
from app.models.strategy import Strategy
from app.models.telemetry import EventLog
from app.models.trading import Fill, Lot, LotClose, Order, Position
from app.models.user import User

pytestmark = pytest.mark.live_paper

_SYMBOL = "SPY"
_REDIS_URL = os.environ.get("ROIGEN_LIVE_REDIS_URL", "redis://localhost:6379/9")
_ENTRY_TIMEOUT = 360.0  # first 1-min bar + optimistic paper fill
_FLATTEN_TIMEOUT = 180.0  # sweep (≤5s) + drive + fill + verify tick (≤30s), with slack
_BOOT_TIMEOUT = 60.0


def _env_gate() -> str | None:
    if os.environ.get("ROIGEN_LIVE_E2E") != "1":
        return "ROIGEN_LIVE_E2E != 1"
    if not (os.environ.get("ALPACA_API_KEY") and os.environ.get("ALPACA_SECRET_KEY")):
        return "ALPACA_API_KEY / ALPACA_SECRET_KEY not set"
    return None


async def _poll(check: Any, timeout: float, interval: float = 2.0, *, what: str) -> Any:
    """Await `check()` returning truthy within `timeout`, else fail with context."""
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        result = await check()
        if result:
            return result
        if asyncio.get_running_loop().time() > deadline:
            pytest.fail(f"timed out after {timeout:.0f}s waiting for: {what}")
        await asyncio.sleep(interval)


class _EngineProc:
    """The real engine as a child process, torn down hard on exit.

    Logs go to a TEMP FILE, never a PIPE: an undrained 64KB pipe fills under
    DEBUG logging within a minute and the child's blocking stdout write would
    freeze the entire engine mid-test (review finding). ``start_new_session``
    + process-group kill means a SIGKILL'd pytest cannot orphan a live trading
    process against the paper account.
    """

    def __init__(self, env: dict[str, str]) -> None:
        backend_dir = Path(__file__).resolve().parents[2]
        self._log = tempfile.NamedTemporaryFile(  # noqa: SIM115 — lifetime spans the test
            mode="w+b", prefix="roigen-e2e-engine-", suffix=".log", delete=False
        )
        self._proc = subprocess.Popen(  # noqa: S603 — our own module, our own env
            [sys.executable, "-m", "app.engine_main"],
            cwd=backend_dir,
            env=env,
            stdout=self._log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

    def stop(self) -> str:
        if self._proc.poll() is None:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(os.getpgid(self._proc.pid), signal.SIGTERM)
            try:
                # The engine logs `engine.stopped` promptly but the process can
                # linger ~12s closing websocket TCP sessions; 15s left only ~3s
                # of margin (dress-rehearsal measurement), and a SIGKILL here
                # would look like a teardown bug in an otherwise-passing run.
                self._proc.wait(timeout=45)
            except subprocess.TimeoutExpired:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(os.getpgid(self._proc.pid), signal.SIGKILL)
                self._proc.wait(timeout=5)
        self._log.flush()
        out = Path(self._log.name).read_bytes().decode(errors="replace")
        self._log.close()
        return f"{out}\n(engine log file kept at {self._log.name})"

    @property
    def alive(self) -> bool:
        return self._proc.poll() is None


def _cli(env: dict[str, str], *args: str) -> subprocess.CompletedProcess[bytes]:
    backend_dir = Path(__file__).resolve().parents[2]
    return subprocess.run(  # noqa: S603
        [sys.executable, "-m", "app.cli", *args],
        cwd=backend_dir,
        env=env,
        capture_output=True,
        timeout=60,
        check=False,
    )


async def test_probe_trades_and_kill_switch_flattens_with_full_audit_trail(
    test_db_url: str, db_engine: Any
) -> None:
    reason = _env_gate()
    if reason:
        pytest.skip(reason)

    creds = BrokerCredentials(
        api_key=os.environ["ALPACA_API_KEY"],
        api_secret=os.environ["ALPACA_SECRET_KEY"],
        paper=True,
    )
    adapter = build_alpaca_adapter(creds)
    try:
        clock = await adapter.get_clock()
        if not clock.is_open:
            pytest.skip(f"market closed (next open {clock.next_open})")
        runway = clock.next_close - clock.timestamp
        if runway < timedelta(minutes=20):
            # Inside/near the close window the probe's entry would be
            # risk-blocked (flatten buffer) and the scheduled flatten would
            # race this test's kill-switch assertions.
            pytest.skip(f"only {runway} to the close — need 20m of runway")

        # Precondition: this paper account must be engine-only AND flat, or the
        # audit assertions below are polluted by leftovers.
        preexisting = await adapter.list_positions()
        if any(p.qty != 0 for p in preexisting):
            pytest.skip(f"paper account not flat: {[p.symbol for p in preexisting]}")

        engine = create_async_engine(test_db_url, poolclass=NullPool)
        factory = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with factory() as session:
                portfolio_id = await _seed(session)

            child_env = {
                **os.environ,
                "DATABASE_URL": test_db_url,
                "REDIS_URL": _REDIS_URL,
                "ENGINE_PORTFOLIO_ID": str(portfolio_id),
                "RECONCILE_INTERVAL_SECONDS": "60",
                "DEBUG": "true",
            }
            redis = aioredis.Redis.from_url(_REDIS_URL)
            proc = _EngineProc(child_env)
            try:
                await _run_scenario(factory, adapter, redis, child_env, portfolio_id, proc)
            finally:
                out = proc.stop()
                # The engine's own log is the best forensic artifact on failure.
                sys.stdout.write("\n──── engine subprocess log (tail) ────\n")
                sys.stdout.write(out[-8000:])
                await redis.aclose()
        finally:
            await engine.dispose()
    finally:
        await adapter.aclose()


async def _seed(session: AsyncSession) -> uuid.UUID:
    user = User(email="live-e2e@roigen.local", display_name="live e2e")
    session.add(user)
    await session.flush()
    portfolio = Portfolio(user_id=user.id, name="live-e2e", mode="paper", is_default=True)
    session.add(portfolio)
    await session.flush()
    session.add(
        Strategy(
            portfolio_id=portfolio.id,
            name="probe-e2e",
            kind="probe",
            status=StrategyStatus.paper.value,
            symbols=[_SYMBOL],
            params={"stop_pct": "0.003", "take_profit_pct": "0.006"},
        )
    )
    await session.commit()
    return portfolio.id


async def _run_scenario(
    factory: async_sessionmaker[AsyncSession],
    adapter: Any,
    redis: aioredis.Redis,
    env: dict[str, str],
    portfolio_id: uuid.UUID,
    proc: _EngineProc,
) -> None:
    heartbeat_key = f"engine:heartbeat:{portfolio_id}"

    async def _engine_up() -> bool:
        if not proc.alive:
            pytest.fail("engine subprocess died during boot")
        return await redis.get(heartbeat_key) is not None

    await _poll(_engine_up, _BOOT_TIMEOUT, what="engine heartbeat key")

    # ── Entry: the probe emits off a live bar; risk sizes it; a bracket lands.
    async def _entry_filled() -> Order | None:
        async with factory() as session:
            order = await session.scalar(
                select(Order).where(
                    Order.portfolio_id == portfolio_id,
                    Order.symbol == _SYMBOL,
                    Order.strategy_id.is_not(None),
                    # Bracket LEGS inherit strategy_id; without this the poll
                    # can latch onto a never-filling stop leg (review finding).
                    Order.parent_order_id.is_(None),
                )
            )
            if order is None or order.status != "filled":
                return None
            return order

    entry = await _poll(_entry_filled, _ENTRY_TIMEOUT, what="probe entry order filled")
    assert entry.order_class == "bracket"
    assert entry.risk_approval is not None
    assert entry.stop_price is not None  # iron law #4: the entry carried protection

    async with factory() as session:
        legs = (await session.scalars(select(Order).where(Order.parent_order_id == entry.id))).all()
        entry_fill_qty = await session.scalar(
            select(func.coalesce(func.sum(Fill.qty), 0)).where(Fill.order_id == entry.id)
        )
    assert legs, "bracket legs were not persisted locally"
    assert entry_fill_qty and entry_fill_qty > 0

    # ── Kill switch via the REAL CLI, while the bracket legs are still live
    # (the held-qty cancel/confirm race is the point, not an accident).
    flatten = _cli(env, "flatten", "--reason", "live-e2e kill switch")
    assert flatten.returncode == 0, flatten.stdout.decode() + flatten.stderr.decode()

    async def _flatten_verified() -> EngineCommand | None:
        async with factory() as session:
            cmd = await session.scalar(
                select(EngineCommand).order_by(EngineCommand.seq.desc()).limit(1)
            )
            if cmd is None or cmd.result != "flat_verified":
                return None
            return cmd

    cmd = await _poll(_flatten_verified, _FLATTEN_TIMEOUT, what="flatten flat_verified")
    assert cmd.applied_at is not None

    # flat_verified reads broker truth; the local row's fill lands via the
    # stream writer and can trail by a beat — poll it briefly before asserting.
    async def _liq_filled() -> bool:
        async with factory() as session:
            status = await session.scalar(
                select(Order.status).where(
                    Order.portfolio_id == portfolio_id,
                    Order.client_order_id.startswith("roigen-flatten"),
                )
            )
            return status == "filled"

    await _poll(_liq_filled, 60.0, what="liquidation order filled locally")

    # ── Broker truth: flat, and nothing working.
    assert await adapter.get_position(_SYMBOL) is None
    assert not [o for o in await adapter.list_orders(status="open")]

    # ── The audit chain, walked from rows alone.
    async with factory() as session:
        events = {
            e.event_type
            for e in (
                await session.scalars(select(EventLog).where(EventLog.portfolio_id == portfolio_id))
            ).all()
        }
        liq = await session.scalar(
            select(Order).where(
                Order.portfolio_id == portfolio_id,
                Order.client_order_id.startswith("roigen-flatten"),
            )
        )
        open_lots = await session.scalar(
            select(func.count())
            .select_from(Lot)
            .where(Lot.portfolio_id == portfolio_id, Lot.qty_open > 0)
        )
        closes = (
            await session.scalars(select(LotClose).where(LotClose.portfolio_id == portfolio_id))
        ).all()
        position = await session.scalar(
            select(Position).where(
                Position.portfolio_id == portfolio_id, Position.symbol == _SYMBOL
            )
        )

    for required in ("order.approved", "flatten.approved", "flatten.executed"):
        assert required in events, f"missing audit event {required}; have {sorted(events)}"
    assert "flatten.completed" in events or "flatten.partial" in events

    assert liq is not None, "liquidation order was not persisted"
    assert liq.status == "filled"
    assert liq.risk_approval is not None
    assert liq.risk_approval.get("command_seq") == cmd.seq  # commands → flatten → order linkage

    assert open_lots == 0, "FIFO lots did not fully close"
    assert closes, "no LotClose rows — realized P&L is missing"
    assert all(c.strategy_id is not None for c in closes), (
        "flatten closes lost strategy attribution — the day breaker would be blind"
    )
    assert position is None or position.qty == 0  # tracker deletes the row at flat
