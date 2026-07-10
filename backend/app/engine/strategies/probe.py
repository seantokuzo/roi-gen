"""Probe strategy — the smallest honest end-to-end entry generator.

On the first bar it sees for each of its symbols on each **ET trading date**
it emits exactly one protected BUY signal (market entry referenced at the bar
close, stop below, take-profit above — iron law #4), then goes quiet until the
next ET day. It exists to exercise the full live path — signal → risk sizing →
bracket submit → trade-updates → lots — with real money mechanics and minimal
opinion.

Dedup key is the **ET date** (same day-boundary convention as
:func:`app.engine.risk.state.et_day_start_utc`): a UTC-date key would treat a
20:05 ET extended-hours bar as a new session and re-enter.

The per-session entry bound is **DB-derived**, not process memory: on the
first bar of each ET day (per symbol) the strategy counts today's entry
``Order`` rows for ``(strategy_id, symbol)`` and seeds its counter from that,
so a restart cannot re-enter beyond ``max_entries_per_session``. Honest
residual: a crash loop that dies *before* persisting an order row is NOT
bounded by this — the real crash-loop bounds are the loss controls
(daily-loss breaker at 2.5× per-trade risk, consecutive-losses halt at 4),
not the no-pyramiding count.

Params (JSONB → parsed ``Decimal(str(x))``; JSONB numbers arrive as float and
``Decimal(float)`` would mint artifacts — iron law #7):

- ``stop_pct``: stop distance as a fraction of entry (default ``0.005``).
- ``take_profit_pct``: TP distance as a fraction of entry (default ``0.01``;
  absent → default, explicitly falsy (``null``/``0``/``false``) → no TP leg).
- ``max_entries_per_session``: entries allowed per symbol per ET day
  (default ``1``).
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from sqlalchemy import func, select

from app.brokers.pricing import quantize_price
from app.core.logging import get_logger
from app.engine.risk.state import ET, et_day_start_utc
from app.engine.strategy import Strategy, registry
from app.models.enums import OrderSide, OrderType, TimeInForce
from app.models.trading import Order

if TYPE_CHECKING:
    import uuid
    from collections.abc import Iterable, Mapping
    from datetime import date, datetime

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from app.brokers.dto import Bar
    from app.engine.bus import EventBus

log = get_logger("engine.strategies.probe")

DEFAULT_STOP_PCT = Decimal("0.005")
DEFAULT_TAKE_PROFIT_PCT = Decimal("0.01")
DEFAULT_MAX_ENTRIES_PER_SESSION = 1

_ONE = Decimal("1")
_ABSENT = object()


@dataclass
class _DayState:
    """Per-symbol session state: which ET date it covers and entries so far."""

    et_date: date
    entries: int


@registry.register("probe")
class ProbeStrategy(Strategy):
    """One protected BUY per symbol per ET trading date. See module docstring."""

    def __init__(
        self,
        *,
        strategy_id: uuid.UUID,
        portfolio_id: uuid.UUID,
        symbols: Iterable[str],
        bus: EventBus,
        params: Mapping[str, Any] | None = None,
        session_factory: async_sessionmaker[AsyncSession] | None = None,
    ) -> None:
        """``session_factory`` is optional: ``None`` (unit tests / backtests)
        means the session bound is pure in-memory; the loader passes the real
        factory so the bound survives restarts."""
        super().__init__(
            strategy_id=strategy_id,
            portfolio_id=portfolio_id,
            symbols=symbols,
            bus=bus,
            params=params,
        )
        self._session_factory = session_factory

        raw_stop = self.params.get("stop_pct")
        self.stop_pct: Decimal = DEFAULT_STOP_PCT if raw_stop is None else Decimal(str(raw_stop))

        raw_tp = self.params.get("take_profit_pct", _ABSENT)
        self.take_profit_pct: Decimal | None
        if raw_tp is _ABSENT:
            self.take_profit_pct = DEFAULT_TAKE_PROFIT_PCT
        elif not raw_tp:  # explicit null/0/false → no take-profit leg
            self.take_profit_pct = None
        else:
            self.take_profit_pct = Decimal(str(raw_tp))

        raw_max = self.params.get("max_entries_per_session")
        self.max_entries_per_session: int = (
            DEFAULT_MAX_ENTRIES_PER_SESSION if raw_max is None else int(raw_max)
        )

        if self.stop_pct <= 0:
            msg = f"probe stop_pct must be > 0, got {self.stop_pct}"
            raise ValueError(msg)
        if self.take_profit_pct is not None and self.take_profit_pct <= 0:
            msg = (
                "probe take_profit_pct must be > 0 (or falsy for no TP), "
                f"got {self.take_profit_pct}"
            )
            raise ValueError(msg)
        if self.max_entries_per_session < 1:
            msg = f"probe max_entries_per_session must be >= 1, got {self.max_entries_per_session}"
            raise ValueError(msg)

        # symbol → this ET day's state; rebuilt (with a DB seed) on day rollover.
        self._day_state: dict[str, _DayState] = {}

    # ── event hooks ──────────────────────────────────────────────────

    async def on_bar(self, bar: Bar) -> None:
        if bar.symbol not in self.symbols:
            return

        et_date = bar.timestamp.astimezone(ET).date()
        state = self._day_state.get(bar.symbol)
        if state is None or state.et_date != et_date:
            # First bar of this ET day for this symbol: seed the counter from
            # the DB exactly once (one query per symbol per day, cached here).
            entries = await self._entries_already_today(bar.symbol, bar.timestamp)
            state = _DayState(et_date=et_date, entries=entries)
            self._day_state[bar.symbol] = state

        if state.entries >= self.max_entries_per_session:
            return

        # Count before emitting: if the publish fails we lose an entry slot
        # rather than risking a double-emit on retry.
        state.entries += 1

        entry_price = bar.close
        stop_price = quantize_price(entry_price * (_ONE - self.stop_pct))
        take_profit_price = (
            quantize_price(entry_price * (_ONE + self.take_profit_pct))
            if self.take_profit_pct is not None
            else None
        )
        await self.emit_signal(
            symbol=bar.symbol,
            side=OrderSide.buy,
            entry_price=entry_price,
            stop_price=stop_price,
            take_profit_price=take_profit_price,
            order_type=OrderType.market,
            time_in_force=TimeInForce.day,
        )

    # ── DB-derived session bound ─────────────────────────────────────

    async def _entries_already_today(self, symbol: str, instant: datetime) -> int:
        """Count today's (ET) entry ``Order`` rows for this (strategy, symbol).

        Entry rows are parents only — ``parent_order_id IS NULL`` — because
        bracket legs inherit ``strategy_id`` from their parent and would
        triple-count a single entry. All statuses count (a ``failed`` or
        rejected attempt still consumed an entry slot: conservative under
        ambiguity). With no ``session_factory`` (unit tests / backtests) the
        bound is in-memory only and this returns 0.
        """
        if self._session_factory is None:
            return 0
        day_start = et_day_start_utc(instant)
        stmt = (
            select(func.count())
            .select_from(Order)
            .where(
                Order.strategy_id == self.strategy_id,
                Order.symbol == symbol,
                Order.parent_order_id.is_(None),
                Order.created_at >= day_start,
            )
        )
        async with self._session_factory() as session:
            count = int((await session.execute(stmt)).scalar_one())
        if count:
            log.info(
                "engine.strategies.probe.session_bound_seeded",
                strategy_id=str(self.strategy_id),
                symbol=symbol,
                entries_today=count,
            )
        return count
