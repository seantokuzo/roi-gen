"""RiskState — the snapshot of inputs the Risk Engine evaluates a signal against.

:class:`RiskEngine` is a *pure* function of ``(signal, state)``. All the IO —
the broker account/clock reads and the DB queries that feed the controls — lives
here in :class:`RiskStateProvider`, so the engine itself stays trivially unit
testable and deterministic.

Day boundaries for P&L and limits are the **ET calendar day** (iron law #5): we
read the broker clock's instant, find its ET date, and filter closed lots from
ET-midnight onward. P&L history comes from the ``lots`` table — the same
``realized_pnl`` column legacy declared but never computed (the FIFO engine that
populates it lands in Phase 2b; until then these queries correctly return zero).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from sqlalchemy import ColumnElement, func, select

from app.models.enums import StrategyStatus
from app.models.strategy import Strategy
from app.models.telemetry import EquitySnapshot
from app.models.trading import Lot

if TYPE_CHECKING:
    import uuid

    from sqlalchemy.ext.asyncio import AsyncSession

    from app.brokers.base import BrokerAdapter

ET = ZoneInfo("America/New_York")

# How many recent closed lots to scan when counting a losing streak. Far above
# any sane consecutive-loss limit; bounds the query without truncating signal.
_CONSECUTIVE_SCAN_LIMIT = 64


def et_day_start_utc(instant: datetime) -> datetime:
    """Return ET-midnight (start of ``instant``'s ET calendar day) as UTC.

    The day boundary for P&L and daily limits is the ET calendar day, not the
    9:30 session open and not UTC midnight (iron law #5).
    """
    et_now = instant.astimezone(ET)
    et_midnight = et_now.replace(hour=0, minute=0, second=0, microsecond=0)
    return et_midnight.astimezone(UTC)


@dataclass(frozen=True, slots=True)
class RiskState:
    """Everything the controls need, captured at one instant for one signal."""

    portfolio_id: uuid.UUID
    strategy_id: uuid.UUID
    symbol: str
    # ── market clock (broker truth) ──────────────────────────────────
    now: datetime
    market_open: bool
    next_close: datetime
    # ── account snapshot ─────────────────────────────────────────────
    equity: Decimal
    last_equity: Decimal
    cash: Decimal
    buying_power: Decimal
    position_market_value: Decimal
    trading_blocked: bool
    account_blocked: bool
    # ── strategy config ──────────────────────────────────────────────
    strategy_proven: bool
    strategy_risk_pct: Decimal | None
    strategy_max_positions: int | None
    # ── derived position / P&L state ─────────────────────────────────
    strategy_open_qty: Decimal
    open_positions_count: int
    day_realized_pnl_strategy: Decimal
    consecutive_losses: int
    peak_equity: Decimal
    last_entry_at: datetime | None
    # ── operator control ─────────────────────────────────────────────
    trading_halted: bool

    @property
    def day_pnl_portfolio(self) -> Decimal:
        """Whole-account day P&L (realized + unrealized) — equity vs prior close.

        Alpaca's ``last_equity`` is equity at the previous session close, so
        ``equity − last_equity`` is the total day move including open positions —
        the right trigger for a portfolio-wide daily-loss halt.
        """
        return self.equity - self.last_equity

    @property
    def drawdown_pct(self) -> Decimal:
        """Portfolio peak-to-trough drawdown as a percentage (0 if no peak)."""
        if self.peak_equity <= 0:
            return Decimal("0")
        return (self.peak_equity - self.equity) / self.peak_equity * Decimal("100")


class RiskStateProvider:
    """Loads a :class:`RiskState` from the broker + DB. Stateless; reuse freely.

    Two broker reads per evaluation (``get_account`` + ``get_clock``); fine at
    single-strategy signal cadence and well under Alpaca's 200 req/min. A later
    phase can inject a per-tick cached account/clock if fan-out grows.
    """

    async def load(
        self,
        session: AsyncSession,
        adapter: BrokerAdapter,
        *,
        portfolio_id: uuid.UUID,
        strategy_id: uuid.UUID,
        symbol: str,
        trading_halted: bool = False,
    ) -> RiskState:
        account = await adapter.get_account()
        clock = await adapter.get_clock()
        et_start = et_day_start_utc(clock.timestamp)

        strategy = await session.get(Strategy, strategy_id)
        strategy_proven = strategy is not None and strategy.status == StrategyStatus.live
        strategy_risk_pct = strategy.risk_per_trade_pct if strategy is not None else None
        strategy_max_positions = strategy.max_positions if strategy is not None else None

        day_realized_strategy = await self._sum_realized(
            session,
            Lot.portfolio_id == portfolio_id,
            Lot.strategy_id == strategy_id,
            Lot.closed_at >= et_start,
        )
        open_positions_count = await self._count_open_symbols(session, portfolio_id, strategy_id)
        consecutive_losses = await self._consecutive_losses(session, portfolio_id, strategy_id)
        strategy_open_qty = await self._strategy_open_qty(
            session, portfolio_id, strategy_id, symbol
        )
        last_entry_at = await self._last_entry_at(session, portfolio_id, strategy_id, symbol)
        peak_equity = await self._peak_equity(session, portfolio_id, account.equity)

        return RiskState(
            portfolio_id=portfolio_id,
            strategy_id=strategy_id,
            symbol=symbol,
            now=clock.timestamp,
            market_open=clock.is_open,
            next_close=clock.next_close,
            equity=account.equity,
            last_equity=account.last_equity,
            cash=account.cash,
            buying_power=account.buying_power,
            position_market_value=account.position_market_value,
            trading_blocked=account.trading_blocked,
            account_blocked=account.account_blocked,
            strategy_proven=strategy_proven,
            strategy_risk_pct=strategy_risk_pct,
            strategy_max_positions=strategy_max_positions,
            strategy_open_qty=strategy_open_qty,
            open_positions_count=open_positions_count,
            day_realized_pnl_strategy=day_realized_strategy,
            consecutive_losses=consecutive_losses,
            peak_equity=peak_equity,
            last_entry_at=last_entry_at,
            trading_halted=trading_halted,
        )

    @staticmethod
    async def _sum_realized(session: AsyncSession, *conditions: ColumnElement[bool]) -> Decimal:
        stmt = select(func.coalesce(func.sum(Lot.realized_pnl), 0)).where(*conditions)
        # coalesce(..., 0) makes this non-null at runtime; the guard is for mypy,
        # which types Result.scalar() as Optional regardless.
        total: Decimal | None = (await session.execute(stmt)).scalar()
        return total if total is not None else Decimal("0")

    @staticmethod
    async def _count_open_symbols(
        session: AsyncSession, portfolio_id: uuid.UUID, strategy_id: uuid.UUID
    ) -> int:
        stmt = select(func.count(func.distinct(Lot.symbol))).where(
            Lot.portfolio_id == portfolio_id,
            Lot.strategy_id == strategy_id,
            Lot.qty_open > 0,
        )
        val = (await session.execute(stmt)).scalar()
        return int(val) if val is not None else 0

    @staticmethod
    async def _consecutive_losses(
        session: AsyncSession, portfolio_id: uuid.UUID, strategy_id: uuid.UUID
    ) -> int:
        stmt = (
            select(Lot.realized_pnl)
            .where(
                Lot.portfolio_id == portfolio_id,
                Lot.strategy_id == strategy_id,
                Lot.closed_at.is_not(None),
            )
            .order_by(Lot.closed_at.desc())
            .limit(_CONSECUTIVE_SCAN_LIMIT)
        )
        recent = (await session.execute(stmt)).scalars().all()
        streak = 0
        for pnl in recent:
            if pnl < 0:
                streak += 1
            else:
                break
        return streak

    @staticmethod
    async def _strategy_open_qty(
        session: AsyncSession, portfolio_id: uuid.UUID, strategy_id: uuid.UUID, symbol: str
    ) -> Decimal:
        """This strategy's gross open quantity in ``symbol`` (sum of open lots).

        Per-strategy: lots carry ``strategy_id`` (the positions table does not),
        so the no-pyramiding guard scopes to the strategy — consistent with the
        open-positions count, and so one strategy holding a symbol never blocks
        another from trading it.
        """
        stmt = select(func.coalesce(func.sum(Lot.qty_open), 0)).where(
            Lot.portfolio_id == portfolio_id,
            Lot.strategy_id == strategy_id,
            Lot.symbol == symbol,
            Lot.qty_open > 0,
        )
        val = (await session.execute(stmt)).scalar()
        return Decimal(val) if val is not None else Decimal("0")

    @staticmethod
    async def _last_entry_at(
        session: AsyncSession, portfolio_id: uuid.UUID, strategy_id: uuid.UUID, symbol: str
    ) -> datetime | None:
        stmt = select(func.max(Lot.opened_at)).where(
            Lot.portfolio_id == portfolio_id,
            Lot.strategy_id == strategy_id,
            Lot.symbol == symbol,
        )
        result: datetime | None = (await session.execute(stmt)).scalar()
        return result

    @staticmethod
    async def _peak_equity(
        session: AsyncSession, portfolio_id: uuid.UUID, current_equity: Decimal
    ) -> Decimal:
        stmt = select(func.max(EquitySnapshot.equity)).where(
            EquitySnapshot.portfolio_id == portfolio_id
        )
        val = (await session.execute(stmt)).scalar()
        peak = Decimal(val) if val is not None else current_equity
        # Current equity can exceed every recorded snapshot (new high since last
        # snapshot) — the peak is at least where we are now.
        return max(peak, current_equity)
