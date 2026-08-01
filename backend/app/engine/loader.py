"""DB → engine strategy loading, gated by the lifecycle law (iron law #8).

The engine never trades a strategy row whose lifecycle status hasn't earned
its portfolio's mode: a **live** portfolio loads only ``status == live`` rows;
a **paper** portfolio loads ``paper`` and ``live`` (a live-proven strategy may
keep paper-trading). ``draft`` / ``backtesting`` / ``paused`` / ``stopped``
NEVER load.

Coverage note (Phase 4 dependency): a ``paused`` / ``stopped`` strategy's
still-open positions are protected only by the portfolio-wide flatten — which
is sufficient *only while every strategy is a day sleeve* the FlattenController
closes by end of session. When swing sleeves arrive (Phase 4 sleeve-typing),
pausing a strategy with open swing positions needs its own story.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select

from app.core.logging import get_logger
from app.engine.strategy import registry
from app.models.enums import PortfolioMode, StrategyStatus
from app.models.strategy import Strategy as StrategyModel

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from app.engine.bus import EventBus
    from app.engine.strategy import Strategy
    from app.models.portfolio import Portfolio

log = get_logger("engine.loader")

_LIVE_STATUSES: tuple[StrategyStatus, ...] = (StrategyStatus.live,)
_PAPER_STATUSES: tuple[StrategyStatus, ...] = (StrategyStatus.paper, StrategyStatus.live)


async def load_active_strategies(
    session: AsyncSession,
    portfolio: Portfolio,
    *,
    bus: EventBus,
    session_factory: async_sessionmaker[AsyncSession],
) -> list[Strategy]:
    """Instantiate every tradeable strategy row for ``portfolio``.

    A row the registry can't build — unknown ``kind``, or params its class
    rejects — is logged at error and skipped: the engine boots with the gap
    visible instead of dying on one bad row. ``session_factory`` is forwarded
    to kinds that declare they want it (see
    :meth:`app.engine.strategy.StrategyRegistry.create`).
    """
    allowed = _LIVE_STATUSES if portfolio.mode == PortfolioMode.live else _PAPER_STATUSES
    rows = (
        (
            await session.execute(
                select(StrategyModel)
                .where(
                    StrategyModel.portfolio_id == portfolio.id,
                    StrategyModel.status.in_([status.value for status in allowed]),
                )
                .order_by(StrategyModel.created_at)
            )
        )
        .scalars()
        .all()
    )

    loaded: list[Strategy] = []
    for row in rows:
        try:
            strategy = registry.create(
                row.kind,
                strategy_id=row.id,
                portfolio_id=portfolio.id,
                symbols=row.symbols,
                bus=bus,
                params=row.params,
                session_factory=session_factory,
            )
        except KeyError:
            log.error(
                "engine.loader.unknown_kind",
                kind=row.kind,
                strategy_id=str(row.id),
                name=row.name,
                registered=registry.kinds(),
            )
            continue
        except (ValueError, TypeError, ArithmeticError) as exc:
            # Bad params (e.g. Decimal("garbage") → InvalidOperation): skip the
            # row, keep the engine up. Anything else is a real bug — let it raise.
            log.error(
                "engine.loader.bad_params",
                kind=row.kind,
                strategy_id=str(row.id),
                name=row.name,
                error=str(exc),
            )
            continue
        loaded.append(strategy)
        log.info(
            "engine.loader.loaded",
            kind=row.kind,
            strategy_id=str(row.id),
            name=row.name,
            status=row.status,
            symbols=list(row.symbols),
        )
    return loaded
