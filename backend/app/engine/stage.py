"""RiskStage — the bus seam where a signal becomes an order (iron law #1).

Every order path converges here: this handler is the single place a
``SignalEvent`` is turned into an ``OrderEvent``, and it cannot do so without a
:class:`~app.engine.risk.approval.RiskApproval`. On each signal it loads the
:class:`~app.engine.risk.state.RiskState` (broker + DB), runs the pure
:class:`~app.engine.risk.engine.RiskEngine`, writes an audit row either way, and
on approval publishes the ``OrderEvent`` carrying the broker order request plus
its approval.

The stage owns its transaction: it is a daemon worker, not a request handler, so
it commits its own unit of work per signal (unlike ``get_db``, which leaves
commits to endpoints).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from app.core.logging import get_logger
from app.engine.events import OrderEvent, SignalEvent
from app.models.enums import EventSource
from app.models.telemetry import EventLog

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from app.brokers.base import BrokerAdapter
    from app.engine.bus import EventBus
    from app.engine.risk.engine import RiskEngine
    from app.engine.risk.state import RiskStateProvider

log = get_logger("engine.risk")


class RiskStage:
    """Subscribes to ``SignalEvent`` and runs the Risk Engine on each one.

    ``halted`` lets the kill switch (Phase 2c) feed a "trading frozen" flag into
    every evaluation without this stage knowing how the switch is implemented;
    it defaults to never-halted.
    """

    def __init__(
        self,
        *,
        bus: EventBus,
        engine: RiskEngine,
        provider: RiskStateProvider,
        session_factory: async_sessionmaker[AsyncSession],
        adapter: BrokerAdapter,
        halted: Callable[[], bool] | None = None,
    ) -> None:
        self._bus = bus
        self._engine = engine
        self._provider = provider
        self._session_factory = session_factory
        self._adapter = adapter
        self._halted: Callable[[], bool] = halted if halted is not None else _never_halted

    def register_handlers(self) -> None:
        self._bus.subscribe(SignalEvent, self._on_signal)

    async def _on_signal(self, signal: SignalEvent) -> None:
        async with self._session_factory() as session:
            state = await self._provider.load(
                session,
                self._adapter,
                portfolio_id=signal.portfolio_id,
                strategy_id=signal.strategy_id,
                symbol=signal.symbol,
                trading_halted=self._halted(),
            )
            decision = self._engine.evaluate(signal, state)

            if (
                decision.approved
                and decision.approval is not None
                and decision.order_request is not None
            ):
                session.add(
                    EventLog(
                        source=EventSource.engine.value,
                        event_type="order.approved",
                        portfolio_id=signal.portfolio_id,
                        strategy_id=signal.strategy_id,
                        payload=decision.approval.audit_payload(),
                    )
                )
                await session.commit()
                log.info(
                    "engine.risk.approved",
                    strategy_id=str(signal.strategy_id),
                    symbol=signal.symbol,
                    side=signal.side.value,
                    qty=str(decision.approval.qty),
                    client_order_id=decision.approval.client_order_id,
                )
                await self._bus.publish(
                    OrderEvent(order_request=decision.order_request, approval=decision.approval)
                )
                return

            session.add(
                EventLog(
                    source=EventSource.engine.value,
                    event_type="order.rejected",
                    portfolio_id=signal.portfolio_id,
                    strategy_id=signal.strategy_id,
                    payload={
                        "signal_id": str(signal.signal_id),
                        "symbol": signal.symbol,
                        "side": signal.side.value,
                        "reason": decision.reason,
                        "checks": [c.to_dict() for c in decision.checks],
                    },
                )
            )
            await session.commit()
            log.info(
                "engine.risk.rejected",
                strategy_id=str(signal.strategy_id),
                symbol=signal.symbol,
                reason=decision.reason,
            )


def _never_halted() -> bool:
    return False
