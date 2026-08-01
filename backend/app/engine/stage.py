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
from app.engine.events import FlattenEvent, FlattenOrderEvent, OrderEvent, SignalEvent
from app.models.enums import EventSource
from app.models.telemetry import EventLog

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from app.brokers.base import BrokerAdapter
    from app.engine.bus import EventBus
    from app.engine.risk.approval import RiskDecision
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
        self._bus.subscribe(FlattenEvent, self._on_flatten)

    async def _on_signal(self, signal: SignalEvent) -> None:
        try:
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
                await self._route(session, signal, decision)
        except Exception as exc:  # noqa: BLE001 — a failed signal must be auditable, not silent
            log.error(
                "engine.risk.signal_error",
                strategy_id=str(signal.strategy_id),
                symbol=signal.symbol,
                error=repr(exc),
            )
            await self._audit_error(signal, exc)

    async def _route(
        self, session: AsyncSession, signal: SignalEvent, decision: RiskDecision
    ) -> None:
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

    async def _on_flatten(self, event: FlattenEvent) -> None:
        """Authorize a flatten and hand it to execution — WITHOUT the halted gate.

        Flatten must run under the kill switch, under feed-staleness, and while
        entries are frozen: it is the mechanism those states rely on to get the
        book safe. ``authorize_flatten`` is pure on the event (no state load),
        so a broker/DB outage cannot block the safety path here — only the
        audit write can fail, and that failure is itself audited.
        """
        try:
            decision = self._engine.authorize_flatten(event)
            async with self._session_factory() as session:
                if decision.approved and decision.approval is not None:
                    session.add(
                        EventLog(
                            source=EventSource.engine.value,
                            event_type="flatten.approved",
                            portfolio_id=event.portfolio_id,
                            payload=decision.approval.audit_payload(),
                        )
                    )
                    await session.commit()
                    log.info(
                        "engine.risk.flatten_approved",
                        flatten_id=str(event.flatten_id),
                        source=event.source,
                        reason=event.reason,
                    )
                    await self._bus.publish(FlattenOrderEvent(approval=decision.approval))
                    return
                session.add(
                    EventLog(
                        source=EventSource.engine.value,
                        event_type="flatten.rejected",
                        portfolio_id=event.portfolio_id,
                        payload={
                            "flatten_id": str(event.flatten_id),
                            "source": event.source,
                            "reason": decision.reason,
                            "checks": [c.to_dict() for c in decision.checks],
                        },
                    )
                )
                await session.commit()
                # A rejected flatten is a wiring bug, not a market condition.
                log.error(
                    "engine.risk.flatten_rejected",
                    flatten_id=str(event.flatten_id),
                    source=event.source,
                    reason=decision.reason,
                )
        except Exception as exc:  # noqa: BLE001 — the safety path must be auditable, not silent
            log.error(
                "engine.risk.flatten_error",
                flatten_id=str(event.flatten_id),
                source=event.source,
                error=repr(exc),
            )
            await self._audit_flatten_error(event, exc)

    async def _audit_flatten_error(self, event: FlattenEvent, exc: Exception) -> None:
        """Mirror of :meth:`_audit_error` for the flatten path.

        The FlattenController re-drives from durable state on its next tick, so
        an error here costs one tick of latency, never the intent — but it must
        still leave a row.
        """
        try:
            async with self._session_factory() as session:
                session.add(
                    EventLog(
                        source=EventSource.engine.value,
                        event_type="flatten.error",
                        portfolio_id=event.portfolio_id,
                        payload={
                            "flatten_id": str(event.flatten_id),
                            "source": event.source,
                            "reason": event.reason,
                            "error": repr(exc),
                        },
                    )
                )
                await session.commit()
        except Exception:  # noqa: BLE001 — last-ditch; never raise out of the audit path
            log.exception(
                "engine.risk.flatten_error_audit_failed", flatten_id=str(event.flatten_id)
            )

    async def _audit_error(self, signal: SignalEvent, exc: Exception) -> None:
        """Record a signal that failed mid-evaluation (broker/DB error) as a
        first-class ``order.error`` event rather than a vanished log line.

        Retry + alerting metrics are a live-readiness concern (Phase 9); here we
        only guarantee the drop is auditable. A fresh session is used in case the
        original was poisoned by the failure; if even this write fails we log and
        give up rather than raise out of the error path.
        """
        try:
            async with self._session_factory() as session:
                session.add(
                    EventLog(
                        source=EventSource.engine.value,
                        event_type="order.error",
                        portfolio_id=signal.portfolio_id,
                        strategy_id=signal.strategy_id,
                        payload={
                            "signal_id": str(signal.signal_id),
                            "symbol": signal.symbol,
                            "side": signal.side.value,
                            "error": repr(exc),
                        },
                    )
                )
                await session.commit()
        except Exception:  # noqa: BLE001 — last-ditch; never raise out of the audit path
            log.exception("engine.risk.error_audit_failed", strategy_id=str(signal.strategy_id))


def _never_halted() -> bool:
    return False
