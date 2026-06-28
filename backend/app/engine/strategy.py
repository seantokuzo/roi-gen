"""Strategy base class, registry, and the market-event runner.

A :class:`Strategy` reacts to market events and PROPOSES entries — ``(symbol,
side, entry, stop)`` — via :meth:`Strategy.emit_signal`. It never sizes and
never touches the broker: the Risk Engine sizes and the execution handler
submits (iron law #1). The identical subclass runs unchanged against the live
Redis feeds and the Phase-3 simulator, because both speak the same events on the
same bus.

The :class:`StrategyRegistry` maps the ``strategies.kind`` column to a class so
the engine can instantiate active strategies from DB rows. The
:class:`StrategyRunner` is the bus handler that routes market events (and fills)
to the strategies that asked for each symbol.
"""

from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING, Any

from app.core.logging import get_logger
from app.engine.events import (
    BarEvent,
    FillEvent,
    QuoteEvent,
    SignalEvent,
    TradeEvent,
)
from app.models.enums import OrderSide, OrderType, TimeInForce

if TYPE_CHECKING:
    import uuid
    from collections.abc import Callable, Iterable, Mapping
    from decimal import Decimal

    from app.brokers.dto import Bar, Quote, Trade
    from app.engine.bus import EventBus

log = get_logger("engine.strategy")


class Strategy:
    """Base for all strategies. Override the ``on_*`` hooks you need; they no-op.

    Subclasses MUST be cheap to construct and hold only their own state — the
    engine may build, start, and stop many of them. State that must survive a
    restart belongs in the DB, never in instance attributes.
    """

    def __init__(
        self,
        *,
        strategy_id: uuid.UUID,
        portfolio_id: uuid.UUID,
        symbols: Iterable[str],
        bus: EventBus,
        params: Mapping[str, Any] | None = None,
    ) -> None:
        self.strategy_id = strategy_id
        self.portfolio_id = portfolio_id
        self.symbols: tuple[str, ...] = tuple(symbols)
        self._bus = bus
        self.params: dict[str, Any] = dict(params or {})

    # ── Lifecycle / event hooks (override as needed) ─────────────────

    async def on_start(self) -> None:
        """Called once when the engine starts the strategy."""

    async def on_stop(self) -> None:
        """Called once when the engine stops the strategy."""

    async def on_bar(self, bar: Bar) -> None:
        """A completed bar for one of this strategy's symbols."""

    async def on_quote(self, quote: Quote) -> None:
        """A quote update for one of this strategy's symbols."""

    async def on_trade(self, trade: Trade) -> None:
        """A tape print for one of this strategy's symbols."""

    async def on_fill(self, fill: FillEvent) -> None:
        """A fill on one of this strategy's orders (Phase 2b producer)."""

    # ── Signal emission (the only output a strategy produces) ────────

    async def emit_signal(
        self,
        *,
        symbol: str,
        side: OrderSide,
        entry_price: Decimal,
        stop_price: Decimal,
        take_profit_price: Decimal | None = None,
        order_type: OrderType = OrderType.market,
        time_in_force: TimeInForce = TimeInForce.day,
        extended_hours: bool = False,
        meta: Mapping[str, Any] | None = None,
    ) -> SignalEvent:
        """Propose a protected entry. The Risk Engine sizes it or rejects it.

        ``stop_price`` is mandatory — a position without a stop is a bug (iron
        law #4) and fixed-fractional sizing needs the entry-to-stop distance.
        """
        signal = SignalEvent(
            portfolio_id=self.portfolio_id,
            strategy_id=self.strategy_id,
            symbol=symbol,
            side=side,
            entry_price=entry_price,
            stop_price=stop_price,
            take_profit_price=take_profit_price,
            order_type=order_type,
            time_in_force=time_in_force,
            extended_hours=extended_hours,
            meta=dict(meta or {}),
        )
        await self._bus.publish(signal)
        log.info(
            "engine.strategy.signal",
            strategy_id=str(self.strategy_id),
            symbol=symbol,
            side=side.value,
            entry=str(entry_price),
            stop=str(stop_price),
        )
        return signal


class StrategyRegistry:
    """Maps a ``strategies.kind`` key to a :class:`Strategy` subclass."""

    def __init__(self) -> None:
        self._kinds: dict[str, type[Strategy]] = {}

    def register(self, kind: str) -> Callable[[type[Strategy]], type[Strategy]]:
        """Decorator: register a strategy class under ``kind``."""

        def decorate(cls: type[Strategy]) -> type[Strategy]:
            if kind in self._kinds:
                msg = f"strategy kind {kind!r} already registered to {self._kinds[kind].__name__}"
                raise ValueError(msg)
            self._kinds[kind] = cls
            return cls

        return decorate

    def create(
        self,
        kind: str,
        *,
        strategy_id: uuid.UUID,
        portfolio_id: uuid.UUID,
        symbols: Iterable[str],
        bus: EventBus,
        params: Mapping[str, Any] | None = None,
    ) -> Strategy:
        """Instantiate the strategy registered under ``kind``."""
        try:
            cls = self._kinds[kind]
        except KeyError as exc:
            msg = f"unknown strategy kind {kind!r} (registered: {sorted(self._kinds)})"
            raise KeyError(msg) from exc
        return cls(
            strategy_id=strategy_id,
            portfolio_id=portfolio_id,
            symbols=symbols,
            bus=bus,
            params=params,
        )

    def kinds(self) -> list[str]:
        return sorted(self._kinds)


# The process-wide default registry. Strategy modules register against this.
registry = StrategyRegistry()


class StrategyRunner:
    """Bus handler that routes market events (and fills) to strategies by symbol."""

    def __init__(self, bus: EventBus) -> None:
        self._bus = bus
        self._strategies: list[Strategy] = []
        self._by_symbol: dict[str, list[Strategy]] = defaultdict(list)
        self._by_id: dict[uuid.UUID, Strategy] = {}

    def add(self, strategy: Strategy) -> None:
        self._strategies.append(strategy)
        self._by_id[strategy.strategy_id] = strategy
        for symbol in strategy.symbols:
            self._by_symbol[symbol].append(strategy)

    def register_handlers(self) -> None:
        """Subscribe to the market + fill events strategies consume."""
        self._bus.subscribe(BarEvent, self._on_bar)
        self._bus.subscribe(QuoteEvent, self._on_quote)
        self._bus.subscribe(TradeEvent, self._on_trade)
        self._bus.subscribe(FillEvent, self._on_fill)

    async def start(self) -> None:
        for strategy in self._strategies:
            await strategy.on_start()

    async def stop(self) -> None:
        for strategy in self._strategies:
            await strategy.on_stop()

    async def _on_bar(self, event: BarEvent) -> None:
        for strategy in self._by_symbol.get(event.symbol, ()):
            await strategy.on_bar(event.bar)

    async def _on_quote(self, event: QuoteEvent) -> None:
        for strategy in self._by_symbol.get(event.symbol, ()):
            await strategy.on_quote(event.quote)

    async def _on_trade(self, event: TradeEvent) -> None:
        for strategy in self._by_symbol.get(event.symbol, ()):
            await strategy.on_trade(event.trade)

    async def _on_fill(self, event: FillEvent) -> None:
        # Prefer exact strategy routing; fall back to symbol for fills with no id.
        if event.strategy_id is not None:
            strategy = self._by_id.get(event.strategy_id)
            if strategy is not None:
                await strategy.on_fill(event)
            return
        for strategy in self._by_symbol.get(event.symbol, ()):
            await strategy.on_fill(event)
