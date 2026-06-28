"""Strategy base, registry, and runner routing."""

from __future__ import annotations

import uuid
from decimal import Decimal
from typing import Any

import pytest

from app.engine.bus import EventBus
from app.engine.events import FillEvent, SignalEvent
from app.engine.strategy import Strategy, StrategyRegistry, StrategyRunner
from app.models.enums import OrderSide, OrderType
from tests.engine.builders import make_bar, make_quote, make_trade


class _RecordingStrategy(Strategy):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.bars: list[Any] = []
        self.quotes: list[Any] = []
        self.trades: list[Any] = []
        self.fills: list[FillEvent] = []
        self.started = False
        self.stopped = False

    async def on_start(self) -> None:
        self.started = True

    async def on_stop(self) -> None:
        self.stopped = True

    async def on_bar(self, bar: Any) -> None:
        self.bars.append(bar)

    async def on_quote(self, quote: Any) -> None:
        self.quotes.append(quote)

    async def on_trade(self, trade: Any) -> None:
        self.trades.append(trade)

    async def on_fill(self, fill: FillEvent) -> None:
        self.fills.append(fill)


def _make_strategy(bus: EventBus, *, symbols: tuple[str, ...] = ("SPY",)) -> _RecordingStrategy:
    return _RecordingStrategy(
        strategy_id=uuid.uuid4(),
        portfolio_id=uuid.uuid4(),
        symbols=symbols,
        bus=bus,
    )


async def test_emit_signal_builds_and_publishes_signal() -> None:
    bus = EventBus()
    strategy = _make_strategy(bus)
    captured: list[SignalEvent] = []

    async def capture(event: SignalEvent) -> None:
        captured.append(event)

    bus.subscribe(SignalEvent, capture)

    returned = await strategy.emit_signal(
        symbol="SPY",
        side=OrderSide.buy,
        entry_price=Decimal("100"),
        stop_price=Decimal("99"),
        take_profit_price=Decimal("103"),
        order_type=OrderType.limit,
    )
    await bus.drain()

    assert returned.portfolio_id == strategy.portfolio_id
    assert returned.strategy_id == strategy.strategy_id
    assert returned.side is OrderSide.buy
    assert returned.entry_price == Decimal("100")
    assert returned.stop_price == Decimal("99")
    assert returned.take_profit_price == Decimal("103")
    assert returned.order_type is OrderType.limit
    assert captured == [returned]


# ── Registry ─────────────────────────────────────────────────────────


async def test_registry_create_instantiates_registered_kind() -> None:
    registry = StrategyRegistry()
    registry.register("recording")(_RecordingStrategy)
    bus = EventBus()

    sid, pid = uuid.uuid4(), uuid.uuid4()
    strategy = registry.create(
        "recording", strategy_id=sid, portfolio_id=pid, symbols=["SPY", "QQQ"], bus=bus
    )

    assert isinstance(strategy, _RecordingStrategy)
    assert strategy.strategy_id == sid
    assert strategy.symbols == ("SPY", "QQQ")
    assert registry.kinds() == ["recording"]


async def test_registry_rejects_duplicate_kind() -> None:
    registry = StrategyRegistry()
    registry.register("dup")(_RecordingStrategy)
    with pytest.raises(ValueError, match="already registered"):
        registry.register("dup")(_RecordingStrategy)


async def test_registry_unknown_kind_raises() -> None:
    registry = StrategyRegistry()
    with pytest.raises(KeyError, match="unknown strategy kind"):
        registry.create(
            "nope",
            strategy_id=uuid.uuid4(),
            portfolio_id=uuid.uuid4(),
            symbols=["SPY"],
            bus=EventBus(),
        )


# ── Runner routing ───────────────────────────────────────────────────


async def test_runner_routes_market_events_by_symbol() -> None:
    bus = EventBus()
    spy = _make_strategy(bus, symbols=("SPY",))
    qqq = _make_strategy(bus, symbols=("QQQ",))
    runner = StrategyRunner(bus)
    runner.add(spy)
    runner.add(qqq)
    runner.register_handlers()

    from app.engine.events import BarEvent, QuoteEvent, TradeEvent

    await bus.publish(BarEvent(make_bar("SPY")))
    await bus.publish(QuoteEvent(make_quote("SPY")))
    await bus.publish(TradeEvent(make_trade("QQQ")))
    await bus.drain()

    assert len(spy.bars) == 1
    assert len(spy.quotes) == 1
    assert spy.trades == []
    assert len(qqq.trades) == 1
    assert qqq.bars == []


async def test_runner_routes_fill_by_strategy_id() -> None:
    bus = EventBus()
    a = _make_strategy(bus, symbols=("SPY",))
    b = _make_strategy(bus, symbols=("SPY",))
    runner = StrategyRunner(bus)
    runner.add(a)
    runner.add(b)
    runner.register_handlers()

    fill = FillEvent(
        portfolio_id=a.portfolio_id,
        order_id=uuid.uuid4(),
        symbol="SPY",
        side=OrderSide.buy,
        qty=Decimal("10"),
        price=Decimal("100"),
        occurred_at=make_bar().timestamp,
        strategy_id=a.strategy_id,
    )
    await bus.publish(fill)
    await bus.drain()

    # Routed precisely to strategy a (by id), not the symbol-mate b.
    assert len(a.fills) == 1
    assert b.fills == []


async def test_runner_start_stop_invokes_lifecycle_hooks() -> None:
    bus = EventBus()
    strategy = _make_strategy(bus)
    runner = StrategyRunner(bus)
    runner.add(strategy)

    await runner.start()
    assert strategy.started is True
    await runner.stop()
    assert strategy.stopped is True
