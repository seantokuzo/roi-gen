# Phase 3 — Simulator + Backtest Parity (Design Spec)

> Status: **SPEC — not implemented.** Written for adversarial review before a line of code exists.
> Mandate: game plan core principle #3 — *"Same `Strategy` classes run against `SimulatedFill` (historical replay through the same event bus) and `AlpacaLive` with zero code changes. This is the hardest, most valuable deliverable."*
> Prerequisites: Phases 1–2 complete (`docs/STATE.md`). Every iron law in `CLAUDE.md` applies unchanged.

---

## 0. The claim this phase must earn

Legacy's fatal pattern was **divergent code paths**: a risk layer two of three order paths skipped, a `realized_pnl` field the learning loop read and nothing wrote. The backtest version of that pattern is a simulator that produces P&L from *different code* than the one tracking real money — you validate a strategy against an engine you will never trade.

So Phase 3's deliverable is not "a backtester". It is a **proof**, mechanically re-verified on every PR, that the code between a bar and a P&L number is byte-for-byte the same code in simulation and in production. Everything else in Phase 3 (fill realism, metrics, walk-forward) is downstream of that proof and worthless without it.

---

## 1. What already exists — the seams 2a–2c built

Phase 2 was designed with this phase in mind; the seams are not hypothetical. Verified against the code at `2249989`.

| Phase 3 need | Seam that already exists | Evidence |
|---|---|---|
| Deterministic event ordering | `EventBus` — single-consumer FIFO, dispatch by concrete type, `drain()` settles a tick | `app/engine/bus.py`; the `drain()` docstring names "the Phase-3 backtest replay" as its reason for existing |
| Identical strategy inputs | `BarEvent`/`QuoteEvent`/`TradeEvent` wrap the same `app.brokers.dto` DTOs the live feed decodes into | `app/engine/events.py`, `app/engine/market_bridge.py` |
| Identical strategy code | `Strategy` base + `StrategyRunner` route by symbol; strategies propose `(symbol, side, entry, stop)` only | `app/engine/strategy.py` |
| Identical risk decisions | `RiskEngine.evaluate(signal, state)` is **pure and synchronous** — no IO, no DB, no broker. All IO is in `RiskStateProvider` | `app/engine/risk/engine.py`, `app/engine/risk/state.py` |
| The simulation boundary | `BrokerAdapter` ABC — **14 abstract methods**, one adapter per account, async context manager. Its own docstring says "a simulated fill engine drops in by implementing these methods" | `app/brokers/base.py` |
| Broker-clock anchoring for all logic time | `RiskState.now` comes from `adapter.get_clock().timestamp`; `FlattenController` re-anchors on the broker clock every wake | `risk/state.py:129,154`; `flatten_controller.py:188` |
| Fill injection without Redis | `TradeUpdateStage.on_trade_update(update: TradeUpdate)` is a plain public coroutine. `RedisTradeUpdateSubscriber` is *a* caller, not *the* interface | `app/engine/execution/trade_updates.py:95` |
| Order-state machine, FIFO lots, `lot_closes`, position tracker | All keyed off `TradeUpdate` → DB, independent of transport | `app/engine/execution/*` |
| Offline E2E patterns to mirror | `tests/engine/test_execution_e2e.py` (signal → realized P&L), `tests/engine/test_safety_e2e.py` (kill switch → flatten → verified flat), both real components + fake broker | `tests/engine/` |
| Injectable timing so sim runs fast | `ExecutionStage(resolve_delays=…, cancel_confirm_delays=…)`, `TradeUpdateStage(match_retry_delays=…)` are constructor params | `handler.py:146`, `trade_updates.py:88` |

**Consequence:** a `SimulatedBrokerAdapter` needs zero new hooks. `ExecutionStage`, `RiskStage`, `TradeUpdateStage`, `ReconciliationService`, and `FlattenController` all take `adapter: BrokerAdapter` by constructor injection today.

### What does NOT exist

| Missing | Notes |
|---|---|
| **Any historical-bars client** | `app/brokers/` has zero references to `data.alpaca.markets`, `/v2/stocks/bars`, or `get_bars`. The live spine is streaming-only |
| **Any bar persistence** | No `bars` table; 3 migrations exist (`5e48fb608876`, `a7c31d90f2e4`, `b5e711f11a58`) and none touch market data |
| **Any simulated adapter** | `tests/engine/builders.py` has `FakeEngineAdapter`/`RecordingAdapter` — canned responses for unit tests, not a state machine |
| **Any run isolation** | One database, one schema, `search_path` untouched. `alembic/env.py` has no `include_schemas`, no `version_table_schema`, no `config.attributes["connection"]` hook |
| **Any metrics harness** | No Sharpe/Sortino/DD/walk-forward code anywhere in `app/` |
| **A virtual clock** | Nothing in the engine can be told what time it is except through `adapter.get_clock()` |
| **A cached market calendar** | `get_calendar` hits Alpaca live; nothing persists sessions (half-days, holidays) |

---

## 2. Decisions locked by Sean (settled — rationale recorded, not re-opened)

### 2.1 Simulate at the broker boundary

`SimulatedBrokerAdapter` implements the `BrokerAdapter` ABC. **Everything above it runs unchanged**: risk controls, `ExecutionStage`, the order state machine, the trade-updates writer, FIFO lots + `LotClose`, the position tracker, reconciliation, the flatten controller.

**Rationale.** Backtest P&L is then produced by the same ledger code that tracks real money. "Zero strategy changes" (the game plan's phrasing) upgrades to "zero *engine* changes" — a strictly stronger and much more useful claim, because the parts most likely to lie about performance (lot accounting, partial-close attribution, the same-day breaker's view of realized P&L) are exercised identically.

**Rejected: an `ExecutionHandler`-level seam.** Faster to build, and it is what the game plan's `SimulatedFill` box in the architecture diagram implies. Rejected because backtest P&L would then come from a different code path than live — the legacy divergent-path failure mode, reintroduced in the one place where you cannot see it happening.

*Naming note:* the game plan calls this component `SimulatedFill`. The boundary moved down a layer; the name in code is `SimulatedBrokerAdapter`, and the fill-matching logic inside it is `FillModel`. Worth a one-line correction in the game plan when this ships.

### 2.2 `BarHistorySource` interface; Alpaca IEX now → Databento later; cache bars in Postgres

**Rationale.** `docs/RESEARCH.md` §7: IEX carries ~2–3% of consolidated volume (AAPL: 12,630 IEX vs 535,136 consolidated trades/day) — unusable for RVOL/VWAP/ORB confirmation, but *price* on liquid large-caps is fine because NBBO keeps quotes near-honest. So IEX proves the plumbing at $0, and the earmarked Databento **$125 free credit** buys honest volume when Phase 4's volume-dependent strategies arrive. An interface, not a client, is what makes that swap a config change.

Postgres cache for reproducibility (a backtest you cannot re-run against identical inputs is not an experiment) and consistency with the pgvector "one store, one backup" decision.

### 2.3 Schema per run

Same tables, same models, same queries — a dedicated Postgres schema per backtest run, selected via `search_path`. Deleting a run is `DROP SCHEMA … CASCADE`.

**Rationale.** Requires **zero changes to any engine query** — which is precisely what makes the parity claim credible. A backtest that had to teach every query about runs would be a different program.

**Rejected: a `backtest_run_id` column.** Every query in the codebase must then filter it; miss one and backtest rows contaminate live P&L, the same-day breaker, or the drawdown ladder. The failure is silent and lands in the risk layer.

### 2.4 Parity spine first

| Slice | Ships | Deliverable |
|---|---|---|
| **3a** | Bar history + cache, run isolation, replay driver, clock virtualization, `SimulatedBrokerAdapter`, **the parity test** | The proof that parity holds |
| **3b** | Fill realism: spread/slippage/partial fills/commissions+fees, gap-through rules, session mechanics (flatten at 15:55, per-session reconcile, equity snapshots), realized-vs-modeled slippage measurement | A backtest whose numbers are pessimistic enough to trust |
| **3c** | Metrics (Sharpe/Sortino/max DD/exposure/hit rate), walk-forward harness, promotion gates wired to the lifecycle | The `paper → live` gate from iron law #8 with real numbers behind it |

---

## 3. Build order for slice 3a (this ordering is load-bearing)

The live-paper E2E has **never run** (`docs/STATE.md` blockers; market closed until Monday). So 3a is deliberately ordered to front-load every unit with **zero exposure to live-broker semantics**, and to isolate all such exposure into exactly one unit built last.

| # | Unit | Live-broker exposure | Why here |
|---|---|---|---|
| 1 | `BarHistorySource` protocol + `AlpacaBarHistory` + Postgres bar/calendar cache + migration | **None** — a read-only data endpoint, no order semantics | Pure data plumbing; nothing Monday can teach us changes it |
| 2 | Per-run schema isolation (create / migrate / drop, `search_path`-scoped sessions) | **None** — infrastructure | Independent of everything above the DB |
| 3 | Replay driver: bars → `BarEvent` on the real bus, `bus.drain()` per tick, virtual clock advance | **Near-zero** — ordering discipline only | Depends on 1+2; testable with a hand-written bar fixture |
| 4 | Virtualize the one logic-bearing wall-clock read (§5) | **None** | Must land before the sim adapter, or the sim's clock is a half-truth |
| 5 | `SimulatedBrokerAdapter` — clock/calendar, account+equity model, order lifecycle (market/limit, bracket parent + OCO legs, held-qty), `TradeUpdate` emission into `TradeUpdateStage.on_trade_update` | **THIS IS THE ONLY UNIT WITH EXPOSURE** | Isolated last, so its uncertainty contaminates nothing else |
| 6 | **The parity test** — the deliverable | Zero by construction (§6) | Needs 1–5 |

### Why this survives Monday

**The parity test asserts that OUR code is identical across paths. It is a claim about our engine, not about Alpaca.** Nothing the live-paper E2E reveals about real broker behavior can falsify it.

Concretely, the two open live questions carried in `docs/STATE.md` —

- whether a **filled** bracket parent's protective legs appear as top-level rows under `list_orders(nested=False)` (the assumption the flatten's un-nested cancel sweep rests on, `handler.py:429`), and
- exact `position_intent=*_to_close` semantics under a quantity race

— are **fill-realism** concerns. They change what `SimulatedBrokerAdapter` should *pretend Alpaca does*. They fold into **3b**, where the sim's behavior is calibrated against observed broker behavior, and they do not touch units 1–4 or the parity assertion at all.

If Monday's run contradicts a sim assumption, the sim changes and the parity test still passes — because both sides of the parity test sit *above* the adapter.

---

## 4. Interfaces (3a)

Sketches, not final signatures. Anything marked ⚠️ is unverified and must be confirmed against source/docs before implementation — never from memory (global rule: never trust training data for library APIs).

### 4.1 `app/market_data/history.py`

```python
class BarTimeframe(StrEnum):
    minute_1 = "1Min"; minute_5 = "5Min"; hour_1 = "1Hour"; day_1 = "1Day"

@runtime_checkable
class BarHistorySource(Protocol):
    async def get_bars(
        self,
        symbols: Sequence[str],
        *,
        start: datetime,          # tz-aware UTC, inclusive
        end: datetime,            # tz-aware UTC, exclusive
        timeframe: BarTimeframe = BarTimeframe.minute_1,
    ) -> AsyncIterator[Bar]: ...  # app.brokers.dto.Bar — the SAME DTO the live stream produces
```

**Contract (invariants a reviewer should hold us to):**

- Yields `app.brokers.dto.Bar`. **There is no backtest-specific bar type.** A second bar shape is a second code path.
- **Total order**: ascending by `(timestamp, symbol)`, symbol as a *lexicographic* secondary key. The bus is FIFO; cross-symbol order within one timestamp is observable to strategies, so it must be deterministic and documented, not incidental.
- Money/prices/volume are `Decimal`, parsed via `Decimal(str(x))` (iron law #7, mirroring `alpaca/rest.py::_dec`).
- Timestamps tz-aware UTC (iron law #5).
- Yields **only bars that exist**. Absence of a bar is not zero volume — see `bar_coverage` below.

**Implementations:**

| Class | Notes |
|---|---|
| `AlpacaBarHistory` | async httpx against the **data** host (`data.alpaca.markets`), NOT the trading host `rest.py` uses. Reuses `AsyncTokenBucket`. ⚠️ Exact path, params (`symbols`/`timeframe`/`start`/`end`/`limit`/`page_token`/`feed`/`adjustment`), response envelope, and pagination token name **must be verified against Alpaca docs and one live call** before coding. Two constraints already known from RESEARCH §7: the free Basic plan **cannot query the most recent 15 minutes** of history, and the trading-API 200 req/min cap is a separate bucket from the data API's — do not assume they share one |
| `CachedBarHistory` | Wraps a source. Reads Postgres, fetches only uncovered ranges, writes back. |
| `DatabentoBarHistory` | Phase 4+, when honest volume is required. |

**`adjustment` is not optional.** Bars must be fetched split-adjusted at minimum. An unadjusted series across a split is not noisy data, it is a fabricated 50% gap that will trigger every stop in the backtest. The adjustment mode is part of the cache key.

### 4.2 Bar cache schema (new Alembic migration)

```
bars                (symbol, timeframe, feed, adjustment, ts) PK
                    open, high, low, close, volume  NUMERIC   -- Decimal, iron law #7
                    trade_count INT NULL, vwap NUMERIC NULL

bar_coverage        (symbol, timeframe, feed, adjustment, range_start, range_end, fetched_at)

market_calendar     (trading_date) PK, rth_open TIMESTAMPTZ, rth_close TIMESTAMPTZ
```

- **`feed` and `adjustment` are in the primary key.** Without them an IEX-volume run and a SIP run silently interleave in one series, and the RVOL number that comes out is meaningless. This is the cheapest possible guard against the single worst data-integrity failure available to us.
- **`bar_coverage` is not optional.** Without it you cannot distinguish "no bars because the symbol did not trade in that minute" from "no bars because we never fetched that range". The second one looks exactly like a quiet market and silently deletes signal.
- **`market_calendar`** is fetched from Alpaca `/v2/calendar` and cached because the sim's clock must be *historically accurate* — including early closes. `CalendarDay` already carries `rth_open`/`rth_close` and its docstring documents the exact trap (RTH `open`/`close` vs extended `session_open`/`session_close`; mapping the wrong pair turns a 15:55 flatten into a 19:55 one). Reuse that DTO; do not re-derive.

**These three tables are shared *inputs*, not per-run outputs.** See §4.3 for the schema-placement trade-off.

### 4.3 `app/backtest/run.py` — run isolation

```python
@dataclass(frozen=True, slots=True)
class BacktestRun:
    run_id: uuid.UUID
    schema: str                                   # f"bt_{run_id.hex}" — 35 chars, well under PG's 63
    engine: AsyncEngine                           # search_path pinned to this run's schema
    session_factory: async_sessionmaker[AsyncSession]

async def create_run(*, label: str) -> BacktestRun: ...
async def drop_run(run: BacktestRun) -> None: ...   # DROP SCHEMA … CASCADE
```

Mechanics:

1. `CREATE SCHEMA bt_<hex>`
2. Run `alembic upgrade head` against a connection whose `search_path` is that schema. ⚠️ **This requires the one infra change in 3a**: `alembic/env.py` today builds its own engine and never checks `config.attributes`. Add the standard recipe — `connectable = config.attributes.get("connection")`, fall back to the current behavior — plus `version_table_schema` so `alembic_version` lands in the run schema. Nothing else in the migration chain changes.
3. Build the run engine with `create_async_engine(url, connect_args={"server_settings": {"search_path": f"{schema}, public"}}, poolclass=NullPool)`. ⚠️ Verify asyncpg accepts `search_path` in `server_settings` (it should — it is a normal GUC delivered as a startup parameter — but verify, do not assume). Pinning at connect time beats a per-session `SET`, which a pooled connection can leak.
4. **Post-migration invariant check (mandatory):** assert every table in `Base.metadata` exists in `bt_<hex>` via `pg_tables`. `search_path` includes `public` as a fallback so extension types resolve; that fallback is exactly how a missing run table silently reads and writes live data. The assertion collapses that risk to a boot check.
5. Seed the run: one `users` row, one `portfolios` row, the `strategies` row(s) under test. No `broker_credentials` needed — the adapter is the simulator.

**Open trade-off (reviewer input wanted).** Where do `bars` / `bar_coverage` / `market_calendar` live?

| Option | Pros | Cons |
|---|---|---|
| **A (recommended)** — schema-less models; the bar reader uses the ordinary `public`-bound engine, the engine stack uses the run-bound engine | Zero Alembic changes beyond the `attributes["connection"]` hook; two explicit engines with obvious ownership | Each run schema gets a duplicate *empty* `bars` table (dead weight, mildly confusing) |
| **B** — `__table_args__ = {"schema": "market_data"}` | No duplicate tables; the split between shared inputs and run outputs is expressed in the schema itself | Needs `include_schemas=True` + an `include_object` filter in `env.py` or every autogenerate wants to drop/recreate them |

Recommend **A** for 3a (smallest blast radius on a working migration chain), revisit at 3c.

**Also note:** `engine_main.py`'s singleton advisory lock keys on `hashtext('roigen-engine-<portfolio_id>')`, which is **database-wide, not schema-scoped**. 3a's replay driver does not run `engine_main`, so this does not bite yet. If a later slice boots the full daemon against a run schema, the lock key must include the run schema or two concurrent backtests will refuse to start.

### 4.4 `app/backtest/clock.py` + `replay.py`

```python
class SimClock:
    def now(self) -> datetime: ...          # tz-aware UTC
    def advance_to(self, instant: datetime) -> None:   # raises if instant < now — monotone only

class ReplayDriver:
    def __init__(self, *, bus: EventBus, clock: SimClock,
                 broker: SimulatedBrokerAdapter,
                 bars: AsyncIterator[Bar],
                 on_tick: Sequence[Callable[[datetime], Awaitable[None]]] = ()) -> None: ...
    async def run(self) -> ReplayStats: ...
```

**The tick loop — the exact ordering is the look-ahead firewall:**

```
for each group of bars sharing one timestamp (ascending; symbols sorted):
  1. clock.advance_to(bar_close_instant)
  2. await broker.on_clock_advance(now, bars_by_symbol)   # match resting orders → TradeUpdates
                                                          # → TradeUpdateStage.on_trade_update
  3. await bus.drain()          # settle FillEvents from step 2
  4. publish BarEvent per symbol, in sorted order
  5. await bus.drain()          # signals → risk → execution → sim acceptance
  6. for hook in on_tick: await hook(now)   # 3b: flatten controller tick, periodic reconcile
  7. await bus.drain()
```

Ordering rationale, each item a live-impossible state it prevents:

- **Fills before bars (2–3 precede 4).** A strategy must never see bar *N* while unaware that its stop filled during bar *N*. In live that state cannot occur; letting it occur in sim is a divergence dressed as a timing detail.
- **A market order emitted on bar *N* fills at bar *N+1*'s open, never at bar *N*'s close.** The strategy has already *seen* bar *N*'s close; filling there is trading on information you already acted on. This is the single most common way a backtest fabricates edge.
- **`drain()` between every phase.** The bus is breadth-first within a drain; without the split, a fill from step 2 could interleave with a signal from step 4 in an order that depends on queue depth.

⚠️ **Must verify before implementation:** whether an Alpaca minute bar's `t` is the interval's **start** or **end**. Everything above assumes *start* (so the close instant is `t + interval`). If it is the end, the offset disappears and every session-boundary comparison shifts by one minute — enough to move a 15:55 flatten or an ORB window. Verify against a real bar from `alpaca/streams.py`'s output, not from documentation memory.

### 4.5 `app/backtest/broker.py` — `SimulatedBrokerAdapter`

```python
class SimulatedBrokerAdapter(BrokerAdapter):
    def __init__(self, *, clock: SimClock, calendar: SessionCalendar,
                 starting_cash: Decimal,
                 on_trade_update: Callable[[TradeUpdate], Awaitable[None]],
                 fill_model: FillModel,
                 margin_multiple: Decimal = Decimal("4")) -> None: ...
    async def on_clock_advance(self, now: datetime, bars: Mapping[str, Bar]) -> None: ...
```

Internal state: `_orders` (by broker id + client-id index), `_positions` (signed qty, avg entry, cost basis), `_cash`, `_last_equity` (equity at the prior session close), `_last_price`, and **monotonic counters** for broker order ids (`sim-{n}`) and execution ids (`simx-{n}`).

All 14 ABC methods:

| Method | Simulated behavior |
|---|---|
| `get_clock` | `MarketClock(timestamp=clock.now(), is_open/next_open/next_close from calendar)`. **This is the virtualization of every logic clock in the engine** — `RiskState.now`, the ET day boundary, the flatten window all flow from here for free |
| `get_calendar` | Slice of the cached real calendar. Half-days included — a synthesized 9:30–16:00 calendar silently un-tests the early-close path |
| `get_account` | `equity = cash + Σ(signed_qty × last_price)`; `buying_power = equity × margin_multiple`; `last_equity` = prior session close equity; `trading_blocked`/`account_blocked` = False. **No PDT fields** (iron law #3) |
| `list_positions` / `get_position` | Non-zero positions, signed qty |
| `list_orders` | Honors `status`, `after`/`until`, `limit`, and **`nested`**: `nested=True` inlines legs on the parent, `nested=False` returns each leg as its own top-level row |
| `get_order` / `get_order_by_client_id` | Lookups; `None` when unknown |
| `submit_order` | Validate → `OrderRejected` on definitive refusal (qty ≤ 0, insufficient buying power, held-qty conflict). Create parent + `held` legs for brackets. Emit a `new`/`accepted` `TradeUpdate`. Return `BrokerOrder` |
| `cancel_order` | Terminal-state guard, emit `canceled`, cancel the OCO sibling |
| `cancel_all_orders` | Loop over working orders |
| `close_position` / `close_all_positions` | Implemented faithfully **even though the engine never calls them** (2c: the flatten path deliberately uses `submit_order` with our own `client_order_id`, `handler.py:526`). A stub here would make a future caller silently different from live |
| `aclose` | No-op |

**Fill matching (`FillModel`) — 3a defaults, deliberately pessimistic:**

| Situation | Rule |
|---|---|
| Resting market order | Fills at the next bar's **open** |
| Limit buy | Eligible when `bar.low <= limit`; price = `min(limit, bar.open)` — never better than the limit *and* never better than the open |
| Limit sell | Mirror |
| Stop triggered within the bar's range | Fill at the stop price |
| Stop **gapped through** (bar opened past it) | Fill at the **open**, not the stop. Filling at the stop on a gap is the most common single lie in retail backtests |
| Bar range spans **both** the stop and the take-profit | **Assume the stop filled.** OHLC cannot order intra-bar events; the pessimistic branch is the only defensible default. A 3b option may disambiguate with finer bars |
| Bracket leg fills | Sibling OCO leg immediately canceled, with its own `TradeUpdate` |

**Hard invariants for this class (each is a testable assertion):**

1. Never emits a `TradeUpdate` whose `timestamp > clock.now()`. *(look-ahead firewall)*
2. Never calls `datetime.now`, `random`, or `uuid.uuid4`. All ids from counters, all times from the clock.
3. Every emitted `TradeUpdate` is `await`ed into `on_trade_update` **before** the driver publishes the next `BarEvent`.
4. `TradeUpdate.position_qty` is the post-fill signed position (the writer forwards it onto `FillEvent`).
5. Money is `Decimal` end to end; quantities are whole shares (the risk engine sizes whole shares).
6. `cash` and `positions` are mutually consistent after every fill — assert `Σ` reconciles.

**Wiring note:** construct the stack with `TradeUpdateStage(match_retry_delays=())` and `ExecutionStage(resolve_delays=(0.0,))`. Both are existing constructor parameters — **no engine change**, and the sim never needs the retry paths because leg rows are committed before any leg fill can be emitted within the same awaited tick.

**Explicitly out of scope for 3a:** `FlattenController`, per-session reconciliation, equity snapshots, partial fills, commissions/fees. All 3b. 3a wires the minimum spine — bus, `StrategyRunner`, `RiskStage`, `ExecutionStage`, `TradeUpdateStage` — because the deliverable is the proof, not the backtest.

---

## 5. Clock virtualization — the audit

There are **three clocks** in this system, and 2c only disciplined one of them.

| Clock | Where | 2c's position | Phase 3's problem |
|---|---|---|---|
| **Broker clock** | `adapter.get_clock().timestamp` | *All logic time anchors here* | Solved for free — the sim adapter owns it |
| **Python wall clock** | `datetime.now(UTC)` | Permitted for audit fields only | Must be audited; §5.1 |
| **Postgres clock** | `server_default=func.now()` on `TimestampMixin` | Never considered | **Not virtualizable.** §5.2 — this is the sharp edge |

### 5.1 `datetime.now(UTC)` audit (complete, verified by grep over `app/`)

| Site | Field | Logic-bearing? |
|---|---|---|
| `services/reconciliation.py:151` | `now` → never-submitted grace window (`:821`), synthesized-fill `occurred_at` fallback (`:514`, `:780`), `EquitySnapshot.ts` (`:160`) | **YES** |
| `engine/feed_health.py:103` | payload age vs stamped window | **YES**, but live-infrastructure only |
| `engine/risk/engine.py:100,141` | `RiskApproval.approved_at` / `FlattenApproval.approved_at` | Audit |
| `engine/flatten_controller.py:252,353` | `verified_at` in the command payload | Audit |
| `engine/commands.py:190` | `EngineCommand.applied_at` | Audit — verified: read only by `cli.py:124` and `schemas/engine.py` for display |
| `engine/events.py:36` | `SignalEvent.created_at`, `FlattenEvent.created_at` | Audit — verified: no logic reads them |
| `engine_main.py:116` | heartbeat timestamp | Ops |
| `brokers/alpaca/streams.py:715` | feed-health payload stamp | Live infrastructure |

**Confirmed: `reconciliation.py:151` is the only logic-bearing wall-clock read on the replay path.** The prompt's premise holds, with two corrections worth flagging:

- **It is logic-bearing in *two* ways, not one.** Besides the grace window, that same `now` becomes the `occurred_at` fallback for synthesized fills (`:514`, `:780`) — and `occurred_at` flows straight into `apply_fill_to_lots`, stamping `Lot.opened_at`, `LotClose.closed_at`, and the cross-strategy time bound `Lot.opened_at <= occurred_at` (`lots.py:132`). A wall-clock `occurred_at` in a historical replay would stamp lots in the future relative to sim time — a direct violation of iron law #10's point-in-time spirit, inside the risk layer's own inputs.
- **`feed_health.py:103` is logic-bearing too**, but it reads a Redis key that does not exist in a backtest. See §5.3.

**Fix:** give `ReconciliationService.reconcile_portfolio` an injectable clock (`now: datetime | None = None`, defaulting to `datetime.now(UTC)`), and pass `sim_clock.now()` from the replay driver. Signature-additive, zero behavior change in live, one call site to update in `engine_main.py`. This lands in 3a even though reconciliation itself is wired in 3b — the audit is cheap now and the finding is fresh.

### 5.2 The Postgres clock is a *fourth* problem, and it changes 3a's scope

`TimestampMixin.created_at` is `server_default=func.now()` — the **Postgres transaction timestamp**, unreachable from application code. Three consequences, in ascending severity:

**(a) Byte-reproducibility is impossible at the row level.** Every `created_at`/`updated_at` differs between runs. The determinism contract must therefore be defined over a **canonical projection** (§7), not a table dump.

**(b) A latent nondeterministic FIFO tie-break.** `lots.py:146` consumes lots ordered by `(opened_at, created_at, id)`. `func.now()` is `transaction_timestamp()` — *constant within a transaction* — so two lots created in the same transaction (reconciliation's synthesis path stages several) share both `opened_at` and `created_at`, and the tie falls to `id`, a **`uuid.uuid4`** (`models/base.py:13`). That is a random tiebreak deciding which lot's cost basis gets consumed, and therefore which strategy's `LotClose` P&L number moves. It is rare and small — and it is a live nondeterminism too, just currently invisible. Recommend fixing it in a separate micro-PR (add a deterministic monotonic tiebreak column, or guarantee lots never share both keys) rather than smuggling it into Phase 3.

**(c) `ProbeStrategy` will behave differently in sim than in live. This one is blocking.**

`probe.py:211` counts today's entries with `Order.created_at >= et_day_start_utc(bar_instant)`. In a replay of, say, 2026-03-02:

- `day_start` = 2026-03-02 05:00 UTC (historical)
- `Order.created_at` = the real Postgres wall clock at replay time (2026-08-xx)
- **Every order ever written by the run satisfies the predicate**

So after the first simulated day, the count is ≥ 1 on every subsequent day and the probe emits **one entry for the entire backtest** instead of one per session. Same code, same bars, different behavior — the exact failure Phase 3 exists to make impossible.

| Option | Assessment |
|---|---|
| **A (recommended)** — source `RiskApproval.approved_at` from `state.now` (the broker clock, already loaded in `RiskState`) instead of `datetime.now(UTC)`; add a nullable market-time column to `orders` stamped from it in `ExecutionStage._persist_pending`; switch the probe's predicate to that column | Correct in live too (a queued write's `created_at` can straddle ET midnight); virtualized for free in sim. Cost: one line in `risk/engine.py`, one in `handler.py`, one migration, one predicate. Touches money-path files → deep-tier review, as it should |
| B — pass `session_factory=None` to strategies in backtests (the probe's docstring already anticipates this) | Cheap, but the backtest then runs a *different code path* than live. That is the thing we are building this phase to prevent |
| C — defer; keep the 3a fixture inside one session | The parity test becomes trivially passable and the bug ships hidden. Rejected |

**Recommend A**, and note the corollary: **the parity fixture MUST span at least two ET sessions**, or the test cannot catch this class of bug at all. `authorize_flatten` is deliberately pure on its event and has no `RiskState`, so its `approved_at` stays wall-clock in 3a — flagged, not fixed.

Finding this *before* writing the simulator is the clearest early evidence that the boundary choice in §2.1 earns its cost.

### 5.3 Documented parity exceptions (state them, do not hide them)

Two gates exist in live that cannot exist in a replay. Both are named here so a reviewer can attack the reasoning rather than discover the gap:

| Gate | Live | Backtest | Justification |
|---|---|---|---|
| `FeedHealth.is_stale` in the halt composite | Redis TTL key, fail-closed | Absent → contributes `False` | The "feed" in replay is a materialized bar series. It cannot go stale; it can only be *incomplete*, which is `bar_coverage`'s job (§4.2), not the staleness gate's |
| Critical-task liveness + `boot_reconciled` in the halt composite | `engine_main` task set | 3a: `boot_reconciled` pre-set, no supervised tasks | 3a runs the spine, not the daemon. When 3b wires reconciliation, `boot_reconciled` gets its real meaning back |

Everything else in the halt composite — the kill switch, the risk controls, the submit-time re-check — is live in the backtest unchanged.

---

## 6. The parity test — the 3a deliverable

**Fixture:** a committed bar set — 2 symbols × **≥ 2 ET sessions** of minute bars (§5.2c), stored as JSON in `tests/fixtures/`, small enough to read in a diff and to run in CI in seconds. One strategy under test (the probe in 3a; a real strategy from Phase 4 later).

**Run A — the simulated path.** Full run-schema stack, `SimulatedBrokerAdapter`, `ReplayDriver`. Record two output streams:
- every `SignalEvent` published
- every `OrderRequest` reaching `submit_order`
- every `TradeUpdate` the sim emitted

**Run B — the live-shaped path.** Fresh run schema, the *existing* `RecordingAdapter`-style fake broker from `tests/engine/builders.py` (not the simulator), the same bus/`RiskStage`/`ExecutionStage`/`TradeUpdateStage`. Bars are fed **through the live decode path** — serialized to the `md:*` channel JSON shape and decoded by `MarketDataBridge._dispatch` — so the test also proves the live bar-decode path does not alter a bar. Fills are supplied by replaying **Run A's recorded `TradeUpdate` stream** into `TradeUpdateStage.on_trade_update`.

**Assertions:**

1. The `SignalEvent` sequences are identical on `(symbol, side, entry_price, stop_price, take_profit_price, order_type, time_in_force, extended_hours, meta)` — everything except `signal_id`/`created_at`.
2. The `OrderRequest` sequences are identical on every field except `client_order_id` (minted from `uuid4`, `risk/engine.py:84`).
3. The `RiskApproval.audit_payload()` **control-check vectors** are identical — all 14 controls saw the same state and returned the same verdicts. This is the assertion that actually proves the risk layer is the same program.
4. The **canonical ledger projection** (§7) over `orders`/`fills`/`lots`/`lot_closes` is identical.

**What this test deliberately does NOT claim — state it in the test docstring:**

> Run B's broker is fed Run A's outputs, so it is not an independent oracle. This test cannot detect an error in the *fill model*. It proves that everything above the `BrokerAdapter` boundary is one program. Fill realism is validated in 3b against observed broker behavior; that is a different claim requiring a different instrument.

Pre-empting that objection in the docstring is cheaper than having a reviewer find it and doubt the rest.

**Keeping it a live guarantee, not a one-off:** the parity test runs in **CI on every PR** against the committed fixture. Divergence is then a red build the day it is introduced, not a discovery six months later when a Phase 6 refactor quietly changed a risk control's inputs. This is the entire answer to "how do we know parity still holds?" — it is checked mechanically, on a fixed input, forever.

---

## 7. Risk register

### R1 — Determinism

**A backtest that is not reproducible is not an instrument.** Nondeterminism sources found in the code, with mitigations:

| Source | Mitigation |
|---|---|
| Wall clock | §5.1: the sim clock owns all logic time; the one logic-bearing `datetime.now` gets an injected clock. 2c's clock discipline already bans wall-clock reads from engine logic, which is why this audit is short |
| `uuid.uuid4` — row PKs (`base.py:13`), `client_order_id` (`risk/engine.py:84`), `signal_id`, `flatten_id` | Not seedable (`os.urandom`). **Determinism is defined over a canonical projection**: business columns only, with UUIDs normalized to appearance-ordered surrogates and server-clock columns excluded. Optionally a strict mode that swaps `uuid.uuid4` for a counter-based factory at process level in the replay harness — no engine change, but ugly; offered, not required |
| The FIFO lot tie-break on a random `id` (§5.2b) | **The projection does NOT fix this** — it changes results, not labels. Separate micro-PR |
| Iteration order | Bars sorted `(timestamp, symbol)`; `StrategyRunner` dispatch order is registration order (`strategy.py:206`), which the loader derives from `ORDER BY strategies.created_at` (`loader.py:64`) — deterministic per run schema. Assert it |
| `asyncio` scheduling | The bus is single-consumer FIFO and the driver `drain()`s to quiescence between phases; nothing in 3a spawns concurrent tasks. `ExecutionStage._on_flatten` *does* spawn (`handler.py:357`) — another reason the flatten controller waits for 3b |

**Acceptance test:** run the same fixture twice in the same process against two fresh run schemas; the canonical projections must be byte-identical.

### R2 — Look-ahead bias

The replay driver must never let a strategy see a bar's close before that bar closes, nor let risk read a `LotClose` stamped after the sim clock (iron law #10's point-in-time discipline, applied to our own ledger).

| Guard | Mechanism |
|---|---|
| Bar visibility | `clock.advance_to(bar_close)` **before** the `BarEvent` is published; strategies observe a completed bar at its close instant, never during formation |
| Fill timing | Market orders fill at the *next* bar's open (§4.5). The sim never fills at a price the strategy has already seen |
| Ledger point-in-time | Every `TradeUpdate.timestamp` the sim emits is `<= clock.now()` (invariant 1). Since `Lot.opened_at` / `LotClose.closed_at` derive from `occurred_at` (`lots.py:174,192,246`), no ledger row can be stamped ahead of the sim clock — which is what makes `RiskState`'s `LotClose.closed_at >= et_start` query point-in-time honest for free |
| Reconciliation's fallback | §5.1: the `occurred_at or now` fallback must use sim time, or synthesized fills land in the future |
| The bar cache itself | `CachedBarHistory` must never return a bar with `timestamp >= end`; assert at the source, not at the consumer |

**Acceptance test:** a canary strategy that emits a signal whose `entry_price` equals the *next* bar's close. It must be impossible to construct — the value is not reachable from any argument `on_bar` receives.

### R3 — Optimism drift

Game plan principle #7: paper fills are optimistic (NBBO, no impact); promotion to live requires paper performance **minus a configurable slippage haircut**, and the platform must measure realized-vs-modeled slippage.

| Guard | Mechanism |
|---|---|
| Pessimistic defaults | §4.5's table: next-bar-open fills, never-better-than-open limits, gap-through fills at the open, stop-wins on an ambiguous bar |
| No free liquidity | 3b: fills capped at a fraction of bar volume; anything beyond becomes a partial. ⚠️ On IEX volume this cap is meaningless (2–3% of consolidated) — the cap is a Databento-era control, and until then the honest statement is "volume-dependent realism is not modeled", not a cap tuned against fake volume |
| Costs are not zero | 3b: Alpaca equities commission is $0, but SEC Section 31 and FINRA TAF fees on **sells** are real. ⚠️ Current rates must be looked up, not recalled — they change |
| Measurement, not faith | 3b ships a realized-vs-modeled slippage table: for every live/paper fill, log modeled price vs actual, per symbol and time-of-day. That table is the input to the 3c haircut, so the gate is empirical rather than a guessed constant |
| Structural pessimism | The haircut is applied by the **promotion gate**, which is code, not judgment (iron law #8) |

### R4 — Speed

DB-backed runs are the price of parity. **Measure before optimizing.**

The load model that makes this affordable: the DB cost scales with **trades, not bars**. A bar producing no signal costs one in-memory bus drain. A signal costs ~6 DB queries in `RiskStateProvider.load` plus the persist/apply/fill transactions. For 2 symbols × 1 year of minute bars (~196k bars) at the probe's 1 entry/session, that is ~500 entries — trivial. A high-frequency strategy inverts the ratio, and that is when to profile.

Mitigations, in order of preference: (1) measure and publish a bars/sec figure with the first real run; (2) `NullPool` + a local socket to keep per-query overhead honest; (3) if it bites, **schema-per-run leaves a faster in-memory variant open without touching engine code** — a tmpfs Postgres, or an ephemeral instance per run. The isolation decision (§2.3) is what preserves that option.

### R5 — Sim/live divergence going unnoticed

The failure mode is not divergence; it is divergence that nobody notices for six months while strategies get promoted on fiction.

| Guard | Mechanism |
|---|---|
| **CI** | The parity test runs on every PR over the committed fixture (§6). Divergence is a red build the day it appears |
| Fixture spans ≥ 2 ET sessions | Catches the whole class of session-boundary bugs, including §5.2c |
| Deep-tier review already covers the seam | `CLAUDE.md` puts `engine/risk*`, `engine/execution*`, `services/broker*` under deep review with a signal→risk→execution→broker trace. `app/backtest/broker.py` implements `BrokerAdapter` and must be added to that list |
| Divergence is *detectable*, not just preventable | 3b's realized-vs-modeled slippage table is a continuous, quantitative sim-vs-reality monitor that keeps working after the tests stop being interesting |
| Documented exceptions | §5.3 — every deliberate difference is written down. An undocumented difference is a bug by definition |
| The live dress rehearsal | Per the 2026-08-01 key decision, booting the real engine against the real broker is now ritual. The simulator does not replace it and must never be argued to |

---

## 8. Iron-law compliance

| Law | How 3a satisfies it |
|---|---|
| #1 Every order through the Risk Engine | Unchanged — the sim replaces the *broker*, not the choke point. `ExecutionStage` still demands a mint-guarded `RiskApproval` |
| #2 No LLM in the hot path | No LLM anywhere in Phase 3 |
| #3 No PDT logic | `SimulatedBrokerAdapter.get_account` returns `BrokerAccount`, which carries no PDT fields |
| #4 Entries carry protection | The sim honors bracket parent + legs and refuses `bracket` + `extended_hours` — `OrderRequest`'s own validator already enforces it before the adapter is reached |
| #5 tz-aware UTC / ET market logic | `SimClock` returns tz-aware UTC; ET boundaries come from `et_day_start_utc` unchanged |
| #6 Alembic for every schema change | One migration for the bar cache; the run-schema mechanism *runs* the migration chain rather than bypassing it — no `create_all` anywhere |
| #7 Money is `Decimal` | Bar cache columns `NUMERIC`; sim account/fill arithmetic `Decimal`; parse via `Decimal(str(x))` |
| #8 Paper before live | Untouched in 3a; 3c wires backtest metrics into the promotion gate |
| #9 Secrets never in git | Bar fixtures contain public OHLCV. Backtest runs need no broker credentials — the adapter is simulated |
| #10 Point-in-time discipline | R2 above; the sim clock bounds every ledger stamp |

---

## 9. Definition of done — slice 3a

- [ ] `BarHistorySource` protocol + `AlpacaBarHistory` + `CachedBarHistory`; unit tests incl. pagination, gap/coverage, and split-adjustment handling
- [ ] Alembic migration: `bars`, `bar_coverage`, `market_calendar`
- [ ] `alembic/env.py` accepts `config.attributes["connection"]` + `version_table_schema`
- [ ] `create_run` / `drop_run` with the **post-migration table-set assertion**
- [ ] `SimClock` + `ReplayDriver` with the §4.4 ordering, plus the look-ahead canary test (R2)
- [ ] `ReconciliationService` takes an injectable `now`
- [ ] The §5.2c decision applied (recommended: option A — market-time stamp on `orders`)
- [ ] `SimulatedBrokerAdapter`: all 14 methods, invariants 1–6 asserted in tests
- [ ] **The parity test**, green, in CI, over a ≥ 2-session committed fixture
- [ ] The determinism test (same fixture twice → identical canonical projection)
- [ ] `docs/STATE.md` updated: Key Decisions Log rows for §2.1–2.4 and for anything the review changes
- [ ] `app/backtest/broker.py` added to the deep-tier review path list in `CLAUDE.md`

---

## 10. Open questions and assumptions — scrutinize these first

Flagged rather than papered over. Everything below is either unverified or a judgment call that a reviewer could reasonably overturn.

1. **⚠️ Alpaca bar timestamp semantics** — start-of-interval vs end-of-interval (§4.4). Assumed *start*. Unverified in this codebase. If wrong, every session-boundary comparison shifts by one bar. **Verify against a real bar before writing the driver.**
2. **⚠️ Alpaca historical-bars API surface** — host, path, params, pagination token, response envelope (§4.1). Deliberately not specified from memory. Verify against docs + one live call.
3. **⚠️ asyncpg `server_settings={"search_path": …}`** (§4.3) — expected to work; verify. If it does not, fall back to a per-session `SET search_path` with a pool-checkout event listener, and re-audit the leak risk.
4. **§5.2c is a real scope addition.** Fixing the probe's server-clock predicate touches `risk/engine.py` and `execution/handler.py` — money-path files under deep review. A reviewer may prefer option B (weaker parity, cheaper) or want it split into its own PR. My recommendation is A, in 3a, because the parity test is worthless if the first strategy it runs diverges.
5. **The FIFO tie-break on a random UUID (§5.2b)** is a *live* nondeterminism I found while writing this. It is out of Phase 3's scope but should not wait for Phase 3.
6. **Bar-cache schema placement (§4.3, options A vs B)** — I recommend A for blast radius, but B is architecturally cleaner. A reviewer with a stronger opinion about Alembic multi-schema should overrule me.
7. **The parity test's Run B is not an independent oracle** (§6). This is by design and stated in the docstring, but it is the most attackable part of the deliverable and deserves a direct challenge: *is there a cheaper independent oracle for the fill model that could land in 3a?* I did not find one that does not require live-broker data.
8. **The two documented parity exceptions (§5.3)** are judgment calls. If a reviewer thinks a stale-feed gate should be *simulable* (e.g. replaying a bar series with a deliberate hole to prove the gate fires), that is a good 3b test and I would take it.
9. **`FlattenController` in replay.** It spawns off-bus tasks and sleeps toward wall-clock targets (`flatten_controller.py:170,231`); `test_safety_e2e.py` drives it by calling `_tick()` directly. 3b must decide between a tick-injection contract and a driver-owned scheduler. Deferred, not solved.
10. **Sim margin model.** `buying_power = equity × 4` matches the post-PDT intraday reality from RESEARCH §1, but real overnight/maintenance margin is more complex. 3a's margin-headroom control (`check_margin_headroom`) will be exercised against a simplification. Acceptable for a parity proof; must be revisited before backtests inform live sizing.
