# Phase 3 — Simulator + Backtest Parity (Design Spec)

> Status: **SPEC — not implemented.** Written for adversarial review before a line of code exists.
> **Revision 2 (2026-08-01)** — folds in `docs/PHASE-3-REVIEW-TRIAGE.md`: four adversarial lenses
> (parity-claim, realism/optimism, data-isolation, claim-verifier/scope) reviewed revision 1 at
> `bf6ba0d`; all four returned `fix-then-ship`. Where the review **overturned** something, this doc
> says so inline — the next reader needs to know which parts were stress-tested and which were merely
> written down.
> Mandate: game plan core principle #3 — *"Same `Strategy` classes run against `SimulatedFill` (historical replay through the same event bus) and `AlpacaLive` with zero code changes. This is the hardest, most valuable deliverable."*
> Prerequisites: Phases 1–2 complete (`docs/STATE.md`). Every iron law in `CLAUDE.md` applies unchanged.

**What changed in revision 2, in one screen:**

| # | Change | Source |
|---|---|---|
| 1 | The A-vs-B differential parity test is **deleted**. Replaced by a golden canonical-projection snapshot + decode round-trip + mutation check + liveness assertions (§6) | triage A1 |
| 2 | Per-session **equity snapshots move into 3a** — without them the drawdown ladder is inert and the backtest selects for strategies that recovered from drawdowns that would have halted them (§4.6) | triage A2 |
| 3 | Old §5.1 (injectable reconcile clock) and old §5.2c (market-time column) are **ONE fix**, and the column is **required**, not an option (§5.2) | triage A3 |
| 4 | 3a is **three PRs plus a prerequisite micro-PR**, not six units in one diff (§3) | triage B13, C1 |
| 5 | `search_path` is pinned to the run schema **alone** — no `, public` fallback. Reproduced against real Postgres: with the fallback, backtest rows land in live P&L silently (§4.3) | triage B1 |

---

## 0. The claim this phase must earn

Legacy's fatal pattern was **divergent code paths**: a risk layer two of three order paths skipped, a `realized_pnl` field the learning loop read and nothing wrote. The backtest version of that pattern is a simulator that produces P&L from *different code* than the one tracking real money — you validate a strategy against an engine you will never trade.

Revision 1 claimed Phase 3 would deliver "a proof, mechanically re-verified on every PR, that the code between a bar and a P&L number is byte-for-byte the same code in simulation and in production." **The review overturned that framing** — not because it is false in spirit, but because the instrument proposed for it (a differential test between two runs of the same engine) could not have proven it, and could not even have run (§6, triage A1). Overselling the guarantee is worse than a smaller honest one, because a reviewer who trusts the claim stops looking.

So, precisely, 3a delivers three things:

1. **Structural parity by construction.** `SimulatedBrokerAdapter` implements the `BrokerAdapter` ABC and nothing above that boundary changes. There is exactly one implementation of the risk controls, the order state machine, the FIFO lot ledger, and the trade-updates writer, and the backtest runs *it*. This is not proven by a test; it is enforced by the type system and by there being no second copy of the code to drift from. The test's job is to notice when someone adds one.
2. **A reviewed, frozen ledger over a fixed input.** The canonical projection (§6.1) of a committed fixture is committed alongside it. The engine must reproduce it byte-for-byte in CI. That converts the vague claim into a concrete one: *the ledger this engine produces over this input is the one a human read and approved.* A future refactor that changes what a risk control sees turns the build red **with a reviewable diff**.
3. **A demonstrated ability to fail.** A mutation check (flip one `RiskLimits` value, assert the golden test goes red) plus liveness assertions (≥N signals, ≥N fills, ≥1 fully-closed lot, non-zero realized P&L, zero anomaly rows). Without these, an inert fixture ships as "parity proven" — the failure mode the review found in revision 1's design, where bars outside RTH make `ProbeStrategy.on_bar` return early and empty-equals-empty passes.

What 3a explicitly does **not** deliver: any evidence that the fill model resembles Alpaca. That is 3b, it requires observed broker behavior, and no test written in 3a can substitute for it (§6.4).

---

## 1. What already exists — the seams 2a–2c built

Phase 2 was designed with this phase in mind; the seams are not hypothetical. Verified against the code at `2249989`, and **independently re-verified by the claim-verifier lens** (triage §F) — every row below survived.

| Phase 3 need | Seam that already exists | Evidence |
|---|---|---|
| Deterministic event ordering | `EventBus` — single-consumer FIFO, dispatch by concrete type, `drain()` settles a tick | `app/engine/bus.py`; the `drain()` docstring names "the Phase-3 backtest replay" as its reason for existing — the seam was built for this, not retrofitted |
| Identical strategy inputs | `BarEvent`/`QuoteEvent`/`TradeEvent` wrap the same `app.brokers.dto` DTOs the live feed decodes into | `app/engine/events.py`, `app/engine/market_bridge.py:72` |
| Identical strategy code | `Strategy` base + `StrategyRunner` route by symbol; strategies propose `(symbol, side, entry, stop)` only | `app/engine/strategy.py` |
| Identical risk decisions | `RiskEngine.evaluate(signal, state)` is **pure and synchronous** — no IO, no DB, no broker. All IO is in `RiskStateProvider` | `app/engine/risk/engine.py`, `app/engine/risk/state.py` |
| The simulation boundary | `BrokerAdapter` ABC — **exactly 14 abstract methods** (`base.py:66-160`), one adapter per account, async context manager | `app/brokers/base.py` |
| Broker-clock anchoring for all logic time | `RiskState.now` comes from `adapter.get_clock().timestamp`; `FlattenController` re-anchors on the broker clock every wake; `risk/controls.py` imports nothing from `datetime` but `timedelta` | `risk/state.py:129,154`; `flatten_controller.py:188` |
| Fill injection without Redis | `TradeUpdateStage.on_trade_update(update: TradeUpdate)` is a plain public coroutine. `RedisTradeUpdateSubscriber` is *a* caller, not *the* interface | `app/engine/execution/trade_updates.py:95` |
| Order-state machine, FIFO lots, `lot_closes`, position tracker | All keyed off `TradeUpdate` → DB, independent of transport | `app/engine/execution/*` |
| Offline E2E patterns to mirror | `tests/engine/test_execution_e2e.py` (signal → realized P&L), `tests/engine/test_safety_e2e.py` (kill switch → flatten → verified flat) | `tests/engine/` |
| Injectable timing so sim runs fast | `ExecutionStage(resolve_delays=…, cancel_confirm_delays=…)`, `TradeUpdateStage(match_retry_delays=…)` are real constructor params | `handler.py:146`, `trade_updates.py:88` |
| Wire format for the decode round-trip (§6.3) | `_publish` serializes `dto.model_dump(mode="json")` + a `type` discriminator; `MarketDataBridge._dispatch` pops `type` and `Bar.model_validate`s the rest | `streams.py:559-569`, `market_bridge.py:72-84` |

**Consequence:** a `SimulatedBrokerAdapter` needs zero new hooks. `ExecutionStage`, `RiskStage`, `TradeUpdateStage`, `ReconciliationService`, and `FlattenController` all take `adapter: BrokerAdapter` by constructor injection today.

### What does NOT exist

| Missing | Notes |
|---|---|
| **Any historical-bars client** | `app/brokers/` has zero references to `data.alpaca.markets`, `/v2/stocks/bars`, or `get_bars`. The live spine is streaming-only |
| **Any bar persistence** | No `bars` table; 3 migrations exist (`5e48fb608876`, `a7c31d90f2e4`, `b5e711f11a58`) and none touch market data |
| **Any simulated adapter** | `tests/engine/builders.py:124` has `FakeEngineAdapter`/`RecordingAdapter` — canned responses with a **frozen** clock and account, not a state machine. This frozen-ness is what killed revision 1's Run B (§6) |
| **Any run isolation** | One database, one schema, `search_path` untouched. `alembic/env.py` has no `include_schemas`, no `version_table_schema`, no `config.attributes["connection"]` hook |
| **Any metrics harness** | No Sharpe/Sortino/DD/walk-forward code anywhere in `app/` |
| **A virtual clock** | Nothing in the engine can be told what time it is except through `adapter.get_clock()` |
| **A cached market calendar** | `get_calendar` hits Alpaca live; nothing persists sessions (half-days, holidays) |
| **A market-time column on `orders`** | Only `created_at` (Postgres wall clock) — §5.2, the single most consequential gap the review found |
| **Any trial ledger** | `DROP SCHEMA` deletes losing runs without trace — §4.2, selection bias invisible to the 3c gate |

---

## 2. Decisions locked by Sean (settled — rationale recorded, not re-opened)

### 2.1 Simulate at the broker boundary

`SimulatedBrokerAdapter` implements the `BrokerAdapter` ABC. **Everything above it runs unchanged**: risk controls, `ExecutionStage`, the order state machine, the trade-updates writer, FIFO lots + `LotClose`, the position tracker, reconciliation, the flatten controller.

**Rationale.** Backtest P&L is then produced by the same ledger code that tracks real money. "Zero strategy changes" (the game plan's phrasing) upgrades to "zero *engine* changes" — a strictly stronger and much more useful claim, because the parts most likely to lie about performance (lot accounting, partial-close attribution, the same-day breaker's view of realized P&L) are exercised identically.

**Rejected: an `ExecutionHandler`-level seam.** Faster to build, and it is what the game plan's `SimulatedFill` box in the architecture diagram implies. Rejected because backtest P&L would then come from a different code path than live — the legacy divergent-path failure mode, reintroduced in the one place where you cannot see it happening.

**Review outcome: confirmed, with a new argument for it.** The realism lens observed that an `ExecutionHandler`-level seam would have **hidden** both A1 (the inert-fixture parity hole) and A2 (the dead drawdown ladder) rather than exposing them — a higher seam never touches `RiskStateProvider`, so neither defect would have been visible at design time. The boundary choice paid for itself before any code was written.

*Naming note:* the game plan calls this component `SimulatedFill`. The boundary moved down a layer; the name in code is `SimulatedBrokerAdapter`, and the fill-matching logic inside it is `FillModel`. Worth a one-line correction in the game plan when this ships.

### 2.2 `BarHistorySource` interface; Alpaca IEX now → Databento later; cache bars in Postgres

**⚠️ This rationale was rewritten. Revision 1 argued from quotes for an artifact built from trades — the review was right and the old paragraph was wrong.**

A minute bar is **derived from prints**, not from quotes. IEX carries ~2–3% of consolidated volume (`docs/RESEARCH.md` §7: AAPL 12,630 IEX vs 535,136 consolidated trades/day). Revision 1 said price on liquid large-caps "is fine because NBBO keeps quotes near-honest." That argument is about the *quote* feed. It does not transfer to a bar, and the failure modes it hides are directional:

| IEX bar artifact | Consequence in a backtest | Direction |
|---|---|---|
| `high`/`low` are the extremes of a ~3% sample → biased **inward** vs the consolidated range | A stop resting inside the consolidated range but outside the IEX range **never triggers** — the backtest keeps losers alive that live would have closed | **Optimistic** |
| An absent bar is indistinguishable from a quiet minute | Resting orders are simply not evaluated in that minute — no fill, no stop check, no leg activation | **Optimistic** |
| `volume` is a ~3% sample | RVOL / VWAP / participation-cap logic is meaningless | Unusable (already known) |
| `close` on a liquid large-cap | Genuinely close to consolidated — the one thing IEX does honestly | OK |

**So the honest statement of why IEX is in 3a:** it makes the plumbing real at $0 — a real HTTP client, real pagination, a real cache, real Decimal parsing, a real bar-shaped input to the replay driver — while the numbers it produces are only good enough to prove the *machinery* runs. The earmarked Databento **$125 free credit** buys honest prints when Phase 4's volume-dependent strategies arrive. An interface, not a client, is what makes that swap a config change.

**Hard rule, enforced in code, not in judgment:**

> **No IEX-derived backtest may inform a promotion decision.** `backtest_runs.feed` records the feed for every run; the 3c promotion gate rejects any run whose `feed` is not in the approved set (`sip`, `databento`). Iron law #8's gate consumes 3c metrics, so this is where it belongs — as a filter on the gate's inputs, not a note in a doc.

**Quality diagnostics (PR-A).** Every run report and `backtest_runs.metrics` carries, per symbol: median/min `Bar.trade_count` per RTH minute, count of RTH minutes with **no bar at all**, and RTH coverage percentage. A run whose fixture window has 40% of its RTH minutes missing is not a backtest; the diagnostics make that visible instead of inferable.

Postgres cache for reproducibility (a backtest you cannot re-run against identical inputs is not an experiment) and consistency with the pgvector "one store, one backup" decision.

### 2.3 Schema per run

Same tables, same models, same queries — a dedicated Postgres schema per backtest run, selected via `search_path`. Deleting a run is `DROP SCHEMA … CASCADE`.

**Rationale.** Requires **zero changes to any engine query** — which is precisely what makes the parity claim credible. A backtest that had to teach every query about runs would be a different program.

**Rejected: a `backtest_run_id` column.** Every query in the codebase must then filter it; miss one and backtest rows contaminate live P&L, the same-day breaker, or the drawdown ladder. The failure is silent and lands in the risk layer.

**Review outcome: decision confirmed, implementation nearly fatal.** Revision 1 specified `search_path = "bt_x, public"`. The isolation lens **reproduced against real Postgres** that this fails open into live data — see §4.3, and treat that fix as the single highest-leverage edit in this revision.

### 2.4 Parity spine first

| Slice | Ships | Deliverable |
|---|---|---|
| **3a** | Bar history + cache, run isolation + trial registry, replay driver, clock virtualization, market-time column, per-session equity snapshots, `SimulatedBrokerAdapter`, **the golden projection test** | The proof that the engine is one program, over a reviewed ledger |
| **3b** | Fill realism: measured spread/slippage, partial fills, commissions+fees, session mechanics (flatten at 15:55, per-session reconcile), realized-vs-modeled slippage measurement, fault injection for the dark REST-first paths | A backtest whose numbers are pessimistic enough to trust |
| **3c** | Metrics (Sharpe/Sortino/max DD/exposure/hit rate), walk-forward harness, trial-count-aware promotion gates wired to the lifecycle | The `paper → live` gate from iron law #8 with real numbers behind it |

---

## 3. Build order — one prerequisite micro-PR, then three PRs

Revision 1 specified six units in one diff. **The review overturned that on calibration grounds** (triage B13): 2b was 66 new tests + 1 migration; 2c was 190 tests + 1 migration and *still* shipped 5 defects that only a live dress rehearsal could reach. As written, 3a was larger than 2b and 2c combined, and landed a money-path migration in the same diff as an ~800-line broker simulator. Revision 2's accepted findings **add** work (equity snapshots, trial ledger, cache hardening), so the split is not optional.

The live-paper E2E has **never run** (`docs/STATE.md`; the note carried into Phase 3). So the ordering still front-loads everything with **zero exposure to live-broker semantics** and isolates all such exposure into PR-C.

### 3.0 Prerequisite micro-PR — `fix/fifo-deterministic-order` (against `main`, NOT Phase 3)

**This is a live money bug in merged code, not a backtest concern** (triage C1, escalating revision 1's own §5.2b). Verified reachable on the **live stream path**: `_synthesize_gap` uses `occurred_at=update.timestamp` and the real fill's `apply_fill_to_lots` uses the *same* timestamp, in one transaction — so `Lot.opened_at` ties, and `created_at` ties too (`func.now()` is `transaction_timestamp()`, constant within a transaction). The tiebreak in `lots.py:146` is `Lot.id`, a random `uuid4` (`models/base.py:13`).

Gap lot 10 @ 100.00 and real lot 10 @ 100.40, later sell 5 @ 101.00 → realized P&L is **$5.00 or $3.00** depending on a UUID. On the flatten path the widened cross-strategy match means the coin flip also decides **which strategy's** same-day breaker moves.

The same defect has a second head: `risk/state.py:207` orders `_consecutive_losses` by `Lot.closed_at.desc()` **with no tiebreak at all**, and `lots.py:174` stamps an identical `closed_at` on every lot a single fill closes. One exit closing three lots with P&L `(−5, +3, −2)` yields a streak of **0, 1, or 2** depending on arbitrary row order — feeding `check_consecutive_losses`, a **halt** control at 4 (`config.py:66`). Different halts → different trades. The canonical projection cannot normalize this away: it changes results, not labels.

**Scope:** add a deterministic monotonic ordering to `lots` (a `BIGSERIAL seq` column, or equivalent) and use it as the final tiebreak in **both** `lots.py:146` and `state.py:207`. One migration, two `order_by` clauses, tests that pin the ordering under identical timestamps. Deep-tier review (touches `risk/`, `execution/`). Sequence **before PR-B** — PR-B's reviewer should not be reading two clock fixes at once.

### 3.1 PR-A — data plumbing and run isolation (zero money-path exposure)

| Unit | Contents |
|---|---|
| A1 | `BarHistorySource` protocol, `AlpacaBarHistory` (data host, pagination, embargo clamp), `CachedBarHistory` |
| A2 | Migration: `bars`, `bar_coverage` (tstzrange + exclusion), `market_calendar` (**four** columns), `corporate_actions`, `public.backtest_runs` |
| A3 | `alembic/env.py`: `config.attributes["connection"]` hook |
| A4 | `app/backtest/run.py`: `create_run` / `drop_run` / `drop_orphans`, `search_path` pinned to the run schema alone, pgvector ensured in `public` first, post-migration invariant checks |
| A5 | Trial registry writes: every run registers before its schema exists and survives `drop_run` |
| A6 | Bar quality diagnostics (§2.2) |

**Why first:** none of it can be falsified by anything the live-paper E2E teaches us, and it unblocks both other PRs.

**Definition of done — PR-A**

- [ ] `BarHistorySource` protocol + `AlpacaBarHistory` + `CachedBarHistory`; unit tests covering pagination, the embargo clamp, coverage gap/merge semantics, split-adjustment cache keying, and the "returned zero rows over an RTH-overlapping range" hard failure
- [ ] Alembic migration creating `bars`, `bar_coverage`, `market_calendar` (all four session columns), `corporate_actions`, `public.backtest_runs`
- [ ] `bar_coverage` half-open `[start, end)` convention documented and enforced; overlapping-range writes are impossible (exclusion constraint) **or** merged on write under lock — with a test that hammers both
- [ ] Coverage recording refuses to claim any range past `now − embargo`; a test proves a 16:05 warm of a 09:30–16:00 window does **not** mark 15:50–16:00 covered
- [ ] Corporate-actions invalidation: a coverage row whose `fetched_at` predates a split ex-date in its range is not served; test with a synthetic split
- [ ] `alembic/env.py` accepts `config.attributes["connection"]`, with the existing standalone-engine path unchanged (existing migration tests still green)
- [ ] `create_run` / `drop_run` / `drop_orphans(older_than=…)`; `search_path` pinned to the run schema **alone**; `NullPool`
- [ ] pgvector ensured in `public` **before** migrating; post-migration invariants assert (i) all 12 model tables + `alembic_version` present in the run schema, (ii) `vector`'s `pg_extension` namespace is `public`, (iii) the shared-input tables are absent from the run schema (§4.3)
- [ ] Isolation tests: a run-engine query for a table that does not exist in the run schema raises `UndefinedTable` (fail-loud), and a `SELECT current_schema()` from `run.session_factory` returns the run schema
- [ ] Registry test: `create_run` → `drop_run` → the `backtest_runs` row is still there with its `dropped_at` set; `drop_orphans` finds and removes a schema whose registry row is `creating` and older than the threshold
- [ ] `BacktestRun` session-factory identity test: `run.session_factory is not app.core.database.async_session_factory` **and** `run.engine is not app.core.database.engine`
- [ ] Bar quality diagnostics computed and stored on `backtest_runs.metrics`

### 3.2 PR-B — the clock fix (small diff, DEEP-TIER review)

Touches `app/engine/risk/`, `app/engine/execution/`, and `app/services/` → `CLAUDE.md`'s deep-tier rule applies: **trace signal → risk → execution → broker end to end, not the diff in isolation.**

| Unit | Contents |
|---|---|
| B1 | `orders.market_created_at` — NOT NULL, backfilled; stamped at both insert sites (§5.2) |
| B2 | `RiskApproval.approved_at` sourced from `state.now` (the broker clock) |
| B3 | `ReconciliationService.reconcile_portfolio` takes an injectable `now`; the grace comparison ages against `market_created_at`, never `created_at` |
| B4 | `ProbeStrategy._entries_already_today` predicate switched to `market_created_at` |
| B5 | The generalized `created_at` invariant + its grep-based CI test (§5.4) |
| B6 | `loader.py:64` ordering → `(created_at, id)` |

**Why alone:** it is the only PR in 3a that changes code the live engine executes tomorrow. A reviewer should be able to hold all of it in their head. The prerequisite micro-PR (§3.0) has already landed the `lots`/`state` ordering fix, so PR-B's reviewer reads exactly one clock change.

**Definition of done — PR-B**

- [ ] Migration adds `orders.market_created_at TIMESTAMPTZ NOT NULL`, backfilled `= created_at` for existing rows (correct: those rows were written under the wall clock, which *was* their market time)
- [ ] Both `Order(...)` construction sites populate it — `handler.py:296` (`_persist_pending`) and `apply.py:107` (`persist_bracket_legs`, inheriting the parent's value). A grep test asserts these remain the only two sites
- [ ] `RiskApproval.approved_at` comes from `state.now`; a live-shaped test asserts it equals the broker clock, not the process clock
- [ ] The flatten path's market time is sourced from the adapter (§5.2), with a test that a liquidation persisted under a simulated clock carries the simulated instant
- [ ] `reconcile_portfolio(..., now=…)` injectable, defaulting to `datetime.now(UTC)`; `_handle_unknown`'s `age_ok` compares against `market_created_at`
- [ ] **The livelock regression test:** a `pending_submit` order stamped with a historical market time, reconciled with a historical `now`, ages into `failed` after the 120s grace (`reconciliation.py:87`). Under revision 1's design this test fails by ≈ −152 days
- [ ] `ProbeStrategy._entries_already_today` filters on `market_created_at`; a two-ET-session test asserts one entry per session, not one entry per backtest
- [ ] The §5.4 grep test is in CI and red when someone adds `created_at >=` to `app/engine/**` or `app/services/**` outside the allowlist
- [ ] `loader.py` orders by `(created_at, id)`; test seeds two strategies in one transaction and asserts a stable dispatch order
- [ ] Full existing suite green — this PR changes live behavior and its blast radius is the whole engine

### 3.3 PR-C — the simulator and the proof

| Unit | Contents |
|---|---|
| C1 | `SimClock` + `ReplayDriver` with the §4.4 tick ordering |
| C2 | `SimulatedBrokerAdapter` — all 14 methods, invariants 1–13 |
| C3 | `FillModel` with non-zero default slippage + the decided spread source |
| C4 | Per-session equity snapshots (§4.6) |
| C5 | The golden canonical-projection test, decode round-trip, mutation check, liveness assertions (§6) |
| C6 | Property tests (§6.5) |

**Definition of done — PR-C**

- [ ] `SimClock` (monotone `advance_to`, tz-aware UTC) + `ReplayDriver` implementing the §4.4 ordering exactly, including the intra-tick fixed point for legs activated by a parent fill in the same tick
- [ ] The look-ahead canary test (R2): a strategy that tries to emit a signal priced at the *next* bar's close must be impossible to write — the value is unreachable from any argument `on_bar` receives
- [ ] `SimulatedBrokerAdapter`: 14 methods, with `close_position` / `close_all_positions` raising `NotImplementedError` citing the 2026-08-01 decision (`docs/STATE.md`)
- [ ] Invariants **1–13** (§4.5) each asserted by at least one test, and each one demonstrated to be able to fail (a deliberately broken variant in the test, xfail-style)
- [ ] `FillModel`: non-zero default slippage, strict limit penetration, gap-through at the open, same-tick leg eligibility with stop-wins ambiguity, spread from the decided source (§4.1)
- [ ] Equity snapshots written on the live cadence (§4.6); a test proves `check_drawdown_halt` fires in a replay engineered to draw down past `drawdown_halt_pct`, and that `drawdown_size_factor` halves past `drawdown_halve_pct`
- [ ] `last_equity` rolls at session close; a test proves a 2% session-1 loss does **not** latch the portfolio daily-loss breaker into session 2
- [ ] **The golden test** (§6.2), green in CI, over a ≥ 2-ET-session committed fixture, with the projection committed as a reviewed artifact
- [ ] **The mutation check** (§6.2): a parameterized variant that perturbs one `RiskLimits` field and asserts the golden comparison fails
- [ ] **Liveness assertions** (§6.2) inside the golden test itself, not a separate test that can be skipped
- [ ] **The decode round-trip test** (§6.3)
- [ ] The determinism test: the same fixture twice, two fresh run schemas, byte-identical projections
- [ ] Property tests (§6.5), including the monotonicity property
- [ ] `BacktestRun` records resolved `RiskLimits`, the settings digest, `fill_model_config`, `feed`, `adjustment`, `git_sha`, and the assumed `Strategy.status`
- [ ] `app/backtest/broker.py` added to the deep-tier review path list in `CLAUDE.md`
- [ ] `docs/STATE.md` Key Decisions Log rows for §2.1–2.4 and for everything the review changed

### Why this survives the still-unrun live E2E

**The golden test asserts that OUR engine reproduces a reviewed ledger over a fixed input. It is a claim about our code, not about Alpaca.** Nothing the live-paper E2E reveals about real broker behavior can falsify it.

The two open live questions carried in `docs/STATE.md` — whether a **filled** bracket parent's protective legs appear as top-level rows under `list_orders(nested=False)` (`handler.py:434`), and exact `position_intent=*_to_close` semantics under a quantity race — are **fill-realism** concerns. They change what `SimulatedBrokerAdapter` should *pretend Alpaca does*. They fold into **3b**. If Monday's run contradicts a sim assumption, the sim changes, the golden projection is regenerated **under review** (§6.2), and the structural parity claim is untouched.

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
        start: datetime,          # tz-aware UTC, INCLUSIVE
        end: datetime,            # tz-aware UTC, EXCLUSIVE  →  half-open [start, end)
        timeframe: BarTimeframe = BarTimeframe.minute_1,
    ) -> AsyncIterator[Bar]: ...  # app.brokers.dto.Bar — the SAME DTO the live stream produces
```

**Contract (invariants a reviewer should hold us to):**

- Yields `app.brokers.dto.Bar` (`dto.py:229-244`). **There is no backtest-specific bar type.** A second bar shape is a second code path.
- **Total order**: ascending by `(timestamp, symbol)`, symbol as a *lexicographic* secondary key. The bus is FIFO; cross-symbol order within one timestamp is observable to strategies, so it must be deterministic and documented, not incidental. (⚠️ This is also a selection bias — see §11.)
- Money/prices/volume are `Decimal`, parsed via `Decimal(str(x))` (iron law #7, mirroring `alpaca/rest.py::_dec`).
- Timestamps tz-aware UTC (iron law #5).
- **Half-open `[start, end)` everywhere** — the protocol, the cache, the coverage ranges, and the replay window all use the same convention. A closed/half-open mismatch is how you double-count a boundary bar or lose one.
- Yields **only bars that exist**. Absence of a bar is not zero volume — see `bar_coverage` below.

**The embargo clamp (triage B3 — reproduced reasoning, and it lands on the flatten window).**

The free Basic plan cannot query the most recent ~15 minutes. Revision 1's cache recorded that we *asked* for a range, not that we *got* it. Warm SPY at 16:05 for 09:30–16:00 → bars arrive through ~15:50 → **15:50–16:00 is marked covered and empty forever.** That is the 15:55 flatten window: every backtest of that day replays a dead tape through the most safety-critical minute of the session, and a flatten test passes on no data.

So: `AlpacaBarHistory` clamps `end` to `min(end, now − EMBARGO)` and `CachedBarHistory` **refuses to record coverage past the clamped end**. A caller asking for an unavailable range gets a short read plus an explicit `coverage_end` in the result, not a silent lie. ⚠️ `EMBARGO` is 15 minutes per RESEARCH §7 — verify against current Alpaca docs and add a safety margin; make it a per-feed constant, since SIP/Databento have none.

**Plausibility check (triage B3, second half).** Every coverage row stores `bar_count`. On write, `CachedBarHistory` computes the expected RTH minute count from `market_calendar` for the overlapped sessions and stores it as `expected_bar_count`.

- `bar_count == 0` over a range that overlaps RTH → **hard failure**, no coverage recorded. This is unambiguous: a live symbol with zero prints in an entire RTH overlap is a fetch failure, not a quiet market.
- `0 < bar_count < expected` → recorded, with `density` surfaced in the quality diagnostics (§2.2). *No magic ratio threshold* — an illiquid symbol legitimately has sparse minutes on IEX, and a hardcoded 25%-style cutoff would either reject real data or pass a broken fetch. Density is a diagnostic a human reads, not a gate. **⚠️ Judgment call — a reviewer who wants a hard gate should say what the number is and defend it.**

**Implementations:**

| Class | Notes |
|---|---|
| `AlpacaBarHistory` | async httpx against the **data** host (`data.alpaca.markets`), NOT the trading host `rest.py` uses. Reuses `AsyncTokenBucket`. ⚠️ Exact path, params (`symbols`/`timeframe`/`start`/`end`/`limit`/`page_token`/`feed`/`adjustment`), response envelope, and pagination token name **must be verified against Alpaca docs and one live call** before coding. Two constraints already known from RESEARCH §7: the embargo above, and the trading-API 200 req/min cap is a separate bucket from the data API's — do not assume they share one |
| `CachedBarHistory` | Wraps a source. Reads Postgres, fetches only uncovered ranges, writes back. Must never return a bar with `timestamp >= end` (R2) |
| `DatabentoBarHistory` | Phase 4+, when honest prints are required |

**`adjustment` is not optional.** Bars must be fetched split-adjusted at minimum. An unadjusted series across a split is not noisy data, it is a fabricated 50% gap that will trigger every stop in the backtest. The adjustment mode is part of the cache key. (⚠️ `split` vs `all` — deferred, §11.)

**The spread source — DECIDED NOW (triage B6).** Revision 1 left this open and the review was right that leaving it open would have forced a redo of §4.1/§4.2 when 3b's "promised realism" arrived. Three candidates were on the table:

| Source | Verdict |
|---|---|
| Real quotes (`QuoteHistorySource`) | **Rejected for 3a.** `BarHistorySource` has no quote path; adding one is a whole subsystem (a second endpoint, a second cache, a second coverage model) and it would be IEX quotes, which are cheap but voluminous. Right answer eventually, wrong answer now |
| `vwap` / `trade_count` proxy | **Rejected outright.** Both fields come from the same ~3% IEX sample. A spread estimated from a 3% sample is noise dressed as measurement, and it would be *believed* because it came from data |
| **Static per-symbol bps table (chosen)** | Honest about being an assumption, auditable, versioned into `backtest_runs.fill_model_config`, and replaced in 3b by the **measured** realized-vs-modeled slippage table — which is a strictly better estimator than any of the above because it is our own fills |

Mechanics: `FillModel` takes `spread_bps: Mapping[str, Decimal]` plus `default_spread_bps: Decimal`. **The default is the *widest* tier, not the narrowest** — an unknown symbol is assumed illiquid, so forgetting to add a symbol costs you edge in the backtest rather than manufacturing it. A marketable buy pays `+half_spread`, a marketable sell pays `−half_spread`, on top of slippage. A `QuoteHistorySource` protocol is **named but not implemented** in 3a, mirroring §2.2's interface-not-client logic, so 3b's swap is a config change.

### 4.2 Bar cache + shared-input schema (new Alembic migration)

```
bars                 PK (symbol, timeframe, feed, adjustment, ts)
                     open, high, low, close, volume   NUMERIC  -- Decimal, iron law #7
                     trade_count INT NULL, vwap NUMERIC NULL

bar_coverage         id, symbol, timeframe, feed, adjustment,
                     range TSTZRANGE            -- half-open [start, end)
                     bar_count INT NOT NULL, expected_bar_count INT NULL,
                     fetched_at TIMESTAMPTZ NOT NULL
                     EXCLUDE USING gist (symbol WITH =, timeframe WITH =, feed WITH =,
                                         adjustment WITH =, range WITH &&)

market_calendar      PK (trading_date)
                     rth_open, rth_close, session_open, session_close   TIMESTAMPTZ

corporate_actions    id, symbol, ex_date, kind, ratio, discovered_at
                     UNIQUE (symbol, ex_date, kind)

public.backtest_runs id (run_id), label, schema_name, status,
                     strategy_kind, strategy_status_assumed,
                     params JSONB, params_hash,
                     window_start, window_end, feed, adjustment,
                     git_sha, settings_digest,
                     risk_limits JSONB, fill_model_config JSONB,
                     metrics JSONB, created_at, dropped_at
```

- **`feed` and `adjustment` are in the primary key.** Without them an IEX run and a SIP run silently interleave in one series, and the RVOL number that comes out is meaningless. Cheapest possible guard against the worst data-integrity failure available to us.
- **`bar_coverage` is not optional.** Without it you cannot distinguish "no bars because the symbol did not trade in that minute" from "no bars because we never fetched that range". The second one looks exactly like a quiet market and silently deletes signal.
- **`bar_coverage` now has interval semantics (triage B4).** Revision 1 had `(range_start, range_end)` with no PK, no uniqueness, and no merge rule → overlapping rows → you either refetch forever or **under-fetch and silently drop bars**. A `tstzrange` with a GiST exclusion constraint makes overlap *impossible*; the writer coalesces adjacent/overlapping ranges in one transaction before inserting. ⚠️ The exclusion constraint needs `btree_gist` (for the `=` operators on the scalar columns) — **verify it is available in the `pgvector/pgvector:pg17` image** before committing to it. Fallback: drop the constraint and implement merge-on-write under an advisory lock keyed on `(symbol, timeframe, feed, adjustment)`, with a test that hammers concurrent writers.
- **Splits poison the cache in a way `adjustment` in the PK does not fix (triage B4, second half).** A split restates every pre-split bar *upstream* while our cache keeps the old basis, and `bar_coverage` still says "covered". Splice old-basis onto new-basis bars and you get a fabricated 4× discontinuity that trips every stop in the backtest. The key is identical; the upstream data changed. Hence `corporate_actions` + the rule: **`CachedBarHistory` refuses to serve any coverage row whose `fetched_at` predates a split ex-date falling inside (or after) its range for that symbol** — it re-fetches instead. ⚠️ Alpaca's corporate-actions endpoint shape is unverified; if it is not usable in PR-A, the fallback is a manual `invalidate_symbol(symbol, since=…)` plus a documented operational step, which is worse but is at least a lever that exists.
- **`market_calendar` persists all four columns now (triage B14).** The migration is being written once; a 3b extended-hours sim needs `session_open`/`session_close`, and a later migration to add them is pure churn. `CalendarDay` (`dto.py:46-63`) keeps exposing exactly two — its docstring documents the trap (RTH `open`/`close` vs extended `session_open`/`session_close`; mapping the wrong pair turns a 15:55 flatten into a 19:55 one). The DTO surface is unchanged; the table simply stops throwing away data we already fetched.
- **`public.backtest_runs` is the trial ledger (triage B5), and it solves two problems.**
  1. **Selection bias is otherwise invisible to the promotion gate.** `DROP SCHEMA` deletes losing runs, so a 200-combination sweep reaches 3c looking like one clean result. Best-of-200 under a true-zero-edge null sits ~2.5–3 SE above zero. Walk-forward controls for *regime* dependence, not *trial count*; only a registry does. This is a **3a** decision because run isolation is 3a — retrofitting cannot reconstruct history that was never written.
  2. **Orphan schemas** (triage B7) — with no way to enumerate runs, any crash or timeout leaves `bt_*` schemas accumulating in the shared test DB forever.
  The row is written **before** `CREATE SCHEMA` with `status='creating'`, so a crash mid-create is still enumerable; it is updated to `complete`/`failed` and **survives `drop_run`** (which sets `dropped_at`, never deletes). `drop_orphans(older_than=…)` sweeps both registry rows stuck in `creating` and any `bt_%` schema in `information_schema.schemata` with no registry row at all.

**These tables are shared *inputs*, not per-run outputs.** See §4.3 for placement — the answer changed.

### 4.3 `app/backtest/run.py` — run isolation

```python
@dataclass(frozen=True, slots=True)
class BacktestRun:
    run_id: uuid.UUID
    schema: str                                   # f"bt_{run_id.hex}" — 35 chars, well under PG's 63
    engine: AsyncEngine                           # search_path pinned to THIS SCHEMA ALONE; NullPool
    session_factory: async_sessionmaker[AsyncSession]
    risk_limits: RiskLimits                       # resolved, recorded, never re-read from env
    settings_digest: str
    fill_model_config: Mapping[str, object]

async def create_run(*, label: str, risk_limits: RiskLimits, ...) -> BacktestRun: ...
async def drop_run(run: BacktestRun) -> None: ...           # DROP SCHEMA … CASCADE; registry survives
async def drop_orphans(*, older_than: timedelta) -> int: ...
```

#### The `search_path` fix — the single highest-leverage edit in this revision (triage B1)

Revision 1 said: `connect_args={"server_settings": {"search_path": f"{schema}, public"}}`, and justified the `, public` fallback as "so extension types resolve".

**The isolation lens reproduced against a real Postgres what that does.** `alembic/env.py:59-62` configures the migration context with no `version_table_schema`. With `public` on the search path, Alembic resolves `alembic_version` **unqualified to public**, reads the head revision that is already there, concludes there is nothing to do, and exits 0. **Zero tables are created in the run schema.** Every subsequent unqualified query — every query the engine makes, because §2.3's entire point is that queries are unqualified — falls through to `public`. Backtest orders, lots, and lot_closes land in **live P&L**, silently, inside the risk layer. That is the legacy failure mode with a new hat on.

**Fix: pin `search_path` to the run schema ALONE.**

```python
create_async_engine(
    url,
    connect_args={"server_settings": {"search_path": schema}},   # NO ", public"
    poolclass=NullPool,
)
```

Verified by the lens: all 12 model tables land in the run schema and `alembic_version` follows automatically — no `version_table_schema` needed, because the version table is resolved through the same pinned path. Any leak then becomes `relation "orders" does not exist`: **fail-loud instead of fail-into-production.** Nothing needs `public` today — there are no vector columns on any model yet, and `now()`, `hashtext()`, and the JSONB operators all live in `pg_catalog`, which is always on the path implicitly.

⚠️ Still unverified: that asyncpg accepts `search_path` in `server_settings`. It should — it is an ordinary GUC delivered as a startup parameter — but verify, do not assume. If it does not work, the fallback is a per-checkout `SET search_path` via a pool event listener, and the leak risk must be re-audited from scratch, because that fallback is exactly the thing `NullPool` exists to survive.

#### pgvector must be ensured in `public` first (triage B2)

`alembic/versions/2026_06_10_1700-5e48fb608876_initial_domain_schema.py:26` runs `CREATE EXTENSION IF NOT EXISTS vector`. With `search_path` pinned to the run schema, an extension with no `SCHEMA` clause installs **into the run schema**. Extensions are database-unique, so:

- on a fresh database, run 1's migration installs `vector` into `bt_1`;
- run 2's `IF NOT EXISTS` is a no-op, and run 2 cannot resolve the `vector` type;
- `DROP SCHEMA bt_1 CASCADE` **uninstalls pgvector for the entire database**, taking Phase 6's memory tables with it.

**Fix:** `create_run` executes `CREATE EXTENSION IF NOT EXISTS vector SCHEMA public` on a **public-bound** connection before creating or migrating the run schema. The step-4 invariant below checks `pg_extension` joined to `pg_namespace`, not just `pg_tables`.

#### Mechanics

1. Insert the `public.backtest_runs` row with `status='creating'` (so a crash is enumerable).
2. `CREATE EXTENSION IF NOT EXISTS vector SCHEMA public` on the public connection.
3. `CREATE SCHEMA bt_<hex>`.
4. Run `alembic upgrade head` against a connection whose `search_path` is that schema **alone**. ⚠️ Requires the one infra change in PR-A: `alembic/env.py:66-80` builds its own engine and never checks `config.attributes`. Add the standard recipe — `connectable = config.attributes.get("connection")`, falling back to current behavior. Nothing else in the migration chain changes.
5. **Drop the shared-input tables from the run schema** (see placement, below).
6. **Post-migration invariant check (mandatory, fail-closed):**
   - every table in `Base.metadata` exists in `bt_<hex>` (`pg_tables`), and `alembic_version` is there too — 13 relations;
   - `vector`'s namespace in `pg_extension` is `public`;
   - `bars`, `bar_coverage`, `market_calendar`, `corporate_actions`, `backtest_runs` are **absent** from `bt_<hex>`;
   - `SELECT current_schema()` from a `session_factory` session returns `bt_<hex>`.
7. Seed the run: one `users` row, one `portfolios` row, the `strategies` row(s) under test. No `broker_credentials` needed — the adapter is the simulator.
8. Update the registry row to `status='ready'`.

#### `NullPool` is a SAFETY invariant, not a performance knob (triage B14)

Revision 1's R4 listed `NullPool` as a performance mitigation, which implied it was negotiable. **It is not.** Two independent reasons:

1. A pooled connection can **outlive `drop_run`**, leaving a checked-in connection whose `search_path` names a schema that no longer exists. The next checkout fails, or worse, resolves somewhere unexpected.
2. Any `SET search_path` issued mid-session — Alembic's own, a future helper, a debugging session — is **transaction-durable and survives a pooled checkout**. The resulting leak is *intermittent*, which is strictly worse than deterministic: it passes tests and fails in the run that matters.

Documented on `BacktestRun` itself, not in a risk register where it reads as advice.

#### `BacktestRun` is the ONLY session source inside a run (triage B11)

`engine_main.py:57` imports the module-level `async_session_factory` (bound to `public`, `core/database.py:29`) and threads it into every stage (`:313`, `:321`, `:329`, `:338`, `:347`). That file is the template any replay harness will be copied from. Copy one of those lines and `ProbeStrategy._entries_already_today` counts **live** orders while everything else writes the run schema. **B1's `search_path` fix does not cover this** — the global factory has its own engine with its own connection settings, so it never sees the pin.

Guards, all in PR-A:

- a test asserting `run.session_factory is not app.core.database.async_session_factory` **and** `run.engine is not app.core.database.engine`;
- a constructor audit test that walks the assembled replay stack and asserts every stage's `_session_factory` is `run.session_factory` by identity;
- a `SELECT current_schema()` assertion from a session obtained through `run.session_factory`.

#### Reproducibility is defined over too narrow a scope without this (triage B10)

`RiskLimits.from_settings(get_settings())` (`risk/controls.py:59-73`) reads `.env`. Every number driving sizing, breakers, and the flatten buffer — `risk_per_trade_pct`, `unproven_risk_per_trade_pct`, `daily_loss_limit_pct`, `max_consecutive_losses`, `drawdown_halve_pct`, `drawdown_halt_pct`, `margin_headroom_factor`, `flatten_buffer_minutes`, `symbol_cooldown_seconds` (`config.py:60-71`) — is an **uncommitted input**. Same fixture, CI vs laptop, different `.env` → different P&L, and revision 1's parity test would have stayed green because both runs read the same `.env`.

So:

- `BacktestRun` carries a **resolved** `RiskLimits` object, recorded verbatim into `backtest_runs.risk_limits`;
- fixtures construct `RiskLimits(...)` **explicitly**, never via `from_settings(get_settings())`;
- `settings_digest` is a sha256 over the sorted, serialized set of `Settings` fields that can affect engine behavior (the risk block above, plus `reconcile_interval_seconds`), recorded per run;
- **`tzdata` is pinned in CI.** `ZoneInfo("America/New_York")` (`risk/state.py:40`) resolves against system tzdata; a tzdata bump that moves a historical DST transition moves every ET day boundary in a replay. ⚠️ Verify whether the container image supplies tzdata or whether the PyPI `tzdata` package is the resolution source, and pin whichever one actually wins.

#### Placement of the shared-input tables — the answer changed

Revision 1 offered A (schema-less models, read through the public engine) vs B (`__table_args__ = {"schema": "market_data"}`) and recommended A. **B1 changed the calculus and neither option is now correct as written:**

- Under A with `search_path` pinned to the run schema alone, every run schema gets a **duplicate empty `bars` table** — and a bar reader that accidentally used the run engine would read *empty results* instead of raising. That converts B1's fail-loud property into fail-silent for exactly the four tables where silence looks like "the market was quiet". Worse than the original con.
- B (a `market_data` schema) is architecturally cleanest, but the migration op `op.create_table("bars", schema="market_data")` runs inside **every** run-schema migration and fails on the second run. Making it work needs Alembic branch labels (two roots, `upgrade run@head`) — real complexity landing on a working migration chain in the same PR as run isolation.

**Recommended: option A′.** Shared-input models stay schema-less and are read/written through the ordinary `public`-bound engine; `create_run` **drops the duplicate empty copies out of the run schema** immediately after migrating (step 5 above), and the step-6 invariant asserts they are gone. Any accidental run-engine read of `bars` then fails with `relation "bars" does not exist` — identical to B1's philosophy, one line of SQL, zero Alembic surgery. The cost is a small, explicit divergence between the run schema's contents and what its `alembic_version` claims, which is documented at the drop site.

⚠️ **New, not reviewed by any lens.** Option A′ is my resolution of an interaction B1 created; it deserves a fresh look. Revisit at 3c, when a `market_data` schema and an Alembic branch may both be worth the churn.

**Also note:** `engine_main.py`'s singleton advisory lock keys on `hashtext('roigen-engine-<portfolio_id>')`, which is **database-wide, not schema-scoped**. 3a's replay driver does not run `engine_main`, so this does not bite yet — see §11.

### 4.4 `app/backtest/clock.py` + `replay.py`

```python
class SimClock:
    def now(self) -> datetime: ...                     # tz-aware UTC
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
  2. await broker.on_clock_advance(now, bars_by_symbol)   # match resting orders → activate legs →
                                                          # re-match legs against THIS SAME bar →
                                                          # repeat to a fixed point → TradeUpdates
                                                          # → TradeUpdateStage.on_trade_update
  3. await bus.drain()          # settle FillEvents from step 2
  4. publish BarEvent per symbol, in sorted order
  5. await bus.drain()          # signals → risk → execution → sim acceptance
  6. for hook in on_tick: await hook(now)   # equity snapshots (§4.6); 3b: flatten tick, reconcile
  7. await bus.drain()
```

Ordering rationale, each item a live-impossible state it prevents:

- **Fills before bars (2–3 precede 4).** A strategy must never see bar *N* while unaware that its stop filled during bar *N*. In live that state cannot occur; letting it occur in sim is a divergence dressed as a timing detail.
- **A market order emitted on bar *N* fills at bar *N+1*'s open, never at bar *N*'s close.** The strategy has already *seen* bar *N*'s close; filling there is trading on information you already acted on. This is the single most common way a backtest fabricates edge.
- **`drain()` between every phase.** The bus is breadth-first within a drain; without the split, a fill from step 2 could interleave with a signal from step 4 in an order that depends on queue depth.
- **Step 2 iterates to a fixed point (new — triage B8e).** When a bracket parent fills at bar *N+1*'s open, its protective legs become active *at that instant*, and bar *N+1*'s own range may already contain the stop. If legs only became eligible on bar *N+2*, max-adverse-excursion would be fiction in exactly the direction that flatters the strategy. So: match → activate → re-match against the same bar → repeat until no new fills. Termination is guaranteed because each iteration strictly reduces the set of unfilled orders. Iteration order within a round is deterministic (by broker order id, which is a monotonic counter).

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

Internal state: `_orders` (by broker id + client-id index), `_positions` (signed qty, avg entry, cost basis), `_cash`, `_last_equity` (equity at the prior session close), `_last_price`, and **monotonic counters** for broker order ids (`sim-{n}`), client order ids for legs (`simleg-{n}`), and execution ids (`simx-{n}`).

All 14 ABC methods:

| Method | Simulated behavior |
|---|---|
| `get_clock` | `MarketClock(timestamp=clock.now(), is_open/next_open/next_close from calendar)`. **This is the virtualization of every logic clock in the engine** — `RiskState.now`, the ET day boundary, the flatten window all flow from here for free |
| `get_calendar` | Slice of the cached real calendar. Half-days included — a synthesized 9:30–16:00 calendar silently un-tests the early-close path |
| `get_account` | `equity = cash + Σ(signed_qty × last_price)`; `buying_power = equity × margin_multiple`; `last_equity` = **prior session close** equity (invariant 13); `trading_blocked`/`account_blocked` = False. **No PDT fields** (iron law #3) |
| `list_positions` / `get_position` | Non-zero positions, signed qty |
| `list_orders` | Honors `status`, `after`/`until`, `limit`, and **`nested`**: `nested=True` inlines legs on the parent, `nested=False` returns each leg as its own top-level row |
| `get_order` / `get_order_by_client_id` | Lookups; `None` when unknown. **Read-only — never emits** (invariant 7) |
| `submit_order` | Validate → `OrderRejected` on definitive refusal (qty ≤ 0, insufficient buying power, held-qty conflict). Create parent + `held` legs for brackets, **each with a minted non-null `client_order_id`** (invariant 10). Emit a `new`/`accepted` `TradeUpdate`. Return `BrokerOrder` |
| `cancel_order` | Terminal-state guard, emit `canceled`, cancel the OCO sibling |
| `cancel_all_orders` | Loop over working orders |
| `close_position` / `close_all_positions` | **`raise NotImplementedError`**, citing the 2026-08-01 decision |
| `aclose` | No-op |

**Review overturned `close_position`.** Revision 1 said "implemented faithfully even though the engine never calls them", reasoning that a stub would make a future caller silently different from live. The review's counter is better: `docs/STATE.md` (2026-08-01) records the decision that *liquidations submit via `submit_order` with OUR `client_order_id`, never the broker's `close_position` endpoint* — because `close_position`'s broker-generated id makes a response timeout an untracked live order whose fills orphan. Emulating an endpoint we have **banned** teaches a future caller that it works. `NotImplementedError` with the decision in the message is free, louder, and more protective.

**Fill matching (`FillModel`) — 3a defaults.** Revision 1 called this table "deliberately pessimistic". The review's verdict: that was true of the three rows it wrote and **false of the table as a whole** — six of nine intrabar unknowables resolved in the strategy's favour. Corrections marked ⟳.

| Situation | Rule |
|---|---|
| Resting market order | Fills at the next bar's **open**, plus adverse slippage and half-spread |
| ⟳ **Slippage** | `slippage_bps` applied **adversely** to every marketable fill, **default non-zero** (`Decimal("5")`). Revision 1's implicit zero is a lie on the loss side of *every* trade: against the probe's 50bps stop, 5bps is 10% of 1R, and on a 40%-hit-rate 2R strategy that is ~30% of the edge. A backtest must never be *accidentally* frictionless |
| ⟳ **Spread** | Static per-symbol bps table, widest-tier default (§4.1). A buy pays `+half_spread`, a sell `−half_spread` |
| ⟳ Limit buy | Eligible on **strict penetration**: `bar.low < limit`, not `<=`. Price = `min(limit, bar.open)`. Touch-only filling keeps the excursions that reversed (the profitable ones) and discards the ones that traded through — it filters *for* winners by construction |
| ⟳ Limit sell | Mirror: `bar.high > limit`, strictly |
| Stop triggered within the bar's range | Fill at the stop price **plus adverse slippage** ⟳ |
| Stop **gapped through** (bar opened past it) | Fill at the **open** plus slippage, not the stop. Filling at the stop on a gap is the most common single lie in retail backtests |
| Bar range spans **both** the stop and the take-profit | **Assume the stop filled.** OHLC cannot order intra-bar events; the pessimistic branch is the only defensible default. A 3b option may disambiguate with finer bars |
| ⟳ **Same-bar entry-then-stop** | Legs are eligible **on the parent's own fill bar** (§4.4 step 2's fixed point). If that bar's range contains the stop, the stop fills in the same tick. Ambiguity resolves to the stop. Otherwise max-adverse-excursion is fiction in the direction that matters |
| Bracket leg fills | Sibling OCO leg immediately canceled, with its own `TradeUpdate` |

**Hard invariants for this class (each a testable assertion; 1–6 carried from revision 1, 7–13 added by the review):**

1. Never emits a `TradeUpdate` whose `timestamp > clock.now()`. *(look-ahead firewall)*
2. Never calls `datetime.now`, `random`, or `uuid.uuid4`. All ids from counters, all times from the clock.
3. Every emitted `TradeUpdate` is `await`ed into `on_trade_update` **before** the driver publishes the next `BarEvent`.
4. `TradeUpdate.position_qty` is the post-fill signed position (the writer forwards it onto `FillEvent`).
5. Money is `Decimal` end to end; quantities are whole shares (the risk engine floors to whole shares, `controls.py:140`).
6. `cash` and `positions` are mutually consistent after every fill — assert `Σ` reconciles.
7. **Emission points are exactly `on_clock_advance`, `submit_order`, `cancel_order` — never a read method, never while an engine-owned transaction is open** (triage B9). If the sim ever emits from `get_order`/`list_orders`, the nested `on_trade_update` opens a second session and blocks on a row lock the outer transaction holds. Because the outer waiter blocks in **Python**, not Postgres, the deadlock detector never fires: **the backtest hangs forever with no error and no log line.** Enforced structurally (emission goes through one private `_emit` that asserts a `_in_read` flag is clear) *and* operationally: the run engine sets `lock_timeout` so any violation surfaces as an error instead of a hang.
8. **No fill price outside `[bar.low, bar.high]` of the matched bar** (triage B8a). The highest-value assertion available and entirely absent from revision 1 — every optimism guard is downstream of it.
9. **At most one leg of an OCO pair ever fills** (B8b). Wrong cancel ordering → both fill → the position flips short and the ledger books two exits against one entry.
10. **Every simulated order, including legs, carries a non-null unique `client_order_id`** (B8c). `apply.py:97-103` **silently skips** legs with a null client id (it logs and continues, correctly — fabricating one would make the row unreconcilable). So a sim that mints null leg ids produces no leg rows at all; every protective-exit fill then orphans, realized P&L is empty, **and a naive golden snapshot regenerated from that state would look perfectly self-consistent.**
11. **Order ids minted once and stable across reads** (B8d) — `get_order`, `get_order_by_client_id`, and both `nested` modes of `list_orders` return the same ids for the same order. Regenerating ids per read makes `persist_bracket_legs` create duplicate leg rows, which apply the same fill twice.
12. **Legs activated by a parent fill in tick T are matched against tick T's own bar** (B8e; §4.4 step 2).
13. **`last_equity` rolls at session close** (B8f). `RiskState.day_pnl_portfolio` is `equity − last_equity` (`state.py:92-99`) and feeds the portfolio daily-loss breaker. A `last_equity` frozen at the initial deposit means a 2% loss in session 1 **latches that breaker for the entire remaining backtest** — every subsequent session halts, and the run looks like a strategy that stopped trading rather than a bug.

**Wiring note:** construct the stack with `TradeUpdateStage(match_retry_delays=())` and `ExecutionStage(resolve_delays=(0.0,))`. Both are existing constructor parameters — **no engine change**, and the sim never needs the retry paths because leg rows are committed before any leg fill can be emitted within the same awaited tick.

**In 3a's spine but not 3b's full stack:** `FlattenController`, per-session reconciliation, partial fills, commissions/fees. **Equity snapshots moved IN** — see §4.6.

### 4.6 `app/backtest/equity.py` — per-session equity snapshots (moved INTO 3a)

**Revision 1 put equity snapshots in 3b. The review overturned that, and this is the finding I'd most want a future reader to understand** (triage A2, found independently by three lenses).

`RiskState.peak_equity` reads `EquitySnapshot` (`risk/state.py:251-262`), whose only writer in the codebase is `reconciliation.py:154-162` — which revision 1 explicitly deferred to 3b. So in every 3a backtest: **no snapshots exist → `_peak_equity` returns `max(None-fallback, current_equity) == current_equity` → `drawdown_pct` is 0 forever** (`state.py:102-106`) → `check_drawdown_halt` never fires at 15%, and `drawdown_size_factor` never halves position size at 10%.

That is not random error, it is **bias with a direction**. A strategy that draws down 22% and recovers backtests as +31%; live, it halts at 15% and never participates in the recovery. So the backtest **systematically selects for strategies that recovered from drawdowns that would have stopped them** — precisely the filter the 3c promotion gate exists to be.

**Why "document it as a parity exception" was rejected.** §5.5's exceptions are gates that *cannot* exist in a replay (a Redis TTL key; a supervised task set). A drawdown ladder can trivially exist in a replay — it needs one `get_account()` call and one INSERT. And critically: **the parity test cannot see this defect**, because in revision 1's design both runs were equally crippled and the assertion stayed green. An inert breaker that no test can detect, feeding a promotion gate, is the exact shape of the legacy failure this project exists to not repeat.

**Design.** A tiny `on_tick` hook, no reconciliation machinery:

```python
class EquitySnapshotWriter:
    async def __call__(self, now: datetime) -> None:
        if not self._due(now):
            return
        account = await self._adapter.get_account()
        async with self._session_factory() as session:
            session.add(EquitySnapshot(portfolio_id=..., equity=account.equity,
                                       cash=account.cash, buying_power=account.buying_power,
                                       ts=now))
            await session.commit()
```

**Cadence — a refinement on the triage, with reasoning.** The triage says "they need only `get_account()` at each session close." Session-close-only is *enough to un-inert the ladder* but it is still biased in the optimistic direction, and by a lot: live writes a snapshot on **every** `reconcile_portfolio` call, which is every `reconcile_interval_seconds` = **300s** (`config.py:46`, `engine_main.py:407-415`) — roughly 78 snapshots per RTH session versus 1. `peak_equity` is a `MAX`, so a coarser sample yields a **lower** peak, a **smaller** `drawdown_pct`, and a ladder that fires **less** often than live's would. Same direction of error as having none, just smaller.

So the spec's cadence is: **write a snapshot every `reconcile_interval_seconds` of *sim* time (matching live exactly), plus one at each session close** (which is also where `last_equity` rolls, invariant 13). Cost is ~78 rows/session — nothing. Benefit is that the drawdown ladder sees the same-resolution peak in sim and in live, which is the whole point of the phase. ⚠️ This is a deliberate deviation from the triage's minimum; flagged in §10.

**Test that proves it works:** a fixture engineered to draw down past `drawdown_halt_pct` must show `check_drawdown_halt` failing in the projection, and one engineered past `drawdown_halve_pct` must show halved sizing. Under revision 1's design both tests are unwritable.

---

## 5. Clock virtualization — the audit

There are **three clocks** in this system, and 2c only disciplined one of them.

| Clock | Where | 2c's position | Phase 3's problem |
|---|---|---|---|
| **Broker clock** | `adapter.get_clock().timestamp` | *All logic time anchors here* | Solved for free — the sim adapter owns it |
| **Python wall clock** | `datetime.now(UTC)` | Permitted for audit fields only | Must be audited; §5.1 |
| **Postgres clock** | `server_default=func.now()` on `TimestampMixin` | Never considered | **Not virtualizable.** §5.2–5.4 — this is the sharp edge |

### 5.1 `datetime.now(UTC)` audit (complete — verified by grep over `app/`, and **independently re-grepped by two review lenses**)

| Site | Field | Logic-bearing? |
|---|---|---|
| `services/reconciliation.py:151` | `now` → never-submitted grace window (`:821`), synthesized-fill `occurred_at` fallback (`:514`, `:780`), `EquitySnapshot.ts` (`:160`) | **YES** |
| `engine/feed_health.py:103` | payload age vs stamped window | **YES**, but live-infrastructure only |
| `engine/risk/engine.py:100,141` | `RiskApproval.approved_at` / `FlattenApproval.approved_at` | Audit → **becomes logic-bearing in §5.2** |
| `engine/flatten_controller.py:252,353` | `verified_at` in the command payload | Audit — verified: **zero readers anywhere in the codebase** |
| `engine/commands.py:190` | `EngineCommand.applied_at` | Audit — read only by `cli.py:124` and `schemas/engine.py` for display |
| `engine/events.py:36` | `SignalEvent.created_at`, `FlattenEvent.created_at` | Audit — verified: no logic reads them |
| `engine_main.py:116` | heartbeat timestamp | Ops |
| `brokers/alpaca/streams.py:715` | feed-health payload stamp | Live infrastructure |

**Confirmed: `reconciliation.py:151` is the only logic-bearing wall-clock read on the replay path**, with two corrections:

- **It is logic-bearing in *three* ways.** Besides the grace window, that same `now` becomes the `occurred_at` fallback for synthesized fills (`:514`, `:780`) — and `occurred_at` flows straight into `apply_fill_to_lots`, stamping `Lot.opened_at`, `LotClose.closed_at`, and the cross-strategy time bound `Lot.opened_at <= occurred_at` (`lots.py:132`). A wall-clock `occurred_at` in a historical replay stamps lots in the future relative to sim time — a direct violation of iron law #10's point-in-time spirit, inside the risk layer's own inputs. And third, it is the `EquitySnapshot.ts` (`:160`), which §4.6 now depends on.
- **`feed_health.py:103` is logic-bearing too**, but it reads a Redis key that does not exist in a backtest. See §5.5.

### 5.2 The market-time column — ONE required fix, not two options

**This is the change the review most insisted on** (triage A3, found by three lenses). Revision 1 split this into "§5.1: inject a clock into reconciliation" and "§5.2c: maybe add a market-time column to `orders` (option A, recommended)". **Shipping the first without the second makes replay strictly worse than doing nothing.**

Here is the failure the split would have caused. `reconciliation.py:821` computes:

```python
age_ok = local.created_at is not None and (now - local.created_at) > _NEVER_SUBMITTED_GRACE
```

`created_at` is `server_default=func.now()` — the **Postgres wall clock** (`models/base.py:17-27`), i.e. *today*. Inject `sim_clock.now()` per revision 1's §5.1 fix and `now` becomes the historical replay instant. On a March replay run in August that difference is ≈ **−152 days**:

- `age_ok` is never true → a `pending_submit` row can **never** age into `failed`;
- in 3b, `FlattenController._count_pending_submit` (`flatten_controller.py:398-417`) reads permanent phantom exposure;
- `_drive` therefore publishes a `FlattenEvent` on **every 30s watch-window tick** — a flatten loop that never terminates;
- over a year-long backtest that is **~250 false overnight-exposure CRITICALs**, each one a real alert in a real alert channel.

Half the fix is worse than none, because none at least leaves both sides on the same (wrong) clock.

**The fix, as one unit (PR-B):**

| Piece | Change |
|---|---|
| Column | `orders.market_created_at TIMESTAMPTZ **NOT NULL**`, backfilled `= created_at` in the migration (correct: pre-existing live rows were written under the wall clock, which *was* their market time) |
| Source, entry path | `RiskApproval.approved_at` sourced from `state.now` (`risk/engine.py:100`) — the broker clock, already loaded in `RiskState`. `_persist_pending` (`handler.py:296`) stamps `market_created_at` from it |
| Source, flatten path | `_persist_pending` is **shared** between entries and liquidations (`handler.py:559`), so the flatten path must supply it too. `authorize_flatten` is deliberately pure on its event and has no `RiskState`, so the instant comes from `await self._adapter.get_clock()` inside `ExecutionStage` on the liquidation path. That is virtualized for free in sim (the adapter *is* the simulator) and costs one extra broker read per liquidation, which is rare. **Alternative considered:** carry a broker instant on `FlattenEvent` (the `FlattenController` already re-anchors on the broker clock every wake, `flatten_controller.py:188`) and pass it through `FlattenApproval`. Tidier long-term; more surface for a PR whose whole virtue is being small. ⚠️ Reviewer call |
| Legs | `persist_bracket_legs` (`apply.py:107`) inherits the parent's `market_created_at` — legs are created in the same submission moment |
| Consumer, reconciliation | `age_ok` compares `now − market_created_at`, with `now` injected: `reconcile_portfolio(..., now: datetime \| None = None)` defaulting to `datetime.now(UTC)`. Signature-additive, zero live behavior change, one call site in `engine_main.py` |
| Consumer, probe | `ProbeStrategy._entries_already_today` (`probe.py:211`) filters `Order.market_created_at >= day_start` |

**Why NOT NULL rather than nullable.** A nullable column means a row without one never ages (`age_ok` stays false) — the exact livelock, now intermittent and per-row. A `COALESCE(market_created_at, created_at)` fallback is worse still: it silently restores the mixed-clock bug for any insert site that forgets. There are exactly **two** `Order(...)` construction sites in `app/` — `handler.py:296` and `apply.py:107` — so NOT NULL is tractable, and a forgotten third site fails loudly with an `IntegrityError` at the moment it is written. A grep test pins the site count.

**And the reason this matters beyond reconciliation:** `probe.py:211` counts today's entries with `Order.created_at >= et_day_start_utc(bar_instant)`. In a replay of 2026-03-02, `day_start` is 2026-03-02 05:00 UTC while `Order.created_at` is the real Postgres clock at replay time (2026-08-xx) — so **every order ever written by the run satisfies the predicate**, the count is ≥ 1 from the second day onward, and the probe emits **one entry for the entire backtest** instead of one per session. Same code, same bars, different behavior: the exact failure Phase 3 exists to make impossible. Which is why **the fixture MUST span at least two ET sessions**, or the test cannot catch this class of bug at all.

Revision 1's option B (pass `session_factory=None` to strategies in backtests) is still rejected: the backtest would then run a *different code path* than live. Option C (keep the fixture inside one session) is still rejected: the golden test becomes trivially passable and the bug ships hidden.

### 5.3 The Postgres clock's other two consequences

**(a) Byte-reproducibility is impossible at the row level.** Every `created_at`/`updated_at` differs between runs. The determinism contract is therefore defined over a **canonical projection** (§6.1), not a table dump.

**(b) The nondeterministic FIFO tie-break is a live bug and ships separately.** `lots.py:146` consumes lots ordered by `(opened_at, created_at, id)`. `func.now()` is `transaction_timestamp()` — *constant within a transaction* — so two lots created in the same transaction share both keys and the tie falls to `id`, a random `uuid4`. Revision 1 rated this "rare and small". **The review escalated it**: it is reachable on the live stream path, not just reconciliation, and its sibling in `risk/state.py:207` (no tiebreak at all on `closed_at.desc()`) changes a **halt** decision. See §3.0 — it ships as a micro-PR against `main`, sequenced before PR-B.

### 5.4 The generalized `created_at` invariant (triage B14)

The probe's `created_at` predicate and the reconciliation grace comparison are two instances of one mistake. The durable deliverable is not two point fixes, it is an invariant with a test:

> **No engine or service code may compare `created_at` / `updated_at` against a market-derived instant.** Market-time columns only. Ordering *within* a single logical write set is fine (it is a tiebreak, not a clock read); comparison against a clock is not.

**CI test (PR-B):** a pytest that greps `app/engine/**` and `app/services/**` for the syntactic patterns `created_at >=`, `created_at >`, `created_at <=`, `created_at <`, `created_at.between`, `updated_at >=` (etc.), and fails on any hit not in an explicit, commented allowlist file. The allowlist starts empty after PR-B. Ordering uses (`lots.py`'s FIFO tiebreak, `loader.py:64`) are untouched by the pattern set, by design.

A grep test is crude. It is also the only kind of test that catches the *next* person who writes the same line, which is the actual failure mode — this exact bug was written twice by the same author in the same codebase.

### 5.5 Documented parity exceptions (state them, do not hide them)

**Three** gates exist in live that cannot exist in a 3a replay. Named here so a reviewer can attack the reasoning rather than discover the gap. This section is the one part of the spec required to be **exhaustive**.

| Gate | Live | Backtest | Justification |
|---|---|---|---|
| `FeedHealth.is_stale` in the halt composite | Redis TTL key, fail-closed | Absent → contributes `False` | The "feed" in replay is a materialized bar series. It cannot go stale; it can only be *incomplete*, which is `bar_coverage`'s job (§4.2), not the staleness gate's |
| Critical-task liveness + `boot_reconciled` in the halt composite | `engine_main` task set | 3a: `boot_reconciled` pre-set, no supervised tasks | 3a runs the spine, not the daemon. When 3b wires reconciliation, `boot_reconciled` gets its real meaning back |
| **Kill-switch command sweeper** (new — triage D) | `EngineCommandSweeper` polls `engine_commands` on poke + a 5s timer, driving `KillSwitch` | 3a: a **real `KillSwitch(run.session_factory)` is constructed and `.load()`ed once** (`kill_switch.py:64`) against the run schema's empty `engine_commands` table; no sweeper runs | Revision 1 asserted the kill switch is "live in the backtest unchanged", which was **false** — `KillSwitch` needs rows and a `.load()`. Constructing and loading it costs nothing and keeps the halt composite genuinely wired rather than stubbed. The residual limitation: **a mid-backtest halt cannot be injected in 3a**, because nothing sweeps. 3b's fault injection covers it |

**A fourth limitation that is not a gate but must be recorded here** (triage B14): the simulator can only ever produce the **stream-first ordering** — trade updates arrive before or with the submit response. The REST-first branch, and with it the whole `_resolve_ambiguous` never-blind-resubmit path (`handler.py:682`), is **dark in every backtest**. That is 2b's most safety-critical code and no replay will ever exercise it. Named as a coverage limitation; fault injection in 3b.

Everything else in the halt composite — the risk controls and the submit-time re-check — is live in the backtest unchanged.

---

## 6. The proof — what 3a actually guarantees

**Revision 1's parity test is deleted.** Three lenses independently found that it could not run, and the parity lens showed that if it could, it would not prove the claim.

**Why it could not run.** Run B used `FakeEngineAdapter` (`tests/engine/builders.py:124`), whose clock and account are frozen constants. `RiskStateProvider.load` calls `get_account()` and `get_clock()` on **every** signal (`state.py:127-129`). So on a ≥2-session fixture — which §5.2 *requires* — `check_session_open` diverges (a frozen clock never enters the 15:55 window), `size_position` diverges the moment equity moves, `et_day_start_utc` filters `LotClose` to the wrong day, and `check_cooldown` goes negative. **The test fails on a correct engine.**

**Why there is no third horn.** Give Run B a real clock and account and you have written a second broker simulator — the divergent-code-path failure this phase exists to prevent, now living in the test harness. Share the simulator's, and Run B *is* Run A: the test asserts `x == x`.

What replaces it is smaller and true.

### 6.1 The canonical projection (definition)

A deterministic, reviewable serialization of a run's ledger. Rules:

- **Tables:** `orders`, `fills`, `lots`, `lot_closes`, plus the `RiskApproval.audit_payload()` control-check vectors extracted from `orders.risk_approval`, plus `equity_snapshots`, plus anomaly rows from `event_log` (which must be empty — see liveness).
- **Excluded columns:** `created_at`, `updated_at` (Postgres clock, §5.3a). `market_created_at` is **included** — it is sim time and therefore reproducible.
- **UUIDs normalized:** every UUID (row PKs, `client_order_id`'s uuid4 suffix from `risk/engine.py:84`, `signal_id`, `flatten_id`) is replaced by an appearance-ordered surrogate (`ord:1`, `ord:2`, …) assigned in the projection's own iteration order. Sim-minted ids (`sim-{n}`, `simx-{n}`) pass through unchanged — they are already deterministic counters.
- **Ordering:** each table sorted by its market-time column then by surrogate id. Never by a wall-clock column.
- **Money:** `Decimal` rendered as its exact string form, no float anywhere.
- **Format:** newline-delimited JSON, one object per row, stable key order — so a diff in CI is readable line by line.

**The projection does NOT normalize away the FIFO tiebreak (§5.3b)** — that changes *results*, not labels. Hence §3.0.

### 6.2 The golden test (the 3a deliverable)

**Fixture:** a committed bar set — 2 symbols × **≥ 2 ET sessions** of minute bars (§5.2), JSON in `tests/fixtures/`, small enough to read in a diff and run in CI in seconds. One strategy under test (the probe in 3a; a real strategy from Phase 4 later). `RiskLimits` constructed **explicitly** in the fixture, never from `get_settings()` (§4.3).

**The test:** run the fixture through the full run-schema stack + `SimulatedBrokerAdapter` + `ReplayDriver`, compute the §6.1 projection, and assert it is **byte-identical** to the committed golden file.

**What that buys, stated exactly:** *the ledger this engine produces over this input is the one a human read and approved.* A future refactor that changes what a risk control sees — a new query in `RiskStateProvider`, a changed lot-matching rule, a reordered tick loop — turns the build red **with a reviewable diff**. That is a weaker claim than revision 1 advertised and a much more useful one than `x == x`.

**Regeneration is a reviewed act, not a command.** `pytest --regen-golden` (or equivalent) rewrites the file; the PR that does so **must** include the projection diff in its description and justify every changed line. A golden test whose snapshot is regenerated reflexively is a rubber stamp with a red-build ritual attached. ⚠️ This ritual is the test's actual weak point — see §10.

**Mutation check (triage A1.3 / realism F2), in the same test module:** a parameterized variant that perturbs exactly one `RiskLimits` field (e.g. `max_consecutive_losses: 4 → 2`) and asserts the golden comparison **fails**. Nobody currently knows this test *can* fail; this is how we find out, on every CI run, forever.

**Liveness assertions (triage A1.4), inside the golden test itself** — not a separate test that can be skipped or marked xfail:

- ≥ N `SignalEvent`s published (N pinned to the fixture);
- ≥ N fills;
- ≥ 1 **fully closed** lot (`Lot.closed_at IS NOT NULL`);
- non-zero realized P&L in `lot_closes`;
- ≥ 1 `EquitySnapshot` per session (§4.6);
- **zero anomaly rows** in `event_log`: `order.error`, `trade_update.orphan`, `lots.unapplied_liquidation_remainder`, `reconcile.never_submitted`.

Without these, the fixture can be silently inert — bars outside RTH make `ProbeStrategy.on_bar` return early, the run produces nothing, and **empty-equals-empty ships as "parity proven".** The zero-anomaly assertion also catches invariant 10's failure mode directly: null leg client ids → skipped leg rows → orphaned exit fills → an anomaly row, in a projection that would otherwise look perfectly self-consistent.

### 6.3 The decode round-trip test

The only genuinely independent claim revision 1's Run B ever carried, extracted as a unit test with no run schema and no simulator:

```
Bar  →  {**bar.model_dump(mode="json"), "type": "bar"}  →  json.dumps
     →  MarketDataBridge._dispatch(raw)  →  BarEvent
```

Assert field-for-field equality against the original `Bar`, including exact `Decimal` string forms. This is the real wire format: `streams.py:559-569` publishes `dto.model_dump(mode="json")` plus a `type` discriminator, and `market_bridge.py:72-84` pops `type` and `Bar.model_validate`s the remainder. The test proves the live bar-decode path does not alter a bar — which is what makes it legitimate to feed the simulator a `Bar` directly instead of through Redis.

### 6.4 What this deliberately does NOT claim — state it in the test docstring

> This test fixes a ledger over a fixed input. It says nothing about whether the fill model resembles Alpaca: the fills it asserts on are the ones our own `FillModel` produced. **Fill realism is validated in 3b against observed broker behavior; that is a different claim requiring a different instrument.**
>
> It also cannot exercise the REST-first order-resolution branch or `_resolve_ambiguous` (§5.5) — the simulator can only produce the stream-first ordering. Nor does it prove the drawdown ladder is *calibrated*, only that it is *live* (§4.6).

Pre-empting these in the docstring is cheaper than a reviewer finding them and doubting the rest.

### 6.5 Property tests — the instrument against future optimism (triage B12)

No market data required; they are the guard that keeps working after the golden test stops being interesting.

| Property | Why |
|---|---|
| Every fill price lies within its matched bar's `[low, high]` | Invariant 8, asserted over generated bars rather than one fixture |
| A market buy never fills below the bar's open; a market sell never above | The next-bar-open rule, stated as a bound |
| A stop fill is never *better* than the stop price | Catches a sign error in the slippage term |
| Aggregate realized slippage vs the signal's reference price ≥ 0 | The whole fill model, in one number |
| **A strictly more pessimistic `FillModel` config yields terminal equity ≤ baseline on the same fixture** | The key one. It does not prove the model is *right*; it proves the model cannot be **accidentally made optimistic** by a future refactor — which is R5's actual failure mode |

⚠️ `hypothesis` is not currently a dependency (`pyproject.toml` has `pytest`, `pytest-asyncio` only). Either add it (check the current version with `uv`/PyPI at implementation time — never from memory) or write table-driven generators over a bar grid. Recommend adding it; the monotonicity property in particular wants a search, not three hand-picked cases.

**Keeping all of this a live guarantee:** everything in §6 runs in **CI on every PR**. Divergence is a red build the day it is introduced, not a discovery six months later when a Phase 6 refactor quietly changed a risk control's inputs.

---

## 7. Risk register

### R1 — Determinism

**A backtest that is not reproducible is not an instrument.**

| Source | Mitigation |
|---|---|
| Wall clock | §5.1–5.2: the sim clock owns all logic time; the market-time column replaces every `created_at` comparison. 2c's clock discipline already bans wall-clock reads from engine logic, which is why this audit is short |
| `uuid.uuid4` — row PKs (`base.py:13`), `client_order_id` (`risk/engine.py:84`), `signal_id`, `flatten_id` | Not seedable (`os.urandom`). Determinism is defined over the canonical projection (§6.1), which normalizes UUIDs to appearance-ordered surrogates |
| **Uncommitted `.env` inputs** | §4.3: `BacktestRun` carries a resolved `RiskLimits` + `settings_digest`; fixtures build `RiskLimits` explicitly. This was invisible to revision 1's design — both runs read the same `.env`, so the test stayed green while the *result* was environment-dependent |
| **tzdata drift** | §4.3: pinned in CI. A tzdata bump that moves a historical DST transition moves every ET day boundary in a replay |
| The FIFO lot tie-break on a random `id`, and `_consecutive_losses` with no tiebreak | **The projection does NOT fix these** — they change results, not labels. §3.0's micro-PR against `main` |
| Iteration order | Bars sorted `(timestamp, symbol)`; `StrategyRunner` dispatch is registration order (`strategy.py:206`), which `loader.py:64` derives from `ORDER BY created_at` — **ties within a seeding transaction**, so PR-B changes it to `(created_at, id)`. Assert it |
| `asyncio` scheduling | The bus is single-consumer FIFO and the driver `drain()`s to quiescence between phases; nothing in 3a spawns concurrent tasks. `ExecutionStage._on_flatten` *does* spawn (`handler.py:357`) — another reason the flatten controller waits for 3b |
| **Re-entrant `on_trade_update`** | Invariant 7 + `lock_timeout` on the run engine. Not a determinism problem so much as a *liveness* one: a violation hangs the backtest forever with no error, because the outer waiter blocks in Python and Postgres's deadlock detector never sees it |

**Acceptance test:** run the same fixture twice in the same process against two fresh run schemas; the canonical projections must be byte-identical.

### R2 — Look-ahead bias

The replay driver must never let a strategy see a bar's close before that bar closes, nor let risk read a `LotClose` stamped after the sim clock (iron law #10's point-in-time discipline, applied to our own ledger).

| Guard | Mechanism |
|---|---|
| Bar visibility | `clock.advance_to(bar_close)` **before** the `BarEvent` is published |
| Fill timing | Market orders fill at the *next* bar's open (§4.5). The sim never fills at a price the strategy has already seen |
| Ledger point-in-time | Every `TradeUpdate.timestamp` is `<= clock.now()` (invariant 1). Since `Lot.opened_at` / `LotClose.closed_at` derive from `occurred_at` (`lots.py:174,192,246`), no ledger row can be stamped ahead of the sim clock — which is what makes `RiskState`'s `LotClose.closed_at >= et_start` query point-in-time honest for free |
| Reconciliation's fallback | §5.2: the `occurred_at or now` fallback uses sim time, or synthesized fills land in the future |
| The bar cache itself | `CachedBarHistory` must never return a bar with `timestamp >= end`; assert at the source, not at the consumer. Half-open `[start, end)` throughout (§4.1) |
| **The embargo clamp** | §4.1 — a range recorded as covered but never fetched is look-ahead's inverse: look-*behind* at a tape that does not exist |

**Acceptance test:** a canary strategy that emits a signal whose `entry_price` equals the *next* bar's close. It must be impossible to construct — the value is not reachable from any argument `on_bar` receives.

### R3 — Optimism drift

Game plan principle #7: paper fills are optimistic (NBBO, no impact); promotion to live requires paper performance **minus a configurable slippage haircut**, and the platform must measure realized-vs-modeled slippage.

Revision 1 claimed pessimistic defaults. **The review found the table optimistic in six of nine intrabar unknowables.** Corrected:

| Guard | Mechanism |
|---|---|
| Genuinely pessimistic defaults | §4.5 as revised: non-zero slippage, half-spread from a widest-default table, **strict** limit penetration, next-bar-open fills, gap-through at the open, stop-wins on an ambiguous bar, **same-bar leg eligibility** |
| **Selection bias from an inert drawdown ladder** | §4.6 — the largest single source of optimism in revision 1, and the one no test could see |
| **Selection bias from a hidden trial count** | §4.2's `public.backtest_runs`. Best-of-200 under a zero-edge null sits ~2.5–3 SE above zero; walk-forward does not control for it |
| **IEX bars bias high/low inward** | §2.2 — stops that should have triggered do not. Mitigated only by the hard rule that no IEX run informs a promotion decision |
| No free liquidity | 3b: fills capped at a fraction of bar volume. ⚠️ On IEX volume this cap is meaningless (2–3% of consolidated) — it is a Databento-era control, and until then the honest statement is "volume-dependent realism is not modeled", not a cap tuned against fake volume |
| Costs are not zero | 3b: Alpaca equities commission is $0, but SEC Section 31 and FINRA TAF fees on **sells** are real. ⚠️ Current rates must be looked up, not recalled. **No schema home yet** — see §11 |
| Measurement, not faith | 3b ships a realized-vs-modeled slippage table; it is the input to the 3c haircut, so the gate is empirical rather than a guessed constant. **⚠️ It has no schema and is circular on paper fills** — §11 |
| **Monotonicity property test** | §6.5 — the model cannot be *accidentally* made optimistic by a future refactor |
| Structural pessimism | The haircut is applied by the **promotion gate**, which is code, not judgment (iron law #8) |

### R4 — Speed

DB-backed runs are the price of parity. **Measure before optimizing.**

The load model that makes this affordable: DB cost scales with **trades, not bars**. A bar producing no signal costs one in-memory bus drain. A signal costs ~6 DB queries in `RiskStateProvider.load` plus the persist/apply/fill transactions. For 2 symbols × 1 year of minute bars (~196k bars) at the probe's 1 entry/session, that is ~500 entries — trivial. §4.6 adds ~78 snapshot INSERTs per session (~20k/year), still trivial. A high-frequency strategy inverts the ratio, and that is when to profile.

Mitigations, in order of preference: (1) measure and publish a bars/sec figure with the first real run; (2) a local socket to keep per-query overhead honest; (3) if it bites, **schema-per-run leaves a faster in-memory variant open without touching engine code** — a tmpfs Postgres, or an ephemeral instance per run.

**Correction from revision 1: `NullPool` is NOT in this list.** It is a safety invariant (§4.3), not a tunable. Any future optimization proposal that reaches for connection pooling must re-argue the isolation case first.

### R5 — Sim/live divergence going unnoticed

The failure mode is not divergence; it is divergence that nobody notices for six months while strategies get promoted on fiction.

| Guard | Mechanism |
|---|---|
| **CI** | §6 runs on every PR over the committed fixture. Divergence is a red build the day it appears |
| **The mutation check** | Proves the golden test *can* go red — the guard on the guard |
| **Liveness assertions** | Prove the fixture is not inert — an empty run cannot pass as parity |
| Fixture spans ≥ 2 ET sessions | Catches the whole class of session-boundary bugs, including §5.2's |
| Deep-tier review already covers the seam | `CLAUDE.md` puts `engine/risk*`, `engine/execution*`, `services/broker*` under deep review with a signal→risk→execution→broker trace. `app/backtest/broker.py` must be added to that list, and **PR-B is deep-tier by that rule already** |
| Divergence is *detectable*, not just preventable | 3b's realized-vs-modeled slippage table is a continuous, quantitative sim-vs-reality monitor |
| Documented exceptions | §5.5 — every deliberate difference is written down, including the dark REST-first path. An undocumented difference is a bug by definition |
| The live dress rehearsal | Per the 2026-08-01 key decision, booting the real engine against the real broker is now ritual. It found 5 defects in 5-lens-reviewed, judge-approved code. **The simulator does not replace it and must never be argued to** |

---

## 8. Iron-law compliance

| Law | How 3a satisfies it |
|---|---|
| #1 Every order through the Risk Engine | Unchanged — the sim replaces the *broker*, not the choke point. `ExecutionStage` still demands a mint-guarded `RiskApproval` |
| #2 No LLM in the hot path | No LLM anywhere in Phase 3 |
| #3 No PDT logic | `SimulatedBrokerAdapter.get_account` returns `BrokerAccount`, which carries no PDT fields |
| #4 Entries carry protection | The sim honors bracket parent + legs and refuses `bracket` + `extended_hours` — `OrderRequest`'s own validator already enforces it before the adapter is reached. Invariant 9 (at most one OCO leg fills) is what keeps a *protected* position from becoming an unprotected flipped one |
| #5 tz-aware UTC / ET market logic | `SimClock` returns tz-aware UTC; ET boundaries come from `et_day_start_utc` unchanged; **tzdata pinned** so those boundaries are reproducible |
| #6 Alembic for every schema change | Three migrations (PR-A's shared-input tables, PR-B's `market_created_at`, §3.0's lot ordering column); the run-schema mechanism *runs* the migration chain rather than bypassing it — no `create_all` anywhere. The one impurity is A′'s post-migration drop of duplicate shared tables, documented at the call site |
| #7 Money is `Decimal` | Bar cache columns `NUMERIC`; sim account/fill arithmetic `Decimal`; parse via `Decimal(str(x))`; the projection renders exact string forms |
| #8 Paper before live | Untouched in 3a; 3c wires backtest metrics into the promotion gate — **and §2.2's feed rule + §4.2's trial ledger are inputs that gate must consume** |
| #9 Secrets never in git | Bar fixtures contain public OHLCV. Backtest runs need no broker credentials — the adapter is simulated |
| #10 Point-in-time discipline | R2 above; the sim clock bounds every ledger stamp; `market_created_at` is the market-time column that makes the discipline expressible at all |

---

## 9. Definition of done — slice 3a

3a is done when §3.0's micro-PR plus PR-A, PR-B, and PR-C are all merged with their per-PR DoDs (§3.1–3.3) satisfied, **and** these cross-cutting items are true:

- [ ] `docs/STATE.md` Key Decisions Log carries rows for §2.1–2.4, the `search_path` pinning (§4.3), the equity-snapshot scope move (§4.6), the market-time column (§5.2), and the spread-source choice (§4.1)
- [ ] `CLAUDE.md`'s deep-tier review path list includes `app/backtest/broker.py`
- [ ] Every ⚠️ in §10 is either resolved with a recorded answer or explicitly re-deferred with a reason
- [ ] The golden fixture's projection is committed as a **reviewed artifact**, with the regeneration ritual (§6.2) documented next to it
- [ ] `docs/PHASE-3-DESIGN.md` §11 (deferred items) is carried forward into the 3b design spec with real DoDs, not restated as a list

---

## 10. Open questions and assumptions — scrutinize these first

Flagged rather than papered over. Items the review **answered** are kept with their answer, so the next reader can see what was settled and why.

**Still unverified (⚠️ blocks implementation):**

1. **⚠️ Alpaca bar timestamp semantics** — start-of-interval vs end-of-interval (§4.4). Assumed *start*. If wrong, every session-boundary comparison shifts by one bar. **Verify against a real bar before writing the driver.**
2. **⚠️ Alpaca historical-bars API surface** — host, path, params, pagination token, response envelope (§4.1). Deliberately not specified from memory. Verify against docs + one live call.
3. **⚠️ asyncpg `server_settings={"search_path": …}`** (§4.3) — **elevated in importance**: this is now the mechanism the entire isolation guarantee rests on. If it does not work, the fallback (per-checkout `SET` via a pool listener) needs the leak risk re-audited from scratch, not patched.
4. **⚠️ `btree_gist` availability** in the `pgvector/pgvector:pg17` image (§4.2) — needed for the `bar_coverage` exclusion constraint. Fallback is merge-on-write under an advisory lock.
5. **⚠️ Alpaca corporate-actions endpoint** (§4.2) — shape unverified. Without it, split invalidation degrades to a manual lever.
6. **⚠️ tzdata resolution source** (§4.3) — container image vs PyPI package. Pin whichever actually wins, and assert an ET DST transition inside the fixture window in a test.
7. **⚠️ `hypothesis` dependency** (§6.5) — not currently in `pyproject.toml`. Check the current version at implementation time.
8. **⚠️ Alpaca `adjustment` enum values** (§11) — `split` vs `all`; verify rather than assume.

**Answered by the review (recorded, not re-opened):**

9. ~~"Is §5.2c worth the scope?"~~ **Answered: it is required, and it is one fix with the reconciliation clock, not two options** (§5.2). Half of it is worse than none.
10. ~~"Should the FIFO tiebreak wait for Phase 3?"~~ **Answered: no — it is a live money bug, reachable on the stream path, and it decides a halt. Micro-PR against `main`, before PR-B** (§3.0).
11. ~~"Is Run B a good enough oracle?"~~ **Answered: Run B cannot run and would prove nothing. Deleted** (§6). The follow-up question stands and remains unanswered: *is there any independent oracle for the fill model that does not require live-broker data?* I still have not found one. 3b's realized-vs-modeled table is the answer, and it needs live fills to exist.
12. ~~"Equity snapshots in 3b?"~~ **Answered: 3a, because an inert ladder biases selection and no test can see it** (§4.6).

**New, opened by this revision:**

13. **Equity-snapshot cadence.** The triage said "session close only"; §4.6 specifies live's cadence (`reconcile_interval_seconds`, 300s) plus session close, because a `MAX` over a coarse sample under-reports the peak and therefore under-fires the ladder — same direction of error as having none. I believe this is right, it is cheap, and it makes sim and live see the same-resolution peak. **A reviewer who thinks matching live's cadence is over-engineering for 3a should say so.**
14. **Shared-table placement, option A′** (§4.3). B1's pinning made both of revision 1's options wrong; A′ (drop the duplicate empty copies out of the run schema post-migration) is my resolution and **no lens has looked at it**. It trades a small documented divergence between schema contents and `alembic_version` for fail-loud behavior. The alternative is Alembic branch labels, which is more correct and more churn.
15. **The golden-snapshot regeneration ritual is the test's weak point** (§6.2). A red build with a one-command fix trains people to run the command. Options: require the projection diff in the PR body (spec'd), require a second reviewer on any PR touching the golden file, or CODEOWNER the fixture directory. Not resolved.
16. **Flatten-path market time** (§5.2) — `await adapter.get_clock()` inside `ExecutionStage` (spec'd, smaller) vs carrying a broker instant on `FlattenEvent` (tidier, more surface). Reviewer call.
17. **Bar-density gating** (§4.1) — density is a diagnostic, not a gate, because any fixed ratio either rejects real illiquid data or passes a broken fetch. A reviewer who wants a hard gate should name the number and defend it.
18. **Spread table sourcing** (§4.1) — a static bps table is an *assumption* until 3b measures. Where do the initial numbers come from? Public spread statistics for the fixture symbols, or a deliberately conservative flat 5bps for everything? I lean flat-and-wide, because a plausible-looking per-symbol table invites belief it has not earned.

**Carried forward unchanged:**

19. **`FlattenController` in replay.** It spawns off-bus tasks and sleeps toward wall-clock targets (`flatten_controller.py:170,231`); `test_safety_e2e.py` drives it by calling `_tick()` directly. 3b must decide between a tick-injection contract and a driver-owned scheduler.
20. **Sim margin model.** `buying_power = equity × 4` matches the post-PDT intraday reality from RESEARCH §1, but real overnight/maintenance margin is more complex. `check_margin_headroom` will be exercised against a simplification. Acceptable for a structural proof; must be revisited before backtests inform live sizing.

---

## 11. Deferred — valid, out of 3a scope, recorded so they are not lost

Each of these was raised by a lens, accepted as real, and scheduled elsewhere. **Recording them here is the whole point** — an accepted finding with no home is a finding that evaporates.

| Item | Why it is real | Where it goes |
|---|---|---|
| **Slippage-measurement loop has no schema, and is circular on paper fills** | The metric should be `fills.price` vs `Order.risk_approval->>'entry_price'` (implementation shortfall), bucketed by symbol × time-of-day × `is_paper`. **Paper and live samples must never pool** — paper fills are NBBO-optimistic, so a haircut calibrated from them is itself optimistic, which is the failure the haircut exists to prevent | **3b, with a real DoD at 3a's fidelity.** Without one it will not get built, and 3c's gate has no empirical input |
| **Fees have no schema home** | `Fill` and `LotClose` have no fee column. A cash-only fee model makes `LotClose.realized_pnl` **gross** while equity is **net** — two P&L numbers in one ledger, and the optimistic one is the one the same-day breaker and the promotion gate read | **3b.** Decide: fee-adjusted fill price, or a column on both tables |
| **Survivorship / point-in-time universe** | A scanner universe built from Alpaca's *current* asset list excludes every blowup by construction | Name a `universe_snapshot` concept now; build in **Phase 8** |
| **`adjustment=split`, not `all`** | Dividend adjustment erases the real ex-date gap, and gap size is an explicit ORB precondition. Also: live receives **raw** prices while backtests receive **adjusted**, which matters for `quantize_price`'s sub-penny boundary and for whole-share sizing (`controls.py:140`) | Record as a §5.5 exception when decided. ⚠️ Verify Alpaca's actual enum values rather than assuming |
| **Halts / LULD / short locates / opening auction** | All unmodeled, all one-directional (each one is a trade the backtest takes and live could not) | A named-limitations section in **3b** — it costs a paragraph |
| **Same-timestamp symbol ordering is a selection bias** | §4.1's lexicographic tiebreak means a binding `max_positions` **always** favours the alphabetically earlier symbol | **3b:** record when the constraint binds; offer a seeded-shuffle sensitivity mode |
| **Ambiguous-exit rate as a first-class diagnostic** | If 30% of exits resolved via "assume the stop filled" (§4.5), the result is resolution-bound noise wearing a Sharpe ratio | **3b:** count it per run, surface it in `backtest_runs.metrics` |
| **`engine_commands` advisory lock is database-scoped** | `hashtext('roigen-engine-<portfolio_id>')` serializes command issuance across all run schemas *and* the live engine | Harmless in 3a (no `engine_main`); fold into the "run-schema in the lock key" fix when a later slice boots the daemon against a run schema |
| **Fault injection for the dark paths** | The REST-first branch and `_resolve_ambiguous` (§5.5) are unreachable in any replay | **3b** |

---

## 12. What the review confirmed and left unchanged

Recorded so a future reader knows which parts were stress-tested and passed, rather than merely surviving by not being looked at (triage §E/§F).

- **Every finding in all four reports was accepted, scheduled, or deferred with a reason. Nothing was rejected.** That is a signal about revision 1's density of assumption, not about the reviewers' leniency.
- **The §5.1 `datetime.now` grep is genuinely complete** — two lenses re-grepped independently and found no additional logic-bearing site.
- **The §2.1 boundary choice is right, and for a reason revision 1 did not know**: an `ExecutionHandler`-level seam would have *hidden* both A1 and A2 rather than exposing them.
- **`BrokerAdapter` is exactly 14 methods**; `RiskEngine.evaluate` is genuinely pure; `bus.drain()`'s docstring really does name "the Phase-3 backtest replay" (the seam was built for this); `resolve_delays` / `match_retry_delays` are real constructor params; the "what does NOT exist" audit (§1) is accurate.
- **2c's broker-clock anchoring claim holds**: `flatten_controller` re-anchors every tick, `risk/state` derives `now` from `clock.timestamp`, and `risk/controls` imports nothing from `datetime` but `timedelta`. The two `datetime.now(UTC)` calls in `flatten_controller` have **zero readers** anywhere in the codebase.
