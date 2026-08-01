# Phase 3 design review — triage decisions

> Four adversarial lenses reviewed `docs/PHASE-3-DESIGN.md` @ `bf6ba0d` **before any code existed**:
> parity-claim, realism/optimism, data-isolation, and claim-verifier/scope.
> All four returned `fix-then-ship`. This document is the orchestrator's disposition of every
> finding — accepted, rejected, or deferred, with reasons. It is the input to the spec revision.
>
> Cross-lens convergence is called out explicitly: a finding reached independently by multiple
> lenses reasoning from different starting points is the highest-confidence signal in the set.

---

## A. ACCEPTED — converged across lenses (fix in the spec before any code)

### A1. The parity test cannot run, and would not prove the claim if it could
**Found by 3 lenses independently** (verifier F1, realism B2, parity B1).

Run B uses `FakeEngineAdapter`, whose clock and account are frozen constants
(`tests/engine/builders.py:128-139`). `RiskStateProvider.load` calls `get_account()` and
`get_clock()` on **every** signal (`risk/state.py:127-129`). So on a ≥2-session fixture:
`check_session_open` diverges (frozen clock never enters the 15:55 window), `size_position`
diverges once equity moves, `et_day_start_utc` filters `LotClose` to the wrong day, and
`check_cooldown` goes negative. The test fails on a **correct** engine.

The parity lens showed the dilemma has no third horn: give Run B a real clock+account and you have
written a second broker simulator (the divergent-code-path failure this phase exists to prevent,
now in the test harness); share the simulator's and Run B *is* Run A.

**DECISION — adopt the parity lens's redesign.** Replace the A-vs-B differential with:
1. **Golden canonical-projection snapshot.** Commit the §7 projection for the fixture; assert the
   engine reproduces it byte-for-byte. Converts a tautology into the guarantee we want: *the ledger
   this engine produces over a fixed input is the one a human reviewed and approved.* A future
   refactor that changes a risk control's inputs turns the build red, and the diff is reviewable.
2. **Decode round-trip unit test** — `Bar → md:bar JSON → MarketDataBridge._dispatch → BarEvent`,
   field-for-field. This is the only genuinely independent claim Run B ever carried.
3. **Mutation check** (realism F2): flip one `RiskLimits` value and assert the golden test goes red.
   Nobody currently knows this test *can* fail.
4. **Liveness assertions**: ≥N signals, ≥N fills, ≥1 fully-closed lot, non-zero realized P&L, and
   zero anomaly rows (`order.error`, `trade_update.orphan`, …). Without these the fixture can be
   silently inert — bars outside RTH make `ProbeStrategy.on_bar` return early, both runs produce
   nothing, and empty-equals-empty ships as "parity proven".

### A2. Backtests are blind to the drawdown ladder — optimistic *by selection*
**Found by 3 lenses** (realism B1, parity B2, isolation A4).

`peak_equity` reads `EquitySnapshot` (`risk/state.py:251-262`), whose only writer is
`reconciliation.py:155` — explicitly out of 3a scope. So `drawdown_pct == 0` forever:
`check_drawdown_halt` never fires at 15%, `drawdown_size_factor` never halves at 10%.

This is not random error. A strategy that draws down 22% and recovers backtests as +31%; live it
halts at 15% and never participates in the recovery. The backtest therefore **systematically
selects for strategies that recovered from drawdowns that would have stopped them** — precisely the
filter the 3c promotion gate exists to be.

**DECISION — wire minimal per-session equity snapshots into 3a.** They need only `get_account()` at
each session close; no reconciliation machinery. Documenting it as a parity exception is NOT
acceptable: an inert breaker corrupts the promotion gate, and the parity test cannot see it
(both runs are equally crippled → assertion green).

### A3. The §5.1 clock fix is incomplete and, alone, makes replay worse
**Found by 3 lenses** (verifier F3, parity B5, isolation B5).

`reconciliation.py:821` computes `now - local.created_at`, where `created_at` is
`server_default=func.now()` — the Postgres wall clock. Injecting `sim_clock.now()` per §5.1 makes
that ≈ **−152 days** on a historical replay → `age_ok` never true → a `pending_submit` row can never
age into `failed`. In 3b: `FlattenController._count_pending_submit` reads permanent phantom
exposure → `_drive` publishes a `FlattenEvent` every 30s watch-window tick → ~250 false
overnight-exposure CRITICALs over a year-long backtest, and a flatten loop that never terminates.

**DECISION — §5.1 and §5.2c are ONE fix, not two.** The market-time column on `orders` is
**required**, not an option, and the grace comparison must age against it. It must be populated on
the flatten path too (`_persist_pending` is shared between entries and liquidations).

### A4. `_consecutive_losses` has no tiebreak, and ties are guaranteed
**Found by 2 lenses** (parity A2, isolation B6). This is a **live bug as well as a sim bug.**

`risk/state.py:207` orders by `Lot.closed_at.desc()` with no tiebreak; `lots.py:174` stamps
identical `closed_at` on every lot a single fill closes. One exit closing three lots with P&L
(−5, +3, −2) yields a streak of **0, 1, or 2** depending on arbitrary row order — feeding
`check_consecutive_losses`, a **halt** control at 4. Different halts → different trades. The
canonical projection cannot normalize this away: it changes results, not labels.

**DECISION — accept.** The §5.2b micro-PR must add a deterministic monotonic ordering that serves
`lots.py:146` **and** `state.py:207`.

---

## B. ACCEPTED — single-lens, verified, high value

### B1. Run isolation fails OPEN into live data (isolation B1 — reproduced against real Postgres)
`search_path = "bt_x", public` + `alembic/env.py:59` (no `version_table_schema`) → Alembic resolves
`alembic_version` unqualified to **public**, reads head, concludes there is nothing to do, exits 0.
**Zero tables created in the run schema**, and every unqualified query falls through to public.
Backtest orders/lots/lot_closes land in live P&L, silently, inside the risk layer.

**DECISION — pin `search_path` to the run schema ALONE (no `, public`).** Verified by the lens:
all 13 tables land in the run schema and `alembic_version` follows automatically. Any leak becomes
`relation "orders" does not exist` — fail-loud instead of fail-into-production. Single highest-
leverage edit in the review. Nothing needs `public` today (no vector columns yet; `now()`,
`hashtext()`, JSONB operators are all `pg_catalog`).

### B2. `DROP SCHEMA … CASCADE` can uninstall pgvector database-wide (isolation B2 — reproduced)
With `search_path` pinned, `CREATE EXTENSION IF NOT EXISTS vector` lands **inside** the run schema;
extensions are database-unique, so run 2 can't resolve the type, and dropping run 1 removes pgvector
for everyone. **DECISION:** `create_run` ensures the extension in `public` before migrating; the
step-4 invariant checks `pg_extension` namespaces, not just `pg_tables`.

### B3. Bar cache records that you *asked*, not that you *got* (isolation B3)
The free plan embargoes the most recent ~15 minutes. Warm SPY at 16:05 for 09:30–16:00 → bars
arrive through ~15:50 → **15:50–16:00 is marked covered and empty forever**. That is the 15:55
flatten window: every backtest of that day replays a dead tape through the most safety-critical
minute of the session, and the flatten test passes on no data.
**DECISION:** clamp `end` to `now − embargo` and refuse to record coverage past it; store the
returned row count and cross-check against `market_calendar` (a coverage range overlapping an RTH
session with implausibly few bars is a fetch failure, not a quiet market).

### B4. `bar_coverage` has no interval semantics; splits silently poison the cache (isolation B4)
No PK/uniqueness/merge rule → overlapping rows → either refetch-forever or **under-fetch and
silently drop bars**. And a split restates every pre-split bar upstream while the cache keeps the
old basis and `bar_coverage` still says "covered" — splicing old-basis onto new-basis bars, a
fabricated 4× discontinuity that trips every stop. `adjustment` in the PK doesn't help; the key is
identical, the upstream data changed.
**DECISION:** `tstzrange` + GiST exclusion (or merge-on-write), documented half-open `[start, end)`
convention, and a corporate-actions check invalidating coverage whose `fetched_at` predates a split.

### B5. No trial ledger — selection bias is invisible to the promotion gate (realism B8)
`DROP SCHEMA` deletes losing runs. A 200-combination sweep reaches 3c looking like one clean result;
best-of-200 under a true-zero-edge null sits ~2.5–3 SE above zero. Walk-forward controls for regime
dependence, **not** trial count. Only a registry does.
**DECISION:** `public.backtest_runs` (run_id, label, strategy_kind, params_hash, params, window,
git_sha, fill_model_config, risk_limits, metrics, created_at) that **survives** `drop_run`. This is
a 3a decision because run isolation is 3a; retrofitting cannot reconstruct history.
**Bonus:** the same table solves isolation B7 (no way to enumerate → orphan schemas accumulate in
the shared test DB after any crash/timeout). Add `drop_orphans(older_than=…)`.

### B6. Fill model is optimistic in six of nine intrabar unknowables (realism B3–B6)
The spec's self-image of "deliberately pessimistic" is true of the three rows it wrote and false of
the table as a whole. **DECISION — accept all four:**
- **Stop-market slippage of zero** is a lie on the loss side of every trade. Against the probe's
  50bps stop, 5bps of slippage is 10% of 1R; on a 40%-hit-rate 2R strategy that is ~30% of the edge.
  `FillModel` gets a slippage parameter with a **non-zero default** so a backtest is never
  accidentally frictionless.
- **No spread, and no data source designed for it** — `BarHistorySource` has no quote path, so 3b's
  promised realism would require redoing §4.1/§4.2. Decide the source NOW (quotes vs `vwap`/
  `trade_count` proxy vs static per-symbol bps table) and write it into §4.1.
- **Limit fills on touch** keep the excursions that reversed (the profitable ones) and discard the
  ones that traded through — it filters *for* winners by construction. Require strict penetration.
- **Same-bar entry-then-stop is unspecified.** Legs must be eligible on the parent's fill bar;
  ambiguity resolves to the stop. Otherwise max-adverse-excursion is fiction in the direction that
  matters.

### B7. IEX bars are trade-derived; §2.2's justification is about quotes (realism B6)
A ~2–3% sample of the tape biases the minute high/low **inward**, so stops inside the consolidated
range never trigger — the backtest keeps losers live would have closed. And an absent IEX bar is
indistinguishable from a quiet minute, so resting orders simply aren't evaluated.
**DECISION:** rewrite §2.2's rationale (it currently leans on an NBBO/quote argument for an artifact
built from trades); add a hard statement that **no IEX-derived backtest may inform a promotion
decision**; surface `Bar.trade_count`-based quality diagnostics in the run report.

### B8. The simulator's six invariants are the wrong six (parity B4)
**DECISION — add, in severity order:**
(a) no fill price outside `[bar.low, bar.high]` of the matched bar — the highest-value assertion
available and currently absent; every optimism guard is downstream of it;
(b) at most one leg of an OCO pair may ever fill (wrong cancel ordering → both fill → position flips
short and the ledger books two exits against one entry);
(c) every simulated order **including legs** carries a non-null unique `client_order_id` —
`persist_bracket_legs` silently *skips* null-id legs, so every protective-exit fill then orphans and
realized P&L is empty **while the parity test stays green**;
(d) order ids minted once and stable across reads, else duplicate leg rows apply fills twice;
(e) legs activated by a parent fill in tick T are matched against tick T's own bar;
(f) `last_equity` rolls at session close — otherwise a 2% session-1 loss latches the portfolio
daily-loss breaker for the entire remaining backtest.

### B9. Re-entrant `on_trade_update` is a silent, undetectable hang (parity A5)
If the sim ever emits from a read method, the nested call opens a second session and blocks on a row
lock the outer transaction holds. Because the outer waiter blocks in **Python**, not Postgres, the
deadlock detector never fires — the backtest hangs forever with no error.
**DECISION:** invariant 7 — emit only from `on_clock_advance` / `submit_order` / `cancel_order`,
never from a read method, never while an engine-owned transaction is open. Plus a `lock_timeout` on
the run engine so a violation surfaces as an error rather than a hang.

### B10. "Reproducible" is defined over too narrow a scope (parity A3)
`RiskLimits.from_settings(get_settings())` reads `.env`. Every number driving sizing, breakers, and
the flatten buffer is an **uncommitted input**. Same fixture, CI vs laptop, different `.env` →
different P&L and a still-green parity test. Same for tzdata drift against `ZoneInfo`.
**DECISION:** `BacktestRun` captures resolved `RiskLimits` + a settings digest; fixtures construct
`RiskLimits` explicitly instead of inheriting `get_settings()`; pin `tzdata` in CI.

### B11. Module-level `public` session factory can leak into a run (isolation B8)
`engine_main.py` — the template any replay harness gets copied from — threads the import-time
`async_session_factory` (bound to public) into every stage. Copy that line and
`ProbeStrategy._entries_already_today` counts **live** orders while everything else writes the run
schema. B1's fix does not cover this (the global factory has its own engine).
**DECISION:** `BacktestRun` is the only session source inside a run, plus a test asserting
identity-inequality against `app.core.database.async_session_factory`.

### B12. Monotonicity property tests — the instrument against future optimism (realism A11)
**DECISION — accept into 3a's DoD, next to the golden test.** Property tests needing no market data:
every fill price within its bar's range; a market buy never fills below the bar's open; a stop fill
is never *better* than the stop; aggregate realized slippage vs the signal reference ≥ 0; and the
key one — **a strictly more pessimistic `FillModel` config yields terminal equity ≤ baseline on the
same fixture**. That doesn't prove the model is right; it proves the model cannot be *accidentally
made optimistic* by a future refactor, which is R5's actual failure mode.

### B13. Scope: 3a is ~9 units and three PRs, not six units and one (verifier F8)
Calibration: 2b was 66 new tests + 1 migration; 2c was 190 tests + 1 migration and still shipped 5
defects the offline suite couldn't reach. As specified, 3a is larger than 2b and 2c combined and
lands a money-path migration in the same diff as an ~800-line broker simulator. The accepted
findings above *add* work (equity snapshots, trial ledger, cache hardening).
**DECISION — three PRs:**
- **PR-A** — bars client + cache + `market_calendar` + `backtest_runs` registry + run isolation.
  Zero money-path exposure; merges fast.
- **PR-B** — market-time column on `orders` + the reconciliation clock fix (ONE fix per A3) +
  the `lots.py`/`state.py` deterministic ordering. Small diff, **deep-tier review** by CLAUDE.md's
  own rule (touches `risk/`, `execution/`).
- **PR-C** — `SimulatedBrokerAdapter` + replay driver + golden parity test + property tests.

### B14. Smaller accepted items
- **Persist all four calendar columns now** (isolation A5) — the migration is being written; a 3b
  extended-hours sim needs `session_open`/`session_close`, and the DTO can keep exposing two.
- **`NullPool` is a safety invariant, not a perf knob** (isolation A2) — a committed `SET
  search_path` survives pooled checkouts; the leak is intermittent, which is worse. Document on
  `BacktestRun`; R4 must stop implying it's negotiable.
- **`close_position`/`close_all_positions` raise `NotImplementedError`** (verifier A7) citing the
  2026-08-01 decision, rather than emulating an endpoint we've banned. Free, and more protective.
- **`loader.py:64` ordering** (parity A2, isolation A3) — `created_at` ties within a seeding
  transaction; order by `(created_at, id)`.
- **Seeded `Strategy.status` must be an explicit, recorded decision** (parity B3) — `paper` sizes at
  0.25% and `live` at 0.75%, a **3× difference in every position size**, and marginal trades round
  to zero at the smaller size, so the trade *sets* differ. `BacktestRun` records which was assumed;
  3c must compare like for like.
- **Document that the sim can only produce the stream-first ordering** (parity A4) — the REST-first
  branch and the whole `_resolve_ambiguous` never-blind-resubmit path are dark in every backtest.
  Name it as a coverage limitation; add fault injection in 3b.
- **`ProbeStrategy` `created_at` bug generalizes** (verifier A9) — the durable deliverable is an
  invariant ("no engine logic may filter or compare `created_at`; market-time columns only") plus a
  grep-based CI test, not two point fixes.

---

## C. ACCEPTED for a separate micro-PR (live bug, not Phase 3)

### C1. FIFO tiebreak falls through to a random UUID — realized P&L is a coin flip
(verifier F7, escalating the spec's own §5.2b.) Verified reachable on the **live stream path**, not
just reconciliation: `_synthesize_gap` uses `occurred_at=update.timestamp` and the real fill's
`apply_fill_to_lots` uses the *same* timestamp, in one transaction — so `opened_at` ties, and
`created_at` ties (`func.now()` is `transaction_timestamp()`). The tiebreak is `Lot.id`, a random
`uuid4` (`lots.py:146`). Gap lot 10 @ 100.00 and real lot 10 @ 100.40, later sell 5 @ 101.00 →
realized P&L is **$5.00 or $3.00** depending on a UUID. On the flatten path the widened
cross-strategy match means the coin flip also decides **which strategy's** same-day breaker moves.

**DECISION:** ship as its own micro-PR against `main`, not inside Phase 3 — it is a live money bug
in merged code. Its fix (a deterministic monotonic ordering column) is the same one A4 needs, so
sequence it before PR-B.

---

## D. DEFERRED (valid, out of 3a scope — recorded so they are not lost)

- **Slippage-measurement loop has no schema and is circular on paper fills** (realism B7). The
  metric should be `fills.price` vs `Order.risk_approval->>'entry_price'` (implementation
  shortfall), bucketed by symbol × time-of-day × `is_paper` — paper and live samples must never
  pool, since paper fills are NBBO-optimistic and a haircut calibrated from them is itself
  optimistic. **Needs a real DoD in 3b**, at 3a's fidelity, or it won't get built.
- **Fees have no schema home** (realism A6) — `Fill`/`LotClose` have no fee column, so a cash-only
  fee model makes `LotClose.realized_pnl` gross while equity is net: two P&L numbers, and the
  optimistic one feeds the gate. Decide in 3b: fee-adjusted fill price, or a column.
- **Survivorship / point-in-time universe** (realism A7) — a scanner universe built from Alpaca's
  *current* asset list excludes every blowup by construction. Name a `universe_snapshot` concept
  now; build in Phase 8.
- **`adjustment=split` (not `all`)** (realism A8) — dividend adjustment erases the real ex-date gap,
  and gap size is an explicit ORB precondition. Also: live receives raw prices while backtests
  receive adjusted, which matters for `quantize_price`'s sub-penny boundary and whole-share sizing.
  Record as a §5.3 exception. ⚠️ Verify Alpaca's actual enum values rather than assuming.
- **Halts / LULD / short locates / opening auction** (realism A9) — all unmodeled, all
  one-directional. A named-limitations section costs a paragraph.
- **Same-timestamp symbol ordering is a selection bias** (realism A3) — alphabetical priority means
  a binding `max_positions` always favours the alphabetically earlier symbol. Record when a
  constraint binds; offer a seeded-shuffle sensitivity mode.
- **Ambiguous-exit rate as a first-class diagnostic** (realism A10) — if 30% of exits resolved via
  "assume the stop filled", the result is resolution-bound noise wearing a Sharpe ratio.
- **`engine_commands` advisory lock is database-scoped** (isolation A6) — serializes command
  issuance across all run schemas and the live engine. Harmless in 3a; include in the
  "run-schema in the lock key" fix when it lands.
- **Kill switch is absent from the 3a stack** (parity A1) — §5.3 asserts it is "live in the backtest
  unchanged", which is false: `KillSwitch` needs `engine_commands` rows and `.load()`. Either seed
  it or make it a third documented exception. It has no replay analogue, so documenting is fine —
  but §5.3 is the one section required to be exhaustive.

---

## E. REJECTED / no action

- **Nothing.** Every finding in the four reports was either accepted, scheduled, or deferred with a
  reason. Two claims were checked and confirmed *correct as the spec had them* rather than being
  findings: the §5.1 `datetime.now` grep is genuinely complete (two lenses re-grepped
  independently), and the §2.1 boundary choice is right — the realism lens noted an
  `ExecutionHandler`-level seam would have **hidden** both A1 and A2 rather than exposing them.

---

## F. What the review confirmed (worth keeping)

The spec's verified-true claims, per the claim-verifier: `BrokerAdapter` is exactly 14 methods;
`RiskEngine.evaluate` is genuinely pure; `bus.drain()`'s docstring really does name "the Phase-3
backtest replay" (the seam was built for this, not retrofitted); `resolve_delays` /
`match_retry_delays` are real constructor params; the "what does NOT exist" audit is accurate; and
2c's broker-clock anchoring claim holds — `flatten_controller` re-anchors every tick, `risk/state`
derives `now` from `clock.timestamp`, and `risk/controls` imports nothing from `datetime` but
`timedelta`. The two `datetime.now(UTC)` calls in `flatten_controller` have **zero readers** anywhere
in the codebase.
