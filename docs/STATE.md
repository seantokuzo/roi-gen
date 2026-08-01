# ROI-GEN State

> Living document. Updated at every phase transition and merged PR.

## Current Phase: 2 — Engine + risk core (in progress — 2a + 2b merged; 2c next)

Phase 1 (domain + broker spine) is **complete** — PRs #1 and #2 merged. Phase 2 is sliced risk-first into 3 PRs; **2a merged as PR #3, 2b (execution core) merged as PR #5**.

| Date | Event |
|---|---|
| 2026-06-10 | Discovery complete: 10-agent legacy audit + 2026 landscape research → `docs/RESEARCH.md` |
| 2026-06-10 | v3 architecture + game plan written → `ROI-GEN-GAME-PLAN.md` |
| 2026-06-10 | Phase 0 scaffold complete: full compose stack verified healthy locally (engine heartbeat on Redis observed), CI green on main |
| 2026-06-10 | **PR #1 merged** (Phase 1a): 10-table schema + pgvector migration, fail-closed Google→JWT auth, portfolios + Fernet-encrypted credentials. 69 tests. Cloud review unavailable — substituted 7-angle local review; fix round added DB-enforced invariants |
| 2026-06-27 | PR auto-review rewired to authenticate via Claude **Max-subscription OAuth token** (`claude setup-token` → `CLAUDE_CODE_OAUTH_TOKEN` secret) instead of metered API key; job skips green until the secret is set |
| 2026-06-27 | **PR #2 merged** (Phase 1b broker spine): BrokerAdapter contract → AlpacaBrokerAdapter (async httpx) + market-data/trade-updates stream consumers + reconciliation + account API + engine wiring. 213 tests. Verified live against Alpaca paper (REST account incl. 4× intraday buying power, md websocket connect/auth/subscribe, staleness watchdog). Cloud review gated on OAuth token (unset) — substituted 3-agent local adversarial review + impartial judge (merge, high confidence); fixes: suspect-empty-positions guard, engine task-death logging |
| 2026-06-28 | **PR #3 merged** (Phase 2a — risk engine + event bus + Strategy spine): deterministic FIFO event bus (the backtest/live parity seam), Strategy base/registry/runner (propose-only), the **Risk Engine** choke point (pure `evaluate(signal, state)`; 13 controls + fixed-fractional whole-share sizing), mint-guarded `RiskApproval` (iron law #1 enforced by the type system), `RiskStage` audit+emit with auditable error path. No schema change. 59 engine tests (275 total). **First real cloud review** (OAuth token now set): R1 approve · 0 blocking · 5 advisory → all addressed (4 fixed, 1 mypy-guard pushback, 1 deferred → issue #4); R2 approve · 0/0; impartial judge → merge (high). |
| 2026-07-06 | **PR #5 merged** (Phase 2b — execution core): `app/engine/execution/` — `ExecutionStage` (the ONLY broker order-mutation caller; requires the mint-guarded `RiskApproval`, rejects impostors/mispairs; persist-before-submit; rejected/failed/ambiguous taxonomy with bounded client-id resolution, never resubmit), shared order state machine (`plan_transition`: terminal absorb + stale-cumulative guard; `done_for_day`/`stopped` dormant; all writers under `FOR UPDATE` + `populate_existing`), trade-updates writer (execution-id dedup, cumulative clamping, gap synthesis on EVERY event, leg adoption, park-and-retry, `failed` resurrection) + buffered Redis subscriber gated on boot reconcile, FIFO lot engine + **`lot_closes` per-close ledger** (partial-close P&L visible to the same-day breaker; 2a risk queries switched), position tracker, reconciliation rework (fill-ledger cursor `SUM(Fill.qty)`, atomic synthesis, ledger-deficit sweep, orders-before-positions lock order, never-submitted grace → `failed`), engine wiring (boot reconcile gates BOTH order entry and the fill stream; periodic reconcile; liveness-halt), 5xx-on-submit → `AmbiguousOrderState`, sub-penny quantization, TIF-day control #14. Migration `a7c31d90f2e4` (`lot_closes` + `ix_lots_strategy_id`, closed #4). 341 tests (66 new). **Review protocol:** 5-lens adversarial design review pre-implementation (25 confirmed findings folded in) + 5-lens local code review (10 confirmed, 0 refuted, fixed pre-push) + cloud R1 approve·0/1 (advisory fixed) + R2 approve·0/0 + impartial judge → merge (high). |

**Next up — Phase 2c (safety + E2E, the Phase 2 deliverable):** kill switch (Redis cmd + API + CLI → the `RiskStage.halted` hook that already gates entries), flatten@15:55 + scheduler (broker clock/calendar-driven; `close_position` path must route via risk), trivial test strategy (registered kind emitting protected signals off live bars), engine strategy-loading from DB rows, live-paper E2E: a bracket order paper-trades end-to-end with full audit trail. Note for 2c: trade-updates deafness → ops/risk signal (liveness-halt covers 2b), submit retry policy deliberately deferred.

## Phase Tracker

- [x] **Phase 0 — Scaffold**: repo, compose stack, CI, review workflow, docs ✅ 2026-06-10
- [x] **Phase 1 — Domain + broker spine**: 1a (models/Alembic/auth/portfolios, PR #1) + 1b (BrokerAdapter, market-data + trade-updates streams, reconciliation, account API, PR #2) ✅ 2026-06-27
- [ ] **Phase 2 — Engine + risk core** (risk-first, 3 PRs): ✅ 2a risk engine + bus + Strategy spine (PR #3, 2026-06-28) · ✅ 2b execution + order state machine + FIFO P&L (PR #5, 2026-07-06) · ☐ 2c kill switch + flatten/scheduler + trivial strategy + live-paper E2E ← *2c next*
- [ ] **Phase 3 — Simulator + backtest parity**
- [ ] **Phase 4 — Strategy roster v1**: noise-area momentum, VWAP trend, regime classifier v1
- [ ] **Phase 5 — Frontend cockpit**
- [ ] **Phase 6 — Intelligence layer**: LLM adapter v2, news/sentiment, pgvector memory, post-mortems, calibration
- [ ] **Phase 7 — Copilot**
- [ ] **Phase 8 — ORB + scanner**
- [ ] **Phase 9 — Live readiness**
- [ ] **Phase 10 — Expansion**

## Blockers / Waiting on Sean

None. Cloud review retired 2026-07-09 (local reviews only); GitHub is now CI + merge plumbing. `MANUAL-SETUP.md` still covers env/key setup.

## Key Decisions Log

| Date | Decision | Rationale |
|---|---|---|
| 2026-06-10 | Custom asyncio engine, no framework | No framework passes Alpaca+intraday+async+embeddable gate (RESEARCH §5) |
| 2026-06-10 | Two-loop architecture: deterministic fast / LLM slow | LLM trading agents are leakage-hype; LLMs excel at sentiment/regime/post-mortems (RESEARCH §6) |
| 2026-06-10 | pgvector, not ChromaDB | SQL joins memories×trades, ACID, one backup (RESEARCH §6) |
| 2026-06-10 | No PDT logic anywhere | FINRA retired PDT 2026-06-04; Alpaca deletes fields 2026-07-06 (RESEARCH §1) |
| 2026-06-10 | Portfolios = logical ledgers over 1 live account; 3 paper accounts for isolation | Alpaca allows one live retail account (RESEARCH §4) |
| 2026-06-10 | Strategy build order: noise-area → VWAP-trend → ORB → MR-behind-gate → pairs | Evidence strength vs automation risk (RESEARCH §2) |
| 2026-06-10 | $0 data during build/paper; $99/mo ATP gate for live | IEX volume data unusable for RVOL/VWAP (RESEARCH §7) |
| 2026-06-27 | PR review auth = Max-subscription OAuth token, not API key | Avoid metered API-console billing; reviews draw on the Max plan (`claude setup-token`) |
| 2026-06-27 | Engine never imports alpaca-py types; broker behind `BrokerAdapter` contract | Keeps a 2nd broker / sim-fill swappable; alpaca-py confined to `app/brokers/alpaca/` |
| 2026-06-28 | Phase 2 sliced risk-first into 3 PRs (2a risk engine → 2b execution+P&L → 2c safety+E2E) | Choke point real from line one; never a stubbed gate; each money surface stays focused for Tier-3 review |
| 2026-06-28 | Iron law #1 enforced structurally: `RiskApproval` is a mint-guarded capability the execution handler will require | Legacy's #1 sin was a risk layer 2/3 paths skipped — make the bypass not type-check and not run |
| 2026-06-28 | In-process FIFO bus is the backtest/live parity seam; pure `RiskEngine`, IO in provider, persistence in stage | Same events + same Strategy code in sim and live (game plan principle #3); risk logic stays exhaustively unit-testable |
| 2026-06-28 | Cloud auto-review now live (OAuth token set) — first real multi-specialist review on the project | Replaces the local-substitute review pattern used for PRs #1–#2 |
| 2026-07-06 | Realized P&L reads the `lot_closes` per-close ledger, not the `Lot.realized_pnl` accumulator | Partial scale-out losses must hit the same-day strategy breaker; multi-day lots attribute per close (design-review critical) |
| 2026-07-06 | Missed-fill synthesis cursor = `SUM(Fill.qty)` per order, never `Order.filled_qty`; synthesis atomic with the order adopt; ledger-deficit sweep each engine reconcile | `filled_qty` is advanced by writers that don't touch lots; keying on it makes dropped fills permanently invisible (design-review critical) |
| 2026-07-06 | Only provable non-placement (422 reject / 429) is locally terminal; 5xx-on-submit and unexpected errors are ambiguous → resolve by client id, never resubmit; `failed` is soft-terminal (broker evidence resurrects) | Guessing "not placed" about a live order orphans its fills — the most dangerous failure mode in the system |
| 2026-07-06 | All order-state writers share one transition rule set under `FOR UPDATE`+`populate_existing`; `done_for_day`/`stopped` are dormant, not terminal; TIF pinned to `day` (control #14) | Three concurrent writers (execution, stream, reconcile) must not regress each other; GTC would silently break dormancy assumptions |
| 2026-07-06 | Boot reconciliation gates BOTH order entry (`RiskStage.halted`) and the fill stream (buffered subscriber) | No execution decisions off an unreconciled book — legacy's recovery blind spot |
| 2026-07-09 | PR reviews are local-only: cloud review workflow deleted; local multi-lens adversarial review + impartial judge is the protocol (root `CLAUDE.md`) | Sean's call — local reviews on PRs #1/#2/#5 pre-push caught more than the cloud rounds; drops the polling loop + OAuth-token dependency entirely |
