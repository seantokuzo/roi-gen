# ROI-GEN State

> Living document. Updated at every phase transition and merged PR.

## Current Phase: 2 — Engine + risk core (in progress — 2a merged; 2b next)

Phase 1 (domain + broker spine) is **complete** — PRs #1 and #2 merged. Phase 2 is sliced risk-first into 3 PRs; **2a (risk engine + bus + Strategy spine) merged as PR #3**.

| Date | Event |
|---|---|
| 2026-06-10 | Discovery complete: 10-agent legacy audit + 2026 landscape research → `docs/RESEARCH.md` |
| 2026-06-10 | v3 architecture + game plan written → `ROI-GEN-GAME-PLAN.md` |
| 2026-06-10 | Phase 0 scaffold complete: full compose stack verified healthy locally (engine heartbeat on Redis observed), CI green on main |
| 2026-06-10 | **PR #1 merged** (Phase 1a): 10-table schema + pgvector migration, fail-closed Google→JWT auth, portfolios + Fernet-encrypted credentials. 69 tests. Cloud review unavailable — substituted 7-angle local review; fix round added DB-enforced invariants |
| 2026-06-27 | PR auto-review rewired to authenticate via Claude **Max-subscription OAuth token** (`claude setup-token` → `CLAUDE_CODE_OAUTH_TOKEN` secret) instead of metered API key; job skips green until the secret is set |
| 2026-06-27 | **PR #2 merged** (Phase 1b broker spine): BrokerAdapter contract → AlpacaBrokerAdapter (async httpx) + market-data/trade-updates stream consumers + reconciliation + account API + engine wiring. 213 tests. Verified live against Alpaca paper (REST account incl. 4× intraday buying power, md websocket connect/auth/subscribe, staleness watchdog). Cloud review gated on OAuth token (unset) — substituted 3-agent local adversarial review + impartial judge (merge, high confidence); fixes: suspect-empty-positions guard, engine task-death logging |
| 2026-06-28 | **PR #3 merged** (Phase 2a — risk engine + event bus + Strategy spine): deterministic FIFO event bus (the backtest/live parity seam), Strategy base/registry/runner (propose-only), the **Risk Engine** choke point (pure `evaluate(signal, state)`; 13 controls + fixed-fractional whole-share sizing), mint-guarded `RiskApproval` (iron law #1 enforced by the type system), `RiskStage` audit+emit with auditable error path. No schema change. 59 engine tests (275 total). **First real cloud review** (OAuth token now set): R1 approve · 0 blocking · 5 advisory → all addressed (4 fixed, 1 mypy-guard pushback, 1 deferred → issue #4); R2 approve · 0/0; impartial judge → merge (high). |

**Next up — Phase 2b (execution core):** ExecutionHandler whose `execute()` *requires* a `RiskApproval` (the gate made physical), order state machine on `submit_order` + persisted `client_order_id`, trade-updates → DB persistence (the order-state writer), FIFO lot → realized-P&L engine, position tracking, boot reconciliation wired into engine startup. Then **2c**: kill switch (Redis cmd + API + CLI) + flatten@15:55/scheduler + trivial test strategy + live-paper E2E (the Phase 2 deliverable).

## Phase Tracker

- [x] **Phase 0 — Scaffold**: repo, compose stack, CI, review workflow, docs ✅ 2026-06-10
- [x] **Phase 1 — Domain + broker spine**: 1a (models/Alembic/auth/portfolios, PR #1) + 1b (BrokerAdapter, market-data + trade-updates streams, reconciliation, account API, PR #2) ✅ 2026-06-27
- [ ] **Phase 2 — Engine + risk core** (risk-first, 3 PRs): ✅ 2a risk engine + bus + Strategy spine (PR #3, 2026-06-28) · ☐ 2b execution + order state machine + FIFO P&L · ☐ 2c kill switch + flatten/scheduler + trivial strategy + live-paper E2E ← *2b next*
- [ ] **Phase 3 — Simulator + backtest parity**
- [ ] **Phase 4 — Strategy roster v1**: noise-area momentum, VWAP trend, regime classifier v1
- [ ] **Phase 5 — Frontend cockpit**
- [ ] **Phase 6 — Intelligence layer**: LLM adapter v2, news/sentiment, pgvector memory, post-mortems, calibration
- [ ] **Phase 7 — Copilot**
- [ ] **Phase 8 — ORB + scanner**
- [ ] **Phase 9 — Live readiness**
- [ ] **Phase 10 — Expansion**

## Blockers / Waiting on Sean

See `MANUAL-SETUP.md`. Phase-0 blockers: Claude GitHub App install + `ANTHROPIC_API_KEY` repo secret (review loop won't run until then; CI runs regardless).

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
