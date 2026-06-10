# ROI-GEN State

> Living document. Updated at every phase transition and merged PR.

## Current Phase: 1 — Domain + broker spine (in progress)

| Date | Event |
|---|---|
| 2026-06-10 | Discovery complete: 10-agent legacy audit + 2026 landscape research → `docs/RESEARCH.md` |
| 2026-06-10 | v3 architecture + game plan written → `ROI-GEN-GAME-PLAN.md` |
| 2026-06-10 | Phase 0 scaffold complete: full compose stack verified healthy locally (engine heartbeat on Redis observed), CI green on main |
| 2026-06-10 | Phase 1 started (PR 1a: domain models + auth + portfolios) |

## Phase Tracker

- [x] **Phase 0 — Scaffold**: repo, compose stack, CI, review workflow, docs ✅ 2026-06-10
- [ ] **Phase 1 — Domain + broker spine**: models/Alembic, auth, BrokerAdapter, market-data spine, trade-updates, reconciliation ← *we are here*
- [ ] **Phase 2 — Engine + risk core**: event bus, Strategy base, Risk Engine, order state machine, FIFO P&L, kill switch
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
