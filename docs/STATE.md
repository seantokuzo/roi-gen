# ROI-GEN State

> Living document. Updated at every phase transition and merged PR.

## Current Phase: 1 — Domain + broker spine (PR 1b in review)

| Date | Event |
|---|---|
| 2026-06-10 | Discovery complete: 10-agent legacy audit + 2026 landscape research → `docs/RESEARCH.md` |
| 2026-06-10 | v3 architecture + game plan written → `ROI-GEN-GAME-PLAN.md` |
| 2026-06-10 | Phase 0 scaffold complete: full compose stack verified healthy locally (engine heartbeat on Redis observed), CI green on main |
| 2026-06-10 | **PR #1 merged** (Phase 1a): 10-table schema + pgvector migration, fail-closed Google→JWT auth, portfolios + Fernet-encrypted credentials. 69 tests. Cloud review unavailable — substituted 7-angle local review; fix round added DB-enforced invariants |
| 2026-06-27 | PR auto-review rewired to authenticate via Claude **Max-subscription OAuth token** (`claude setup-token` → `CLAUDE_CODE_OAUTH_TOKEN` secret) instead of metered API key; job skips green until the secret is set |
| 2026-06-27 | **PR 1b opened** (broker spine): BrokerAdapter contract → AlpacaBrokerAdapter (async httpx) + market-data/trade-updates stream consumers + reconciliation + account API + engine wiring. 211 tests. Verified live against Alpaca paper (REST account + md websocket connect/auth/subscribe + staleness watchdog) |

**Next up — Phase 2 (Engine + risk core):** event bus (MarketEvent→SignalEvent→OrderEvent→FillEvent), Strategy base class, the central Risk Engine (the iron-law #1 choke point), order state machine on top of the adapter's `submit_order` + `client_order_id`, FIFO lot→realized-P&L engine, flatten-at-close, kill switch, boot reconciliation wired into engine startup.

## Phase Tracker

- [x] **Phase 0 — Scaffold**: repo, compose stack, CI, review workflow, docs ✅ 2026-06-10
- [~] **Phase 1 — Domain + broker spine**: 1a (models/Alembic/auth/portfolios) ✅ merged; 1b (BrokerAdapter, market-data + trade-updates streams, reconciliation, account API) — PR open ← *we are here*
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
| 2026-06-27 | PR review auth = Max-subscription OAuth token, not API key | Avoid metered API-console billing; reviews draw on the Max plan (`claude setup-token`) |
| 2026-06-27 | Engine never imports alpaca-py types; broker behind `BrokerAdapter` contract | Keeps a 2nd broker / sim-fill swappable; alpaca-py confined to `app/brokers/alpaca/` |
