# ROI-GEN — Project Instructions

> Extends the global `~/.claude/CLAUDE.md`. Project-specific only; never contradicts global.

## What This Is

Single-user (Sean) autonomous intraday trading platform on Alpaca. Paper + live, multiple portfolios, multiple concurrent strategies, deterministic engine + LLM intelligence layer. **Read `ROI-GEN-GAME-PLAN.md` before any architectural work; `docs/RESEARCH.md` holds the evidence behind every major decision; `docs/STATE.md` tracks where we are.**

## Iron Laws (violations = blocking review findings)

1. **Every order passes through the Risk Engine.** No code path may call the broker adapter's order methods except the execution handler, and no execution without a risk-engine approval token. Legacy died on this.
2. **No LLM calls in the engine's hot path.** The fast loop is deterministic. LLM output feeds parameters/posture via the intelligence layer only.
3. **No PDT logic.** FINRA retired the rule 2026-06-04. Use `buying_power` + margin-headroom guard. The legacy fields are deleted from Alpaca's API.
4. **Entries carry protection.** Bracket orders in RTH; self-managed limit exits in extended hours. A position without a stop is a bug.
5. **Timezone-aware UTC in code, ET for market logic** — never naive datetimes. Day boundaries for P&L/limits are ET.
6. **Alembic migration for every schema change.** No `create_all`.
7. **Money is `Decimal`/`Numeric`** — never float.
8. **Paper before live**: strategy lifecycle draft→backtesting→paper→live is enforced in code; live activation requires prior paper status + slippage-haircut gate.
9. **Secrets never in git.** Live broker keys never in plain `.env` (encrypted DB records, Phase 9).
10. **Point-in-time discipline in memory/RAG**: retrieval for retrospective analysis filters `created_at <= as_of`.

## Stack Conventions

- **Backend**: Python 3.12, uv-managed (`backend/`). FastAPI (api) + custom asyncio engine (separate `engine` container). SQLAlchemy 2.0 async + Alembic. Postgres 17 + pgvector. Redis pub/sub for engine⇄API + UI fan-out. alpaca-py for websocket streams + Pydantic models; async httpx for REST behind `BrokerAdapter`. ruff + mypy strict + pytest(-asyncio). Run tooling via `uv run`.
- **Frontend**: React 19 + Vite 7 + TS + Tailwind v4 (CSS-first `@theme` in `src/index.css`, NO tailwind.config) + shadcn/ui importing from unified `radix-ui` package. TanStack Query (server state) + Zustand (client state only). lightweight-charts v5 (`chart.addSeries(AreaSeries, opts)` v5 API). Never regenerate shadcn components in the old per-package style.
- **Engine patterns**: FIFO event bus (asyncio.Queue), MarketEvent→SignalEvent→OrderEvent→FillEvent, Strategy-as-class (`on_bar`/`on_quote`/`on_trade_update`), ExecutionHandler interface (AlpacaLive | SimulatedFill — same Strategy code in backtest and live), `client_order_id` persisted before submission, boot reconciliation against broker, never resubmit on ambiguous timeout.
- **Testing**: every engine/risk change needs tests; the simulator is the test harness. CI must be green before merge (global rule).

## PR Review Tiers (this project)

- Sticky marker: `<!-- ROIGEN-VERDICT-STICKY -->` (round-completion signal per global workflow).
- Tier 1: `@claude` mentions for Q&A. Tier 2: auto review on every PR (`.github/workflows/claude-code-review.yml`). Tier 3 (deep): auto-escalated for changes touching `backend/app/engine/risk*`, `backend/app/engine/execution*`, `backend/app/services/broker*`, `.github/workflows/**` — these are the money-losing surfaces.
- Merge style: `--merge` (global default). Max 4 rounds then human-decides.

## Domain Gotchas (from legacy forensics + research)

- Alpaca: one market-data websocket per account (2nd conn → 406); 200 req/min trading API; trade-updates stream is the order-state source of truth (never poll); paper fills are optimistic (NBBO, no impact) — haircut before live promotion.
- IEX free feed ≈ 2–3% of consolidated volume: fine for price on liquid names, **lies about volume** — RVOL/VWAP signals need SIP ($99/mo ATP) before live.
- Bracket/trailing orders are RTH-only. Fractional/notional = TIF day only; notional orders can't be replaced.
- Feed silence during RTH = block new entries (staleness watchdog). Alpaca has real multi-hour outage history.
- Options assignments: REST polling only, no stream events.
