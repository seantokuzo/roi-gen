# ROI-GEN — Autonomous Intraday Trading Platform

## Game Plan & Architecture v3

> Created: 2026-06-10
> Status: Greenfield rebuild of `roi-gen-legacy` (which reached Phase 8 of the v2 plan)
> Mission: **An always-on platform that trades intelligently and autonomously throughout the day — paper and live — with proven strategies, mechanical risk enforcement, an adaptive intelligence layer, and a full trader's cockpit.**

---

## Why a Rebuild

The legacy build (v2) proved the 4-layer idea — code does the math, AI does the thinking — but it was architecturally **swing-trading shaped** and the rebuild targets **aggressive intraday autonomy**. The forensic audit of legacy found:

| Legacy reality | Why it can't carry forward |
|---|---|
| Daily-cadence spine (4:30 PM bars → 5 PM briefs), 1–3 LLM evals/day | Intraday needs streaming bars/quotes and second-scale reaction |
| Zero websockets; fills discovered by 2-min REST polling | Unacceptable at intraday speed |
| **No bracket orders — AI stop-losses stored in DB but never sent to broker** | Every autonomous position sat unprotected |
| **SafetyGuard bypassed on 2 of 3 order paths** (manual trades + approved signals) | Risk layer must be an unavoidable choke point |
| **`realized_pnl` never computed anywhere** — learning loop + metrics ran on a dead field | The entire feedback loop was fiction |
| LLM call inside every trade decision | LLM latency/cost in the hot path; wrong division of labor |
| In-process APScheduler, MemoryJobStore, dies with web server | Trading daemon must survive restarts mid-session |
| `create_all` on boot, no migrations; no CI; no tests | Can't iterate schema or trust changes |
| One global Alpaca client, paper XOR live at boot | Can't run paper + live portfolios concurrently |

What legacy got **right** (and we harvest): the 4-layer philosophy, domain vocabulary (Strategy/Signal/Trade/Portfolio), strategy lifecycle (draft→backtest→paper→live), the LLM provider-tier adapter concept, research-brief prompt engineering (classified labels not raw numbers), the learning-loop write-ordering, ConfidenceCalibrator math, performance math (Sharpe/Sortino/DD), the dark UI design language, and the React/Vite/Tailwind v4/shadcn scaffold.

---

## Research Foundations (June 2026)

Full distilled findings with sources live in [docs/RESEARCH.md](docs/RESEARCH.md). The decisions below are evidence-driven:

1. **PDT rule is dead.** FINRA retired Pattern Day Trader rules effective **2026-06-04**. Unlimited day trades, 4x intraday buying power at $2k equity. Alpaca deletes all PDT API fields by 2026-07-06 → **we build zero PDT logic**, use `buying_power` + an app-level margin-headroom guard.
2. **Best-evidenced retail intraday strategies** (in build order): SPY/QQQ intraday time-series momentum ("noise area", peer-reviewed JFE 2018 + Sharpe 1.33 net 2007-2024), VWAP trend-following (Sharpe ~2 QQQ), "stocks-in-play" 5-min ORB with relative-volume scanner (Sharpe 2.81 published — expect a fraction live), VWAP 2σ mean-reversion **only** behind a chop-regime gate. Gap stats become *features*, not a strategy. Pairs trading later as a market-neutral chop sleeve.
3. **Honest base rates:** 70–97% of day traders lose. What separates winners: pessimistic cost modeling, walk-forward validation, **mechanical** risk enforcement, regime awareness, continuous edge monitoring. All four are *structural advantages of software* — that's the thesis of this platform.
4. **No off-the-shelf engine fits** (Alpaca-native + event-driven intraday + asyncio + embeddable): NautilusTrader has no Alpaca adapter; Lumibot polls; backtrader is abandonware; LEAN wants to be the platform. → **Custom asyncio event-driven engine**, deliberately copying NautilusTrader's proven patterns (event bus, reconciliation, order state machine).
5. **LLMs don't pick trades.** Published "agentic trading" returns are leakage-contaminated; LLM latency >> bar latency. LLMs demonstrably help at: news/sentiment, regime narrative, post-trade post-mortems, strategy parameter review, chat copilot. → **Two-loop architecture** (deterministic fast loop, LLM slow loop).
6. **pgvector over ChromaDB.** SQL joins between memories and trade rows, ACID, one backup story, zero extra containers. Layered memory with time decay + regime/strategy metadata filters.
7. **Data costs:** Build + paper phase = $0 (Alpaca free IEX websocket + free Benzinga-powered news stream). Go-live = $99/mo Alpaca Algo Trader Plus (full SIP — required because IEX-only carries ~2-3% of volume, which destroys RVOL/VWAP signal quality). Optional later: Databento ($199/mo) as second feed.

---

## Architecture

```
┌────────────────────────────── Browser ──────────────────────────────┐
│  Dashboard │ Portfolios │ Strategies │ Blotter │ Charts │ Copilot   │
│  live WS updates ─ kill switch ─ regime monitor ─ performance       │
└──────────────────────────────┬──────────────────────────────────────┘
                               │ REST + WebSocket
┌──────────────────────────────▼──────────────────────────────────────┐
│  API service (FastAPI)                                               │
│  auth · portfolios · strategies · orders · positions · performance  │
│  copilot (Claude tool-use over service layer) · admin/kill-switch   │
└──────────────┬───────────────────────────────────┬──────────────────┘
               │ Postgres (+pgvector) + Redis      │ commands via Redis
┌──────────────▼───────────────────────────────────▼──────────────────┐
│  ENGINE service (asyncio daemon — the always-on trader)             │
│                                                                      │
│   Market Data Spine          Event Bus           Execution Core      │
│   ┌───────────────┐   MarketEvent → SignalEvent  ┌──────────────┐   │
│   │ Alpaca WS     │──▶ → OrderEvent → FillEvent ─▶ Risk Engine  │   │
│   │ (1 conn, fan- │   ┌───────────────────────┐  │ (choke point)│   │
│   │ out internal) │   │ Strategies (on_bar,   │  ├──────────────┤   │
│   │ reconnect +   │   │ on_quote, on_fill)    │  │ Execution    │   │
│   │ backfill +    │   │ noise-area, VWAP-     │  │ Handler:     │   │
│   │ staleness     │   │ trend, ORB, ...       │  │ AlpacaLive │ │   │
│   │ watchdog      │   └───────────▲───────────┘  │ SimulatedFill│   │
│   └───────────────┘               │              └──────────────┘   │
│   Indicators: VWAP, OR, RVOL, ATR │              trade-updates WS   │
│   Regime classifier (trend/range/event) ──────▶  reconciliation     │
│   Time-of-day scheduler · flatten@15:55 · EOD jobs                  │
└──────────────────────────────┬──────────────────────────────────────┘
                               │ slow loop (minutes–daily, not per-tick)
┌──────────────────────────────▼──────────────────────────────────────┐
│  INTELLIGENCE (LLM slow loop — advisory, never order authority)     │
│  news stream → sentiment (FinBERT/Haiku) → burst detection          │
│  regime briefs (Sonnet) · post-trade post-mortems → pgvector memory │
│  confidence calibration · strategy parameter review (Opus, weekly)  │
│  chat copilot with tools over app state + search_memory             │
└──────────────────────────────────────────────────────────────────────┘
```

### Core principles

1. **Two loops.** The fast loop (engine) is 100% deterministic code — indicators, rules, risk, execution; milliseconds; no LLM calls ever. The slow loop (intelligence) runs minutes-to-daily cadence and adjusts *parameters and posture* (regime label, strategy enable/disable, size multipliers, calibrated confidence), writes memory, and explains. **LLM output is advisory input to deterministic limits — never direct order authority.**
2. **One risk choke point.** Every order — autonomous, manual from UI, copilot-initiated — passes through the Risk Engine. No second path. (Legacy's #1 sin.)
3. **Backtest/live parity.** Same `Strategy` classes run against `SimulatedFill` (historical replay through the same event bus) and `AlpacaLive` with zero code changes. This is the hardest, most valuable deliverable.
4. **Always recoverable.** `client_order_id` idempotency on every order, event log in Postgres, boot-time reconciliation against broker (`GET /orders` + `/positions`, diff, synthesize missed events), never resubmit on ambiguous timeout.
5. **Stale data = no trading.** Feed watchdog blocks new entries if the stream goes quiet during RTH; kill switch flattens/freezes. Alpaca had a 7-hour outage in Nov 2025 — assume it recurs mid-session.
6. **Bracket-first orders.** Entries carry broker-side stop-loss + take-profit (RTH). Extended-hours strategies self-manage exits with limit orders.
7. **Paper ≠ proof.** Paper fills are optimistic (NBBO, no impact). Promotion to live requires paper performance *minus a configurable slippage haircut*, and the platform automatically measures realized-vs-modeled slippage.

### Risk Engine (built in Phase 2, before any strategy trades)

| Control | Default | Scope |
|---|---|---|
| Risk per trade (fixed-fractional) | 0.75% of equity (0.25% while strategy unproven; 2% hard ceiling) | per strategy |
| Position sizing | qty = (equity × risk%) / (entry − stop). Strategies propose (symbol, side, entry, stop); risk engine returns qty or rejects | all orders |
| Daily loss limit | 2.5× per-trade risk → halt new entries until next session | per strategy, per portfolio, global |
| Consecutive losses | 4 straight losers → strategy paused pending review | per strategy |
| Drawdown ladder | −50% size at 10% peak-to-trough DD; full halt at 15% with manual re-arm | per strategy + portfolio |
| Volatility halt | pause new entries on realized-vol/VIX spike | global |
| Margin headroom | projected exposure ≤ intraday buying power × 0.85 | global |
| Time guards | market hours via Alpaca clock/calendar API (holidays, half-days); mandatory flatten 15:55 for day sleeves; per-symbol cooldowns; max hold time | engine |
| Kill switch | cancel all open orders + optionally flatten all; independent of strategy code; one tap in dashboard + API + CLI | global |
| Real daily P&L | realized (FIFO lot engine) + unrealized, ET day boundary | everywhere |

### Portfolios over one Alpaca account

Alpaca allows **one live account** per person (no retail sub-accounts) and **3 paper accounts**. So:

- **Paper:** up to 3 truly isolated paper accounts (separate keys, separate trade-update streams).
- **Live:** portfolios are **logical ledgers** over the single live account — the app tracks per-portfolio cash/positions/P&L and continuously reconciles the sum against the real account.
- Broker credentials are per-portfolio DB records (encrypted), not one global env flag. Paper and live portfolios run concurrently in one engine.

### Time-of-day scheduler (encoded, not vibes)

| Window (ET) | Active |
|---|---|
| 9:30–11:00 | ORB, gap-informed momentum, noise-area checks |
| 11:30–14:00 | mean-reversion/pairs only, or nothing (midday chop bleeds breakout strategies) |
| 15:00–16:00 | last-half-hour momentum (the documented intraday-TSM effect) |
| 15:55 | mandatory flatten for all day-trading sleeves |
| 16:05+ | settlement sweep, post-mortems, performance snapshots, calibration |
| 20:00–4:00 | overnight session available (limit-only) — later phase |

---

## Tech Stack

### Backend
| Concern | Choice | Why |
|---|---|---|
| Language | Python 3.12 via **uv** | ecosystem maturity for quant libs; uv manages interpreter + deps |
| API | FastAPI + uvicorn | proven in legacy; async-native |
| Engine | **custom asyncio event-driven daemon** (separate container) | survives API restarts; Nautilus-patterned |
| ORM / migrations | SQLAlchemy 2.0 async + **Alembic from commit one** | legacy's create_all must die |
| DB | Postgres 17 + **pgvector** | one store for relational + vectors + FTS |
| Cache / bus | Redis 7 (pub/sub for engine⇄API commands + live UI fan-out) | already proven |
| Broker | **alpaca-py for websocket streams + Pydantic models; thin async httpx adapter for REST** behind our own `BrokerAdapter` interface | alpaca-py REST is sync; interface keeps a second broker possible |
| Indicators | own implementations on numpy/pandas (VWAP, OR, RVOL, ATR, ADX, EMA…) | intraday multi-timeframe; `ta` is daily-shaped |
| LLM | model-agnostic adapter v2 — **Anthropic primary** (Haiku 4.5 fast / Sonnet smart / Opus premium), Gemini/Cohere free tiers as fallback; tool use, streaming, prompt caching, cost tracking, Batches for nightly jobs | Sean's preference + research |
| Sentiment | FinBERT local (CPU) or Haiku batch | bulk headline scoring ≠ frontier-model job |
| Embeddings | local sentence-transformers (bge-m3 or nomic-embed-text-v2), pinned | zero cost, no provider drift; index never needs re-embedding |
| Scheduling | engine-internal async scheduler (market-clock aware) + cron-style EOD jobs | APScheduler-in-webserver was the legacy trap |
| Tests | pytest + pytest-asyncio; engine simulator doubles as test harness | |
| Quality | ruff (lint+format) + mypy strict | |

### Frontend
| Concern | Choice |
|---|---|
| Stack | React 19 + Vite 7 + TypeScript 5.9 + Tailwind v4 (CSS-first `@theme`) + shadcn/ui (unified `radix-ui` pkg) — harvested from legacy scaffold |
| Server state | TanStack Query 5 + **WebSocket layer hydrating the query cache** (the #1 legacy gap) |
| Client state | Zustand |
| Charts | lightweight-charts v5 — candlesticks, incremental `series.update()`, entry/exit markers, volume panes |
| Forms | RHF + Zod |
| Key surfaces | Live dashboard (day P&L, positions, risk utilization, kill switch), portfolio switcher (paper/live explicit), strategy manager + promotion lifecycle, live blotter/activity feed (replaces approval queue), charts, copilot chat (SSE streaming, markdown, tool-call visibility), regime monitor, performance analytics |

### Infrastructure
| Concern | Choice |
|---|---|
| Containers | docker compose: `db` (pgvector/pg17), `redis`, `api`, `engine`, `frontend` (nginx, WS/SSE-ready) — `restart: unless-stopped` |
| CI | GitHub Actions: backend ruff+mypy+pytest, frontend lint+build, on every PR |
| Reviews | Claude GitHub App auto-review (tiered, VERDICT-STICKY marker) per global workflow |
| Hosting | local Docker now; Hetzner VPS when live trading needs always-on (Phase 9) |
| Backups | scheduled pg_dump (covers vectors too — that's the point of pgvector) |

---

## Build Phases

Each phase = one or more PRs through the autonomous review loop. Strategy code never goes live without passing backtest → paper burn-in → slippage-haircut gate.

- **Phase 0 — Scaffold** ✦ repo layout, docker compose stack, CI, Claude review workflow, project CLAUDE.md, env contract, docs. *Deliverable: green CI on a hello-world API + engine heartbeat + frontend shell.*
- **Phase 1 — Domain + broker spine** ✦ models + Alembic (portfolios, broker_credentials, strategies, orders, fills, positions, lots, equity_snapshots, event_log), single-user auth, BrokerAdapter (async REST + streams), market-data consumer with fan-out/reconnect/backfill/watchdog, trade-updates consumer, account/position sync + reconciliation, REST surface for portfolios/account state. *Deliverable: live paper-account state visible via API, streaming bars in logs.*
- **Phase 2 — Engine + risk core** ✦ event bus, Strategy base class, Risk Engine (all controls above), order state machine with client_order_id, ExecutionHandler (AlpacaLive), FIFO lot → realized P&L engine, flatten-at-close, kill switch, boot reconciliation. *Deliverable: a trivial test strategy paper-trades bracket orders end-to-end with full audit trail.*
- **Phase 3 — Simulator + backtest parity** ✦ SimulatedFill (spread/slippage/partial fills), historical replay through the same bus, walk-forward harness, metrics, promotion gates. *Deliverable: same strategy class backtests and paper-trades identically.*
- **Phase 4 — Strategy roster v1** ✦ indicator library (multi-timeframe), noise-area SPY/QQQ momentum, VWAP trend-following, regime classifier v1 (trend/range/event from gap, RVOL, ADX, realized vol), time-of-day scheduler, intraday-grain per-strategy performance. *Deliverable: 2 evidenced strategies in paper burn-in.*
- **Phase 5 — Frontend cockpit** ✦ scaffold harvest, auth, layout, live dashboard + kill switch, portfolios, strategy manager, blotter, charts, WebSocket live layer. *Deliverable: full visual control of the paper-trading system.*
- **Phase 6 — Intelligence layer** ✦ LLM adapter v2, Alpaca news websocket ingestion, sentiment pipeline + burst detection, pgvector memory schema (layered, time-decay, point-in-time discipline), post-trade post-mortems, confidence calibration (per strategy/regime/time-of-day, Wilson intervals), regime briefs. *Deliverable: the system learns from every round-trip and adjusts posture.*
- **Phase 7 — Copilot** ✦ Claude tool-use chat over the service layer (~8 read tools + search_memory; mutating tools behind confirmation), server-side conversations, SSE streaming, prompt-caching discipline. *Deliverable: "how are we doing today and why?" answered with live data.*
- **Phase 8 — ORB + scanner** ✦ stocks-in-play universe scanner (liquidity + RVOL ranking at 9:35), 5-min ORB strategy, gap features, short-availability handling. *Deliverable: the highest-edge strategy, paper burn-in.*
- **Phase 9 — Live readiness** ✦ live credentials flow, ATP data flip (sip), margin-headroom guard verification, alerting (big loss / breaker trip / feed stale / fill anomalies), ops runbook, hosting, secrets management, scheduled backups. *Deliverable: first real-money strategy at 0.25% risk.*
- **Phase 10 — Expansion** ✦ VWAP mean-reversion behind chop gate, pairs sleeve, last-half-hour TSM, overnight/extended-hours experiments, options/crypto exploration, Mac Studio local LLM tier.

---

## Cost Model

| Phase | Monthly |
|---|---|
| Build + paper (now) | **$0** — Alpaca free data (IEX, 30 symbols) + free news stream + free LLM tiers / modest Anthropic usage |
| Anthropic API (Phase 6+) | ~$5–20 (Haiku bulk + Sonnet briefs + Opus weekly, Batches 50% off nightly) |
| Live trading | **$99** Alpaca Algo Trader Plus (full SIP — non-negotiable for volume-based signals) |
| Hosting (Phase 9) | ~$10–20 Hetzner VPS |
| Optional second feed | $199 Databento Standard — only when live P&L justifies |

---

## What Sean Does Manually

Tracked in [MANUAL-SETUP.md](MANUAL-SETUP.md) — credentials, app installs, account settings. Everything else is the platform's job.
