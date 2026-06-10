# Research Record — June 2026 Discovery

> Distilled from a 10-agent discovery pass (2026-06-10): 5 agents audited `../roi-gen-legacy`, 5 researched the 2026 landscape. This is the durable evidence base behind the v3 architecture. Confidence tags reflect source quality.

---

## 1. Regulatory: PDT rule retired (HIGH confidence, verify during Phase 1)

- FINRA's amended Rule 4210 took effect **2026-06-04**: PDT designation, $25k minimum, day-trade counting, 3-round-trips-per-5-days — all gone.
- Replacement: **Intraday Buying Power** — continuously computed from equity, positions, intraday P&L. Pre-trade checks reject orders creating a margin deficit. 4x intraday leverage from **$2,000** equity (Reg-T minimum).
- Margin call mechanics: Intraday Margin Deficit triggers a call due in 2 business days; de minimis exception < $1,000 or 5% of equity; unmet by day 5 → 90-day freeze on new debits.
- **Alpaca removes `pattern_day_trader`, `daytrade_count`, `last_daytrade_count`, `daytrading_buying_power`, `last_daytrading_buying_power` from the API by 2026-07-06.** Build on `buying_power` only. Zero day-trade-counting logic anywhere.
- Rule is days old — **verify actual API behavior in paper during Phase 1** (intraday BP computation, pre-trade margin rejections, short locates).

## 2. Strategy evidence (build order rationale)

| # | Strategy | Evidence | Expectation management |
|---|---|---|---|
| 1 | **Intraday time-series momentum on SPY ("noise area")** | Gao/Han/Li/Zhou JFE 2018 (peer-reviewed: first half-hour predicts last half-hour, R²~1.6–2.6%); tradeable variant 19.6% ann., Sharpe 1.33, 2007–2024 net | Single instrument, fixed-timestamp checks → lowest automation risk. Build first. |
| 2 | **VWAP trend-following (QQQ)** | Zarattini & Aziz 2023: Sharpe ~2.0, 671% on QQQ 2018–2023 net of commissions | Trivial to compute; same-author caveat applies |
| 3 | **Stocks-in-play 5-min ORB** | Zarattini/Barbon/Aziz 2024: Sharpe 2.81, 41.6% ann. IRR 2016–2023; QuantConnect replication confirms Sharpe ~2.4 in-sample BUT ~17% win rate (expectancy from rare 10R winners), heavy period dependence | Needs RVOL scanner (top-20 by first-5-min volume / 14-day same-window avg, universe ~1000 liquid stocks >$5, ATR>$0.50), entry 9:35, stop at opposite side of 5-min bar, 10R target, 1% risk sizing, EOD flat. Short legs often hard-to-borrow; slippage at open materially worse than modeled. Expect a fraction of published Sharpe live. |
| 4 | **VWAP 2σ mean-reversion** | ~61–63% win rate at 2σ, ~71% reversion at 3σ — but drops toward 45% without a trend/chop regime filter | Only ship behind regime gate. Complement to #2, not independent. |
| 5 | **Pairs / stat-arb sleeve** | PCA pairs 2005–2024: Sharpe ~0.90–0.95 with realistic costs | Latency-tolerant, market-neutral diversifier for chop regimes. Low returns; later phase. |
| — | Gap-and-go | Mechanical index gap trades: ~0.04–0.06%/trade → dead after costs. Catalyst-driven gap trading = stocks-in-play ORB with a gap precondition | Gap size/type/fill-stats become **features** for ORB + regime, not a strategy |
| — | Overnight drift | Real and large on paper (AQR: ~4bps/day close-to-open, intraday ~0) but spread×2/day eats it | Use as session-bias prior + MOC/MOO paper experiments only |

**Time-of-day structure (very high confidence):** volume/volatility U-shape — open spike (9:30–10:30), midday trough (11:30–14:00, breakout strategies bleed), close ramp (15:00–16:00, where intraday-TSM concentrates). Scheduler must encode this.

**Base rates:** Brazil study: ~3% of day traders profitable, ~1.1% above minimum wage; Taiwan: ~1% of 360k consistently beat market; 13% still trading after 3 years. Winners separate via: (1) pessimistic cost/slippage modeling (#1 silent killer — edges vanish at ~0.4% round-trip), (2) walk-forward validation, (3) mechanical risk enforcement, (4) regime awareness, (5) edge-decay monitoring. Success case = Sharpe 1–2, 10–25%/yr — NOT the papers' 40–116% in-sample.

## 3. Risk management norms (HIGH confidence, converged)

- Risk per trade: 0.5–1% fixed-fractional (0.25–0.5% unproven strategies; 2% absolute ceiling). Position size is the OUTPUT: qty = equity × risk% / (entry − stop).
- Daily loss limit: 2–3× per-trade risk (or 3–5% equity) → hard halt until next session.
- Consecutive-loss breaker: 3–5 straight losses pauses strategy (catches regime breaks AND bugs).
- Drawdown ladder: −50% size at 10% DD, full halt at 15%, human re-arm.
- Volatility halt + unconditional kill switch (cancel-all + flatten), independent of strategy code.
- Scopes: per-strategy, per-portfolio, global.

## 4. Alpaca platform facts (June 2026)

- **Orders:** market/limit/stop/stop-limit/trailing-stop; classes: bracket (entry+TP+SL, TIF day/gtc), OCO (exit-only, main must be limit), OTO. **Bracket + trailing don't work extended-hours** → EH strategies self-manage exits. Sub-penny rules: ≥$1 → 2dp, <$1 → 4dp. Fractional/notional = TIF day only; notional can't be replaced (cancel+resubmit).
- **Sessions:** pre 4:00–9:30, RTH, after 16:00–20:00, **overnight 20:00–4:00 ET (Blue Ocean ATS)** — all EH limit-only with `extended_hours:true`. Crypto 24/7 (20+ assets, ~56 pairs, 15–25bps fees retail tier, no shorting/margin). Options L1–L3 incl. multi-leg `mleg` (≤4 legs); assignments are REST-poll only.
- **Accounts:** ONE live account per person (no retail sub-accounts; OmniSub = Broker API partners only). **Max 3 paper accounts**, each with own keys/streams; default $100k, customizable. Paper fills: NBBO-optimistic, random partials 10% of time, no impact/latency/borrow-fees → haircut required for promotion decisions.
- **Data plans:** Basic (free): realtime **IEX-only** websocket (≈2–3% of consolidated volume — AAPL example: 12,630 IEX vs 535,136 consolidated trades/day), 30-symbol cap, 200 calls/min, **cannot query most recent 15 min of historical**. Algo Trader Plus **$99/mo**: full SIP, unlimited symbols, 10k calls/min, realtime OPRA options. IEX-only is fine for build/paper on liquid large-caps (NBBO keeps quotes near-honest) but **destroys volume-based signals (RVOL/VWAP/ORB confirmation)** → ATP mandatory before live. Fills are unaffected by data plan (routing is independent).
- **Websockets:** ONE concurrent market-data connection per account (2nd → error 406) → single consumer service, internal fan-out. Trade-updates stream is separate, per trading account. Channels: trades, quotes, bars, updatedBars, dailyBars, statuses (halts), lulds, corrections, cancelErrors.
- **Rate limits:** Trading API 200 req/min per account, hard, rarely raised → token-bucket everything, never poll order status. Paper accounts have separate budgets.
- **SDK:** alpaca-py 0.43.4 (2026-04-29), py3.8–3.14. Streams are asyncio-native; **REST clients are sync** → thin async httpx adapter for REST, alpaca-py for streams/models. Verify version at install.
- **Reliability:** ~7h app outage Nov 21 2025; API outage Nov 18 2025 (bad deploy); OPRA outage Oct 13 2025; Saturday maintenance windows. Community-documented websocket disconnects + per-symbol gaps at market open even on SIP → reconnect-with-backfill, per-symbol gap detection, staleness watchdog blocking entries, kill switch on stale data. Second feed (Databento $199/mo) only when live P&L justifies.
- **News:** Alpaca News API **free** (Benzinga-powered): websocket `v1beta1/news` + REST history to 2015. Same content costs $99/mo elsewhere. Primary news source, $0.

## 5. Engine: build vs framework (HIGH confidence)

- **NautilusTrader** (v1.228.0, Rust core, 23.4k stars): technical gold standard, but **no Alpaca adapter** (RFC #3374 open since Jan 2026, zero code) and architected as standalone TradingNode, not embeddable. Watch the RFC — if it ships, evaluate as second-opinion backtest validator only.
- **Lumibot** (v4.5.47): first-class Alpaca, actively maintained, but polling `on_trading_iteration`/sleeptime model, threaded not asyncio, low-fidelity fills — wrong paradigm for intraday.
- **backtrader**: abandonware (last release 2023-04, classifiers py3.2–3.7). **freqtrade**: crypto-only upstream. **vectorbt**: OSS frozen, PRO $25/mo, research-only — optional sidecar for parameter sweeps. **LEAN**: official Alpaca plugin but wants to own process/data/deployment; C#/Python debugging tax.
- **Verdict: custom asyncio engine** copying Nautilus patterns: FIFO event bus (asyncio.Queue), MarketEvent→SignalEvent→OrderEvent→FillEvent, Strategy-as-class (on_bar/on_quote/on_trade_update), Portfolio/Risk layer separate, ExecutionHandler interface (AlpacaLive | SimulatedFill), client_order_id idempotency persisted pre-submission, event log → full state recovery, boot reconciliation diffing local vs broker, never-resubmit-on-ambiguous-timeout, definitive-rejection vs local-denial vs ambiguous-failure distinction.

## 6. LLM + RAG in trading (HIGH confidence on architecture, MEDIUM on specifics)

- **Agentic-trading papers (FinMem, TradingAgents, FinGPT et al.): returns are leakage-contaminated** (Profit Mirage: gains collapse ~88% past knowledge cutoff; StockBench: marginal vs buy-and-hold). Steal components, not premise: FinMem's layered memory + time decay, TradingAgents' bull/bear debate prompt for daily analysis, reflection loops for post-mortems.
- **Where LLMs add value:** news/sentiment, earnings analysis, regime narrative, post-mortems, anomaly explanation, copilot. **Where they fail:** tick-level decisions, price prediction, latency paths. Lopez-Lira/Tang: LLM headline sentiment predicts *next-day* returns (daily-cadence signal, decaying).
- **Sentiment tiering:** frontier LLMs beat FinBERT by ~6–10% accuracy, but FinBERT (110M, CPU, ms-scale, free) wins for high-volume intraday scoring. Don't use reasoning modes for classification. Tier: FinBERT/Haiku 4.5 bulk → Sonnet/Opus for briefs, earnings, post-mortems. Batches API 50% off for nightly jobs.
- **Embeddings:** open/proprietary gap closed. bge-m3 (1024d, MIT) or nomic-embed-text-v2 local via sentence-transformers, CPU-fine at our volume. FinMTEB: general-MTEB rank poorly predicts financial tasks; finance-tuned Fin-E5 too big to self-host; voyage-finance-2 = API fallback if retrieval disappoints. Financial text is jargon-dense/literal → keep **hybrid search** (pgvector + Postgres FTS).
- **pgvector over ChromaDB — decisive:** SQL joins (memories × trades × strategies × regime in one query), ACID (post-mortem + embedding commit atomically), one backup, zero new containers; pgvector 0.8.x HNSW handles 1–5M vectors single-digit-ms (we'll have tens of thousands). Memory = one table: embedding, memory_type (working/episodic/semantic), strategy_id, portfolio_id, symbols, regime_label, created_at, outcome; retrieval = similarity × exp(−age) × SQL filters. **Point-in-time discipline:** never retrieve memories created after the moment being analyzed (RAG lookahead bias).
- **Copilot pattern:** tool-use over the app's own service layer beats context stuffing. ~8 read tools (get_portfolio_summary, get_positions, get_strategy_status, get_recent_trades, get_regime_assessment, get_market_snapshot…) + `search_memory(query, filters)` as a tool. Mutating tools (place/cancel/params) behind explicit confirmation; manual agent loop for those, SDK runner for reads. `strict:true` schemas; prescriptive "call this when…" descriptions; structured outputs via `output_config.format`/`messages.parse()` for anything parsed programmatically.
- **Prompt-cache discipline:** frozen system prompt + stable tool list before cache_control breakpoint; live state injected late in messages (or mid-conversation system messages beta) — never interpolate timestamps/portfolio values into the system prompt.
- **Security:** adversarial headlines demonstrably redirect LLM trading decisions (arXiv 2601.13082; TradeTrap 2512.02261). Mitigations: LLM advisory-only, guardrails outside LLM layer, source-weighted news, sentiment never a sole trigger, confirmation on mutating copilot tools.
- Current Anthropic models/pricing (June 2026): Opus 4.8 $5/$25 per MTok, Haiku 4.5 $1/$5; verify at implementation time via the claude-api skill.

## 7. Data/news stack decision

- **Now ($0):** Alpaca free (realtime IEX websocket, 30 symbols) + Alpaca News (free Benzinga websocket + history) + Finnhub free (60/min, sanity-check quotes, earnings calendar) + Reddit free OAuth (ticker mentions, later).
- **Live trigger ($99/mo):** Alpaca Algo Trader Plus the moment real money trades OR a paper strategy depends on RVOL/VWAP/volume. Config flip `iex` → `sip`.
- **Later (+$199/mo, only if P&L justifies):** Databento Standard as independent second feed (their $125 free credit → historical tick data for backtesting on day one regardless). Massive (ex-Polygon, rebranded) Stocks Advanced $199/mo equivalent; their $29/$79 tiers are 15-min delayed = useless intraday.
- **Skip:** NewsAPI ($449/mo, not finance-grade), Benzinga direct (enterprise sales; we get it free via Alpaca), StockTwits (API closed to new registrations), Reddit commercial ($12k/mo), Twelve Data (credit metering hostile to streaming), Tiingo as live feed (post-2025 IEX licensing → derived prices only; still fine for cheap EOD history).
- Signals off 1-min consolidated bars; subscribe quotes only for symbols in active entry/exit (spread-aware limits). Tick-grade live feeds buy backtest fidelity, not live edge, at retail latency (50–200ms).

## 8. Legacy audit — landmines we must not reimport

- `realized_pnl` never computed → all legacy performance/learning stats ran on dead data. FIFO lot engine is a Phase 2 must.
- SafetyGuard only guarded the automated pipeline; manual trades + approved signals bypassed it.
- AI stop_loss/take_profit persisted but never sent to broker (no bracket orders).
- Daily-loss breaker measured *unrealized* P&L only; market-hours check hardcoded (no holidays); trade counting in UTC not ET; quote "last" was actually the ask.
- Sync alpaca-py calls inside async handlers blocked the event loop.
- `outcome_days` was order-fill latency, not holding period — RAG "lessons" cited fake hold times.
- `avg_sentiment_7d` hardcoded 0.0 — the AI read a placebo field for months.
- economic_collector NameError on every successful sync (silently failing); analyst.py ZeroDivisionError for any new strategy.
- Embedding dimension mismatch landmine (Cohere 1024 vs Gemini 768 in same collection on fallback).
- No Alembic, no CI, no tests, stale .env.example, scheduler docstring lied about persistence.

**Harvest list (verbatim or near):** frontend scaffold configs (vite/tsconfig/eslint/components.json/Tailwind v4 @theme dark palette), 19 shadcn components, layout trio, Dockerfiles + nginx.conf (+ WS upgrade headers already present), .dockerignore, backup.sh, ALPACA_STATUS_MAP (12-state), Sharpe/Sortino/DD math, LLM JSON-parse hardening, EVALUATOR system-prompt language, ConfidenceCalibrator (±0.15 bound, min-sample, 60/40 blend), research-brief classified-label prompt style, learning-loop write-ordering (RAG before flag), strategy lifecycle enum + paper-before-live gate, ownership-via-join pattern, auth flow (relocated to core/security), `.agents/skills/` directory, real credentials from legacy `.env` (Alpaca paper, Anthropic, Gemini, Cohere, Finnhub, FRED, Google OAuth).

---

*Full agent reports with complete source URL lists archived from workflow run `wf_be64e918-130` (2026-06-10). Key sources: docs.alpaca.markets, alpaca.markets/data, FINRA Notice 26-10, SSRN (Zarattini/Aziz/Barbon papers), JFE 2018 intraday momentum, QuantConnect replications, nautilustrader.io, arXiv (Profit Mirage 2510.02209, TradeTrap 2512.02261, Adversarial News 2601.13082, FinMTEB 2502.10990), platform.claude.com docs, massive.com, databento.com.*
