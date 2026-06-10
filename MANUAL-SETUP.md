# Manual Setup — Sean's Checklist

> Everything the platform needs that only you can do. Ordered by phase. Items marked ♻️ can be carried over from `../roi-gen-legacy/.env` (already populated there).

## Now (Phase 0–1, all free)

- [ ] ♻️ **Alpaca paper keys** — carry over from legacy `.env` (`ALPACA_API_KEY`/`ALPACA_SECRET_KEY`). Log into https://app.alpaca.markets and confirm the paper account still exists. While there: note that you can create up to **3 paper accounts** (each with its own keys) — create a second one named e.g. `roi-gen-dev` so testing doesn't pollute the main paper account's history.
- [ ] ♻️ **Anthropic API key** — carry over (`ANTHROPIC_API_KEY`). Confirm it's active at https://console.anthropic.com and has a few dollars of credit (Phase 6+ uses it; build phases barely touch it).
- [ ] ♻️ **Gemini + Cohere keys** — carry over (free-tier fallback providers).
- [ ] ♻️ **Finnhub + FRED keys** — carry over (earnings calendar, macro events).
- [ ] ♻️ **Google OAuth client** — carry over (`GOOGLE_CLIENT_ID`/`GOOGLE_CLIENT_SECRET`, `ALLOWED_EMAIL=seantokuzo@gmail.com`). If the OAuth consent screen lists allowed origins, add the new app's URL(s) (`http://localhost:4300` once the frontend lands).
- [ ] **GitHub repo plumbing** for https://github.com/seantokuzo/roi-gen:
  - [ ] Install the **Claude GitHub App** on the repo (https://github.com/apps/claude) — needed for the auto-review loop.
  - [ ] Add repo secret `ANTHROPIC_API_KEY` (Settings → Secrets → Actions) — the review workflow uses it.
  - [ ] Branch protection on `main` (require PR + passing CI) — optional but recommended.

## Before Phase 9 (going live with real money)

- [ ] **Alpaca live account**: complete/verify the live brokerage application, fund it (margin account, ≥$2,000 for 4x intraday buying power — PDT rule is retired as of 2026-06-04, no $25k needed). Generate LIVE API keys. **Do not put live keys in .env until we've built encrypted credential storage (Phase 9).**
- [ ] **Alpaca Algo Trader Plus** — $99/mo data subscription (Dashboard → Market Data). Required before any live volume/VWAP-based strategy.
- [ ] **Hosting**: Hetzner account (or confirm always-on Mac) — we'll decide together in Phase 9.
- [ ] **Alerting channel**: decide where you want alerts (email is default; Pushover/Telegram if you want push).

## Optional / later

- [ ] **Databento** account — free $125 credit; we'll burn it on historical tick data for backtest validation (worth doing whenever, costs nothing).
- [ ] **Reddit API** app (free OAuth tier) — only when we wire social sentiment.
- [ ] **Mac Studio** — local Ollama tier for the LLM adapter (the adapter is built provider-agnostic so this is a config swap).

## Explicitly NOT needed (researched, skip)

- ~~OpenAI key~~ (Anthropic primary, free tiers as fallback)
- ~~Polygon/Massive subscription~~ (Alpaca ATP wins at $99 vs $199 for realtime)
- ~~NewsAPI, Benzinga direct, StockTwits, Twelve Data, Tiingo~~ (dominated or dead — see docs/RESEARCH.md §7)
- ~~ChromaDB anything~~ (pgvector in Postgres now)
