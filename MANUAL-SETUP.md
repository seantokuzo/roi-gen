# Manual Setup — Sean's Checklist

> Everything the platform needs that only you can do. Ordered by phase. Items marked ♻️ can be carried over from `../roi-gen-legacy/.env` (already populated there).

## Now (Phase 0–1, all free)

- [ ] ♻️ **Alpaca paper keys** — carry over from legacy `.env` (`ALPACA_API_KEY`/`ALPACA_SECRET_KEY`). Log into https://app.alpaca.markets and confirm the paper account still exists. While there: note that you can create up to **3 paper accounts** (each with its own keys) — create a second one named e.g. `roi-gen-dev` so testing doesn't pollute the main paper account's history.
- [ ] ♻️ **Anthropic API key** — carry over (`ANTHROPIC_API_KEY`) for the *app's* in-product LLM calls (Phase 6+ intelligence layer, copilot). This is separate from PR review auth below. Confirm active at https://console.anthropic.com. Build phases barely touch it.
- [ ] ♻️ **Gemini + Cohere keys** — carry over (free-tier fallback providers).
- [ ] ♻️ **Finnhub + FRED keys** — carry over (earnings calendar, macro events).
- [ ] ♻️ **Google OAuth client** — carry over (`GOOGLE_CLIENT_ID`/`GOOGLE_CLIENT_SECRET`, `ALLOWED_EMAIL=seantokuzo@gmail.com`). If the OAuth consent screen lists allowed origins, add the new app's URL(s) (`http://localhost:4300` once the frontend lands).
- [ ] **GitHub repo plumbing** for https://github.com/seantokuzo/roi-gen:
  - ~~Claude GitHub App + `CLAUDE_CODE_OAUTH_TOKEN` secret~~ — **retired 2026-07-09**: PR reviews are local-only now (multi-lens adversarial review + impartial judge, see `CLAUDE.md`); the cloud review workflow was deleted. The secret can be removed from repo settings if it's still there.
  - [ ] Branch protection on `main` (require PR + passing CI) — optional but recommended.

## Live-paper E2E runbook (Phase 2c — the Phase 2 deliverable proof)

Runs the REAL engine as a subprocess against the REAL Alpaca paper API: probe
strategy enters a bracket off a live bar → kill-switch `flatten` via the real
CLI → broker-verified flat → full audit chain asserted from rows. Never runs in
CI; deselected from plain `pytest` by marker.

Preconditions (all local):
- Postgres up (compose `db` or the brew fallback) — the suite creates/migrates its own `*_test` DB.
- Redis up. The test uses DB index 9 by default (`ROIGEN_LIVE_REDIS_URL` to override).
- Paper keys in the environment (`ALPACA_API_KEY`/`ALPACA_SECRET_KEY`) — use the **`roi-gen-dev` paper account, and treat it as ENGINE-ONLY**: the flatten controller cancels/closes EVERYTHING in the account, including anything you placed by hand, and the test skips if the account isn't flat.
- Market open, with ≥20 minutes before the close (entries are risk-blocked inside the flatten buffer).
- No other engine running against that account (a second market-data socket 406s; the advisory lock also blocks a same-DB second engine).

Run:

```bash
cd backend
ROIGEN_LIVE_E2E=1 uv run pytest -m live_paper tests/live/ -q -s
```

Expected: one test, ~2–6 minutes (dominated by waiting for the first 1-minute
bar). On failure the engine subprocess's log tail is printed for forensics.
What paper proves: plumbing + audit trail (order flow, stream writer, FIFO
lots, command lifecycle, flatten completion). What it can't prove: fill
realism (paper fills are optimistic NBBO — the Phase-3+ slippage haircut
exists for that).

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
