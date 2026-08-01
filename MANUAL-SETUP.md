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

### Preconditions (all local)

- **Postgres up** (compose `db`, or the brew fallback — brew Postgres 17 + pgvector also works and is what's currently listening on 5432). The suite creates/migrates its own `*_test` DB.
- **Redis up** — this is a real prerequisite, not an assumption. It is NOT started by the test:

  ```bash
  cd /path/to/roi-gen && docker compose up -d redis
  docker exec roigen-redis redis-cli ping   # → PONG
  ```

  The test uses **DB index 9** by default (`ROIGEN_LIVE_REDIS_URL` to override), so it never collides with the dev engine on DB 0.
- **Paper keys** — `tests/live/conftest.py` loads the **repo-root `.env`** into `os.environ` before the suite runs, so `ALPACA_API_KEY`/`ALPACA_SECRET_KEY` come from `.env` automatically. You do NOT need to export anything. Details:
  - It is gated on `ROIGEN_LIVE_E2E=1`, so a plain `pytest` run is completely unaffected (verified: zero env vars added).
  - It never overrides an already-exported variable — `ALPACA_API_KEY=... uv run pytest ...` still wins.
  - Why it's needed: `Settings` uses `env_file=".env"`, which pydantic-settings resolves relative to **CWD**. The runbook runs from `backend/`, where there is no `.env`. Before this shim the run skipped with `ALPACA_API_KEY / ALPACA_SECRET_KEY not set` even mid-session.
  - The engine + CLI subprocesses inherit all of it (the test builds `child_env` from `os.environ`).
- **The paper account must be FLAT and ENGINE-ONLY.** Use the `roi-gen-dev` paper account. The flatten controller cancels/closes EVERYTHING in the account, including anything you placed by hand, and the test skips with `paper account not flat: [...]` if any position is open. Check before you start:

  ```bash
  # positions must be [], open orders must be []
  ```

  (Verified flat on 2026-08-01: account `PA3E3NSRUF91`, equity $5,000, 0 positions, 0 orders ever.)
- **Market open**, with ≥20 minutes before the close (entries are risk-blocked inside the flatten buffer). Outside RTH the test self-skips with `market closed (next open ...)` — that skip is the correct, healthy outcome, and it proves the env gate passed.
- **No other engine running** against that account: a second market-data socket gets a 406, and the Postgres advisory lock refuses a same-portfolio second engine (`engine.singleton_lock_held`). Check with `pgrep -fl 'app.engine_main'`.

### Run

```bash
cd backend
ROIGEN_LIVE_E2E=1 uv run pytest -m live_paper tests/live/ -q -rs
```

`-rs` prints the skip reason, which is what you want when the gates don't pass.
(`-s` instead of `-rs` if you want the engine log tail streamed live.)

Expected: one test, ~2–6 minutes (dominated by waiting for the first 1-minute
bar). On failure the engine subprocess's log tail is printed for forensics, and
the full log file path is echoed.

What paper proves: plumbing + audit trail (order flow, stream writer, FIFO
lots, command lifecycle, flatten completion). What it can't prove: fill
realism (paper fills are optimistic NBBO — the Phase-3+ slippage haircut
exists for that).

### Known blockers found in the closed-market dress rehearsal (2026-08-01)

The engine was booted for real against the paper account with the market closed.
Boot reconcile, heartbeat/task-liveness, the trade-updates websocket, the
kill-switch round trip, the singleton lock and graceful shutdown all passed. Two
defects surfaced that the offline suite cannot see:

1. **FIXED — `market_data` could die permanently on a transient Redis error at
   boot.** `AlpacaMarketDataConsumer.start()` awaited the first feed-health
   `SETEX` *outside* the supervisor's try/except, so one `redis.TimeoutError`
   killed the task for good; because that task is `critical`, the composite
   `halted()` then blocked every entry for the life of the process — behind a
   heartbeat that still reported "running". Hit once in five cold boots. The
   health/status writes are now best-effort (fail toward "stale", the safe
   direction), with regression tests.

2. **OPEN — the staleness gate false-positives on a bars-only feed.** The engine
   subscribes to **minute bars only**, but `staleness_seconds` is 30s. Bars
   arrive every 60s, so on a perfectly healthy feed the level flips
   `ok → stale → ok` every minute and `alpaca.feed.stale` fires every minute.
   `FeedHealth` polls that level every 5s, so at the instant a bar arrives the
   gate still reads STALE — and the probe emits its signal on exactly that
   bar, with `max_entries_per_session = 1`. **Decide this before the next
   live-paper attempt**; until then expect the entry to be risk-rejected and
   the run to time out at `probe entry order filled`.

### Note on DEBUG logs

`DEBUG=true` (which the test forces on the child) turns on the websockets frame
logger, which prints the trade-stream `authenticate` frame — including a partial
`key_id`. The engine log file and the on-failure tail therefore contain a
fragment of the paper API key. Fine locally; don't paste a raw tail into an
issue or a PR.

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
