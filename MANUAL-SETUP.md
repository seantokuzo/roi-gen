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

- **Postgres up** (compose `db`, or the brew Postgres 17 + pgvector fallback — either works; check with `pg_isready -h localhost -p 5432`). The suite creates/migrates its own `*_test` DB.
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
  cd backend && uv run python -c "
import asyncio
from app.core.config import get_settings
from app.brokers.credentials import BrokerCredentials
from app.brokers.alpaca.factory import build_alpaca_adapter

async def main():
    s = get_settings()
    a = build_alpaca_adapter(BrokerCredentials(
        api_key=s.alpaca_api_key, api_secret=s.alpaca_secret_key, paper=True))
    try:
        acct = await a.get_account()
        pos = await a.list_positions()
        orders = await a.list_orders(status='open', nested=False)
        print('account ', acct.account_number, 'equity', acct.equity)
        print('positions', [(p.symbol, str(p.qty)) for p in pos] or 'FLAT')
        print('open orders', [(o.symbol, o.status.value) for o in orders] or 'NONE')
    finally:
        await a.aclose()
asyncio.run(main())"
  ```

  Run this from the repo root so `.env` resolves (or export the keys). It also
  prints the **account number the keys actually resolve to** — confirm that is
  the account you intend to have flattened, because the `.env` auto-load means
  you are no longer picking it explicitly at the command line.

  (Verified flat on 2026-08-01: account `PA3E3NSRUF91`, equity $5,000, 0 positions, 0 orders ever.)
- **Market open**, with ≥20 minutes before the close (entries are risk-blocked inside the flatten buffer). Outside RTH the test self-skips with `market closed (next open ...)` — that skip is the correct, healthy outcome, and it proves the env gate passed.
- **No other engine running** against that account: a second market-data socket gets a 406, and the Postgres advisory lock refuses a same-portfolio second engine (`engine.singleton_lock_held`). Check with `pgrep -fl 'app.engine_main'`.

### Run

```bash
cd backend
ROIGEN_LIVE_E2E=1 uv run pytest -m live_paper tests/live/ -q -rs
```

`-rs` prints the skip reason, which is what you want when the gates don't
pass. It is orthogonal to `-s` (which disables output capture) — use
`-rs -s` together if you also want the engine log tail streamed live.

Expected: one test, ~2–6 minutes (dominated by waiting for the first 1-minute
bar). On failure the engine subprocess's log tail is printed for forensics, and
the full log file path is echoed.

What paper proves: plumbing + audit trail (order flow, stream writer, FIFO
lots, command lifecycle, flatten completion). What it can't prove: fill
realism (paper fills are optimistic NBBO — the Phase-3+ slippage haircut
exists for that).

### Defects found by the closed-market dress rehearsal (2026-08-01) — all FIXED

The engine was booted for real against the paper account with the market closed.
Boot reconcile, heartbeat/task-liveness, the trade-updates websocket, the
kill-switch round trip, the singleton lock and graceful shutdown all passed.
Three defects surfaced that the offline suite cannot see — all now fixed and
regression-tested, so the run below is expected to work:

1. **FIXED — `market_data` could die permanently on a transient Redis error at
   boot.** `AlpacaMarketDataConsumer.start()` awaited the first feed-health
   `SETEX` *outside* the supervisor's try/except, so one `redis.TimeoutError`
   killed the task for good; because that task is `critical`, the composite
   `halted()` then blocked every entry for the life of the process — behind a
   heartbeat that still reported "running". Hit once in five cold boots. The
   health/status writes are now best-effort (fail toward "stale", the safe
   direction), with regression tests.

2. **FIXED — the staleness gate false-positived on a bars-only feed.** The
   engine subscribes to **minute bars only**, but `staleness_seconds` was 30s.
   Bars arrive every 60s, so on a perfectly healthy feed the level flipped
   `ok → stale → ok` every minute. `FeedHealth` polls that level every 5s, so
   at the instant a bar arrived the gate still read STALE — and the probe
   emits its signal on exactly that bar, with `max_entries_per_session = 1`, so
   the run would have timed out at `probe entry order filled`.
   The threshold is now floored at **2× the slowest SUBSCRIBED cadence**:
   bars-only ⇒ 120s; subscribing quotes or trades (continuous channels)
   honours the requested number unchanged. The invariant lives in code, so it
   can't be re-broken by changing the subscription set. The health key's TTL
   and the watchdog poll derive from the *effective* window, and the reader
   now checks the payload's own age — a Redis that rejects writes while still
   serving reads (MISCONF/OOM) can no longer leave a stale `ok` believed.
   *Trade-off to know:* genuine-blackout detection for entries is now ~2 min
   on a bars-only feed. Enabling quotes buys it back to ~35s and is worth
   doing before live trading (tracked for Phase 9).

3. **FIXED — pub/sub subscribers churned a reconnect every 5 seconds.** The
   command channel and the market bridge used the blocking `pubsub.listen()`
   against a client with `socket_timeout=5.0`, so every 5s lull raised and
   forced a resubscribe (28 cycles in a 4-minute run), adding command latency
   and leaving a window where a bar could be missed. Both now use
   `get_message(timeout=1.0)` like the trade-updates subscriber. Because a
   *user* timeout returns `None` without disconnecting, the engine's Redis
   client also sets `health_check_interval=15` — otherwise a half-open socket
   would strand a subscriber silently, forever, behind a green heartbeat.

### Note on DEBUG logs

`DEBUG=true` (which the test forces on the child) turns on the websockets frame
logger, which prints the trade-stream `authenticate` frame — including a partial
`key_id`. The engine log file and the on-failure tail therefore contain a
fragment of the paper API key. Note the log is written to a temp file that is
deliberately NOT deleted (the path is echoed at the end of the run, so a failure
is diagnosable) — so that fragment persists on disk after every run. Fine
locally; don't paste a raw tail into an issue or a PR, and clear out old
`roigen-e2e-engine-*.log` files periodically.

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
