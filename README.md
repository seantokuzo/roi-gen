# ROI-GEN

Autonomous intraday trading platform. Single-user. Alpaca-powered. Paper + live, multiple portfolios, multiple concurrent strategies, deterministic event-driven engine with an LLM intelligence layer (regime, sentiment, post-mortems, copilot chat with RAG memory).

**Core principle:** code does the math, AI does the thinking, risk limits do the enforcing — and the LLM never has order authority.

| Doc | Purpose |
|---|---|
| [ROI-GEN-GAME-PLAN.md](ROI-GEN-GAME-PLAN.md) | Vision, architecture, phased roadmap |
| [docs/RESEARCH.md](docs/RESEARCH.md) | Evidence base (June 2026 discovery) |
| [docs/STATE.md](docs/STATE.md) | Where we are right now |
| [MANUAL-SETUP.md](MANUAL-SETUP.md) | Human-required setup (credentials etc.) |
| [CLAUDE.md](CLAUDE.md) | Project conventions + iron laws |

## Quick Start

```bash
cp .env.example .env   # fill in keys (see MANUAL-SETUP.md)
docker compose up -d   # db (pgvector) + redis + api + engine + frontend
open http://localhost:4300
```

### Dev (outside Docker)

```bash
# backend API
cd backend && uv sync && uv run uvicorn app.main:app --reload --port 8000

# engine
cd backend && uv run python -m app.engine_main

# frontend
cd frontend && npm install && npm run dev   # http://localhost:5173, proxies /api → :8000
```

## Architecture (one paragraph)

A custom asyncio **engine** daemon owns the single Alpaca market-data websocket, fans bars/quotes out to strategy instances over an internal event bus (MarketEvent → SignalEvent → OrderEvent → FillEvent), funnels **every** order through a central Risk Engine (fixed-fractional sizing, daily-loss/consecutive-loss/drawdown breakers, kill switch), and executes via an ExecutionHandler that is either live Alpaca or a fill simulator — the same strategy code backtests and trades live. A FastAPI **api** service serves the React cockpit and the Claude-powered copilot (tool-use over the app's own service layer, pgvector memory). The **intelligence** slow loop (news sentiment, regime classification, post-trade post-mortems, confidence calibration) adjusts strategy posture on a minutes-to-daily cadence but never places orders.

⚠️ **Paper trading by default.** Live trading requires explicit per-portfolio promotion through the strategy lifecycle gates.
