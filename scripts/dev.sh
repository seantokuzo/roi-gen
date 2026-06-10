#!/bin/bash
#
# ROI-GEN dev helper: start infra (db + redis) in Docker, run app on the host.
#
# Usage: ./scripts/dev.sh
#

set -e

cd "$(dirname "$0")/.."

docker compose up -d db redis

echo ""
echo "db (pgvector/pg17) on :5432 and redis on :6379 are up."
echo ""
echo "Next steps:"
echo "  backend api:    cd backend && uv run uvicorn app.main:app --reload --port 8000"
echo "  backend engine: cd backend && uv run python -m app.engine_main"
echo "  frontend:       cd frontend && npm run dev        # Vite on :5173"
echo ""
echo "Full stack in Docker instead:  docker compose up -d   # frontend on :4300"
