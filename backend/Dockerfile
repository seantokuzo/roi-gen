# ROI-GEN backend — multi-stage uv build.
# Same image serves both compose services:
#   api    → default CMD (uvicorn)
#   engine → compose overrides command: ["python", "-m", "app.engine_main"]

# ── Stage 1: build the virtualenv with uv ───────────────────────────────
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0

WORKDIR /app

# Install dependencies first (cached layer — invalidates only on lock change).
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-dev --no-install-project

# Then the application source.
COPY app ./app
COPY alembic ./alembic
COPY alembic.ini ./

# ── Stage 2: slim runtime, non-root ─────────────────────────────────────
FROM python:3.12-slim-bookworm

RUN useradd --create-home --uid 1000 appuser

WORKDIR /app
COPY --from=builder --chown=appuser:appuser /app /app

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

USER appuser

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
