"""FastAPI application factory and /health endpoint."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text

from app import __version__
from app.api.v1.router import api_router
from app.core.config import get_settings
from app.core.database import engine
from app.core.logging import get_logger, setup_logging

_HEALTH_CHECK_TIMEOUT_SECONDS = 2.0


async def _check_database() -> bool:
    try:
        async with asyncio.timeout(_HEALTH_CHECK_TIMEOUT_SECONDS):
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


async def _check_redis(redis_url: str) -> bool:
    client = aioredis.Redis.from_url(
        redis_url,
        socket_connect_timeout=_HEALTH_CHECK_TIMEOUT_SECONDS,
        socket_timeout=_HEALTH_CHECK_TIMEOUT_SECONDS,
    )
    try:
        await client.ping()
        return True
    except Exception:
        return False
    finally:
        await client.aclose()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    setup_logging(debug=settings.debug)
    log = get_logger("api")
    log.info("api.started", version=__version__, debug=settings.debug)
    yield
    await engine.dispose()
    log.info("api.stopped")


def create_app() -> FastAPI:
    """Build the FastAPI application."""
    settings = get_settings()

    app = FastAPI(
        title="ROI-GEN API",
        version=__version__,
        lifespan=lifespan,
        docs_url="/docs" if settings.debug else None,
        redoc_url=None,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins_list,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/health")
    async def health() -> dict[str, str | bool]:
        db_ok, redis_ok = await asyncio.gather(
            _check_database(),
            _check_redis(settings.redis_url),
        )
        return {
            "status": "ok" if (db_ok and redis_ok) else "degraded",
            "version": __version__,
            "database": db_ok,
            "redis": redis_ok,
        }

    app.include_router(api_router, prefix="/api/v1")

    return app


app = create_app()
