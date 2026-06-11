"""Shared async test fixtures.

Tests run against a dedicated ``*_test`` database derived from
``settings.DATABASE_URL`` — the dev database is never touched. The test
database is created on demand and migrated to head once per session;
tables are truncated between tests.
"""

import asyncio
from collections.abc import AsyncGenerator
from pathlib import Path

import asyncpg
import httpx
import pytest_asyncio
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

import app.models  # noqa: F401  — populate Base.metadata for truncation
from alembic import command
from app.core.config import get_settings
from app.core.database import Base, get_db
from app.models import User

TEST_EMAIL = "test-user@roigen.test"

_BACKEND_DIR = Path(__file__).resolve().parent.parent


def _test_database_url() -> str:
    """Derive the test db URL: append ``_test`` to the configured db name."""
    url = make_url(get_settings().database_url)
    if url.database is None:  # pragma: no cover — DATABASE_URL always has a db
        msg = "DATABASE_URL has no database name"
        raise RuntimeError(msg)
    if not url.database.endswith("_test"):
        url = url.set(database=f"{url.database}_test")
    return url.render_as_string(hide_password=False)


async def _ensure_test_database(test_url: str) -> None:
    """CREATE DATABASE if missing (via the ``postgres`` maintenance db)."""
    url = make_url(test_url)
    conn = await asyncpg.connect(
        user=url.username,
        password=url.password,
        host=url.host or "localhost",
        port=url.port or 5432,
        database="postgres",
    )
    try:
        exists = await conn.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", url.database)
        if not exists:
            await conn.execute(f'CREATE DATABASE "{url.database}"')
    finally:
        await conn.close()


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def test_db_url() -> str:
    """Create the test database (if needed) and migrate it to head."""
    test_url = _test_database_url()
    await _ensure_test_database(test_url)

    cfg = Config(str(_BACKEND_DIR / "alembic.ini"))
    cfg.set_main_option("sqlalchemy.url", test_url)
    # alembic's command API is sync (env.py calls asyncio.run internally),
    # so run it in a worker thread with its own event loop.
    await asyncio.to_thread(command.upgrade, cfg, "head")
    return test_url


@pytest_asyncio.fixture
async def db_engine(test_db_url: str) -> AsyncGenerator[AsyncEngine, None]:
    """Per-test engine bound to the test db; truncates all tables first."""
    engine = create_async_engine(test_db_url, poolclass=NullPool)
    tables = ", ".join(table.name for table in Base.metadata.sorted_tables)
    async with engine.begin() as conn:
        await conn.execute(text(f"TRUNCATE {tables} RESTART IDENTITY CASCADE"))
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
async def db_session(db_engine: AsyncEngine) -> AsyncGenerator[AsyncSession, None]:
    """Async session bound to the per-test engine."""
    factory = async_sessionmaker(db_engine, expire_on_commit=False)
    async with factory() as session:
        yield session


@pytest_asyncio.fixture
async def seeded_user(db_session: AsyncSession) -> User:
    """A committed User row to hang portfolios off."""
    user = User(email=TEST_EMAIL, display_name="Test User")
    db_session.add(user)
    await db_session.commit()
    await db_session.refresh(user)
    return user


@pytest_asyncio.fixture
async def app_client(db_engine: AsyncEngine) -> AsyncGenerator[httpx.AsyncClient, None]:
    """httpx client over ASGITransport with get_db overridden to the test db."""
    from app.main import app  # late import: pulls in the full router tree

    factory = async_sessionmaker(db_engine, expire_on_commit=False)

    async def _override_get_db() -> AsyncGenerator[AsyncSession, None]:
        async with factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_get_db
    transport = httpx.ASGITransport(app=app)
    try:
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            yield client
    finally:
        app.dependency_overrides.pop(get_db, None)
