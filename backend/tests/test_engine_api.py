"""Kill-switch API tests (``/engine``).

``require_user`` and ``get_redis`` are dependency-overridden (fake user, fake
Redis); ``get_db`` is overridden to the test database by ``app_client``. The
resume-guard logic itself is exercised in depth in
``test_engine_commands_service.py`` — here we pin the HTTP contract: status
codes, response shapes, ordering, limits, and the auth gate.
"""

from __future__ import annotations

import json
from collections.abc import AsyncGenerator

import httpx
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.deps import get_redis, require_user
from app.core.config import get_settings
from app.engine.commands import CHANNEL_ENGINE_COMMANDS, heartbeat_key
from app.engine.kill_switch import RESULT_FLAT_VERIFIED
from app.models import User
from app.models.engine_command import EngineCommand
from app.services.engine_commands import heartbeat_portfolio_id
from tests.conftest import TEST_EMAIL

API = "/api/v1/engine"


class FakeRedis:
    """Capture-only stand-in for ``redis.asyncio.Redis`` (publish + get)."""

    def __init__(self) -> None:
        self.published: list[tuple[str, str]] = []
        self.values: dict[str, str] = {}

    async def publish(self, channel: str, message: str) -> int:
        self.published.append((channel, message))
        return 1

    async def get(self, name: str) -> str | None:
        return self.values.get(name)


def _status_heartbeat_key() -> str:
    """The heartbeat key the /engine/status endpoint will read (env-dependent)."""
    return heartbeat_key(heartbeat_portfolio_id(get_settings().engine_portfolio_id))


@pytest_asyncio.fixture
async def auth_client(
    app_client: httpx.AsyncClient, seeded_user: User
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """``app_client`` with ``require_user`` overridden to the seeded user."""
    from app.main import app

    app.dependency_overrides[require_user] = lambda: seeded_user
    try:
        yield app_client
    finally:
        app.dependency_overrides.pop(require_user, None)


@pytest_asyncio.fixture
async def fake_redis(app_client: httpx.AsyncClient) -> AsyncGenerator[FakeRedis, None]:
    """Override ``get_redis`` with a capture-only fake for the app's lifetime."""
    from app.main import app

    fake = FakeRedis()

    async def _override() -> AsyncGenerator[FakeRedis, None]:
        yield fake

    app.dependency_overrides[get_redis] = _override
    try:
        yield fake
    finally:
        app.dependency_overrides.pop(get_redis, None)


# ── POST /engine/commands ────────────────────────────────────────────


async def test_post_halt_creates_command_and_pokes(
    auth_client: httpx.AsyncClient, fake_redis: FakeRedis, db_session: AsyncSession
) -> None:
    resp = await auth_client.post(
        f"{API}/commands", json={"action": "halt", "reason": "fat finger"}
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["seq"] == 1
    assert body["action"] == "halt"
    assert body["scope"] == "global"
    assert body["reason"] == "fat finger"
    assert body["actor"] == f"api:{TEST_EMAIL}"
    assert body["issued_at"] is not None
    assert body["applied_at"] is None
    assert body["result"] is None
    assert "id" in body

    # Row is durable and the poke went out on the engine channel.
    row = await db_session.scalar(select(EngineCommand).where(EngineCommand.seq == 1))
    assert row is not None
    assert [channel for channel, _ in fake_redis.published] == [CHANNEL_ENGINE_COMMANDS]


async def test_post_resume_refused_409_after_unverified_flatten(
    auth_client: httpx.AsyncClient, fake_redis: FakeRedis
) -> None:
    flatten = await auth_client.post(
        f"{API}/commands", json={"action": "flatten", "reason": "get me out"}
    )
    assert flatten.status_code == 201, flatten.text

    resume = await auth_client.post(f"{API}/commands", json={"action": "resume"})
    assert resume.status_code == 409, resume.text
    assert "flat_verified" in resume.json()["detail"]
    # Only the flatten poked the engine; the refused resume did not.
    assert len(fake_redis.published) == 1


async def test_post_resume_allowed_after_flat_verified(
    auth_client: httpx.AsyncClient, fake_redis: FakeRedis, db_session: AsyncSession
) -> None:
    flatten = await auth_client.post(
        f"{API}/commands", json={"action": "flatten", "reason": "get me out"}
    )
    assert flatten.status_code == 201
    row = await db_session.scalar(
        select(EngineCommand).where(EngineCommand.seq == flatten.json()["seq"])
    )
    assert row is not None
    row.result = RESULT_FLAT_VERIFIED
    await db_session.commit()

    resume = await auth_client.post(f"{API}/commands", json={"action": "resume"})
    assert resume.status_code == 201, resume.text
    assert resume.json()["reason"] == "resume"  # blank reason defaults for resume


async def test_post_blank_reason_422(auth_client: httpx.AsyncClient, fake_redis: FakeRedis) -> None:
    resp = await auth_client.post(f"{API}/commands", json={"action": "halt", "reason": "   "})
    assert resp.status_code == 422, resp.text
    assert "reason" in resp.json()["detail"]
    assert fake_redis.published == []


async def test_post_invalid_action_422(
    auth_client: httpx.AsyncClient, fake_redis: FakeRedis
) -> None:
    resp = await auth_client.post(
        f"{API}/commands", json={"action": "self-destruct", "reason": "no"}
    )
    assert resp.status_code == 422


# ── GET /engine/commands ─────────────────────────────────────────────


async def test_get_commands_newest_first_with_limit(
    auth_client: httpx.AsyncClient, fake_redis: FakeRedis
) -> None:
    for reason in ("one", "two", "three"):
        resp = await auth_client.post(f"{API}/commands", json={"action": "halt", "reason": reason})
        assert resp.status_code == 201

    resp = await auth_client.get(f"{API}/commands", params={"limit": 2})
    assert resp.status_code == 200, resp.text
    rows = resp.json()
    assert [r["reason"] for r in rows] == ["three", "two"]  # newest first
    assert rows[0]["seq"] > rows[1]["seq"]


async def test_get_commands_default_limit_and_empty(
    auth_client: httpx.AsyncClient, fake_redis: FakeRedis
) -> None:
    resp = await auth_client.get(f"{API}/commands")
    assert resp.status_code == 200
    assert resp.json() == []


async def test_get_commands_limit_capped_at_100(
    auth_client: httpx.AsyncClient, fake_redis: FakeRedis
) -> None:
    resp = await auth_client.get(f"{API}/commands", params={"limit": 500})
    assert resp.status_code == 422


# ── GET /engine/status ───────────────────────────────────────────────


async def test_status_armed_engine_down(
    auth_client: httpx.AsyncClient, fake_redis: FakeRedis
) -> None:
    resp = await auth_client.get(f"{API}/status")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["halted"] is False
    assert body["flattening"] is False
    assert body["latest_command"] is None
    assert body["engine_alive"] is False
    assert body["engine_tasks"] is None


async def test_status_flattening_with_heartbeat_task_health(
    auth_client: httpx.AsyncClient, fake_redis: FakeRedis
) -> None:
    flatten = await auth_client.post(
        f"{API}/commands", json={"action": "flatten", "reason": "get me out"}
    )
    assert flatten.status_code == 201
    fake_redis.values[_status_heartbeat_key()] = json.dumps(
        {
            "timestamp": "2026-07-09T14:00:00+00:00",
            "status": "running",
            "tasks": {"event_bus": True, "flatten_controller": False},
        }
    )

    resp = await auth_client.get(f"{API}/status")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["halted"] is True
    assert body["flattening"] is True
    assert body["latest_command"]["action"] == "flatten"
    assert body["latest_command"]["result"] is None  # still in flight
    assert body["engine_alive"] is True
    assert body["engine_heartbeat_at"] == "2026-07-09T14:00:00+00:00"
    assert body["engine_tasks"] == {"event_bus": True, "flatten_controller": False}


async def test_status_flattening_false_after_flat_verified(
    auth_client: httpx.AsyncClient, fake_redis: FakeRedis, db_session: AsyncSession
) -> None:
    """Once the controller writes flat_verified, /engine/status must report the
    drive as done — halted-only — not "flattening" forever (result-aware
    derivation, shared with the engine's own KillSwitch)."""
    flatten = await auth_client.post(
        f"{API}/commands", json={"action": "flatten", "reason": "get me out"}
    )
    assert flatten.status_code == 201
    row = await db_session.scalar(
        select(EngineCommand).where(EngineCommand.seq == flatten.json()["seq"])
    )
    assert row is not None
    row.result = RESULT_FLAT_VERIFIED
    await db_session.commit()

    resp = await auth_client.get(f"{API}/status")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["halted"] is True
    assert body["flattening"] is False
    assert body["latest_command"]["result"] == RESULT_FLAT_VERIFIED


# ── Auth gate ────────────────────────────────────────────────────────


async def test_engine_endpoints_require_auth(
    app_client: httpx.AsyncClient, fake_redis: FakeRedis
) -> None:
    # No require_user override → the real auth dependency rejects the request.
    post = await app_client.post(f"{API}/commands", json={"action": "halt", "reason": "x"})
    assert post.status_code in (401, 403)
    listing = await app_client.get(f"{API}/commands")
    assert listing.status_code in (401, 403)
    status_resp = await app_client.get(f"{API}/status")
    assert status_resp.status_code in (401, 403)
