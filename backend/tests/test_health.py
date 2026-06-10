"""Health endpoint contract: 200 with connectivity flags even when deps are down."""

import httpx

from app import __version__
from app.main import app


async def test_health_returns_200_with_connectivity_flags() -> None:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/health")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] in {"ok", "degraded"}
    assert body["version"] == __version__
    assert isinstance(body["database"], bool)
    assert isinstance(body["redis"], bool)


async def test_health_status_matches_connectivity() -> None:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/health")

    body = response.json()
    expected = "ok" if (body["database"] and body["redis"]) else "degraded"
    assert body["status"] == expected
