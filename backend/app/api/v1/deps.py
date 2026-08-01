"""Shared API dependencies.

``require_user`` is a thin delegate around
:func:`app.core.security.get_current_user` so tests can override a single,
stable dependency object without touching the auth module.
"""

from collections.abc import AsyncGenerator
from typing import Annotated

import redis.asyncio as aioredis
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.brokers.base import BrokerAdapterFactory
from app.core.config import get_settings
from app.core.database import get_db
from app.core.security import get_current_user
from app.models import User

_REDIS_TIMEOUT_SECONDS = 5.0


async def require_user(user: Annotated[User, Depends(get_current_user)]) -> User:
    """Resolve the authenticated user (401 on failure, raised by the delegate)."""
    return user


async def get_redis() -> AsyncGenerator[aioredis.Redis, None]:
    """Yield a per-request Redis client, closed after the response.

    Ephemeral by design (mirroring the ``/health`` check in
    :mod:`app.main`): the API's Redis use is a rare single-shot poke or key
    read, not a hot path worth a shared client's lifecycle management. Tests
    override this dependency with a fake.
    """
    client = aioredis.Redis.from_url(
        get_settings().redis_url,
        socket_connect_timeout=_REDIS_TIMEOUT_SECONDS,
        socket_timeout=_REDIS_TIMEOUT_SECONDS,
    )
    try:
        yield client
    finally:
        await client.aclose()


def get_broker_factory() -> BrokerAdapterFactory:
    """Provide the broker-adapter factory (the DB-backed Alpaca resolver).

    The concrete ``AlpacaAdapterFactory`` is imported LAZILY (function body, not
    module top) for two reasons: (1) tests override this dependency with a fake
    factory and must import this module cleanly even if the sibling Alpaca module
    isn't present yet during parallel development; (2) it keeps the heavy
    vendor-SDK import off the API import path until a request actually needs an
    adapter. Callers depend on the :class:`BrokerAdapterFactory` Protocol, never
    on the concrete class.
    """
    from app.brokers.alpaca.factory import AlpacaAdapterFactory

    return AlpacaAdapterFactory()


CurrentUser = Annotated[User, Depends(require_user)]
DbSession = Annotated[AsyncSession, Depends(get_db)]
BrokerFactory = Annotated[BrokerAdapterFactory, Depends(get_broker_factory)]
RedisClient = Annotated[aioredis.Redis, Depends(get_redis)]
