"""Account-state API: live broker snapshots + on-demand reconciliation.

Every endpoint is auth-gated and ownership-enforced — the portfolio must
belong to the caller, or it is a 404 (indistinguishable from non-existent,
exactly like :mod:`app.api.v1.endpoints.portfolios`). An adapter is resolved
per request via the injected :class:`BrokerAdapterFactory` and used inside an
``async with`` block so its transport is always closed.

These are all READS plus a read-only reconcile. No endpoint here submits or
cancels an order (iron law #1 — order mutation lives behind the execution
handler + risk engine). Broker exceptions are mapped to honest HTTP statuses
so a bad key or a broker outage never surfaces as an opaque 500.
"""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from fastapi import APIRouter, HTTPException, Query, status
from sqlalchemy import select

from app.api.v1.deps import BrokerFactory, CurrentUser, DbSession
from app.brokers.errors import (
    AmbiguousOrderState,
    BrokerAuthError,
    BrokerError,
    BrokerRateLimited,
    BrokerUnavailable,
    CredentialsNotFound,
)
from app.core.logging import get_logger
from app.models import Portfolio, User
from app.schemas.account import AccountOut, OrderOut, PositionOut, ReconcileResultOut
from app.services.reconciliation import ReconciliationService

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from sqlalchemy.ext.asyncio import AsyncSession

    from app.brokers.base import BrokerAdapter

log = get_logger("account")

router = APIRouter()


async def _assert_owned(db: AsyncSession, user: User, portfolio_id: uuid.UUID) -> None:
    """404 unless ``portfolio_id`` belongs to ``user`` (ownership at the SQL level)."""
    owned = await db.scalar(
        select(Portfolio.id).where(Portfolio.id == portfolio_id, Portfolio.user_id == user.id)
    )
    if owned is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Portfolio not found")


def _broker_http_error(exc: BrokerError) -> HTTPException:
    """Map a broker exception to its honest HTTP status (never a raw 500)."""
    if isinstance(exc, CredentialsNotFound):
        # The portfolio simply has no keys configured yet.
        return HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Portfolio has no broker credentials configured",
        )
    if isinstance(exc, BrokerAuthError):
        return HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Broker rejected the portfolio's credentials",
        )
    if isinstance(exc, BrokerRateLimited):
        headers = (
            {"Retry-After": str(int(exc.retry_after))} if exc.retry_after is not None else None
        )
        return HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Broker rate limit exceeded; retry shortly",
            headers=headers,
        )
    if isinstance(exc, BrokerUnavailable | AmbiguousOrderState):
        return HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Broker is temporarily unavailable; retry shortly",
        )
    # Any other BrokerError (e.g. OrderRejected — not expected on these read
    # paths) still maps to a deliberate 502 rather than leaking as a 500.
    return HTTPException(
        status_code=status.HTTP_502_BAD_GATEWAY,
        detail="Broker request failed",
    )


@asynccontextmanager
async def _adapter_for(
    factory: BrokerFactory,
    db: AsyncSession,
    portfolio_id: uuid.UUID,
) -> AsyncIterator[BrokerAdapter]:
    """Resolve and enter an adapter for ``portfolio_id``, mapping broker errors.

    Credential lookup (which can raise :class:`CredentialsNotFound`) happens
    here, so a portfolio without keys 409s before any transport is opened.
    """
    try:
        adapter = await factory.get_adapter_for_portfolio(db, portfolio_id)
    except BrokerError as exc:
        raise _broker_http_error(exc) from exc
    async with adapter:
        yield adapter


async def _call_broker[T](op: Callable[[], Awaitable[T]]) -> T:
    """Run a broker read, translating broker exceptions into HTTP errors."""
    try:
        return await op()
    except BrokerError as exc:
        raise _broker_http_error(exc) from exc


@router.get("/{portfolio_id}/account")
async def get_account(
    portfolio_id: uuid.UUID, user: CurrentUser, db: DbSession, factory: BrokerFactory
) -> AccountOut:
    """Live broker account snapshot for the portfolio (no PDT fields)."""
    await _assert_owned(db, user, portfolio_id)
    async with _adapter_for(factory, db, portfolio_id) as adapter:
        account = await _call_broker(adapter.get_account)
    return AccountOut.from_broker(account)


@router.get("/{portfolio_id}/positions")
async def list_positions(
    portfolio_id: uuid.UUID, user: CurrentUser, db: DbSession, factory: BrokerFactory
) -> list[PositionOut]:
    """Live open positions for the portfolio."""
    await _assert_owned(db, user, portfolio_id)
    async with _adapter_for(factory, db, portfolio_id) as adapter:
        positions = await _call_broker(adapter.list_positions)
    return [PositionOut.from_broker(p) for p in positions]


@router.get("/{portfolio_id}/orders")
async def list_orders(
    portfolio_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
    factory: BrokerFactory,
    status_filter: str = Query("open", alias="status"),
) -> list[OrderOut]:
    """Live orders for the portfolio, filtered by ``status`` (default ``open``)."""
    await _assert_owned(db, user, portfolio_id)
    async with _adapter_for(factory, db, portfolio_id) as adapter:
        orders = await _call_broker(lambda: adapter.list_orders(status=status_filter))
    return [OrderOut.from_broker(o) for o in orders]


@router.post("/{portfolio_id}/sync")
async def sync_portfolio(
    portfolio_id: uuid.UUID, user: CurrentUser, db: DbSession, factory: BrokerFactory
) -> ReconcileResultOut:
    """Reconcile local state against the broker (read/diff/persist; never mutates orders)."""
    await _assert_owned(db, user, portfolio_id)
    service = ReconciliationService()
    async with _adapter_for(factory, db, portfolio_id) as adapter:
        try:
            result = await service.reconcile_portfolio(db, portfolio_id, adapter)
        except BrokerError as exc:
            await db.rollback()
            raise _broker_http_error(exc) from exc
    # Reconciliation stages all writes on the session; this endpoint owns the
    # transaction (repo convention: get_db does not commit).
    await db.commit()
    return ReconcileResultOut.from_result(result)
