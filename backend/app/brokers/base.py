"""The broker-agnostic adapter contract.

:class:`BrokerAdapter` is the single seam between the trading system and any
broker. Every later layer (execution handler, reconciliation, account API)
depends on THIS interface, never on a concrete SDK — so a second broker, or a
simulated fill engine, drops in by implementing these methods.

One adapter instance is bound to exactly one account: it owns that account's
credentials and the paper-vs-live base URL, and is an async context manager so
its transport (httpx client, etc.) is deterministically closed.

Iron law #1 lives one layer up: only the execution handler may call the
order-mutating methods here, and only with a risk-engine approval token. This
contract does not (and must not) encode that policy — it just exposes the
capability the execution handler gates.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date, datetime
from decimal import Decimal
from types import TracebackType
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from app.brokers.dto import (
    BrokerAccount,
    BrokerOrder,
    BrokerPosition,
    CalendarDay,
    MarketClock,
    OrderRequest,
)

if TYPE_CHECKING:
    import uuid

    from sqlalchemy.ext.asyncio import AsyncSession


class BrokerAdapter(ABC):
    """Abstract broker adapter bound to a single account.

    Subclasses implement every abstract method against a concrete broker and
    own a transport that :meth:`aclose` releases. Use as an async context
    manager::

        async with adapter:
            account = await adapter.get_account()
    """

    async def __aenter__(self) -> BrokerAdapter:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    # ── Market calendar / clock ──────────────────────────────────────

    @abstractmethod
    async def get_clock(self) -> MarketClock:
        """Return the current market clock (open state + next open/close)."""

    @abstractmethod
    async def get_calendar(self, start: date, end: date) -> list[CalendarDay]:
        """Return trading sessions in ``[start, end]`` (inclusive)."""

    # ── Account / positions ──────────────────────────────────────────

    @abstractmethod
    async def get_account(self) -> BrokerAccount:
        """Return the account snapshot (equity, buying power, blocks — no PDT)."""

    @abstractmethod
    async def list_positions(self) -> list[BrokerPosition]:
        """Return all open positions for the account."""

    @abstractmethod
    async def get_position(self, symbol: str) -> BrokerPosition | None:
        """Return the open position for ``symbol``, or ``None`` if flat."""

    # ── Orders (reads) ───────────────────────────────────────────────

    @abstractmethod
    async def list_orders(
        self,
        *,
        status: str = "open",
        after: datetime | None = None,
        until: datetime | None = None,
        limit: int = 100,
        nested: bool = True,
    ) -> list[BrokerOrder]:
        """List orders filtered by ``status`` (``open`` / ``closed`` / ``all``).

        ``nested=True`` requests bracket/OCO children inlined as ``legs``.
        ``after`` / ``until`` bound submission time (tz-aware UTC).
        """

    @abstractmethod
    async def get_order_by_client_id(self, client_order_id: str) -> BrokerOrder | None:
        """Look up an order by ``client_order_id`` — the reconciliation key.

        This is the method callers use after an :class:`AmbiguousOrderState` to
        discover whether a submit actually landed.
        """

    @abstractmethod
    async def get_order(self, broker_order_id: str) -> BrokerOrder | None:
        """Look up an order by the broker's own id, or ``None`` if not found."""

    # ── Orders (mutations — execution-handler only, iron law #1) ─────

    @abstractmethod
    async def submit_order(self, req: OrderRequest) -> BrokerOrder:
        """Submit ``req`` and return the broker's accepted order.

        The caller MUST have persisted ``req.client_order_id`` before calling.
        On a definitive broker refusal this raises
        :class:`~app.brokers.errors.OrderRejected`. On an ambiguous failure
        (timeout / dropped connection mid-submit) it raises
        :class:`~app.brokers.errors.AmbiguousOrderState`; the caller then
        reconciles via :meth:`get_order_by_client_id` and NEVER blind-resubmits.
        """

    @abstractmethod
    async def cancel_order(self, broker_order_id: str) -> None:
        """Request cancellation of a single order (idempotent best-effort)."""

    @abstractmethod
    async def cancel_all_orders(self) -> None:
        """Request cancellation of every open order for the account."""

    @abstractmethod
    async def close_position(
        self,
        symbol: str,
        *,
        qty: Decimal | None = None,
        percentage: Decimal | None = None,
    ) -> BrokerOrder:
        """Liquidate ``symbol`` via a market order and return that order.

        Pass at most one of ``qty`` (absolute shares) or ``percentage``
        (fraction of the position); omit both to close the whole position.
        """

    @abstractmethod
    async def close_all_positions(self, *, cancel_orders: bool = True) -> None:
        """Liquidate every position; also cancel open orders when requested."""

    # ── Lifecycle ────────────────────────────────────────────────────

    @abstractmethod
    async def aclose(self) -> None:
        """Release the underlying transport. Safe to call more than once."""


@runtime_checkable
class BrokerAdapterFactory(Protocol):
    """Resolves a per-portfolio :class:`BrokerAdapter`.

    Stage 2c implements this against the DB (load + decrypt the portfolio's
    credentials, then construct the concrete adapter). Defined as a Protocol so
    callers depend on the abstraction without importing the implementation —
    and the ``session`` is typed loosely to avoid a hard ORM import cycle here.
    """

    async def get_adapter_for_portfolio(
        self, session: AsyncSession | Any, portfolio_id: uuid.UUID
    ) -> BrokerAdapter:
        """Return a ready adapter bound to ``portfolio_id``'s account."""
        ...
