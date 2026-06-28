"""Construct Alpaca adapters — from raw credentials or from a portfolio row.

:func:`build_alpaca_adapter` is the pure constructor (credentials in, adapter
out). :class:`AlpacaAdapterFactory` is the DB-aware resolver the API layer will
inject: it loads + decrypts a portfolio's credentials and builds the adapter,
satisfying the :class:`~app.brokers.base.BrokerAdapterFactory` Protocol so
callers depend on the abstraction, not this concrete class.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from app.brokers.alpaca.rest import AlpacaBrokerAdapter
from app.brokers.credentials import BrokerCredentials, load_credentials

if TYPE_CHECKING:
    import uuid

    from sqlalchemy.ext.asyncio import AsyncSession


def build_alpaca_adapter(creds: BrokerCredentials) -> AlpacaBrokerAdapter:
    """Build an :class:`AlpacaBrokerAdapter` from decrypted credentials."""
    return AlpacaBrokerAdapter(creds)


class AlpacaAdapterFactory:
    """DB-backed :class:`~app.brokers.base.BrokerAdapterFactory` for Alpaca.

    Stateless: every call loads the portfolio's current (decrypted) credentials
    and returns a freshly-bound adapter the caller owns and must ``aclose``.
    """

    async def get_adapter_for_portfolio(
        self, session: AsyncSession | Any, portfolio_id: uuid.UUID
    ) -> AlpacaBrokerAdapter:
        """Resolve and build the adapter bound to ``portfolio_id``'s account.

        Raises :class:`~app.brokers.errors.CredentialsNotFound` (via
        :func:`load_credentials`) when the portfolio has no credential row.
        """
        creds = await load_credentials(session, portfolio_id)
        return build_alpaca_adapter(creds)
