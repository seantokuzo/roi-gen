"""Per-portfolio broker credentials: load + decrypt.

This is the ONE module in the broker contract layer that reaches into
``app.models`` (the :class:`BrokerCredential` row) and ``app.services.crypto``
(to decrypt the keys at rest, iron law #9). Keeping that dependency surface
isolated here lets the rest of the contract stay pure and broker-agnostic.

The plaintext keys live only in the returned :class:`BrokerCredentials` value
object — never logged, never persisted, never round-tripped back to the DB.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict
from sqlalchemy import select

from app.brokers.errors import CredentialsNotFound
from app.models import BrokerCredential
from app.services.crypto import decrypt_str

if TYPE_CHECKING:
    import uuid

    from sqlalchemy.ext.asyncio import AsyncSession


class BrokerCredentials(BaseModel):
    """Decrypted broker credentials for one account (immutable).

    ``paper`` selects the paper vs. live base URL downstream; ``broker``
    identifies which concrete adapter to construct (default ``"alpaca"``).
    """

    model_config = ConfigDict(frozen=True)

    api_key: str
    api_secret: str
    paper: bool
    broker: str = "alpaca"


async def load_credentials(session: AsyncSession, portfolio_id: uuid.UUID) -> BrokerCredentials:
    """Load and decrypt the broker credentials for ``portfolio_id``.

    Raises :class:`~app.brokers.errors.CredentialsNotFound` when the portfolio
    has no credential row configured.
    """
    row = (
        await session.execute(
            select(BrokerCredential).where(BrokerCredential.portfolio_id == portfolio_id)
        )
    ).scalar_one_or_none()

    if row is None:
        msg = f"no broker credentials configured for portfolio {portfolio_id}"
        raise CredentialsNotFound(msg)

    return BrokerCredentials(
        api_key=decrypt_str(row.api_key_encrypted),
        api_secret=decrypt_str(row.api_secret_encrypted),
        paper=row.paper,
        broker=row.broker,
    )
