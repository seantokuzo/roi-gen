"""Portfolio + encrypted broker-credential endpoints.

Every query filters on ``user_id`` — ownership is enforced at the SQL level,
so another user's portfolio id is indistinguishable from a missing one (404).
Credentials are Fernet-encrypted before they touch the DB and never echoed
back; clients only ever see the ``has_credentials`` flag.
"""

import uuid

from fastapi import APIRouter, HTTPException, Response, status
from sqlalchemy import exists, func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.api.v1.deps import CurrentUser, DbSession
from app.core.config import get_settings
from app.core.logging import get_logger
from app.models import BrokerCredential, Order, Portfolio, PortfolioMode, Strategy, User
from app.schemas.portfolio import (
    CredentialsIn,
    PortfolioCreate,
    PortfolioOut,
    PortfolioUpdate,
)
from app.services.crypto import encrypt_str

log = get_logger("portfolios")

router = APIRouter()

BOOTSTRAP_PORTFOLIO_NAME = "Primary Paper"


def _to_out(portfolio: Portfolio, *, has_credentials: bool | None = None) -> PortfolioOut:
    """Build the public view; ``has_credentials`` falls back to the loaded relationship."""
    if has_credentials is None:
        has_credentials = portfolio.broker_credential is not None
    return PortfolioOut(
        id=portfolio.id,
        name=portfolio.name,
        mode=PortfolioMode(portfolio.mode),
        description=portfolio.description,
        is_default=portfolio.is_default,
        has_credentials=has_credentials,
        created_at=portfolio.created_at,
    )


async def _get_owned_portfolio(db: AsyncSession, user: User, portfolio_id: uuid.UUID) -> Portfolio:
    """Fetch the user's portfolio (credential relationship eager-loaded) or 404."""
    portfolio = (
        await db.execute(
            select(Portfolio)
            .where(Portfolio.id == portfolio_id, Portfolio.user_id == user.id)
            .options(selectinload(Portfolio.broker_credential))
        )
    ).scalar_one_or_none()
    if portfolio is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Portfolio not found")
    return portfolio


@router.post("/", status_code=status.HTTP_201_CREATED)
async def create_portfolio(
    payload: PortfolioCreate, user: CurrentUser, db: DbSession
) -> PortfolioOut:
    """Create a portfolio. The user's first portfolio becomes the default."""
    existing_count = await db.scalar(
        select(func.count()).select_from(Portfolio).where(Portfolio.user_id == user.id)
    )
    if payload.mode is PortfolioMode.live:
        # Phase 9 adds live-credential gates + paper→live promotion checks.
        log.warning(
            "portfolio.live_mode_created",
            user_id=str(user.id),
            portfolio_name=payload.name,
            note="LIVE portfolio created before Phase 9 promotion gates — paper-test first",
        )
    portfolio = Portfolio(
        user_id=user.id,
        name=payload.name,
        mode=payload.mode,
        description=payload.description,
        is_default=existing_count == 0,
    )
    db.add(portfolio)
    try:
        await db.commit()
    except IntegrityError as exc:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"A portfolio named {payload.name!r} already exists",
        ) from exc
    await db.refresh(portfolio, attribute_names=["created_at", "updated_at"])
    return _to_out(portfolio, has_credentials=False)


@router.get("/")
async def list_portfolios(user: CurrentUser, db: DbSession) -> list[PortfolioOut]:
    """List the current user's portfolios, oldest first."""
    portfolios = (
        await db.scalars(
            select(Portfolio)
            .where(Portfolio.user_id == user.id)
            .options(selectinload(Portfolio.broker_credential))
            .order_by(Portfolio.created_at)
        )
    ).all()
    return [_to_out(p) for p in portfolios]


@router.post("/bootstrap", status_code=status.HTTP_201_CREATED)
async def bootstrap_portfolio(user: CurrentUser, db: DbSession, response: Response) -> PortfolioOut:
    """Idempotent first-run setup.

    No portfolios + env Alpaca paper keys → create the default "Primary Paper"
    portfolio with encrypted env credentials (201). Portfolios already exist →
    return the default one (200). Neither → 422.
    """
    portfolios = (
        await db.scalars(
            select(Portfolio)
            .where(Portfolio.user_id == user.id)
            .options(selectinload(Portfolio.broker_credential))
            .order_by(Portfolio.created_at)
        )
    ).all()
    if portfolios:
        response.status_code = status.HTTP_200_OK
        default = next((p for p in portfolios if p.is_default), portfolios[0])
        return _to_out(default)

    settings = get_settings()
    if not (settings.alpaca_api_key and settings.alpaca_secret_key):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                "Cannot bootstrap: no portfolios exist and ALPACA_API_KEY / "
                "ALPACA_SECRET_KEY are not configured in the environment. "
                "Set the env keys or create a portfolio manually."
            ),
        )

    portfolio = Portfolio(
        user_id=user.id,
        name=BOOTSTRAP_PORTFOLIO_NAME,
        mode=PortfolioMode.paper,
        description="Auto-created from environment Alpaca paper keys",
        is_default=True,
    )
    portfolio.broker_credential = BrokerCredential(
        api_key_encrypted=encrypt_str(settings.alpaca_api_key),
        api_secret_encrypted=encrypt_str(settings.alpaca_secret_key),
        paper=True,
    )
    db.add(portfolio)
    await db.commit()
    await db.refresh(portfolio, attribute_names=["created_at", "updated_at"])
    log.info("portfolio.bootstrapped", portfolio_id=str(portfolio.id))
    return _to_out(portfolio, has_credentials=True)


@router.get("/{portfolio_id}")
async def get_portfolio(portfolio_id: uuid.UUID, user: CurrentUser, db: DbSession) -> PortfolioOut:
    """Fetch one of the current user's portfolios."""
    return _to_out(await _get_owned_portfolio(db, user, portfolio_id))


@router.patch("/{portfolio_id}")
async def update_portfolio(
    portfolio_id: uuid.UUID, payload: PortfolioUpdate, user: CurrentUser, db: DbSession
) -> PortfolioOut:
    """Update name / description / is_default. Default flag is exclusive."""
    portfolio = await _get_owned_portfolio(db, user, portfolio_id)
    changes = payload.model_dump(exclude_unset=True)
    if changes.get("is_default") is True:
        # Single UPDATE clears the flag on all siblings — no read-modify-write.
        await db.execute(
            update(Portfolio)
            .where(Portfolio.user_id == user.id, Portfolio.id != portfolio.id)
            .values(is_default=False)
        )
    for field, value in changes.items():
        setattr(portfolio, field, value)
    try:
        await db.commit()
    except IntegrityError as exc:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Update violates a constraint (duplicate name?)",
        ) from exc
    return _to_out(portfolio)


@router.delete("/{portfolio_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_portfolio(portfolio_id: uuid.UUID, user: CurrentUser, db: DbSession) -> None:
    """Delete a portfolio. 409 if strategies/orders reference it; credentials cascade."""
    portfolio = await _get_owned_portfolio(db, user, portfolio_id)
    referenced = await db.scalar(
        select(
            exists().where(Strategy.portfolio_id == portfolio.id)
            | exists().where(Order.portfolio_id == portfolio.id)
        )
    )
    if referenced:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Portfolio has strategies or orders attached; remove them first",
        )
    await db.delete(portfolio)
    await db.commit()


@router.put("/{portfolio_id}/credentials")
async def put_credentials(
    portfolio_id: uuid.UUID, payload: CredentialsIn, user: CurrentUser, db: DbSession
) -> PortfolioOut:
    """Upsert the portfolio's broker credentials (encrypted at rest, never echoed)."""
    portfolio = await _get_owned_portfolio(db, user, portfolio_id)
    api_key_encrypted = encrypt_str(payload.api_key)
    api_secret_encrypted = encrypt_str(payload.api_secret)
    credential = portfolio.broker_credential
    if credential is None:
        portfolio.broker_credential = BrokerCredential(
            api_key_encrypted=api_key_encrypted,
            api_secret_encrypted=api_secret_encrypted,
            paper=payload.paper,
        )
    else:
        credential.api_key_encrypted = api_key_encrypted
        credential.api_secret_encrypted = api_secret_encrypted
        credential.paper = payload.paper
    await db.commit()
    log.info(
        "portfolio.credentials_upserted",
        portfolio_id=str(portfolio.id),
        paper=payload.paper,
    )
    return _to_out(portfolio, has_credentials=True)
