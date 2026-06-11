"""Pydantic v2 schemas for portfolios and broker credentials.

Secrets only ever travel inbound (:class:`CredentialsIn`). No Out schema
carries key material — clients see only the ``has_credentials`` flag.
"""

import uuid
from datetime import datetime

from pydantic import BaseModel, Field

from app.models.enums import PortfolioMode


class PortfolioCreate(BaseModel):
    """Payload for creating a portfolio."""

    name: str = Field(min_length=1, max_length=100)
    mode: PortfolioMode
    description: str | None = None


class PortfolioUpdate(BaseModel):
    """Partial update — only provided fields are applied."""

    name: str | None = Field(default=None, min_length=1, max_length=100)
    description: str | None = None
    is_default: bool | None = None


class CredentialsIn(BaseModel):
    """Inbound broker credentials — encrypted before they touch the DB."""

    api_key: str = Field(min_length=1)
    api_secret: str = Field(min_length=1)
    paper: bool


class PortfolioOut(BaseModel):
    """Public portfolio view. Never includes secret material."""

    id: uuid.UUID
    name: str
    mode: PortfolioMode
    description: str | None
    is_default: bool
    has_credentials: bool
    created_at: datetime
    updated_at: datetime
