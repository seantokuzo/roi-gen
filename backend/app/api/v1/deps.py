"""Shared API dependencies.

``require_user`` is a thin delegate around
:func:`app.core.security.get_current_user` so tests can override a single,
stable dependency object without touching the auth module.
"""

from typing import Annotated

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import get_current_user
from app.models import User


async def require_user(user: Annotated[User, Depends(get_current_user)]) -> User:
    """Resolve the authenticated user (401 on failure, raised by the delegate)."""
    return user


CurrentUser = Annotated[User, Depends(require_user)]
DbSession = Annotated[AsyncSession, Depends(get_db)]
