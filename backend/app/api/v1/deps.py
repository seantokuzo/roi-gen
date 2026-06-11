"""Shared API dependencies.

``require_user`` is a thin delegate around
:func:`app.core.security.get_current_user`, imported lazily at call time so
this module never hard-depends on the auth module at import time (it is built
in parallel) and tests can override a single, stable dependency object.
"""

from importlib import import_module
from typing import Annotated, cast

from fastapi import Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.models import User

_bearer_scheme = HTTPBearer(auto_error=False)


async def require_user(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer_scheme)],
    db: Annotated[AsyncSession, Depends(get_db)],
) -> User:
    """Resolve the authenticated user (401 on failure, raised by the delegate)."""
    security = import_module("app.core.security")
    return cast("User", await security.get_current_user(credentials=credentials, db=db))


CurrentUser = Annotated[User, Depends(require_user)]
DbSession = Annotated[AsyncSession, Depends(get_db)]
