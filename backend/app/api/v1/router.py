"""API v1 router. Feature routers get included here as phases land."""

from fastapi import APIRouter

from app.api.v1.endpoints.auth import router as auth_router
from app.api.v1.endpoints.portfolios import router as portfolios_router

api_router = APIRouter()
api_router.include_router(auth_router, prefix="/auth", tags=["auth"])
api_router.include_router(portfolios_router, prefix="/portfolios", tags=["portfolios"])
