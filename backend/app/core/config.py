"""Application settings.

Maps every variable in the repo-root ``.env.example`` (the env contract).
Money-adjacent percentages are ``Decimal`` per project iron law #7.
"""

from decimal import Decimal
from functools import lru_cache
from typing import Literal, Self

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_DEV_SECRET_KEY = "dev-only-secret-key-do-not-use-in-prod"  # noqa: S105


class Settings(BaseSettings):
    """Typed view of the environment contract (.env.example)."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ── Application ─────────────────────────────────────────────
    debug: bool = True
    secret_key: str = ""  # required when DEBUG=false; dev default injected below
    allowed_email: str = "seantokuzo@gmail.com"

    # ── Database / cache ─────────────────────────────────────────
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/roigen"
    redis_url: str = "redis://localhost:6379/0"

    # ── Auth (Google OAuth → JWT) ────────────────────────────────
    google_client_id: str = ""
    google_client_secret: str = ""

    # ── Alpaca (paper keys; live keys are encrypted DB records) ──
    alpaca_api_key: str = ""
    alpaca_secret_key: str = ""
    alpaca_data_feed: Literal["iex", "sip"] = "iex"

    # ── LLM providers ────────────────────────────────────────────
    anthropic_api_key: str = ""
    gemini_api_key: str = ""
    cohere_api_key: str = ""
    llm_provider_fast: str = "anthropic"
    llm_provider_smart: str = "anthropic"
    llm_provider_premium: str = "anthropic"

    # ── Data sources ─────────────────────────────────────────────
    finnhub_api_key: str = ""
    fred_api_key: str = ""

    # ── Global risk defaults (per-strategy overrides live in DB) ─
    risk_per_trade_pct: Decimal = Decimal("0.75")
    daily_loss_limit_pct: Decimal = Decimal("2.0")
    max_consecutive_losses: int = 4
    drawdown_halve_pct: Decimal = Decimal("10.0")
    drawdown_halt_pct: Decimal = Decimal("15.0")
    margin_headroom_factor: Decimal = Decimal("0.85")

    # ── CORS / frontend ──────────────────────────────────────────
    cors_origins: str = "http://localhost:4300,http://localhost:5173"

    @property
    def cors_origins_list(self) -> list[str]:
        """CORS_ORIGINS split into a list (comma-separated in env)."""
        return [origin.strip() for origin in self.cors_origins.split(",") if origin.strip()]

    @model_validator(mode="after")
    def _require_secret_key_in_prod(self) -> Self:
        if not self.secret_key:
            if not self.debug:
                msg = "SECRET_KEY must be set when DEBUG=false (generate: openssl rand -hex 32)"
                raise ValueError(msg)
            self.secret_key = _DEV_SECRET_KEY
        return self


@lru_cache
def get_settings() -> Settings:
    """Singleton settings accessor."""
    return Settings()
