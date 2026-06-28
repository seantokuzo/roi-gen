"""Settings parsing sanity checks against the .env.example contract."""

import os
from decimal import Decimal

import pytest
from pydantic import ValidationError

from app.core.config import Settings, get_settings


@pytest.fixture(autouse=True)
def _clean_settings_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Default-asserting tests must not see ambient env (CI exports DATABASE_URL)."""
    for key in list(os.environ):
        if key.lower() in Settings.model_fields:
            monkeypatch.delenv(key, raising=False)


def test_defaults_match_env_contract() -> None:
    settings = Settings(_env_file=None)

    assert settings.debug is True
    assert settings.allowed_email == "seantokuzo@gmail.com"
    assert settings.database_url == "postgresql+asyncpg://postgres:postgres@localhost:5432/roigen"
    assert settings.redis_url == "redis://localhost:6379/0"
    assert settings.alpaca_data_feed == "iex"
    assert settings.llm_provider_fast == "anthropic"
    assert settings.llm_provider_smart == "anthropic"
    assert settings.llm_provider_premium == "anthropic"


def test_risk_defaults_are_decimal() -> None:
    settings = Settings(_env_file=None)

    assert settings.risk_per_trade_pct == Decimal("0.75")
    assert settings.unproven_risk_per_trade_pct == Decimal("0.25")
    assert settings.max_risk_per_trade_pct == Decimal("2.0")
    assert settings.daily_loss_limit_pct == Decimal("2.0")
    assert settings.daily_loss_risk_multiple == Decimal("2.5")
    assert settings.max_consecutive_losses == 4
    assert settings.drawdown_halve_pct == Decimal("10.0")
    assert settings.drawdown_halt_pct == Decimal("15.0")
    assert settings.margin_headroom_factor == Decimal("0.85")
    assert settings.flatten_buffer_minutes == 5
    assert settings.symbol_cooldown_seconds == 60
    assert isinstance(settings.risk_per_trade_pct, Decimal)


def test_cors_origins_parses_comma_separated() -> None:
    settings = Settings(_env_file=None)
    assert settings.cors_origins_list == [
        "http://localhost:4300",
        "http://localhost:5173",
    ]

    custom = Settings(_env_file=None, cors_origins="http://a.example, http://b.example")
    assert custom.cors_origins_list == ["http://a.example", "http://b.example"]


def test_secret_key_dev_default_when_debug() -> None:
    settings = Settings(_env_file=None, debug=True, secret_key="")
    assert settings.secret_key  # dev default injected


def test_secret_key_required_when_not_debug() -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, debug=False, secret_key="")


def test_explicit_secret_key_wins() -> None:
    settings = Settings(_env_file=None, debug=False, secret_key="abc123")
    assert settings.secret_key == "abc123"


def test_alpaca_data_feed_rejects_unknown_value() -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, alpaca_data_feed="bloomberg")  # type: ignore[arg-type]


def test_get_settings_is_cached_singleton() -> None:
    assert get_settings() is get_settings()
