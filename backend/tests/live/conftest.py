"""Env bootstrap for the live-paper suite: repo-root ``.env`` → ``os.environ``.

Why this file exists. :class:`app.core.config.Settings` declares
``SettingsConfigDict(env_file=".env")``, and pydantic-settings resolves that
path RELATIVE TO THE PROCESS CWD. The runbook runs pytest from ``backend/``,
where no ``.env`` exists — the real one lives at the repo root — so settings
silently fall back to their defaults. Worse, the live test reads
``os.environ["ALPACA_API_KEY"]`` directly in its env gate, and pytest populates
no dotenv at all. Without this bootstrap the documented command skips with
"ALPACA_API_KEY / ALPACA_SECRET_KEY not set" even mid-session, which reads like
a missing key rather than a missing loader.

Scope and safety, in order of how badly each could bite:

* **Gated on ``ROIGEN_LIVE_E2E=1``.** pytest imports every conftest along the
  collected path, and marker deselection (``addopts = "-m 'not live_paper'"``)
  happens AFTER collection — so this module IS imported during a plain
  ``pytest`` run. The gate is what guarantees the normal suite's environment is
  left exactly as it was found.
* **Import-time, not fixture-time.** ``tests/live/test_paper_e2e.py`` reads
  ``ROIGEN_LIVE_REDIS_URL`` at module import, so a fixture would be too late.
  conftest import precedes test-module import.
* **Never overrides an exported variable.** ``ALPACA_API_KEY=... uv run pytest
  ...`` beats the file, always: explicit env wins.
* **Stdlib parser, not ``python-dotenv``.** dotenv is only a TRANSITIVE
  dependency here (pydantic-settings pulls it in); depending on it directly
  would make the live suite break the day that edge is dropped or pinned out,
  and adding it to the dev group buys nothing over ~10 lines of parsing for a
  file this project already owns the format of.

The engine and CLI subprocesses the test spawns inherit all of this for free:
the test builds ``child_env`` as ``{**os.environ, ...}``.
"""

from __future__ import annotations

import os
from pathlib import Path

# backend/tests/live/conftest.py → backend/tests → backend → <repo root>
_REPO_ROOT_ENV = Path(__file__).resolve().parents[3] / ".env"


def _parse_env_file(path: Path) -> dict[str, str]:
    """Minimal ``KEY=VALUE`` reader: skip blanks/comments, strip one quote pair.

    Deliberately does NOT do variable interpolation, multi-line values, or
    inline-comment stripping — the repo ``.env`` uses none of them, and a
    loader that guesses is worse than one that is boring.
    """
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip().removeprefix("export ").strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        if key:
            values[key] = value
    return values


def _load_repo_root_env() -> None:
    """Populate missing env vars from the repo-root ``.env`` — live runs only."""
    if os.environ.get("ROIGEN_LIVE_E2E") != "1":
        return  # a plain `pytest` collects this file too; leave its env alone
    if not _REPO_ROOT_ENV.is_file():
        return  # the test's own env gate reports the resulting miss
    for key, value in _parse_env_file(_REPO_ROOT_ENV).items():
        os.environ.setdefault(key, value)  # explicit export wins

    # `Settings` is lru_cached and `tests/conftest.py` is imported BEFORE this
    # file, so anything that primed the cache did so against the pre-load
    # environment. Drop it, or the `*_test` database this suite migrates could
    # be derived from the default DATABASE_URL rather than the one .env names.
    from app.core.config import get_settings

    get_settings.cache_clear()


_load_repo_root_env()
