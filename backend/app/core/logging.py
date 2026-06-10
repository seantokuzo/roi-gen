"""structlog configuration: JSON in prod, pretty console when DEBUG."""

import logging
import sys

import structlog


def setup_logging(*, debug: bool = False) -> None:
    """Configure structlog and the stdlib root logger.

    DEBUG → human-friendly colored console output at DEBUG level.
    Otherwise → machine-parseable JSON at INFO level.
    """
    level = logging.DEBUG if debug else logging.INFO

    shared_processors: list[structlog.typing.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.TimeStamper(fmt="iso", utc=True),
    ]

    renderer: structlog.typing.Processor
    if debug:
        renderer = structlog.dev.ConsoleRenderer()
    else:
        shared_processors.append(structlog.processors.format_exc_info)
        renderer = structlog.processors.JSONRenderer()

    structlog.configure(
        processors=[*shared_processors, renderer],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        logger_factory=structlog.PrintLoggerFactory(sys.stdout),
        cache_logger_on_first_use=True,
    )

    # Keep stdlib loggers (uvicorn, sqlalchemy, alembic) at the same level.
    logging.basicConfig(level=level, stream=sys.stdout, format="%(message)s")


def get_logger(name: str) -> structlog.typing.FilteringBoundLogger:
    """Convenience accessor for a named structlog logger."""
    logger: structlog.typing.FilteringBoundLogger = structlog.get_logger(name)
    return logger
