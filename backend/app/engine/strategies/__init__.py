"""Concrete strategy implementations.

Importing this package IS the registration step: each strategy module
decorates its class with ``@registry.register(kind)`` against the
process-wide default registry (:data:`app.engine.strategy.registry`), so a
single ``import app.engine.strategies`` — engine_main does it once at boot —
makes every kind here loadable by
:func:`app.engine.loader.load_active_strategies`.
"""

from app.engine.strategies.probe import ProbeStrategy

__all__ = ["ProbeStrategy"]
