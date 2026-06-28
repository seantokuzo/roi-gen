"""Typed broker exception hierarchy.

The whole point of this hierarchy is *intent disambiguation*. The engine's
never-resubmit-on-ambiguous-timeout discipline (project CLAUDE.md, engine
patterns) depends on these being distinct types: a definitive
:class:`OrderRejected` is safe to treat as "never placed", whereas an
:class:`AmbiguousOrderState` means the order's fate is UNKNOWN and the caller
MUST reconcile against the broker by ``client_order_id`` rather than blindly
resubmit (which would risk a duplicate live order).
"""

from __future__ import annotations


class BrokerError(Exception):
    """Base for every broker-layer failure.

    Carries the originating HTTP ``status_code`` when one exists (``None`` for
    connection-level failures with no response).
    """

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code


class BrokerAuthError(BrokerError):
    """Credentials are bad / unauthorized (HTTP 401 or 403).

    Not retryable: the same keys will keep failing. Surface to the operator.
    """


class BrokerRateLimited(BrokerError):
    """Rate limit exceeded (HTTP 429).

    ``retry_after`` is the broker-advised back-off in seconds when supplied
    (Alpaca's trading API caps at 200 req/min — see project CLAUDE.md).
    """

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = 429,
        retry_after: float | None = None,
    ) -> None:
        super().__init__(message, status_code=status_code)
        self.retry_after = retry_after


class OrderRejected(BrokerError):
    """DEFINITIVE order-level rejection (HTTP 422/400).

    The broker received the request and refused it (bad symbol, insufficient
    buying power, malformed bracket, etc.). The order was NOT placed, so it is
    safe to treat as not-placed and surface the rejection — do not reconcile,
    there is nothing on the broker side to find.
    """


class BrokerUnavailable(BrokerError):
    """Transient broker / transport failure (HTTP 5xx or a connection error).

    Retryable: the request may not have reached the broker, or the broker is
    temporarily down. For *order submission* specifically, prefer raising
    :class:`AmbiguousOrderState` instead whenever the request may have been
    received — only use this for failures that provably never placed an order
    (e.g. a pre-flight connection refusal) or for non-mutating reads.
    """


class AmbiguousOrderState(BrokerError):
    """The order's fate is UNKNOWN — submit timed out or the connection dropped
    mid-flight, so the broker may or may not have accepted it.

    Callers MUST reconcile by looking the order up via its ``client_order_id``
    (which is persisted BEFORE submission) and adopt the broker's truth. They
    must NEVER blind-resubmit, because a resubmit after a silently-accepted
    order would create a duplicate live order. This is the single most
    dangerous failure mode in the system; treat it with paranoia.
    """


class CredentialsNotFound(BrokerError):
    """No broker-credential row exists for the requested portfolio.

    Raised by :func:`app.brokers.credentials.load_credentials` when a portfolio
    has not had its keys configured yet.
    """
