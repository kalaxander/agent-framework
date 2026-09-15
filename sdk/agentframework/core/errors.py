"""Typed errors so callers can branch on failure kind, not string-match messages.

See docs/Rules.md > Error Handling.
"""


class AgentFrameworkError(Exception):
    """Base class for all framework errors."""

    retryable: bool = False


class FlowValidationError(AgentFrameworkError):
    """Raised when a Flow definition is invalid (e.g. missing dependency, cycle)."""

    retryable = False


class ToolError(AgentFrameworkError):
    """Raised when a tool/action call fails."""

    def __init__(self, message: str, *, retryable: bool = True):
        super().__init__(message)
        self.retryable = retryable


class TaskTimeoutError(AgentFrameworkError):
    """Raised when a task exceeds its configured timeout."""

    retryable = True


class GuardrailViolation(AgentFrameworkError):
    """Raised when a pre/post-execution guardrail rejects a task. Always fails closed."""

    retryable = False


class ApprovalRejected(AgentFrameworkError):
    """Raised when a human-in-the-loop task (Task(requires_approval=True)) is rejected via
    AsyncOrchestrator.resume(..., approved=False). Fails closed, same as a guardrail violation."""

    retryable = False


class MemoryError_(AgentFrameworkError):  # noqa: N818 (avoid shadowing builtin MemoryError)
    """Raised when a memory backend read/write fails."""

    retryable = True


class StateStoreError(AgentFrameworkError):
    """Raised when a StateStore backend (e.g. PostgresStateStore) hits a database-level error —
    connection failure, constraint violation, etc. Wraps the underlying driver exception so
    callers (and fastapi_ingress's global error handler) branch on this typed error instead of
    a raw asyncpg/db-driver exception leaking out of the state store's interface boundary.

    `retryable` is set per-instance (not per-class) because some database errors are transient
    (connection drops -> retryable) and others aren't (a UNIQUE constraint violation on a
    duplicate execution-attempt insert is a real bug, not a "try again" situation)."""

    def __init__(self, message: str, *, retryable: bool = False):
        super().__init__(message)
        self.retryable = retryable
