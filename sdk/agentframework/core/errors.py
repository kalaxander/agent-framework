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
