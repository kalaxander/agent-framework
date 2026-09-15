"""Phase 4 — Tool interface (docs/PRD.md > Tools/Actions, docs/Rules.md > Phase 4 guardrails).

`validate_input`/`validate_output` are the "tool-level guardrail hooks" called for by
docs/Phases.md Phase 4 — deliberately minimal (raise GuardrailViolation to reject). The fuller,
composable pre/post-execution guardrail *pipeline* (rate limits, PII filtering, etc., pluggable
per task or flow) is Phase 6 scope — see docs/Memory.md for the split.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class Tool(ABC):
    """A named, callable capability a Task can invoke via `Task(tool="name")`.

    Subclasses set `name` and implement `run`. `validate_input`/`validate_output` default to
    no-ops; override to reject malformed input/output before/after `run()`.
    """

    name: str

    @abstractmethod
    async def run(self, input: dict[str, Any]) -> Any: ...

    def validate_input(self, input: dict[str, Any]) -> None:
        """Raise GuardrailViolation (see core.errors) to reject before run() is called."""
        pass

    def validate_output(self, output: Any) -> None:
        """Raise GuardrailViolation (see core.errors) to reject after run() completes."""
        pass
