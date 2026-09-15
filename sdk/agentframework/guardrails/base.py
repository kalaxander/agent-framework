"""Phase 6 — Guardrails (docs/PRD.md > Guardrails: "pre/post execution policy checks").

A `Guardrail` can be attached to a `Task` (`Task(guardrails=[...])`) or to a whole `Flow`
(`Flow(guardrails=[...])`, applied to every task in it) — composable per docs/Phases.md Phase 6.
`pre_execute` runs before a task's fn/tool is invoked; `post_execute` runs after. Raise
`GuardrailViolation` (core.errors) from either to reject — per docs/Rules.md, guardrail
violations always short-circuit and are never retried (`GuardrailViolation.retryable = False`,
enforced by the executor/orchestrator retry loop).

Methods are plain sync calls (not async) — guardrails are meant to be fast, local, in-process
checks (schema shape, a counter, a substring scan), not network calls; keeping them sync avoids
needing an async/sync bridge like Tool.run needs.
"""
from __future__ import annotations

from abc import ABC
from typing import Any


class Guardrail(ABC):
    name: str

    def pre_execute(self, task_name: str, input: dict[str, Any]) -> None:
        """Raise GuardrailViolation to reject before the task's fn/tool runs."""
        pass

    def post_execute(self, task_name: str, output: Any) -> None:
        """Raise GuardrailViolation to reject after the task's fn/tool produced output."""
        pass
