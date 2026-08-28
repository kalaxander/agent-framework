"""Phase 1 — core flow model.

A Flow is a composition of Tasks forming a DAG. This module defines the schema and
DAG-resolution logic (topological order, dependency validation, cycle detection) used by
every executor in the framework (sync, async orchestrator, Airflow adapter, ...).

NOTE (flagged per docs/Rules.md — architecture changes must be flagged, not silent):
The core engine (this module + executor/orchestrator/state_store) uses stdlib
`dataclasses` instead of pydantic. Reason: keeps the execution core dependency-free so it
runs anywhere with just Python 3.11+, no install step required. Pydantic remains the
planned choice for the *edge* of the system (FastAPI request/response schemas in the
ingress layer, Phase 3) where wire-format validation genuinely benefits from it. See
docs/Architecture.md.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from agentframework.core.errors import FlowValidationError
from agentframework.guardrails.base import Guardrail

# A task's runnable body: takes the accumulated context (outputs of upstream tasks, keyed by
# task name, plus the flow's original inputs under "__inputs__") and returns its own output.
# May be a regular function or an `async def` — both executor and orchestrator handle either.
TaskFn = Callable[[dict[str, Any]], Any]


@dataclass
class RetryPolicy:
    max_attempts: int = 3
    backoff_seconds: float = 1.0
    backoff_multiplier: float = 2.0


@dataclass
class Task:
    """A single unit of work in a Flow.

    `tool` names a registered Tool (resolved via a ToolRegistry — see tools/registry.py — passed
    to the executor/orchestrator) OR `fn` is a direct Python callable — exactly one of the two
    must be set. `tool_input` maps the accumulated context to the dict passed as the tool's
    input; if omitted, the whole context is passed through unchanged. `guardrails` (Phase 6) run
    around this task's execution in addition to any Flow-level guardrails. `requires_approval`
    (Phase 9, human-in-the-loop — AsyncOrchestrator only) pauses the run (status WAITING) before
    this task executes, until AsyncOrchestrator.resume(run_id, task_name, approved=...) is called.
    """

    name: str
    tool: Optional[str] = None
    tool_input: Optional[Callable[[dict[str, Any]], dict[str, Any]]] = None
    fn: Optional[TaskFn] = None
    depends_on: list[str] = field(default_factory=list)
    timeout_seconds: float = 30.0
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    on_failure: Optional[str] = None  # name of a task to route to if this task fails terminally
    guardrails: list[Guardrail] = field(default_factory=list)
    requires_approval: bool = False


@dataclass
class Flow:
    """A named, composable graph of Tasks. `guardrails` (Phase 6) run around every task in the
    flow, in addition to that task's own `guardrails`."""

    name: str
    tasks: dict[str, Task] = field(default_factory=dict)
    guardrails: list[Guardrail] = field(default_factory=list)

    def add_task(self, task: Task) -> "Flow":
        if task.name in self.tasks:
            raise FlowValidationError(f"Task '{task.name}' already exists in flow '{self.name}'")
        self.tasks[task.name] = task
        return self

    def validate(self) -> None:
        """Raise FlowValidationError if dependencies are missing or a cycle exists."""
        for task in self.tasks.values():
            for dep in task.depends_on:
                if dep not in self.tasks:
                    raise FlowValidationError(
                        f"Task '{task.name}' depends on unknown task '{dep}'"
                    )
            if task.on_failure and task.on_failure not in self.tasks:
                raise FlowValidationError(
                    f"Task '{task.name}' has unknown on_failure target '{task.on_failure}'"
                )
        self.topological_order()  # raises FlowValidationError on cycle

    def topological_order(self) -> list[str]:
        """Kahn's algorithm. Returns task names in a valid execution order."""
        in_degree = {name: 0 for name in self.tasks}
        for task in self.tasks.values():
            for _dep in task.depends_on:
                in_degree[task.name] += 1

        ready = [name for name, deg in in_degree.items() if deg == 0]
        ordered: list[str] = []

        dependents: dict[str, list[str]] = {name: [] for name in self.tasks}
        for task in self.tasks.values():
            for dep in task.depends_on:
                dependents[dep].append(task.name)

        while ready:
            ready.sort()  # deterministic order
            current = ready.pop(0)
            ordered.append(current)
            for dependent in dependents[current]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    ready.append(dependent)

        if len(ordered) != len(self.tasks):
            remaining = set(self.tasks) - set(ordered)
            raise FlowValidationError(
                f"Cycle detected in flow '{self.name}' involving tasks: {sorted(remaining)}"
            )
        return ordered

    def levels(self) -> list[list[str]]:
        """Group task names into dependency 'levels' — each level's tasks can run concurrently
        because none of them depend on another task in the same level. Used by the async
        Orchestrator (Phase 2) to parallelize independent tasks instead of running strictly
        one-at-a-time like the Phase 1 SyncExecutor.
        """
        remaining = dict(self.tasks)
        done: set[str] = set()
        result: list[list[str]] = []

        while remaining:
            level = [
                name
                for name, task in remaining.items()
                if all(dep in done for dep in task.depends_on)
            ]
            if not level:
                raise FlowValidationError(
                    f"Cycle detected in flow '{self.name}' involving tasks: {sorted(remaining)}"
                )
            level.sort()
            result.append(level)
            for name in level:
                done.add(name)
                del remaining[name]

        return result
