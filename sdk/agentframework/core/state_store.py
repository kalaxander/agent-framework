"""Phase 2 — state store.

Owns the *run lifecycle* (see docs/Architecture.md > Orchestrator): every run and every task
within it has a persisted state, so any run is reconstructable after the fact (audit
requirement from docs/PRD.md > Non-Functional Requirements).

`InMemoryStateStore` is the reference implementation used by tests, the demo, and anywhere a
real Postgres isn't available. `integrations/postgres_state_store.py` implements the same
`StateStore` interface backed by Postgres for production use — swap one for the other without
touching Orchestrator code.
"""
from __future__ import annotations

import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class RunStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    WAITING = "waiting"  # reserved for human-in-the-loop (Phases.md stretch goal)
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"  # e.g. an on_failure branch that wasn't taken


@dataclass
class TaskState:
    name: str
    status: TaskStatus = TaskStatus.PENDING
    attempt: int = 0
    result: Any = None
    error: Optional[str] = None
    started_at: Optional[float] = None
    finished_at: Optional[float] = None


@dataclass
class RunRecord:
    run_id: str
    flow_name: str
    inputs: dict[str, Any]
    status: RunStatus = RunStatus.QUEUED
    tasks: dict[str, TaskState] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)

    @staticmethod
    def new(flow_name: str, inputs: dict[str, Any]) -> "RunRecord":
        return RunRecord(run_id=str(uuid.uuid4()), flow_name=flow_name, inputs=inputs)


class StateStore(ABC):
    """Persistence interface the Orchestrator depends on. Implement this to swap backends."""

    @abstractmethod
    async def create_run(self, run: RunRecord) -> None: ...

    @abstractmethod
    async def get_run(self, run_id: str) -> Optional[RunRecord]: ...

    @abstractmethod
    async def update_run_status(self, run_id: str, status: RunStatus) -> None: ...

    @abstractmethod
    async def update_task_state(self, run_id: str, task_state: TaskState) -> None: ...

    @abstractmethod
    async def audit_trail(self, run_id: str) -> list[TaskState]:
        """Return every task's final recorded state for a run, in a stable order — the
        reconstructable "what happened" record required by docs/PRD.md."""
        ...


class InMemoryStateStore(StateStore):
    """Reference StateStore. Not durable across process restarts — use
    integrations.postgres_state_store.PostgresStateStore for that."""

    def __init__(self) -> None:
        self._runs: dict[str, RunRecord] = {}

    async def create_run(self, run: RunRecord) -> None:
        self._runs[run.run_id] = run

    async def get_run(self, run_id: str) -> Optional[RunRecord]:
        return self._runs.get(run_id)

    async def update_run_status(self, run_id: str, status: RunStatus) -> None:
        run = self._runs[run_id]
        run.status = status
        run.updated_at = time.time()

    async def update_task_state(self, run_id: str, task_state: TaskState) -> None:
        run = self._runs[run_id]
        run.tasks[task_state.name] = task_state
        run.updated_at = time.time()

    async def audit_trail(self, run_id: str) -> list[TaskState]:
        run = self._runs[run_id]
        return [run.tasks[name] for name in sorted(run.tasks)]
