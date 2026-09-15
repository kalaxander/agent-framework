"""Phase 2 — state store. Extended (see docs/Memory.md, "database integration") with a durable
execution-attempt history and an append-only audit-event log.

Owns the *run lifecycle* (see docs/Architecture.md > Orchestrator): every run and every task
within it has a persisted state, so any run is reconstructable after the fact (audit
requirement from docs/PRD.md > Non-Functional Requirements).

`InMemoryStateStore` is the reference implementation used by tests, the demo, and anywhere a
real Postgres isn't available. `integrations/postgres_state_store.py` implements the same
`StateStore` interface backed by Postgres for production use — swap one for the other without
touching Orchestrator code.

Two persistence concepts, kept deliberately separate:
  - `update_task_state` / `run.tasks` — the *latest* state of each task, unchanged in shape from
    before this extension. Every existing caller (fastapi_ingress's GET /v1/runs/{id}, the
    stdlib RestIngress, audit_trail()) keeps working exactly as it did.
  - `get_execution_history` — every attempt of every task, in order, never overwritten. This is
    additive: nothing that used to read only the "latest" view breaks, but retries are no longer
    silently destroyed the way the original task_states upsert did.
  - `record_audit_event` / `get_audit_events` — a durable state-*transition* log (run created,
    waiting-for-approval, approved/rejected, ...), independent of the per-task latest/history
    views above. `audit_trail()` (per-task final states) is unchanged; this is a new, separate
    capability, not a replacement for it.
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


@dataclass
class AuditLogEntry:
    """One durable state-transition event for a run — e.g. RUN_CREATED, TASK_FAILED,
    RUN_WAITING_APPROVAL. `task_name` is None for run-level events. See docs/Architecture.md >
    State & Memory for the full event list."""

    run_id: str
    event: str
    task_name: Optional[str] = None
    ts: float = field(default_factory=time.time)


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
        """Return every task's *latest* recorded state for a run, in a stable order — the
        reconstructable "what happened" record required by docs/PRD.md. Unchanged contract from
        before this module's database-integration extension; see get_execution_history() for
        full per-attempt history and get_audit_events() for the durable transition log."""
        ...

    @abstractmethod
    async def get_execution_history(
        self, run_id: str, task_name: Optional[str] = None
    ) -> list[TaskState]:
        """Every attempt of every task for a run (or of one task if task_name is given), in
        attempt order — never overwritten, unlike the "latest state" tracked by
        update_task_state/run.tasks. Answers "what happened on attempt 1 vs attempt 2" which the
        latest-only view can't."""
        ...

    @abstractmethod
    async def record_audit_event(
        self, run_id: str, event: str, task_name: Optional[str] = None
    ) -> None:
        """Append one durable state-transition event. Independent of task execution state —
        covers run-level transitions (RUN_CREATED, RUN_WAITING_APPROVAL, ...) that don't fit the
        per-task TaskState shape."""
        ...

    @abstractmethod
    async def get_audit_events(self, run_id: str) -> list[AuditLogEntry]:
        """Every audit event recorded for a run, in chronological order."""
        ...


class InMemoryStateStore(StateStore):
    """Reference StateStore. Not durable across process restarts — use
    integrations.postgres_state_store.PostgresStateStore for that."""

    def __init__(self) -> None:
        self._runs: dict[str, RunRecord] = {}
        # Full per-attempt history, kept alongside (not instead of) run.tasks' latest-only view —
        # mirrors PostgresStateStore's task_executions table so both backends answer
        # get_execution_history() consistently instead of only the durable one being able to.
        self._execution_history: dict[str, list[TaskState]] = {}
        self._audit_events: dict[str, list[AuditLogEntry]] = {}

    async def create_run(self, run: RunRecord) -> None:
        self._runs[run.run_id] = run
        self._execution_history[run.run_id] = []
        self._audit_events[run.run_id] = []

    async def get_run(self, run_id: str) -> Optional[RunRecord]:
        return self._runs.get(run_id)

    async def update_run_status(self, run_id: str, status: RunStatus) -> None:
        run = self._runs[run_id]
        run.status = status
        run.updated_at = time.time()

    async def update_task_state(self, run_id: str, task_state: TaskState) -> None:
        run = self._runs[run_id]
        run.tasks[task_state.name] = task_state  # latest-only view — unchanged behavior
        run.updated_at = time.time()
        # Full history records one entry per COMPLETED attempt (succeeded/failed), not the
        # transient RUNNING write at the start of each attempt — that's already covered by the
        # TASK_STARTED audit event below, and recording it here too would mean two history rows
        # for what's really one attempt (mirrors PostgresStateStore's UNIQUE(run_id, task_name,
        # attempt) constraint, which the same reasoning drives).
        event = {
            TaskStatus.RUNNING: "TASK_STARTED",
            TaskStatus.SUCCEEDED: "TASK_SUCCEEDED",
            TaskStatus.FAILED: "TASK_FAILED",
        }.get(task_state.status)
        if event is not None:
            await self.record_audit_event(run_id, event, task_state.name)
        if task_state.status in (TaskStatus.SUCCEEDED, TaskStatus.FAILED):
            self._execution_history[run_id].append(task_state)

    async def audit_trail(self, run_id: str) -> list[TaskState]:
        run = self._runs[run_id]
        return [run.tasks[name] for name in sorted(run.tasks)]

    async def get_execution_history(
        self, run_id: str, task_name: Optional[str] = None
    ) -> list[TaskState]:
        history = self._execution_history.get(run_id, [])
        if task_name is not None:
            history = [ts for ts in history if ts.name == task_name]
        return history

    async def record_audit_event(
        self, run_id: str, event: str, task_name: Optional[str] = None
    ) -> None:
        self._audit_events.setdefault(run_id, []).append(
            AuditLogEntry(run_id=run_id, event=event, task_name=task_name)
        )

    async def get_audit_events(self, run_id: str) -> list[AuditLogEntry]:
        return list(self._audit_events.get(run_id, []))
