"""Reference audit trail: one event per task attempt.

Phase 1 used this in-memory. Phase 2's StateStore (core/state_store.py) is the persistent,
queryable audit trail; this AuditEvent shape is shared by both so log/metric consumers don't
need to change when the backing store changes (in-memory -> Postgres, see docs/Architecture.md).
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class AuditEvent:
    flow_name: str
    task_name: str
    event: str  # "task_succeeded" | "task_failed" | ...
    duration_ms: float
    attempt: int
    error: Optional[str] = None
    ts: float = field(default_factory=time.time)


class InMemoryAuditLog:
    def __init__(self) -> None:
        self.events: list[AuditEvent] = []

    def record(self, event: AuditEvent) -> None:
        self.events.append(event)

    def for_flow(self, flow_name: str) -> list[AuditEvent]:
        return [e for e in self.events if e.flow_name == flow_name]
