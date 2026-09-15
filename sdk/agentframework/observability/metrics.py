"""Metrics (docs/PRD.md > Observability: "latency, success/error rate, and token/cost usage").
`InMemoryMetrics` is the reference collector — a real deployment would export these to
Prometheus/etc. (docs/Architecture.md mentions Prometheus-style metrics) instead of keeping them
in a Python list; the aggregation methods below (`success_rate`, `avg_latency_ms`, ...) are the
contract a Prometheus-backed implementation would need to preserve.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class TaskMetric:
    flow_name: str
    task_name: str
    status: str  # "succeeded" | "failed"
    duration_ms: float
    attempt: int
    tokens: Optional[int] = None
    cost: Optional[float] = None


class InMemoryMetrics:
    def __init__(self) -> None:
        self.records: list[TaskMetric] = []

    def record(self, metric: TaskMetric) -> None:
        self.records.append(metric)

    def _filtered(self, task_name: Optional[str]) -> list[TaskMetric]:
        if task_name is None:
            return self.records
        return [r for r in self.records if r.task_name == task_name]

    def success_rate(self, task_name: Optional[str] = None) -> float:
        relevant = self._filtered(task_name)
        if not relevant:
            return 0.0
        successes = sum(1 for r in relevant if r.status == "succeeded")
        return successes / len(relevant)

    def avg_latency_ms(self, task_name: Optional[str] = None) -> float:
        durations = [r.duration_ms for r in self._filtered(task_name) if r.status == "succeeded"]
        return sum(durations) / len(durations) if durations else 0.0

    def retry_count(self, task_name: Optional[str] = None) -> int:
        """Number of recorded attempts beyond the first, across all tasks matched."""
        return sum(1 for r in self._filtered(task_name) if r.attempt > 1)

    def total_tokens(self, task_name: Optional[str] = None) -> int:
        return sum(r.tokens or 0 for r in self._filtered(task_name))

    def total_cost(self, task_name: Optional[str] = None) -> float:
        return sum(r.cost or 0.0 for r in self._filtered(task_name))

    def summary(self) -> dict:
        task_names = sorted({r.task_name for r in self.records})
        return {
            "overall": {
                "success_rate": self.success_rate(),
                "avg_latency_ms": round(self.avg_latency_ms(), 2),
                "retry_count": self.retry_count(),
                "total_tokens": self.total_tokens(),
                "total_cost": round(self.total_cost(), 6),
            },
            "by_task": {
                name: {
                    "success_rate": self.success_rate(name),
                    "avg_latency_ms": round(self.avg_latency_ms(name), 2),
                    "retry_count": self.retry_count(name),
                }
                for name in task_names
            },
        }
