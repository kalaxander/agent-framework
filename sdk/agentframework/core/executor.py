"""Phase 1 — minimal synchronous executor.

Runs a validated Flow of Python-callable tasks end to end, honoring retry_policy and
timeout_seconds, and emitting one audit event per task via observability.audit_log.
This is intentionally simple (no Kafka/Postgres) so Phase 1 is runnable/testable in isolation;
Phase 2 replaces this with the async Orchestrator + persistent state store, same Flow model.

Phase 4 adds named-tool resolution (Task(tool="...")) via an optional ToolRegistry, alongside
the existing Task(fn=...) path — see docs/Phases.md Phase 4.
Phase 5 adds an optional memory context (context["__memory__"]) — see docs/Phases.md Phase 5.
Since Phase 1 has no RunRecord, a throwaway run_id is generated per SyncExecutor.run() call
purely to scope short-term memory (long-term memory scoping still uses the passed session_id).
Phase 6 adds Flow-/Task-level Guardrails (pre/post-execution, fail-closed — not retried) and
optional metrics/logger, same semantics as AsyncOrchestrator — see docs/Phases.md Phase 6.
"""
from __future__ import annotations

import asyncio
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from typing import Any, Optional

from agentframework.core.errors import AgentFrameworkError, TaskTimeoutError
from agentframework.core.flow import Flow, Task
from agentframework.guardrails.base import Guardrail
from agentframework.memory.base import LongTermMemory, MemoryHandle, ShortTermMemory
from agentframework.observability.audit_log import AuditEvent, InMemoryAuditLog
from agentframework.observability.logger import JsonLineLogger
from agentframework.observability.metrics import InMemoryMetrics, TaskMetric
from agentframework.tools.registry import ToolRegistry


class SyncExecutor:
    """Runs a Flow's tasks in topological order, in-process. Reference/test executor."""

    def __init__(self, audit_log: InMemoryAuditLog | None = None,
                 tool_registry: Optional[ToolRegistry] = None,
                 short_term_memory: Optional[ShortTermMemory] = None,
                 long_term_memory: Optional[LongTermMemory] = None,
                 metrics: Optional[InMemoryMetrics] = None,
                 logger: Optional[JsonLineLogger] = None):
        self.audit_log = audit_log or InMemoryAuditLog()
        self.tool_registry = tool_registry
        self.short_term_memory = short_term_memory
        self.long_term_memory = long_term_memory
        self.metrics = metrics
        self.logger = logger

    def run(self, flow: Flow, inputs: dict[str, Any],
            session_id: Optional[str] = None) -> dict[str, Any]:
        flow.validate()
        order = flow.topological_order()
        context: dict[str, Any] = {"__inputs__": inputs}
        if self.short_term_memory is not None or self.long_term_memory is not None:
            run_id = str(uuid.uuid4())
            context["__memory__"] = MemoryHandle(
                run_id=run_id,
                session_id=session_id or run_id,
                short_term=self.short_term_memory,
                long_term=self.long_term_memory,
            )

        for task_name in order:
            task = flow.tasks[task_name]
            context[task_name] = self._run_task_with_retry(flow, task, context)

        return context

    @staticmethod
    def _resolve_task_input(task: Task, context: dict[str, Any]) -> dict[str, Any]:
        if task.tool is not None:
            return task.tool_input(context) if task.tool_input else context
        return context

    def _run_task_with_retry(self, flow: Flow, task: Task, context: dict[str, Any]) -> Any:
        policy = task.retry_policy
        guardrails: list[Guardrail] = list(flow.guardrails) + list(task.guardrails)
        input_data = self._resolve_task_input(task, context)
        attempt = 0
        delay = policy.backoff_seconds
        last_error: Exception | None = None

        while attempt < policy.max_attempts:
            attempt += 1
            start = time.monotonic()
            try:
                for guardrail in guardrails:
                    guardrail.pre_execute(task.name, input_data)
                result = self._run_with_timeout(task, context)
                for guardrail in guardrails:
                    guardrail.post_execute(task.name, result)

                duration_ms = (time.monotonic() - start) * 1000
                self.audit_log.record(AuditEvent(
                    flow_name=flow.name, task_name=task.name, event="task_succeeded",
                    duration_ms=duration_ms, attempt=attempt,
                ))
                self._record_observability(flow.name, task.name, "succeeded", duration_ms,
                                            attempt, result)
                return result
            except Exception as exc:  # noqa: BLE001 - deliberately broad; classified below
                last_error = exc
                duration_ms = (time.monotonic() - start) * 1000
                self.audit_log.record(AuditEvent(
                    flow_name=flow.name, task_name=task.name, event="task_failed",
                    duration_ms=duration_ms, attempt=attempt, error=str(exc),
                ))
                self._record_observability(flow.name, task.name, "failed", duration_ms,
                                            attempt, None, error=str(exc))

                non_retryable = isinstance(exc, AgentFrameworkError) and not exc.retryable
                if non_retryable:
                    break  # fail closed — do not retry guardrail violations, etc.
                if attempt < policy.max_attempts:
                    time.sleep(delay)
                    delay *= policy.backoff_multiplier

        assert last_error is not None
        raise last_error

    def _record_observability(self, flow_name: str, task_name: str, status: str,
                               duration_ms: float, attempt: int, result: Any,
                               error: Optional[str] = None) -> None:
        if self.metrics is not None:
            usage = result.get("usage") if isinstance(result, dict) else None
            self.metrics.record(TaskMetric(
                flow_name=flow_name, task_name=task_name, status=status,
                duration_ms=duration_ms, attempt=attempt,
                tokens=(usage or {}).get("tokens"), cost=(usage or {}).get("cost"),
            ))
        if self.logger is not None:
            self.logger.log(
                f"task_{status}", task_name=task_name, duration_ms=round(duration_ms, 2),
                attempt=attempt, **({"error": error} if error else {}),
            )

    def _tool_call_sync(self, task: Task, context: dict[str, Any]) -> Any:
        """Bridge: SyncExecutor is sync, Tool.run is async — run one tool call to completion in
        its own event loop, inside the worker thread ThreadPoolExecutor already gives us."""
        input_data = self._resolve_task_input(task, context)
        return asyncio.run(self.tool_registry.invoke(task.tool, input_data))

    def _run_with_timeout(self, task: Task, context: dict[str, Any]) -> Any:
        if task.fn is not None:
            fn = task.fn
        elif task.tool is not None:
            if self.tool_registry is None:
                raise NotImplementedError(
                    f"Task '{task.name}' references tool '{task.tool}' but this SyncExecutor "
                    "has no tool_registry configured. Pass tool_registry=... to SyncExecutor()."
                )
            fn = lambda ctx: self._tool_call_sync(task, ctx)  # noqa: E731
        else:
            raise NotImplementedError(
                f"Task '{task.name}' has neither fn nor tool set — nothing to run."
            )
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(fn, context)
            try:
                return future.result(timeout=task.timeout_seconds)
            except FutureTimeoutError as exc:
                raise TaskTimeoutError(
                    f"Task '{task.name}' exceeded timeout of {task.timeout_seconds}s"
                ) from exc
