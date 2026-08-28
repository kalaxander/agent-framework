"""Phase 2 — async Orchestrator.

Replaces Phase 1's SyncExecutor for production use: same Flow/Task model (docs say adding
Phase 2 must not change the flow-definition API), but now:
  - tasks in the same dependency "level" (see Flow.levels()) run concurrently, not one-at-a-time
  - every state transition is persisted via a StateStore (audit requirement)
  - run lifecycle is explicit: queued -> running -> succeeded/failed/cancelled
  - task fn may be sync or async

This is the in-house orchestration core described in docs/Architecture.md — it does not depend
on Airflow; an Airflow adapter (Phase 7, optional) would compile a Flow to a DAG and call out to
this same task-execution logic, or vice versa, depending on which scheduler is fronting a given
deployment.

Phase 6 adds: Flow-level + Task-level Guardrails run around every task (pre_execute before,
post_execute after); a GuardrailViolation (or any AgentFrameworkError with retryable=False) is
never retried — it fails the task on the first attempt (docs/Rules.md: "fail closed"). Optional
`metrics`/`logger` record per-task-attempt observability data alongside the existing StateStore
audit trail.

Phase 9 adds human-in-the-loop: Task(requires_approval=True) pauses the run (RunStatus.WAITING)
immediately before that task executes, until external code calls
`orchestrator.resume(run_id, task_name, approved=...)`. Implemented as a real asyncio.Event
await — the run's coroutine is genuinely suspended, not polled — so `run()` and `resume()` are
expected to be awaited from different tasks/coroutines (see run_demo_phase9.py).
"""
from __future__ import annotations

import asyncio
import inspect
import time
from typing import Any, Optional

from agentframework.core.errors import AgentFrameworkError, ApprovalRejected, TaskTimeoutError
from agentframework.core.flow import Flow, Task
from agentframework.core.state_store import (
    InMemoryStateStore,
    RunRecord,
    RunStatus,
    StateStore,
    TaskState,
    TaskStatus,
)
from agentframework.guardrails.base import Guardrail
from agentframework.memory.base import LongTermMemory, MemoryHandle, ShortTermMemory
from agentframework.observability.logger import JsonLineLogger
from agentframework.observability.metrics import InMemoryMetrics, TaskMetric
from agentframework.tools.registry import ToolRegistry


class AsyncOrchestrator:
    def __init__(self, state_store: Optional[StateStore] = None,
                 tool_registry: Optional[ToolRegistry] = None,
                 short_term_memory: Optional[ShortTermMemory] = None,
                 long_term_memory: Optional[LongTermMemory] = None,
                 metrics: Optional[InMemoryMetrics] = None,
                 logger: Optional[JsonLineLogger] = None):
        self.state_store = state_store or InMemoryStateStore()
        self.tool_registry = tool_registry
        self.short_term_memory = short_term_memory
        self.long_term_memory = long_term_memory
        self.metrics = metrics
        self.logger = logger
        self._approval_events: dict[str, asyncio.Event] = {}
        self._approval_results: dict[str, bool] = {}

    async def resume(self, run_id: str, task_name: str, approved: bool) -> None:
        """Unblocks a Task(requires_approval=True) that's currently paused waiting for this
        exact run_id/task_name. A resume() call for a task that isn't currently waiting is a
        no-op (the event just won't exist yet/anymore) — callers should check run status via
        the StateStore first if they need to distinguish "not waiting yet" from "already past."
        """
        key = f"{run_id}:{task_name}"
        self._approval_results[key] = approved
        event = self._approval_events.get(key)
        if event is not None:
            event.set()

    async def run(self, flow: Flow, inputs: dict[str, Any],
                   session_id: Optional[str] = None) -> RunRecord:
        flow.validate()
        run = RunRecord.new(flow.name, inputs)
        for name in flow.tasks:
            run.tasks[name] = TaskState(name=name)
        await self.state_store.create_run(run)
        await self.state_store.update_run_status(run.run_id, RunStatus.RUNNING)
        if self.logger:
            self.logger.log("run_started", run_id=run.run_id, flow_name=flow.name)

        context: dict[str, Any] = {"__inputs__": inputs}
        if self.short_term_memory is not None or self.long_term_memory is not None:
            # session_id defaults to run_id (i.e. no cross-run long-term recall unless the
            # caller explicitly passes a stable session_id shared across runs).
            context["__memory__"] = MemoryHandle(
                run_id=run.run_id,
                session_id=session_id or run.run_id,
                short_term=self.short_term_memory,
                long_term=self.long_term_memory,
            )
        levels = flow.levels()

        try:
            for level in levels:
                # Tasks within a level have no dependency on each other -> run concurrently.
                results = await asyncio.gather(
                    *(self._run_task_with_retry(run.run_id, flow.name, flow.tasks[name],
                                                 context, flow.guardrails)
                      for name in level),
                    return_exceptions=True,
                )
                for name, result in zip(level, results):
                    if isinstance(result, Exception):
                        await self.state_store.update_run_status(run.run_id, RunStatus.FAILED)
                        if self.logger:
                            self.logger.log("run_failed", run_id=run.run_id,
                                             flow_name=flow.name, error=str(result))
                        raise result
                    context[name] = result

            await self.state_store.update_run_status(run.run_id, RunStatus.SUCCEEDED)
            if self.logger:
                self.logger.log("run_succeeded", run_id=run.run_id, flow_name=flow.name)
        except Exception:
            # status already set to FAILED above for task errors; this also catches flow-level
            # errors (e.g. validate() issues surfaced late) so a run never gets stuck RUNNING.
            current = await self.state_store.get_run(run.run_id)
            if current and current.status == RunStatus.RUNNING:
                await self.state_store.update_run_status(run.run_id, RunStatus.FAILED)
            raise

        return await self.state_store.get_run(run.run_id)  # type: ignore[return-value]

    async def audit_trail(self, run_id: str):
        """Convenience passthrough to the configured StateStore's audit trail (docs/Phases.md
        Phase 6: "the audit-trail query API built on Phase 2's state store")."""
        return await self.state_store.audit_trail(run_id)

    @staticmethod
    def _resolve_task_input(task: Task, context: dict[str, Any]) -> dict[str, Any]:
        """What gets passed to guardrails and (for tool tasks) to the tool itself. For `fn`
        tasks there's no separately-declared input shape, so the whole context stands in."""
        if task.tool is not None:
            return task.tool_input(context) if task.tool_input else context
        return context

    async def _await_approval(self, run_id: str, task_name: str) -> None:
        """Pause the run (RunStatus.WAITING) and block until AsyncOrchestrator.resume() is
        called for this exact run_id/task_name. Raises ApprovalRejected if resumed with
        approved=False — same fail-closed treatment as a GuardrailViolation."""
        key = f"{run_id}:{task_name}"
        event = asyncio.Event()
        self._approval_events[key] = event
        await self.state_store.update_run_status(run_id, RunStatus.WAITING)
        if self.logger:
            self.logger.log("run_waiting_for_approval", run_id=run_id, task_name=task_name)

        await event.wait()

        approved = self._approval_results.pop(key, False)
        self._approval_events.pop(key, None)
        await self.state_store.update_run_status(run_id, RunStatus.RUNNING)
        if self.logger:
            self.logger.log("approval_resolved", run_id=run_id, task_name=task_name,
                             approved=approved)

        if not approved:
            state = TaskState(name=task_name, status=TaskStatus.FAILED, attempt=1,
                               error="rejected by human-in-the-loop approval",
                               started_at=time.time(), finished_at=time.time())
            await self.state_store.update_task_state(run_id, state)
            raise ApprovalRejected(f"Task '{task_name}' was not approved")

    async def _run_task_with_retry(
        self, run_id: str, flow_name: str, task: Task, context: dict[str, Any],
        flow_guardrails: list[Guardrail],
    ) -> Any:
        policy = task.retry_policy
        guardrails = list(flow_guardrails) + list(task.guardrails)
        input_data = self._resolve_task_input(task, context)
        attempt = 0
        delay = policy.backoff_seconds
        last_error: Optional[Exception] = None

        if task.requires_approval:
            await self._await_approval(run_id, task.name)  # raises ApprovalRejected if denied

        while attempt < policy.max_attempts:
            attempt += 1
            state = TaskState(name=task.name, status=TaskStatus.RUNNING, attempt=attempt,
                               started_at=time.time())
            await self.state_store.update_task_state(run_id, state)
            try:
                for guardrail in guardrails:
                    guardrail.pre_execute(task.name, input_data)
                result = await self._run_with_timeout(task, context)
                for guardrail in guardrails:
                    guardrail.post_execute(task.name, result)

                duration_ms = (time.time() - state.started_at) * 1000
                state = TaskState(
                    name=task.name, status=TaskStatus.SUCCEEDED, attempt=attempt, result=result,
                    started_at=state.started_at, finished_at=time.time(),
                )
                await self.state_store.update_task_state(run_id, state)
                self._record_observability(flow_name, task.name, "succeeded", duration_ms,
                                            attempt, result, run_id)
                return result
            except Exception as exc:  # noqa: BLE001 - classified into typed errors upstream
                last_error = exc
                duration_ms = (time.time() - state.started_at) * 1000
                state = TaskState(
                    name=task.name, status=TaskStatus.FAILED, attempt=attempt, error=str(exc),
                    started_at=state.started_at, finished_at=time.time(),
                )
                await self.state_store.update_task_state(run_id, state)
                self._record_observability(flow_name, task.name, "failed", duration_ms,
                                            attempt, None, run_id, error=str(exc))

                non_retryable = isinstance(exc, AgentFrameworkError) and not exc.retryable
                if non_retryable:
                    break  # fail closed — do not retry guardrail violations, etc.
                if attempt < policy.max_attempts:
                    await asyncio.sleep(delay)
                    delay *= policy.backoff_multiplier

        assert last_error is not None
        raise last_error

    def _record_observability(self, flow_name: str, task_name: str, status: str,
                               duration_ms: float, attempt: int, result: Any, run_id: str,
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
                f"task_{status}", run_id=run_id, task_name=task_name,
                duration_ms=round(duration_ms, 2), attempt=attempt,
                **({"error": error} if error else {}),
            )

    async def _run_with_timeout(self, task: Task, context: dict[str, Any]) -> Any:
        try:
            if task.fn is not None:
                if inspect.iscoroutinefunction(task.fn):
                    coro = task.fn(context)
                else:
                    loop = asyncio.get_running_loop()
                    coro = loop.run_in_executor(None, task.fn, context)
            elif task.tool is not None:
                if self.tool_registry is None:
                    raise NotImplementedError(
                        f"Task '{task.name}' references tool '{task.tool}' but this "
                        "AsyncOrchestrator has no tool_registry configured. Pass "
                        "tool_registry=... to AsyncOrchestrator()."
                    )
                input_data = self._resolve_task_input(task, context)
                coro = self.tool_registry.invoke(task.tool, input_data)
            else:
                raise NotImplementedError(
                    f"Task '{task.name}' has neither fn nor tool set — nothing to run."
                )
            return await asyncio.wait_for(coro, timeout=task.timeout_seconds)
        except asyncio.TimeoutError as exc:
            raise TaskTimeoutError(
                f"Task '{task.name}' exceeded timeout of {task.timeout_seconds}s"
            ) from exc
