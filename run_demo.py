"""Run Phase 1 (SyncExecutor) and Phase 2 (AsyncOrchestrator) against the *same* Flow and
compare results — this is the "run them together" demo.

No external dependencies (stdlib only). Run with:
    PYTHONPATH=sdk python3 run_demo.py
"""
from __future__ import annotations

import asyncio
import sys
import time

sys.path.insert(0, "sdk")  # allow running without an editable install

from agentframework import Flow, Task
from agentframework.core.executor import SyncExecutor
from agentframework.core.orchestrator import AsyncOrchestrator
from agentframework.core.flow import RetryPolicy


def build_flow() -> Flow:
    """A small support-ticket-triage-shaped flow: classify -> (fetch_docs, check_priority in
    parallel) -> draft, so Phase 2's level-based concurrency actually has something to show."""

    def classify(ctx):
        return {"category": "billing"}

    def fetch_docs(ctx):
        time.sleep(0.05)  # simulate IO
        return ["billing-faq#12", "refund-policy#3"]

    def check_priority(ctx):
        time.sleep(0.05)  # simulate IO — runs concurrently with fetch_docs under Phase 2
        return "normal"

    def draft(ctx):
        category = ctx["classify"]["category"]
        docs = ctx["fetch_docs"]
        priority = ctx["check_priority"]
        return f"[{priority}] Re: {category} — see {', '.join(docs)}"

    flow = Flow(name="support-ticket-triage")
    flow.add_task(Task(name="classify", fn=classify))
    flow.add_task(Task(name="fetch_docs", fn=fetch_docs, depends_on=["classify"]))
    flow.add_task(Task(name="check_priority", fn=check_priority, depends_on=["classify"]))
    flow.add_task(Task(name="draft", fn=draft, depends_on=["fetch_docs", "check_priority"]))
    return flow


def run_phase1():
    print("=== Phase 1: SyncExecutor ===")
    flow = build_flow()
    start = time.monotonic()
    result = SyncExecutor().run(flow, inputs={"ticket_id": 42})
    elapsed = time.monotonic() - start
    print(f"draft output : {result['draft']}")
    print(f"wall time    : {elapsed:.3f}s (tasks run strictly one-at-a-time)")
    print()


async def run_phase2():
    print("=== Phase 2: AsyncOrchestrator ===")
    flow = build_flow()
    orchestrator = AsyncOrchestrator()
    start = time.monotonic()
    run = await orchestrator.run(flow, inputs={"ticket_id": 42})
    elapsed = time.monotonic() - start
    print(f"run_id       : {run.run_id}")
    print(f"run status   : {run.status.value}")
    print(f"draft output : {run.tasks['draft'].result}")
    print(f"wall time    : {elapsed:.3f}s (fetch_docs + check_priority ran concurrently)")
    print()
    print("audit trail:")
    for task_state in await orchestrator.state_store.audit_trail(run.run_id):
        print(f"  {task_state.name:15s} status={task_state.status.value:10s} "
              f"attempt={task_state.attempt}")
    print()


def run_phase2_retry_demo():
    print("=== Phase 2: retry + typed-timeout behavior ===")

    attempts = {"count": 0}

    def flaky(ctx):
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise RuntimeError("simulated transient failure")
        return "recovered"

    flow = Flow(name="retry-demo")
    flow.add_task(
        Task(
            name="flaky_task",
            fn=flaky,
            retry_policy=RetryPolicy(max_attempts=5, backoff_seconds=0.01, backoff_multiplier=1.0),
        )
    )
    run = asyncio.run(AsyncOrchestrator().run(flow, inputs={}))
    assert run.tasks["flaky_task"].result == "recovered"
    assert attempts["count"] == 3
    print(f"flaky_task recovered after {attempts['count']} attempts, "
          f"final status={run.status.value}")
    print()


if __name__ == "__main__":
    run_phase1()
    asyncio.run(run_phase2())
    run_phase2_retry_demo()
    print("Phase 1 and Phase 2 both ran the same Flow definition successfully.")
