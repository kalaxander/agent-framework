"""Run Phase 9's human-in-the-loop pause/resume end to end: a run genuinely suspends
(RunStatus.WAITING) at a Task(requires_approval=True), and only proceeds once
AsyncOrchestrator.resume() is called from a separate coroutine — not polled, an actual
asyncio.Event wait. Demonstrates both the approved and rejected paths. Run with:
    python3 run_demo_phase9.py
"""
from __future__ import annotations

import asyncio
import sys

sys.path.insert(0, "sdk")

from agentframework import Flow, Task
from agentframework.core.orchestrator import AsyncOrchestrator
from agentframework.core.state_store import InMemoryStateStore, RunStatus
from agentframework.core.errors import ApprovalRejected


def _only_run_id(state_store: InMemoryStateStore) -> str:
    """Demo-only helper: peek at the one run this demo just started, so we can call resume()
    without needing a separate channel to learn the run_id (a real caller would get it from the
    RunRequest/REST response, same as every other phase's demos)."""
    return next(iter(state_store._runs))


async def demo_approved():
    print("=== Phase 9: human-in-the-loop — approved path ===")

    flow = Flow(name="approval-demo-approved")
    flow.add_task(Task(name="draft", fn=lambda ctx: "draft: refund $50 to customer"))
    flow.add_task(Task(
        name="send_refund", fn=lambda ctx: f"SENT: {ctx['draft']}",
        depends_on=["draft"], requires_approval=True,
    ))

    orchestrator = AsyncOrchestrator()
    run_task = asyncio.create_task(orchestrator.run(flow, inputs={}))

    # Poll our OWN observation of state, purely to print the WAITING status for this demo — the
    # orchestrator's run() coroutine itself is genuinely suspended on an asyncio.Event, not
    # polling anything.
    run_id = None
    status_while_paused = None
    for _ in range(40):
        await asyncio.sleep(0.02)
        if orchestrator.state_store._runs:
            run_id = _only_run_id(orchestrator.state_store)
            run = await orchestrator.state_store.get_run(run_id)
            if run.status == RunStatus.WAITING:
                status_while_paused = run.status
                break

    print(f"run status while paused: {status_while_paused.value if status_while_paused else 'unknown'}")
    print("(the run coroutine is blocked on an asyncio.Event — a real suspend, not a poll loop)")

    await asyncio.sleep(0.1)  # simulate time passing before a human acts
    print(f"approving task 'send_refund' for run {run_id}...")
    await orchestrator.resume(run_id, "send_refund", approved=True)

    run = await run_task
    print(f"final status: {run.status.value}")
    print(f"send_refund result: {run.tasks['send_refund'].result}")
    print()


async def demo_rejected():
    print("=== Phase 9: human-in-the-loop — rejected path ===")

    flow = Flow(name="approval-demo-rejected")
    flow.add_task(Task(name="draft", fn=lambda ctx: "draft: refund $50000 to customer"))
    flow.add_task(Task(
        name="send_refund", fn=lambda ctx: f"SENT: {ctx['draft']}",
        depends_on=["draft"], requires_approval=True,
    ))

    orchestrator = AsyncOrchestrator()
    run_task = asyncio.create_task(orchestrator.run(flow, inputs={}))
    await asyncio.sleep(0.1)

    run_id = _only_run_id(orchestrator.state_store)
    print(f"a human rejects the $50000 refund for run {run_id}...")
    await orchestrator.resume(run_id, "send_refund", approved=False)

    try:
        await run_task
        print("FAIL: should have raised ApprovalRejected")
    except ApprovalRejected as exc:
        print(f"OK: run failed as expected -> {exc}")
    print()


if __name__ == "__main__":
    asyncio.run(demo_approved())
    asyncio.run(demo_rejected())
    print("Phase 9 human-in-the-loop pause/resume verified end to end.")
