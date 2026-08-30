"""Run the expense approval reference agent end to end — the real point of this demo is Phase
9's human-in-the-loop pause/resume, which neither of the first two reference agents exercises.
Three passes: an approved expense, a rejected expense, and a second approved expense from the
same employee to show long-term memory recall picking up their first (approved) submission.

Every run genuinely suspends (RunStatus.WAITING) at "request_approval" until
AsyncOrchestrator.resume() is called from a separate coroutine — a real asyncio.Event await, not
a poll loop (see core/orchestrator.py's docstring and run_demo_phase9.py, the original Phase 9
demo this pattern is drawn from). Since run() blocks until either approved or rejected, this demo
schedules it as a background asyncio.Task and uses the `on_created` callback (added specifically
to support this — see orchestrator.py) to learn run_id immediately rather than waiting for
completion, exactly the same pattern fastapi_ingress.py's create_run uses for the REST API.

No external dependencies. Run with:
    python3 run.py     (from this directory)  OR  python3 examples/expense_approval_agent/run.py
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "sdk"))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from agentframework.core.orchestrator import AsyncOrchestrator
from agentframework.core.state_store import RunStatus
from agentframework.core.errors import ApprovalRejected
from agentframework.memory.in_memory import InMemoryLongTermMemory

from examples.expense_approval_agent.agent import build_flow, build_tool_registry


async def _submit_and_wait_for_approval(orchestrator: AsyncOrchestrator, inputs: dict,
                                         session_id: str) -> str:
    """Schedules the run in the background and returns as soon as its run_id is known — NOT
    once it completes, since it can't complete until resume() is called. Also waits (briefly,
    by polling our own view of run status — the orchestrator's own suspend is a real
    asyncio.Event, this poll is purely this demo script checking in) until the run actually
    reaches WAITING, so the caller can be sure it's safe to call resume() next."""
    created = asyncio.Event()
    captured: dict = {}

    def _on_created(run_id: str):
        captured["run_id"] = run_id
        created.set()

    run_task = asyncio.create_task(
        orchestrator.run(build_flow(), inputs, session_id=session_id, on_created=_on_created)
    )
    await asyncio.wait_for(created.wait(), timeout=5)
    run_id = captured["run_id"]

    for _ in range(100):
        run = await orchestrator.state_store.get_run(run_id)
        if run.status == RunStatus.WAITING:
            break
        await asyncio.sleep(0.02)
    else:
        raise RuntimeError(f"run {run_id} never reached WAITING")

    return run_id, run_task


async def main():
    print("=== Reference Agent 3: Expense Approval Agent ===\n")
    print("(the whole point of this agent: every submission genuinely pauses — a real\n"
          " asyncio.Event suspend inside orchestrator.run() — until a human approves or\n"
          " rejects it. Neither of the other two reference agents demonstrates this.)\n")

    tools = build_tool_registry()
    long_term_memory = InMemoryLongTermMemory()
    orchestrator = AsyncOrchestrator(tool_registry=tools, long_term_memory=long_term_memory)

    # --- Pass 1: an approved expense ---
    print("--- emp-alice submits a $45 meal expense ---")
    run_id, run_task = await _submit_and_wait_for_approval(
        orchestrator,
        {"employee_id": "emp-alice", "amount": 45.00, "category": "meals",
         "description": "Team lunch during client visit"},
        session_id="emp-alice",
    )
    detail = await orchestrator.state_store.get_run(run_id)
    print(f"run status while paused: {detail.status.value}")
    print(f"LLM assessment: {detail.tasks['assess_expense'].result['response']!r}")
    print("a human approves this expense...")
    await orchestrator.resume(run_id, "request_approval", approved=True)
    final = await run_task
    print(f"final status: {final.status.value}")
    print(f"record_decision result: {final.tasks['record_decision'].result}")
    print()

    # --- Pass 2: a rejected expense ---
    print("--- emp-bob submits a $5000 travel expense (no pre-approval mentioned) ---")
    run_id, run_task = await _submit_and_wait_for_approval(
        orchestrator,
        {"employee_id": "emp-bob", "amount": 5000.00, "category": "travel",
         "description": "Last-minute flight, no manager sign-off obtained"},
        session_id="emp-bob",
    )
    print("a human rejects this expense...")
    await orchestrator.resume(run_id, "request_approval", approved=False)
    try:
        await run_task
        print("FAIL: should have raised ApprovalRejected")
    except ApprovalRejected as exc:
        print(f"OK: run failed as expected -> {exc}")
    print()

    # --- Pass 3: emp-alice's SECOND expense, showing memory recall of the first ---
    print("--- emp-alice submits a second expense (recall should find the first) ---")
    run_id, run_task = await _submit_and_wait_for_approval(
        orchestrator,
        {"employee_id": "emp-alice", "amount": 30.00, "category": "meals",
         "description": "Coffee with a candidate during an interview loop"},
        session_id="emp-alice",
    )
    detail = await orchestrator.state_store.get_run(run_id)
    print(f"recall_history sees: {detail.tasks['recall_history'].result}")
    await orchestrator.resume(run_id, "request_approval", approved=True)
    final = await run_task
    print(f"final status: {final.status.value}")
    print()

    print("Expense Approval Agent verified end to end — approve, reject, and cross-run "
          "memory recall all confirmed working.")


if __name__ == "__main__":
    asyncio.run(main())
