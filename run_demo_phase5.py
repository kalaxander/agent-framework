"""Run Phase 5 end to end: short-term memory (per-run scratchpad) and long-term memory
(cross-run/session remember+recall), both injected into task context as context["__memory__"].
No external dependencies. Run with:
    python3 run_demo_phase5.py
"""
from __future__ import annotations

import asyncio
import sys

sys.path.insert(0, "sdk")

from agentframework import Flow, Task
from agentframework.core.orchestrator import AsyncOrchestrator
from agentframework.memory.in_memory import InMemoryShortTermMemory, InMemoryLongTermMemory


async def demo_short_term():
    print("=== Phase 5: short-term memory (per-run scratchpad) ===")

    async def note_preference(ctx):
        await ctx["__memory__"].remember_short("tone", "friendly and concise")
        return "noted"

    async def draft_using_preference(ctx):
        tone = await ctx["__memory__"].recall_short("tone")
        return f"Draft written in a '{tone}' tone."

    flow = Flow(name="scratchpad-demo")
    flow.add_task(Task(name="note_preference", fn=note_preference))
    flow.add_task(Task(name="draft", fn=draft_using_preference, depends_on=["note_preference"]))

    short_term = InMemoryShortTermMemory()
    orchestrator = AsyncOrchestrator(short_term_memory=short_term)
    run = await orchestrator.run(flow, inputs={})
    print(f"draft result: {run.tasks['draft'].result}")

    # scratchpad is per-run: a second run shouldn't see the first run's note.
    run2 = await orchestrator.run(flow, inputs={})
    print(f"second run also works independently: {run2.tasks['draft'].result}")
    print()


async def demo_long_term():
    print("=== Phase 5: long-term memory (cross-run recall, same session_id) ===")

    long_term = InMemoryLongTermMemory()
    orchestrator = AsyncOrchestrator(long_term_memory=long_term)
    session_id = "user-42"

    async def remember_ticket(ctx):
        await ctx["__memory__"].remember_long(
            "Customer reported a billing overcharge on invoice #881, refunded on 2026-07-01.",
            metadata={"ticket_id": 881},
        )
        return "remembered"

    async def remember_other_ticket(ctx):
        await ctx["__memory__"].remember_long(
            "Customer asked about shipping delay for order #204, resolved with expedited shipping.",
            metadata={"ticket_id": 204},
        )
        return "remembered"

    flow1 = Flow(name="ticket-881")
    flow1.add_task(Task(name="remember", fn=remember_ticket))
    await orchestrator.run(flow1, inputs={}, session_id=session_id)

    flow2 = Flow(name="ticket-204")
    flow2.add_task(Task(name="remember", fn=remember_other_ticket))
    await orchestrator.run(flow2, inputs={}, session_id=session_id)

    # A third, separate run recalls across both prior runs because they share session_id.
    async def recall_billing_history(ctx):
        records = await ctx["__memory__"].recall_long("billing overcharge refund", top_k=3)
        return [r.text for r in records]

    flow3 = Flow(name="new-billing-ticket")
    flow3.add_task(Task(name="recall", fn=recall_billing_history))
    run3 = await orchestrator.run(flow3, inputs={}, session_id=session_id)

    print(f"recalled (session-scoped, across 2 prior separate runs): "
          f"{run3.tasks['recall'].result}")

    # a *different* session_id must not see user-42's memories.
    async def recall_for_stranger(ctx):
        records = await ctx["__memory__"].recall_long("billing overcharge refund", top_k=3)
        return [r.text for r in records]

    flow4 = Flow(name="isolated-check")
    flow4.add_task(Task(name="recall", fn=recall_for_stranger))
    run4 = await orchestrator.run(flow4, inputs={}, session_id="user-99")
    print(f"different session_id sees nothing (isolation check): {run4.tasks['recall'].result}")
    print()


if __name__ == "__main__":
    asyncio.run(demo_short_term())
    asyncio.run(demo_long_term())
    print("Phase 5 (short-term + long-term memory) verified end to end.")
