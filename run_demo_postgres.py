"""Verify PostgresStateStore against a REAL Postgres database — the "does state actually
persist, not just live in a Python dict" verification (docs/Architecture.md > State & Memory).

Setup — get a free Postgres in a couple minutes:
    Supabase: https://supabase.com  (New project -> Settings -> Database -> Connection string,
              "URI" tab, mode "Session" not "Transaction")
    Neon:     https://neon.tech     (New project -> Connection Details -> copy the connection
              string)

Then:
    cd sdk && pip install -e ".[storage]" && cd ..
    export DATABASE_URL="postgresql://user:password@host:port/dbname"
    python3 run_demo_postgres.py

What this actually proves (that in-memory-only demos can't): the run is written by one
PostgresStateStore instance, that instance's connection pool is fully torn down (simulating the
process exiting), and a SECOND, independent PostgresStateStore instance — a fresh connection,
nothing shared in Python memory — reads the exact same run back. If that works, your data
genuinely survives a restart, not just a function call.

Honesty note (see docs/Memory.md): I (Claude, building this) have no network access to a
Postgres instance in my sandbox, so this script's logic has been reviewed carefully but never
executed against a real database. Running it yourself is the actual first live-database
verification of this integration — the same situation the Gemini provider was in before you
ran it (and found a real bug in the process).
"""
from __future__ import annotations

import asyncio
import os
import sys

sys.path.insert(0, "sdk")

from agentframework import Flow, Task
from agentframework.core.orchestrator import AsyncOrchestrator


def build_flow() -> Flow:
    flow = Flow(name="postgres-persistence-check")
    flow.add_task(Task(name="step_one", fn=lambda ctx: {"note": "hello from Postgres"}))
    flow.add_task(Task(name="step_two", fn=lambda ctx: f"processed: {ctx['step_one']['note']}",
                        depends_on=["step_one"]))
    return flow


async def main():
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        print(
            "No DATABASE_URL environment variable found.\n\n"
            "To run this against a real (free) Postgres:\n"
            "  1. Get one at https://supabase.com or https://neon.tech (a couple minutes, no "
            "card needed for the free tier)\n"
            "  2. cd sdk && pip install -e \".[storage]\" && cd ..\n"
            "  3. export DATABASE_URL=\"postgresql://user:password@host:port/dbname\"\n"
            "  4. python3 run_demo_postgres.py\n"
        )
        return

    try:
        import asyncpg  # noqa: F401
    except ImportError:
        print("The 'asyncpg' package isn't installed. Run: cd sdk && pip install -e \".[storage]\"")
        return

    from agentframework.integrations.postgres_state_store import PostgresStateStore

    print("=== Writing a run to real Postgres ===\n")
    writer_store = PostgresStateStore(dsn)
    try:
        await writer_store.init_schema()
        orchestrator = AsyncOrchestrator(state_store=writer_store)
        run = await orchestrator.run(build_flow(), inputs={})
        run_id = run.run_id
        print(f"run_id: {run_id}")
        print(f"status: {run.status.value}")
        print(f"step_two result: {run.tasks['step_two'].result}")
    finally:
        await writer_store.close()  # fully tear down this connection pool

    print("\nConnection pool closed — simulating this process having exited.\n")
    print("=== Reading the SAME run back with a brand new, independent connection ===\n")

    reader_store = PostgresStateStore(dsn)  # a completely separate instance, no shared Python state
    try:
        recovered_run = await reader_store.get_run(run_id)
        if recovered_run is None:
            print("FAIL: run not found — persistence did NOT work.")
            return

        print(f"recovered status: {recovered_run.status.value}")
        print(f"recovered step_two result: {recovered_run.tasks['step_two'].result}")

        audit = await reader_store.audit_trail(run_id)
        print(f"\naudit trail ({len(audit)} task states):")
        for state in audit:
            print(f"  {state.name}: {state.status.value}, attempt {state.attempt}")

        matches = (
            recovered_run.status == run.status and
            recovered_run.tasks["step_two"].result == run.tasks["step_two"].result
        )
        print(f"\ndata survived the simulated restart intact: {matches}")
    finally:
        await reader_store.close()


if __name__ == "__main__":
    asyncio.run(main())
