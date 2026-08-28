"""Production StateStore backed by Postgres (docs/Architecture.md > State & Memory).

Implements the same `StateStore` interface as `core.state_store.InMemoryStateStore` so the
Orchestrator never needs to know which backend it's talking to. Requires the `storage` extra:

    pip install -e ".[storage]"   # asyncpg

Imports are deferred into `__init__` so importing `agentframework` never requires asyncpg to be
installed — only constructing a PostgresStateStore does.

Connection string: standard `postgresql://user:password@host:port/dbname` DSN. Free-tier hosted
Postgres (Supabase, Neon, etc.) almost always requires SSL — their connection strings normally
already include `?sslmode=require`; if yours doesn't and you get an SSL-related connection
error, append it.

Schema (created by `PostgresStateStore.init_schema()`):
    runs(run_id PK, flow_name, inputs JSONB, status, created_at, updated_at)
    task_states(run_id FK, task_name, status, attempt, result JSONB, error, started_at,
                finished_at, PRIMARY KEY (run_id, task_name))

Note on JSONB parameters: asyncpg does not auto-encode Python str -> jsonb without either a
custom type codec or an explicit `::jsonb` cast in the SQL. This file uses the explicit-cast
approach (simpler, no per-connection codec setup needed) — every INSERT/UPDATE touching a jsonb
column casts its parameter with `::jsonb`. Reads are unaffected: asyncpg returns jsonb columns
as plain text by default, which is exactly what `json.loads()` expects.
"""
from __future__ import annotations

import json
from typing import Any, Optional

from agentframework.core.state_store import RunRecord, RunStatus, StateStore, TaskState, TaskStatus

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    flow_name TEXT NOT NULL,
    inputs JSONB NOT NULL,
    status TEXT NOT NULL,
    created_at DOUBLE PRECISION NOT NULL,
    updated_at DOUBLE PRECISION NOT NULL
);
CREATE TABLE IF NOT EXISTS task_states (
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    task_name TEXT NOT NULL,
    status TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    result JSONB,
    error TEXT,
    started_at DOUBLE PRECISION,
    finished_at DOUBLE PRECISION,
    PRIMARY KEY (run_id, task_name)
);
"""


class PostgresStateStore(StateStore):
    def __init__(self, dsn: str):
        try:
            import asyncpg  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "PostgresStateStore requires the 'storage' extra: "
                "pip install -e '.[storage]'"
            ) from exc
        self._dsn = dsn
        self._pool = None

    async def _get_pool(self):
        import asyncpg

        if self._pool is None:
            self._pool = await asyncpg.create_pool(self._dsn)
        return self._pool

    async def close(self) -> None:
        """Release the connection pool. Call this on shutdown (or in a test's finally block) —
        otherwise the process may hang waiting for pooled connections to be garbage collected."""
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def init_schema(self) -> None:
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            await conn.execute(_SCHEMA_SQL)

    async def create_run(self, run: RunRecord) -> None:
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO runs (run_id, flow_name, inputs, status, created_at, updated_at) "
                "VALUES ($1, $2, $3::jsonb, $4, $5, $6)",
                run.run_id, run.flow_name, json.dumps(run.inputs), run.status.value,
                run.created_at, run.updated_at,
            )

    async def get_run(self, run_id: str) -> Optional[RunRecord]:
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM runs WHERE run_id = $1", run_id)
            if row is None:
                return None
            task_rows = await conn.fetch(
                "SELECT * FROM task_states WHERE run_id = $1", run_id
            )
        run = RunRecord(
            run_id=row["run_id"], flow_name=row["flow_name"],
            inputs=json.loads(row["inputs"]), status=RunStatus(row["status"]),
            created_at=row["created_at"], updated_at=row["updated_at"],
        )
        for tr in task_rows:
            run.tasks[tr["task_name"]] = TaskState(
                name=tr["task_name"], status=TaskStatus(tr["status"]), attempt=tr["attempt"],
                result=json.loads(tr["result"]) if tr["result"] else None, error=tr["error"],
                started_at=tr["started_at"], finished_at=tr["finished_at"],
            )
        return run

    async def update_run_status(self, run_id: str, status: RunStatus) -> None:
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE runs SET status = $1, updated_at = extract(epoch from now()) "
                "WHERE run_id = $2",
                status.value, run_id,
            )

    async def update_task_state(self, run_id: str, task_state: TaskState) -> None:
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO task_states (run_id, task_name, status, attempt, result, error, "
                "started_at, finished_at) VALUES ($1,$2,$3,$4,$5::jsonb,$6,$7,$8) "
                "ON CONFLICT (run_id, task_name) DO UPDATE SET "
                "status=EXCLUDED.status, attempt=EXCLUDED.attempt, result=EXCLUDED.result, "
                "error=EXCLUDED.error, started_at=EXCLUDED.started_at, "
                "finished_at=EXCLUDED.finished_at",
                run_id, task_state.name, task_state.status.value, task_state.attempt,
                json.dumps(task_state.result) if task_state.result is not None else None,
                task_state.error, task_state.started_at, task_state.finished_at,
            )

    async def audit_trail(self, run_id: str) -> list[TaskState]:
        run = await self.get_run(run_id)
        if run is None:
            return []
        return [run.tasks[name] for name in sorted(run.tasks)]
