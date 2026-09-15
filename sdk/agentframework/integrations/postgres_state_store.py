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

Schema (created by `PostgresStateStore.init_schema()`) — see docs/Memory.md for the "database
integration" decision log entry this replaced the original two-table (runs, task_states) schema
with:

    runs(run_id PK, flow_name, inputs JSONB, status, created_at, updated_at)

    task_executions(execution_id PK, run_id FK, task_name, attempt, status, result JSONB, error,
                     started_at, finished_at, UNIQUE(run_id, task_name, attempt))
        Append-only: one row per attempt, never overwritten. The original task_states table used
        an upsert on (run_id, task_name), which silently destroyed every earlier attempt's
        record on retry — this table exists specifically to stop losing that history.

    audit_logs(id PK, run_id FK, task_name NULLABLE, event, ts)
        Append-only durable state-transition log (RUN_CREATED, TASK_FAILED,
        RUN_WAITING_APPROVAL, ...) — independent of task_executions; task_name is NULL for
        run-level events.

Note on JSONB parameters: asyncpg does not auto-encode Python str -> jsonb without either a
custom type codec or an explicit `::jsonb` cast in the SQL. This file uses the explicit-cast
approach (simpler, no per-connection codec setup needed) — every INSERT/UPDATE touching a jsonb
column casts its parameter with `::jsonb`. Reads are unaffected: asyncpg returns jsonb columns
as plain text by default, which is exactly what `json.loads()` expects.

Error handling: every method wraps asyncpg exceptions in `core.errors.StateStoreError` so
callers (and fastapi_ingress's global AgentFrameworkError handler) never see a raw driver
exception cross the StateStore interface boundary. A UNIQUE violation on
(run_id, task_name, attempt) — which would mean the same attempt was recorded twice — is
surfaced as non-retryable (it's a bug, not a transient condition); connection-level failures are
retryable.
"""
from __future__ import annotations

import json
from typing import Any, Optional

from agentframework.core.errors import StateStoreError
from agentframework.core.state_store import (
    AuditLogEntry,
    RunRecord,
    RunStatus,
    StateStore,
    TaskState,
    TaskStatus,
)

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    flow_name TEXT NOT NULL,
    inputs JSONB NOT NULL,
    status TEXT NOT NULL,
    created_at DOUBLE PRECISION NOT NULL,
    updated_at DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_runs_flow_name_created_at ON runs (flow_name, created_at);

CREATE TABLE IF NOT EXISTS task_executions (
    execution_id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    task_name TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    status TEXT NOT NULL,
    result JSONB,
    error TEXT,
    started_at DOUBLE PRECISION,
    finished_at DOUBLE PRECISION,
    UNIQUE (run_id, task_name, attempt)
);
CREATE INDEX IF NOT EXISTS idx_task_executions_run_task
    ON task_executions (run_id, task_name);
CREATE INDEX IF NOT EXISTS idx_task_executions_task_status
    ON task_executions (task_name, status);

CREATE TABLE IF NOT EXISTS audit_logs (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    task_name TEXT,
    event TEXT NOT NULL,
    ts DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_audit_logs_run_ts ON audit_logs (run_id, ts);

CREATE OR REPLACE VIEW run_summary AS
SELECT
    r.run_id,
    r.flow_name,
    r.status,
    COUNT(te.execution_id) FILTER (WHERE te.attempt = 1) AS task_count,
    COUNT(DISTINCT te.task_name) FILTER (WHERE te.status = 'failed') AS failed_task_count,
    r.updated_at - r.created_at AS total_duration_seconds
FROM runs r
LEFT JOIN task_executions te ON te.run_id = r.run_id
GROUP BY r.run_id, r.flow_name, r.status, r.updated_at, r.created_at;
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
        try:
            async with pool.acquire() as conn:
                await conn.execute(_SCHEMA_SQL)
        except Exception as exc:
            raise StateStoreError(f"failed to initialize schema: {exc}", retryable=True) from exc

    async def create_run(self, run: RunRecord) -> None:
        pool = await self._get_pool()
        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO runs (run_id, flow_name, inputs, status, created_at, "
                    "updated_at) VALUES ($1, $2, $3::jsonb, $4, $5, $6)",
                    run.run_id, run.flow_name, json.dumps(run.inputs), run.status.value,
                    run.created_at, run.updated_at,
                )
        except Exception as exc:
            raise StateStoreError(f"create_run failed for {run.run_id}: {exc}",
                                   retryable=True) from exc

    async def get_run(self, run_id: str) -> Optional[RunRecord]:
        pool = await self._get_pool()
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow("SELECT * FROM runs WHERE run_id = $1", run_id)
                if row is None:
                    return None
                # DISTINCT ON (task_name) ... ORDER BY task_name, attempt DESC: the *latest*
                # attempt per task, matching run.tasks' pre-existing "latest state" contract —
                # full history lives in task_executions and is reached via get_execution_history.
                task_rows = await conn.fetch(
                    "SELECT DISTINCT ON (task_name) * FROM task_executions "
                    "WHERE run_id = $1 ORDER BY task_name, attempt DESC",
                    run_id,
                )
        except Exception as exc:
            raise StateStoreError(f"get_run failed for {run_id}: {exc}", retryable=True) from exc
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
        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    "UPDATE runs SET status = $1, updated_at = extract(epoch from now()) "
                    "WHERE run_id = $2",
                    status.value, run_id,
                )
        except Exception as exc:
            raise StateStoreError(f"update_run_status failed for {run_id}: {exc}",
                                   retryable=True) from exc

    async def update_task_state(self, run_id: str, task_state: TaskState) -> None:
        """Called once per attempt-start (status=RUNNING) and again once per attempt-end
        (SUCCEEDED/FAILED). Only the attempt-end call writes a task_executions row — one row per
        (run_id, task_name, attempt), matching the UNIQUE constraint; writing on RUNNING too
        would mean two rows for one attempt and violate it. The RUNNING call still gets a
        TASK_STARTED audit event, so "an attempt began" isn't lost, just not duplicated into
        task_executions. The terminal-status path writes its task_executions row and matching
        audit event in the SAME transaction — a task's execution row and its audit trail must
        never disagree about whether that attempt succeeded or failed (see docs/Memory.md,
        Phase 6: "the framework must never end up with execution=completed but result=missing")."""
        pool = await self._get_pool()
        if task_state.status == TaskStatus.RUNNING:
            try:
                async with pool.acquire() as conn:
                    await conn.execute(
                        "INSERT INTO audit_logs (run_id, task_name, event, ts) "
                        "VALUES ($1, $2, 'TASK_STARTED', extract(epoch from now()))",
                        run_id, task_state.name,
                    )
            except Exception as exc:
                raise StateStoreError(f"update_task_state (start) failed for {run_id}: {exc}",
                                       retryable=True) from exc
            return

        event = "TASK_SUCCEEDED" if task_state.status == TaskStatus.SUCCEEDED else "TASK_FAILED"
        try:
            async with pool.acquire() as conn, conn.transaction():
                await conn.execute(
                    "INSERT INTO task_executions (run_id, task_name, attempt, status, result, "
                    "error, started_at, finished_at) VALUES ($1,$2,$3,$4,$5::jsonb,$6,$7,$8)",
                    run_id, task_state.name, task_state.attempt, task_state.status.value,
                    json.dumps(task_state.result) if task_state.result is not None else None,
                    task_state.error, task_state.started_at, task_state.finished_at,
                )
                await conn.execute(
                    "INSERT INTO audit_logs (run_id, task_name, event, ts) "
                    "VALUES ($1, $2, $3, extract(epoch from now()))",
                    run_id, task_state.name, event,
                )
        except Exception as exc:
            import asyncpg
            if isinstance(exc, asyncpg.UniqueViolationError):
                raise StateStoreError(
                    f"duplicate execution attempt recorded for run {run_id}, "
                    f"task {task_state.name}, attempt {task_state.attempt}: {exc}",
                    retryable=False,
                ) from exc
            raise StateStoreError(f"update_task_state failed for {run_id}: {exc}",
                                   retryable=True) from exc

    async def audit_trail(self, run_id: str) -> list[TaskState]:
        """Unchanged contract: every task's *latest* state, in stable order — see docstring at
        the top of core/state_store.py for why this stays separate from get_audit_events()."""
        run = await self.get_run(run_id)
        if run is None:
            return []
        return [run.tasks[name] for name in sorted(run.tasks)]

    async def get_execution_history(
        self, run_id: str, task_name: Optional[str] = None
    ) -> list[TaskState]:
        pool = await self._get_pool()
        try:
            async with pool.acquire() as conn:
                if task_name is not None:
                    rows = await conn.fetch(
                        "SELECT * FROM task_executions WHERE run_id = $1 AND task_name = $2 "
                        "ORDER BY attempt",
                        run_id, task_name,
                    )
                else:
                    rows = await conn.fetch(
                        "SELECT * FROM task_executions WHERE run_id = $1 "
                        "ORDER BY task_name, attempt",
                        run_id,
                    )
        except Exception as exc:
            raise StateStoreError(f"get_execution_history failed for {run_id}: {exc}",
                                   retryable=True) from exc
        return [
            TaskState(
                name=r["task_name"], status=TaskStatus(r["status"]), attempt=r["attempt"],
                result=json.loads(r["result"]) if r["result"] else None, error=r["error"],
                started_at=r["started_at"], finished_at=r["finished_at"],
            )
            for r in rows
        ]

    async def record_audit_event(
        self, run_id: str, event: str, task_name: Optional[str] = None
    ) -> None:
        pool = await self._get_pool()
        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO audit_logs (run_id, task_name, event, ts) "
                    "VALUES ($1, $2, $3, extract(epoch from now()))",
                    run_id, task_name, event,
                )
        except Exception as exc:
            raise StateStoreError(f"record_audit_event failed for {run_id}: {exc}",
                                   retryable=True) from exc

    async def get_audit_events(self, run_id: str) -> list[AuditLogEntry]:
        pool = await self._get_pool()
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT * FROM audit_logs WHERE run_id = $1 ORDER BY ts, id", run_id
                )
        except Exception as exc:
            raise StateStoreError(f"get_audit_events failed for {run_id}: {exc}",
                                   retryable=True) from exc
        return [
            AuditLogEntry(run_id=r["run_id"], event=r["event"], task_name=r["task_name"],
                          ts=r["ts"])
            for r in rows
        ]

    async def get_agent_statistics(self, flow_name: str) -> dict[str, Any]:
        """Analytics query (docs/Memory.md, Phase 10): run counts/failure rate for one agent
        (flow_name) plus per-task average duration and retry counts from task_executions."""
        pool = await self._get_pool()
        try:
            async with pool.acquire() as conn:
                run_stats = await conn.fetchrow(
                    "SELECT COUNT(*) AS total_runs, "
                    "COUNT(*) FILTER (WHERE status = 'succeeded') AS succeeded_runs, "
                    "COUNT(*) FILTER (WHERE status = 'failed') AS failed_runs "
                    "FROM runs WHERE flow_name = $1",
                    flow_name,
                )
                task_stats = await conn.fetch(
                    "SELECT te.task_name, "
                    "COUNT(*) AS total_attempts, "
                    "COUNT(*) FILTER (WHERE te.status = 'failed') AS failed_attempts, "
                    "AVG(te.finished_at - te.started_at) FILTER (WHERE te.finished_at IS NOT NULL) "
                    "AS avg_duration_seconds, "
                    "MAX(te.attempt) AS max_attempt "
                    "FROM task_executions te JOIN runs r ON r.run_id = te.run_id "
                    "WHERE r.flow_name = $1 GROUP BY te.task_name ORDER BY te.task_name",
                    flow_name,
                )
        except Exception as exc:
            raise StateStoreError(f"get_agent_statistics failed for {flow_name}: {exc}",
                                   retryable=True) from exc
        total = run_stats["total_runs"] or 0
        failed = run_stats["failed_runs"] or 0
        return {
            "flow_name": flow_name,
            "total_runs": total,
            "succeeded_runs": run_stats["succeeded_runs"] or 0,
            "failed_runs": failed,
            "failure_rate": (failed / total) if total else 0.0,
            "tasks": [
                {
                    "task_name": t["task_name"],
                    "total_attempts": t["total_attempts"],
                    "failed_attempts": t["failed_attempts"],
                    "avg_duration_seconds": float(t["avg_duration_seconds"])
                    if t["avg_duration_seconds"] is not None else None,
                    "max_attempt": t["max_attempt"],
                }
                for t in task_stats
            ],
        }
