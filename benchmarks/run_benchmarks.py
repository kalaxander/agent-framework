"""Phase 9 — performance benchmarks (docs/PRD.md > Deliverables: "performance benchmarks").
Measures actual overhead/throughput of this framework's core (no external services — Kafka/
Postgres/Redis introduce their own latency that would swamp the framework's own overhead and
this sandbox can't run them anyway; see docs/benchmarks/results.md for that caveat stated
plainly). Run with:
    python3 benchmarks/run_benchmarks.py
Writes results to benchmarks/results.md (raw numbers from whatever machine actually ran it —
re-run and regenerate rather than trusting numbers from a different machine).
"""
from __future__ import annotations

import asyncio
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "sdk"))

from agentframework import Flow, Task
from agentframework.core.executor import SyncExecutor
from agentframework.core.orchestrator import AsyncOrchestrator
from agentframework.core.flow import RetryPolicy


def _noop_flow(n_tasks: int, parallel_branches: int = 1) -> Flow:
    """A flow of `n_tasks` trivial tasks. If parallel_branches > 1, the tasks are independent
    (no depends_on) so AsyncOrchestrator's level-based concurrency has something to exercise;
    otherwise they're chained sequentially so both executors do the same amount of "real" work
    per run and only orchestration overhead differs."""
    flow = Flow(name="bench")
    if parallel_branches > 1:
        for i in range(n_tasks):
            flow.add_task(Task(name=f"t{i}", fn=lambda ctx: 1))
    else:
        prev = None
        for i in range(n_tasks):
            deps = [prev] if prev else []
            flow.add_task(Task(name=f"t{i}", fn=lambda ctx: 1, depends_on=deps))
            prev = f"t{i}"
    return flow


def bench_sync_executor_overhead(n_runs: int = 200, tasks_per_run: int = 5) -> dict:
    """Wall-clock cost of SyncExecutor.run() for a flow of trivial (near-zero-work) tasks —
    isolates orchestration/state-tracking overhead from actual task work."""
    flow = _noop_flow(tasks_per_run)
    durations = []
    for _ in range(n_runs):
        start = time.perf_counter()
        SyncExecutor().run(flow, inputs={})
        durations.append((time.perf_counter() - start) * 1000)
    return _stats(durations, n_runs, tasks_per_run)


def bench_async_orchestrator_overhead(n_runs: int = 200, tasks_per_run: int = 5) -> dict:
    """Same measurement for AsyncOrchestrator (sequential-dependency flow, so this isolates
    orchestration overhead — including the extra StateStore read/writes per task — the same way
    as the SyncExecutor benchmark, for a fair comparison)."""
    flow = _noop_flow(tasks_per_run)

    async def _run_all():
        durations = []
        for _ in range(n_runs):
            start = time.perf_counter()
            await AsyncOrchestrator().run(flow, inputs={})
            durations.append((time.perf_counter() - start) * 1000)
        return durations

    durations = asyncio.run(_run_all())
    return _stats(durations, n_runs, tasks_per_run)


def bench_concurrency_benefit(n_tasks: int = 10, task_work_ms: float = 20.0) -> dict:
    """Demonstrates AsyncOrchestrator's level-based concurrency (Flow.levels()) against
    SyncExecutor's strictly-sequential execution, for `n_tasks` *independent* (parallelizable)
    tasks that each do `task_work_ms` of (simulated, blocking) work."""

    def slow_task(ctx):
        time.sleep(task_work_ms / 1000)
        return 1

    sync_flow = Flow(name="concurrency-sync")
    for i in range(n_tasks):
        sync_flow.add_task(Task(name=f"t{i}", fn=slow_task))

    start = time.perf_counter()
    SyncExecutor().run(sync_flow, inputs={})
    sync_ms = (time.perf_counter() - start) * 1000

    async_flow = Flow(name="concurrency-async")
    for i in range(n_tasks):
        async_flow.add_task(Task(name=f"t{i}", fn=slow_task))

    start = time.perf_counter()
    asyncio.run(AsyncOrchestrator().run(async_flow, inputs={}))
    async_ms = (time.perf_counter() - start) * 1000

    return {
        "n_tasks": n_tasks,
        "task_work_ms": task_work_ms,
        "sync_executor_total_ms": round(sync_ms, 2),
        "async_orchestrator_total_ms": round(async_ms, 2),
        "speedup": round(sync_ms / async_ms, 2) if async_ms > 0 else None,
        "theoretical_min_ms_if_fully_parallel": round(task_work_ms, 2),
    }


def bench_retry_overhead(n_runs: int = 100) -> dict:
    """Cost of a task that fails twice then succeeds, vs. one that succeeds immediately — i.e.
    the overhead retries/backoff add on the failure path, not just the base per-task cost."""

    def flaky(ctx):
        flaky.calls += 1
        if flaky.calls % 3 != 0:
            raise RuntimeError("transient")
        return "ok"
    flaky.calls = 0

    def immediate(ctx):
        return "ok"

    flaky_flow = Flow(name="retry-bench-flaky")
    flaky_flow.add_task(Task(
        name="t", fn=flaky,
        retry_policy=RetryPolicy(max_attempts=3, backoff_seconds=0.001, backoff_multiplier=1.0),
    ))

    immediate_flow = Flow(name="retry-bench-immediate")
    immediate_flow.add_task(Task(name="t", fn=immediate))

    async def _run_flaky():
        durations = []
        for _ in range(n_runs):
            flaky.calls = 0
            start = time.perf_counter()
            await AsyncOrchestrator().run(flaky_flow, inputs={})
            durations.append((time.perf_counter() - start) * 1000)
        return durations

    async def _run_immediate():
        durations = []
        for _ in range(n_runs):
            start = time.perf_counter()
            await AsyncOrchestrator().run(immediate_flow, inputs={})
            durations.append((time.perf_counter() - start) * 1000)
        return durations

    flaky_durations = asyncio.run(_run_flaky())
    immediate_durations = asyncio.run(_run_immediate())
    return {
        "n_runs": n_runs,
        "immediate_success_avg_ms": round(statistics.mean(immediate_durations), 3),
        "two_retries_then_success_avg_ms": round(statistics.mean(flaky_durations), 3),
        "retry_overhead_ms": round(
            statistics.mean(flaky_durations) - statistics.mean(immediate_durations), 3
        ),
        "note": "retry_policy.backoff_seconds=0.001 for this benchmark; production backoff "
                "values will dominate this number — this isolates the framework's own overhead "
                "per retry attempt, not backoff sleep time.",
    }


def _stats(durations_ms: list[float], n_runs: int, tasks_per_run: int) -> dict:
    return {
        "n_runs": n_runs,
        "tasks_per_run": tasks_per_run,
        "avg_ms_per_run": round(statistics.mean(durations_ms), 3),
        "p50_ms_per_run": round(statistics.median(durations_ms), 3),
        "p95_ms_per_run": round(statistics.quantiles(durations_ms, n=20)[18], 3)
                          if n_runs >= 20 else None,
        "avg_ms_per_task": round(statistics.mean(durations_ms) / tasks_per_run, 4),
        "runs_per_second": round(1000 / statistics.mean(durations_ms), 1),
    }


def main():
    print("Running benchmarks (this takes a few seconds)...\n")

    results = {
        "sync_executor_overhead": bench_sync_executor_overhead(),
        "async_orchestrator_overhead": bench_async_orchestrator_overhead(),
        "concurrency_benefit": bench_concurrency_benefit(),
        "retry_overhead": bench_retry_overhead(),
    }

    for name, data in results.items():
        print(f"=== {name} ===")
        for k, v in data.items():
            print(f"  {k}: {v}")
        print()

    write_results_md(results)
    print("Results written to benchmarks/results.md")


def write_results_md(results: dict) -> None:
    import datetime
    import platform

    lines = [
        "# Benchmark Results",
        "",
        f"Generated: {datetime.datetime.now().isoformat(timespec='seconds')}",
        f"Platform: {platform.system()} {platform.release()}, Python {platform.python_version()}",
        "",
        "**Caveat**: these measure the framework's own orchestration overhead using in-memory "
        "reference implementations (InMemoryStateStore, no Kafka/Postgres/Redis) — this sandbox "
        "has no network access to run those, and their latency would dominate these numbers "
        "anyway. Re-run `python3 benchmarks/run_benchmarks.py` on your own machine; these are "
        "not portable performance guarantees, just this run's actual measurements.",
        "",
        "## SyncExecutor overhead (sequential, near-zero-work tasks)",
        f"```\n{_fmt(results['sync_executor_overhead'])}\n```",
        "",
        "## AsyncOrchestrator overhead (sequential-dependency flow, same task count)",
        f"```\n{_fmt(results['async_orchestrator_overhead'])}\n```",
        "",
        "## Concurrency benefit (independent tasks, AsyncOrchestrator vs. SyncExecutor)",
        f"```\n{_fmt(results['concurrency_benefit'])}\n```",
        "",
        "## Retry overhead (AsyncOrchestrator, 2 failures then success vs. immediate success)",
        f"```\n{_fmt(results['retry_overhead'])}\n```",
        "",
    ]
    out_path = Path(__file__).resolve().parent / "results.md"
    out_path.write_text("\n".join(lines))


def _fmt(d: dict) -> str:
    return "\n".join(f"{k}: {v}" for k, v in d.items())


if __name__ == "__main__":
    main()
