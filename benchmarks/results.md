# Benchmark Results

Generated: 2026-08-20T09:19:10
Platform: Linux 6.18.5-fc-v20, Python 3.12.3

**Caveat**: these measure the framework's own orchestration overhead using in-memory reference implementations (InMemoryStateStore, no Kafka/Postgres/Redis) — this sandbox has no network access to run those, and their latency would dominate these numbers anyway. Re-run `python3 benchmarks/run_benchmarks.py` on your own machine; these are not portable performance guarantees, just this run's actual measurements.

## SyncExecutor overhead (sequential, near-zero-work tasks)
```
n_runs: 200
tasks_per_run: 5
avg_ms_per_run: 0.65
p50_ms_per_run: 0.589
p95_ms_per_run: 1.19
avg_ms_per_task: 0.13
runs_per_second: 1538.3
```

## AsyncOrchestrator overhead (sequential-dependency flow, same task count)
```
n_runs: 200
tasks_per_run: 5
avg_ms_per_run: 0.478
p50_ms_per_run: 0.471
p95_ms_per_run: 0.55
avg_ms_per_task: 0.0957
runs_per_second: 2090.0
```

## Concurrency benefit (independent tasks, AsyncOrchestrator vs. SyncExecutor)
```
n_tasks: 10
task_work_ms: 20.0
sync_executor_total_ms: 204.49
async_orchestrator_total_ms: 42.65
speedup: 4.79
theoretical_min_ms_if_fully_parallel: 20.0
```

## Retry overhead (AsyncOrchestrator, 2 failures then success vs. immediate success)
```
n_runs: 100
immediate_success_avg_ms: 0.117
two_retries_then_success_avg_ms: 2.701
retry_overhead_ms: 2.584
note: retry_policy.backoff_seconds=0.001 for this benchmark; production backoff values will dominate this number — this isolates the framework's own overhead per retry attempt, not backoff sleep time.
```
