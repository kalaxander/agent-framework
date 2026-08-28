# Phases.md — Build Plan

Each phase should be runnable/testable before moving to the next. See `Memory.md` for live
status and any deviations made during the build.

## Phase 1 — Core Flow Model ✅ done
- `Task`, `Flow` (DAG) definitions (stdlib dataclasses — see Memory.md for why).
- In-memory DAG resolution (topological execution order, dependency validation, cycle detection).
- Minimal synchronous executor (`SyncExecutor`) that runs a flow of pure-Python tasks end to end.

## Phase 2 — Orchestrator & State Store ✅ done
- `AsyncOrchestrator`: run lifecycle (queued/running/waiting/succeeded/failed/cancelled),
  concurrent execution of independent tasks (`Flow.levels()`), retry policy (max attempts,
  backoff) + per-task timeout — same guarantees as Phase 1, now async and persisted.
- `StateStore` interface + `InMemoryStateStore` (reference/test) +
  `PostgresStateStore` (production, `integrations/postgres_state_store.py`, same interface).

## Phase 3 — Ingress & Executors ✅ done
- REST ingress: `POST /v1/runs`, `GET /v1/runs/{id}`, `GET /v1/runs/{id}/audit` — stdlib
  reference server (`io/rest_ingress.py`) + production FastAPI adapter, same routes
  (`integrations/fastapi_ingress.py`).
- `MessageQueue` interface + `InMemoryMessageQueue` (reference) + `KafkaMessageQueue`
  (production, `integrations/kafka_message_queue.py`).
- `ExecutorWorker` (`io/worker.py`): consumes `RunRequest`s off a queue, runs the flow via the
  Phase 2 `AsyncOrchestrator`, publishes a result message, dispatches output actions. Run 1+ as
  separate processes against a shared queue + state store for real horizontal scaling.
- Output actions: `LogOutputAction` (default/test) + `WebhookOutputAction` (stdlib `urllib`,
  POSTs JSON) — see docs/Memory.md for the per-task-vs-per-flow executor scope note.

## Phase 4 — Tools Registry ✅ done
- `Tool` interface (`tools/base.py`) + `ToolRegistry` (`tools/registry.py`): register by name,
  resolve at run time via `Task(tool="name", tool_input=...)`, wired into both `SyncExecutor`
  and `AsyncOrchestrator` (pass `tool_registry=...` to either).
- Built-in tools: `HttpTool` (stdlib `urllib`, real HTTP calls), `LlmTool` + pluggable
  `LLMProvider` interface (`MockLLMProvider` reference/test implementation — no API key needed;
  swap in a real provider without touching `LlmTool`), `SimpleSearchTool` (in-memory keyword
  search reference implementation — swap for a real search/vector backend later).
- Tool-level guardrail hooks: `Tool.validate_input`/`validate_output`, called automatically by
  `ToolRegistry.invoke()`, raising `GuardrailViolation` to reject.

## Phase 5 — Memory ✅ done
- `ShortTermMemory` interface (`memory/base.py`) + `InMemoryShortTermMemory` (reference) +
  `RedisShortTermMemory` (production, `integrations/redis_memory.py`, lazy-import) — per-run
  scratchpad, keyed by `run_id`.
- `LongTermMemory` interface + `InMemoryLongTermMemory` (reference, keyword-scored recall) +
  `ChromaLongTermMemory` (production, `integrations/vector_memory.py`, lazy-import, real
  embedding-based semantic recall) — cross-run/session memory, keyed by `session_id`.
- `MemoryHandle`: injected into task context as `context["__memory__"]` when either store is
  configured on the executor/orchestrator (`remember_short`/`recall_short`/`remember_long`/
  `recall_long`/`forget_long`). `AsyncOrchestrator.run()`/`SyncExecutor.run()` both take an
  optional `session_id` param (defaults to the run's own id, i.e. no cross-run recall unless a
  stable session_id is explicitly passed in).

## Phase 6 — Guardrails & Observability ✅ done
- `Guardrail` interface (`guardrails/base.py`, sync `pre_execute`/`post_execute`) — composable:
  attach per `Task(guardrails=[...])` and/or per `Flow(guardrails=[...])` (flow-level applies to
  every task). Built-in guardrails (`guardrails/builtin.py`): `RequiredKeysGuardrail`,
  `RateLimitGuardrail`, `BudgetGuardrail` (pre-execution), `ContentFilterGuardrail`
  (post-execution).
- Fail-closed retries: any `GuardrailViolation` (or other `AgentFrameworkError` with
  `retryable=False`) is never retried, regardless of the task's `retry_policy.max_attempts` —
  enforced in both `SyncExecutor` and `AsyncOrchestrator`.
- Structured logging: `JsonLineLogger` (`observability/logger.py`), one JSON line per event,
  matching `docs/Design.md`'s format.
- Metrics: `InMemoryMetrics`/`TaskMetric` (`observability/metrics.py`) — success rate, avg
  latency, retry count, token/cost usage (pulled automatically from a task result's `"usage"`
  key, e.g. `LlmTool`'s output), aggregated overall and per-task via `.summary()`.
- Audit-trail query API: `AsyncOrchestrator.audit_trail(run_id)` — thin passthrough to the
  Phase 2 `StateStore`.

## Phase 7 — Apache Integration Depth ✅ done
- Airflow adapter (`integrations/airflow_adapter.py`): `compile_to_airflow_dag()` generates
  Airflow DAG source from a Flow (task per `PythonOperator`, dependency edges via `>>`);
  `run_task_for_airflow()` is what each generated task calls at run time — rebuilds the Flow,
  pulls upstream results from Airflow's XCom, and runs the task through
  `SyncExecutor._run_task_with_retry` (same retries/guardrails/audit log as everywhere else).
  Verified: the generated DAG source is checked for syntactic validity and correct structure,
  and `run_task_for_airflow` is run against a stub XCom and checked to produce results identical
  to `SyncExecutor` running the same Flow directly — see `run_demo_phase7.py`.
- Camel route (`integrations/camel/route.yaml`): a real Camel YAML-DSL route bridging a
  file-drop folder to this framework's REST ingress. Documented rather than executed — Camel is
  a JVM framework with no Python binding, and this sandbox has no JVM/network access; see
  `integrations/camel/README.md`.
- "Core engine vs. optional Apache adapter": everything in `core/`, `io/`, `tools/`, `memory/`,
  `guardrails/`, `observability/` has zero required dependencies and runs standalone (see
  `docs/Memory.md`'s pydantic-drop note from Phase 1). Every Apache/production piece
  (Kafka, Postgres, Redis, Chroma, Airflow, Camel) lives under `integrations/`, is lazy-imported
  or documentation-only, and the core never imports from `integrations/`.

## Phase 8 — Reference Agents (2 minimum) ✅ done
- **Agent 1 — Customer Support Agent** (`examples/customer_support_agent/`): ingest a ticket,
  search a KB for relevant docs (Phase 4 tool), recall the customer's past tickets (Phase 5
  long-term memory, `session_id=customer_id`), draft a reply, guardrail-check it (Phase 6), and
  remember the ticket. Output dispatched via the Phase 3 queue-driven `ExecutorWorker` to a log
  action and a real local webhook.
- **Agent 2 — Research Agent** (`examples/research_agent/`): ingest a question, search → fetch
  (real HTTP) → summarize → draft, then a bounded critique-and-revise reflection pass (stretch
  goal) — a plain-Python critique step catches a real citation gap in the draft, and the final
  LLM pass closes it, verified programmatically rather than eyeballed. A flow-level
  `RateLimitGuardrail` caps total calls across the whole flow.
- Both verified end to end (`run.py` in each directory) and covered in `smoke_test.py` (42
  checks total) — including a targeted check that the research agent's reflection pass actually
  fixes the specific gap its own critique step found, not just "runs without erroring."

## Phase 9 — Design Doc & Wrap-up ✅ done
- `Design.md`/`Architecture.md` finalized with an explicit deviations section each, documenting
  where the as-built system differs from the original draft (stdlib-only core, flow-level
  executor decoupling, no YAML flow loader, no `/v1/flows` endpoint, non-schema-first tool
  validation, error response shape, log format) — see `docs/Memory.md` for the full reasoning
  log.
- Performance benchmarks (`benchmarks/run_benchmarks.py`, real measured numbers in
  `benchmarks/results.md`, regenerated by re-running the script — not hand-written): per-run
  orchestration overhead for both executors, the concurrency benefit of `AsyncOrchestrator`'s
  level-based parallelism vs. `SyncExecutor`'s strictly-sequential execution on independent
  tasks, and retry-path overhead.
- Stretch goal closed: **human-in-the-loop pause/resume**. `Task(requires_approval=True)` +
  `AsyncOrchestrator.resume(run_id, task_name, approved)` — a real `asyncio.Event`-based
  suspend, not polling — finally implements `RunStatus.WAITING`, which had been stubbed since
  Phase 2. Verified in `run_demo_phase9.py` (both approved and rejected paths, with the run
  observed to actually reach `WAITING` before being resumed) and 2 `smoke_test.py` checks (45
  total).
- Stretch goals not done: multi-agent collaboration (no primitive for one flow to spawn/await
  another); the research agent's reflection pass is bounded/single-pass, not an open-ended loop
  (no "repeat until satisfied" construct exists in the Flow/Task model) — see open questions in
  `docs/Memory.md`.
