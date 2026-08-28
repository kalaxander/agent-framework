# Architecture.md

## 1. High-Level Flow
```
Ingress (REST / Kafka queue)
        │
        ▼
   Orchestrator  ── reads/writes ──▶  State Store (Postgres) + Memory Store (Redis / vector DB)
        │
        ▼
    Executors  ── call ──▶  Tools / Actions (LLM, search, DB, HTTP)
        │
        ▼
  Output Actions (webhook / Kafka topic / DB write)

   Observability (logs, metrics, traces) taps every stage above.
```

## 2. Component Responsibilities

### Ingress
- **REST API** (FastAPI) — synchronous "start a flow run" / "get run status" endpoints.
- **Queue consumer** (Apache Kafka topic) — async trigger for event-driven flows.
- Both paths normalize input into a `RunRequest` and hand off to the Orchestrator.

### Orchestrator
- Owns the **flow graph** (DAG or state machine) and the **run lifecycle** (queued → running →
  waiting/human-in-the-loop → succeeded/failed/cancelled).
- Resolves task dependencies, decides what's runnable next, applies **retry/timeout policy** per
  task, and persists state transitions (for audit).
- Implemented in-house (this is the "not using crew.ai/AutoGen/n8n" core); may use **Apache
  Airflow** as the underlying DAG scheduler for batch-style flows, and a lightweight custom engine
  for low-latency conversational/event-driven flows. **Apache Camel** can handle
  routing/transformation between ingress and internal buses if needed.

### Executors
- Consume `RunRequest`s off a queue (decoupled from ingress) and execute the whole flow through
  the same `AsyncOrchestrator` used by the REST path; scale by running more worker processes
  against a shared queue + state store. **As-built note**: this decouples at the *flow* level,
  not the finer per-task Kafka task-assigned/task-completed split described below — see §6.

### Tools / Actions
- Common `Tool` interface (`name`, `run(input) -> output`, plus `validate_input`/
  `validate_output` hooks). Registry pattern: tools/output-actions are registered by name and
  resolved at run time; adding one never touches orchestrator code.

### State & Memory
- **State store** (Postgres): flow definitions, run records, task states, audit log — source of
  truth for "what happened."
- **Memory store**: short-term (per-run scratchpad, in Redis) and long-term (cross-run/session,
  vector DB e.g. Chroma/pgvector) — source of "what the agent knows."

### Guardrails
- Pluggable pre-execution (input validation, policy/permission check, rate/budget limit) and
  post-execution (output schema validation, PII/content filtering) hooks, attachable per task
  and/or per flow, run by both the sync executor and the async orchestrator around each task.
  Violations fail closed — never retried, regardless of the task's own retry policy.

### Observability
- Structured JSON logs per task/run.
- Metrics: latency, success/error rate, retries, token/cost usage per flow/task.
- Audit trail: every run reconstructable — inputs, task states, results, errors, timestamps —
  queryable through the state store.

## 3. Apache Components Used
| Concern | Component | Status |
|---|---|---|
| Messaging / task queue | Apache Kafka | Adapter built (lazy-import), verified against an in-memory reference implementation only — see §6 |
| Batch/DAG scheduling (optional) | Apache Airflow | Adapter built and verified (compiler + task-execution logic); real Airflow itself not run — see §6 |
| Ingress routing / transformation (optional) | Apache Camel | Documented (a real route config), not executed — see §6 |
| Metrics scraping (optional) | (Prometheus — not Apache, but pairs with Kafka export) | Not built; `InMemoryMetrics` is the reference collector |

## 4. Repository / Folder Structure
```
agent-framework/
├── docs/                      # PRD, Architecture, Rules, Phases, Design, Memory
├── sdk/
│   └── agentframework/
│       ├── core/               # Flow, Task, SyncExecutor, AsyncOrchestrator, StateStore
│       ├── io/                 # REST ingress, MessageQueue, ExecutorWorker, output actions
│       ├── tools/               # Tool interface + registry + built-in tools
│       ├── memory/             # Short-term + long-term memory interfaces + reference impls
│       ├── guardrails/          # Guardrail interface + built-in guardrails
│       ├── observability/       # Structured logger, metrics
│       └── integrations/        # Kafka, Postgres, Redis, Chroma, Airflow, FastAPI (lazy-import); Camel (documented route config)
├── examples/
│   ├── shared_flows.py          # importable flow factory, needed by the Airflow adapter
│   ├── customer_support_agent/  # reference agent #1
│   └── research_agent/          # reference agent #2
├── benchmarks/                  # run_benchmarks.py + results.md (real measured numbers)
└── tests/
```

## 5. Technical Stack (as built)
- **Language**: Python 3.11+. **Core** (`core/`, `io/`, `tools/`, `memory/`, `guardrails/`,
  `observability/`) has **zero required dependencies** — stdlib only (dataclasses instead of
  Pydantic, `http.server` instead of FastAPI, `urllib` instead of `requests`) — see §6 for why.
  Every production/Apache dependency lives under `integrations/`, lazy-imported so its absence
  never breaks the core.
- **Orchestration core**: custom async engine (`asyncio`) implementing DAG execution, with
  level-based concurrency (independent tasks run in parallel via `asyncio.gather`).
- **Messaging**: `InMemoryMessageQueue` (reference) / Kafka via `aiokafka` (production, optional
  extra).
- **State store**: `InMemoryStateStore` (reference) / PostgreSQL via `asyncpg` (production,
  optional extra).
- **Memory**: `InMemoryShortTermMemory`/`InMemoryLongTermMemory` (reference, keyword-scored) /
  Redis + Chroma (production, optional extras, Chroma giving real embedding-based recall).
- **API**: `RestIngress` (stdlib `http.server`, reference) / FastAPI (production, optional
  extra) — identical routes/response shapes either way.
- **Batch scheduling**: `integrations/airflow_adapter.py` compiles a Flow to Airflow DAG source
  and provides the runner every generated task calls (optional extra for real Airflow itself).

## 6. As-Built Deviations From This Document's Original Design
(See `docs/Memory.md` for the full running log with reasoning; this section is the summary.)

1. **No required dependencies in the core.** Originally specified Pydantic, FastAPI, aiokafka,
   asyncpg as core dependencies. Built environment had no network/pip access, so the core uses
   only the Python standard library; every one of those became an optional, lazy-imported
   `integrations/` adapter with an identical interface. This turned out to be a genuine
   improvement independent of the original constraint — the core now runs anywhere with zero
   install step — so it was kept rather than reverted once real dependency access became
   available for the design-doc phase.
2. **Executors decouple at the flow level, not the task level.** This document's Executors
   section describes individual tasks hopping across Kafka task-assigned/task-completed topics
   to physically separate processes. What's built: an `ExecutorWorker` consumes a whole
   `RunRequest` and executes the entire flow through `AsyncOrchestrator` — still horizontally
   scalable (run more workers against a shared queue + state store), just coarser-grained.
   Finer per-task decoupling remains a possible future Phase 7+ extension, not implemented.
3. **REST ingress is a stdlib `http.server` implementation**, with FastAPI as an optional
   drop-in adapter exposing the identical routes — not the other way around as originally
   implied.
4. **Camel integration is a documented route config, not an executed one.** Apache Camel is a
   JVM framework with no legitimate Python binding; building a fake one would misrepresent the
   integration. `integrations/camel/route.yaml` is a real, correctly-structured route.
5. **Airflow's DAG-compile half is verified without a real Airflow install** — the generated DAG
   source is checked for syntactic validity and structure, and the per-task runner logic is
   exercised against a stub XCom object and diffed against `SyncExecutor`'s own output. Real
   Airflow itself was never installed/run in this environment.
6. **A bounded, single-pass reflection mechanism**, not an open-ended loop — the Flow/Task model
   is a DAG with no "repeat until satisfied" primitive. The research reference agent's
   critique-then-revise pass is what "reflection loops" (a stretch goal) became in practice; a
   real loop construct is an open question for a future phase.
7. **Human-in-the-loop is implemented** (`Task(requires_approval=True)` +
   `AsyncOrchestrator.resume()`, a real `asyncio.Event`-based suspend/resume) — this was listed
   only as a stretch goal but was small enough to complete, closing out the previously-stubbed
   `RunStatus.WAITING`.
