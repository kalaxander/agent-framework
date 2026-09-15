# Rules.md — Boundaries for building this project

## Hard Constraints
- **Never** add crew.ai, AutoGen, LangGraph-as-orchestrator, n8n, or any other all-in-one agent
  framework as a dependency. Thin, single-purpose libraries (e.g. an HTTP client, a Kafka client,
  Pydantic) are fine.
- Apache projects (Kafka, Airflow, Camel, etc.) are allowed and encouraged for messaging,
  orchestration, and storage — but the **flow/task execution semantics** (the DAG/state-machine
  engine itself) must be implemented in this codebase, not delegated to a third-party agent
  framework.
- Every flow run must be **auditable**: no task may execute without a persisted state transition
  (start, input, output/error, timestamp).

## Libraries: Prefer / Avoid
- **Prefer**: FastAPI, Pydantic, SQLAlchemy/asyncpg, aiokafka, redis-py, pytest, structlog.
- **Avoid**: any dependency that hides orchestration logic behind an opaque "agent" abstraction
  (defeats the purpose of building the framework); heavy unmaintained packages; anything requiring
  a paid API key to run the test suite (mock external LLM/tool calls in tests).

## Error Handling
- Every task execution is wrapped with a **timeout** and a **retry policy** (max attempts, backoff)
  defined per-task, with a sane framework default.
- Failures are typed (`ToolError`, `TimeoutError`, `GuardrailViolation`, `MemoryError`) so callers
  can branch on failure kind, not string-match messages.
- A task that exhausts retries fails the run (or routes to an `on_failure` task if the flow defines
  one) — it never fails silently.
- Guardrail violations always short-circuit execution before a tool call is made (fail closed).

## Coding Conventions
- Python 3.11+, fully type-hinted, Pydantic models for all public schemas (flow, task, run, tool
  I/O).
- Public SDK surface lives under `agentframework/`; internals are not imported directly by
  examples — examples only use the public API.
- No hardcoded secrets/API keys; read from environment variables via a single `settings.py`.
- Each module (`core`, `io`, `tools`, `memory`, `guardrails`, `observability`) has its own tests;
  no cross-module test coupling.

## What the AI/agent assistant building this project should and shouldn't do
- Should generate code incrementally per `Phases.md`, keeping each phase runnable/testable before
  moving to the next.
- Should keep `Memory.md` (added once coding starts) up to date after each meaningful step, so
  context isn't lost across sessions/tools.
- Should not invent Apache product capabilities — if unsure how a component behaves, note
  the assumption explicitly rather than presenting it as fact.
- Should not silently change the architecture (e.g. swapping Kafka for another queue) without
  flagging the change in `Architecture.md`.
