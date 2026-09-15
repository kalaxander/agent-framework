# Design.md

This project is an SDK/framework, not a visual application, so "design" here covers **API design,
configuration format, and naming conventions** rather than colors/typography. (If a monitoring
dashboard is added later, its visual theme would be specified in this file too.)

## 1. Flow Definition Style (as built)
Only the code-first Python style below is actually implemented. A declarative YAML/config
loader was planned in this document's original draft but never built — see §6.

```python
from agentframework import Flow, Task

flow = Flow(name="support-ticket-triage")
flow.add_task(Task(name="classify", tool="llm_call", tool_input=lambda ctx: {"prompt": ...}))
flow.add_task(Task(name="fetch_docs", tool="search", depends_on=["classify"]))
flow.add_task(Task(name="draft_reply", tool="llm_call", depends_on=["fetch_docs"]))
```

## 2. Naming Conventions
- Flow/task names: `kebab-case` for flow names (e.g. `"support-ticket-triage"`), `snake_case`
  for task names (e.g. `"fetch_docs"`) — followed consistently across every example/demo.
- Public classes: `PascalCase` (`Flow`, `Task`, `AsyncOrchestrator`, `SyncExecutor`,
  `RunRecord`, `ToolRegistry`, `Guardrail`).
- Environment-variable convention (`AGENTFW_<COMPONENT>_<SETTING>`) was specified but never
  implemented — nothing in this codebase currently reads configuration from environment
  variables (every constructor takes explicit Python arguments instead, e.g.
  `PostgresStateStore(dsn=...)`, `RedisShortTermMemory(redis_url=...)`). Worth doing before a
  real deployment; not done here.

## 3. API Design Principles (as built)
- REST endpoints are resource-oriented and versioned: `/v1/runs`, `/v1/runs/{id}`,
  `/v1/runs/{id}/audit`. (`/v1/flows` was mentioned in this document's original draft but never
  built — flow discovery isn't exposed over REST; flows are registered in-process via
  `FlowRegistry`.)
- Every response includes a `run_id` for correlating with logs/audit trail.
- **Deviation**: tool interfaces are NOT schema-first/Pydantic-based as originally specified —
  `Tool.validate_input`/`validate_output` are plain Python methods a tool author implements
  imperatively, not declared schemas guardrails introspect generically. This followed from the
  core's stdlib-only constraint (see Architecture.md §6); a schema-first version remains
  possible to add later without changing the `Tool` interface's shape.
- **Deviation (partially closed)**: the FastAPI production ingress (`integrations/
  fastapi_ingress.py`) now returns the typed `{error_type, message, retryable}` shape originally
  specified, via two global exception handlers — one for the `AgentFrameworkError` hierarchy
  (mapping each subclass to an appropriate status code), one for FastAPI's own `HTTPException`
  (e.g. the 404 for an unknown run_id). This also closed a real gap beyond the typed shape
  itself: previously, only `FlowValidationError` was explicitly caught in `create_run` — any
  other `AgentFrameworkError` an orchestrator run could raise (`GuardrailViolation`,
  `ApprovalRejected`, `TaskTimeoutError`, `ToolError`) would have propagated as a raw, unhandled
  500. The global handlers fix that uniformly. The stdlib reference server (`io/rest_ingress.py`)
  still returns the older `{"error": str(exception)}` shape — not yet updated, since it's the
  dependency-free reference implementation exercised by `tests/smoke_test.py` rather than what's
  actually deployed; closing it there too remains a small, separate follow-up.

## 4. Logging/Audit Format (as built)
- One JSON line per event via `JsonLineLogger`: `{ts, event, ...fields}`, where `fields` varies
  by event type — e.g. `run_started`/`run_succeeded` carry `run_id, flow_name`;
  `task_succeeded`/`task_failed` carry `run_id, task_name, duration_ms, attempt` (+`error` on
  failure). This is close to but not byte-identical to this document's original
  `{ts, run_id, task_name, event, payload, duration_ms}` shape — `payload` was dropped as
  redundant once the state store already carries the full task result, and fields are flat
  kwargs rather than nested under a `payload` key. See `observability/logger.py`.
- Audit trail is a separate, durable mechanism (`StateStore.audit_trail()`, Postgres-backed in
  production), not literally "a filtered read of the same event stream" as originally
  specified — the JSON log stream (`JsonLineLogger`) is meant to be disposable/shippable to a
  log aggregator, while the audit trail is the source of truth kept in the state store. Two
  separate, deliberately-decoupled mechanisms rather than one shared one.

## 5. If a Dashboard Is Added Later
- Dark-first theme, monospace for logs/IDs, a single accent color for status (green=succeeded,
  amber=running, red=failed, gray=cancelled) — kept minimal since the primary consumers are
  developers/SREs, not end users. Not built — `RunStatus`/`TaskStatus` (`core/state_store.py`)
  already have the five states this would visualize, if/when a dashboard gets built.

## 6. Deviations Summary
See `docs/Architecture.md` §6 for the architecture-level deviations (stdlib-only core, flow-
level executor decoupling, Camel documented-not-executed, etc.) — this file's deviations above
are the design/API-shape-level consequences of those same decisions, not separate ones.
