# agent-framework

[![Tests](https://github.com/kalaxander/agent-framework/actions/workflows/tests.yml/badge.svg)](https://github.com/kalaxander/agent-framework/actions/workflows/tests.yml)

A build-your-own AI Agent Framework: define agentic workflows as composable task flows (DAG /
state machine), execute them reliably, and monitor/audit every run. Built from scratch — no
crew.ai / AutoGen / n8n — with optional Apache components (Kafka, Airflow, Camel) for messaging
and orchestration infrastructure.

See `/docs` for the project docs:
- `PRD.md` — what & why
- `Architecture.md` — system design
- `Rules.md` — build constraints/conventions
- `Phases.md` — build plan
- `Design.md` — API/config design conventions
- `Memory.md` — (added once coding starts) running build progress/context

## Status
All 9 original phases are done: core flow model, async orchestrator + state store, REST ingress
+ queue-driven executor workers + output actions, tools registry, short-term + long-term memory,
guardrails + observability, an Airflow adapter + documented Camel route, 2 reference agents, and
a finalized design doc + real measured benchmarks + human-in-the-loop pause/resume — see
`docs/Memory.md` for full details and every deviation from the original design, and
`docs/Architecture.md`/`docs/Design.md` (§6 in each) for the as-built summary.

Beyond the original 9 phases: a live public deployment (see `DEPLOY.md`), a frontend, CI, and a
**third reference agent** — Expense Approval — built specifically to demonstrate Phase 9's
human-in-the-loop pause/resume, which neither of the original two agents actually used despite
it being built and tested since Phase 9. See `examples/expense_approval_agent/README.md`.

A real LLM provider has also been added — both **Gemini** (`GeminiLLMProvider`, free tier, no
billing) and **Anthropic** (`AnthropicLLMProvider`) — swap either into any reference agent (or
any `LlmTool`) in place of `MockLLMProvider` with one line; see below.

## Quickstart (no install required — everything except real Airflow/Camel/a live LLM is stdlib-only)
```bash
python3 run_demo.py            # Phase 1 + Phase 2 run the same Flow, side by side
python3 run_demo_phase3.py     # Phase 3: real HTTP + queue-driven worker + webhook output
python3 run_demo_phase4.py     # Phase 4: ToolRegistry + built-in tools inside a Flow
python3 run_demo_phase5.py     # Phase 5: short-term scratchpad + cross-run long-term recall
python3 run_demo_phase6.py     # Phase 6: guardrails (pass + 3 rejections) + metrics/logs
python3 run_demo_phase7.py     # Phase 7: Flow -> Airflow DAG compile + simulated-XCom execution
python3 examples/customer_support_agent/run.py   # Phase 8, reference agent 1
python3 examples/research_agent/run.py           # Phase 8, reference agent 2 (real reflection pass)
python3 run_demo_phase9.py     # Phase 9: human-in-the-loop pause/resume
python3 examples/expense_approval_agent/run.py   # reference agent 3 (approve/reject/reject over real memory)
python3 benchmarks/run_benchmarks.py   # Phase 9: real measured perf numbers
python3 tests/smoke_test.py    # dependency-free test suite (69 checks)
```

## Running against a real LLM
**Gemini (free tier, no billing — recommended):**
```bash
cd sdk && pip install -e ".[gemini]" && cd ..
export GEMINI_API_KEY=...          # get one free at https://aistudio.google.com/app/apikey
python3 run_demo_real_llm.py
```
**Anthropic (alternative):**
```bash
cd sdk && pip install -e ".[llm]" && cd ..
export ANTHROPIC_API_KEY=sk-ant-...
python3 run_demo_real_llm.py
```
`run_demo_real_llm.py` checks for `GEMINI_API_KEY` first, then `ANTHROPIC_API_KEY`. Swapping
either agent from mock to real is one line:
```python
from agentframework.integrations.gemini_llm_provider import GeminiLLMProvider
tools = build_tool_registry(llm_provider=GeminiLLMProvider())
```

## Running against a real Postgres database
Proves state actually persists across a restart, not just within one Python process — see
`run_demo_postgres.py` for exactly what this checks.
```bash
# Get a free Postgres (a couple minutes, no card needed): https://supabase.com or https://neon.tech
cd sdk && pip install -e ".[storage]" && cd ..
export DATABASE_URL="postgresql://user:password@host:port/dbname"
python3 run_demo_postgres.py
```
Swapping `AsyncOrchestrator`'s state store from in-memory to Postgres is one line:
```python
from agentframework.integrations.postgres_state_store import PostgresStateStore
store = PostgresStateStore(dsn=os.environ["DATABASE_URL"])
await store.init_schema()
orchestrator = AsyncOrchestrator(state_store=store)
```
**Confirmed working against a real Neon Postgres instance** — verified persistence survives a
simulated process restart (separate connection, same data recovered intact).

## Continuous integration
`.github/workflows/tests.yml` runs on every push/PR to `main`, across three tiers:
- **Smoke tests** — the dependency-free suite above, needs no installs.
- **pytest suite** — the Phase 1 core tests, needs `pip install -e ".[dev]"`.
- **Server integration** — `tests/test_server_integration.py`, using FastAPI's real `TestClient`
  (not a stub). This tier exists specifically because it's the first place this project's real
  FastAPI/Pydantic code gets automated coverage without stubbing — the two real production bugs
  found during deployment (see `docs/Memory.md`) both lived exactly in code paths a stub can't
  reach. These tests are direct regressions for both.

## Deploying this to a real, public URL
`server.py` is the deployment entrypoint — it uses real Gemini/Postgres automatically if
`GEMINI_API_KEY`/`DATABASE_URL` are set (falling back to mocks/in-memory otherwise), and serves
both a REST API and a small frontend (`frontend/index.html` — a dispatch-desk-themed page
showing each pipeline stage's real result as a ticket is processed) over the same FastAPI app.
See **`DEPLOY.md`** for a full step-by-step guide to pushing this to GitHub and deploying it
free on Render.

Locally:
```bash
cd sdk && pip install -e ".[server]" && cd ..
python server.py
# then visit http://localhost:8000  (frontend) or http://localhost:8000/docs (raw API)
```
With network/pip access, the pytest suite also works: `cd sdk && pip install -e ".[dev]" && pytest`.

```python
from agentframework import Flow, Task
from agentframework.core.executor import SyncExecutor       # Phase 1: simple, sequential
from agentframework.core.orchestrator import AsyncOrchestrator  # Phase 2: async, concurrent, audited

def classify(ctx): return {"category": "billing"}
def draft(ctx): return f"Re: {ctx['classify']['category']} — thanks for reaching out."

flow = Flow(name="demo")
flow.add_task(Task(name="classify", fn=classify))
flow.add_task(Task(name="draft", fn=draft, depends_on=["classify"]))

# Phase 1
result = SyncExecutor().run(flow, inputs={})
print(result["draft"])

# Phase 2 (same Flow definition)
import asyncio
run = asyncio.run(AsyncOrchestrator().run(flow, inputs={}))
print(run.status, run.tasks["draft"].result)
```
