"""Deployment entrypoint. Wires together whichever backends are configured via environment
variables — real ones if the env vars are set, safe in-memory/mock fallbacks otherwise — into
the FastAPI REST ingress (integrations/fastapi_ingress.py).

Environment variables (all optional — the server runs with zero config using mocks/in-memory):
    GEMINI_API_KEY      -> real Gemini LLM; unset -> MockLLMProvider
    ANTHROPIC_API_KEY   -> real Anthropic LLM (checked if GEMINI_API_KEY isn't set)
    DATABASE_URL        -> real Postgres state store; unset -> InMemoryStateStore
    PORT                -> what port to listen on (Render/Railway set this automatically;
                            defaults to 8000 for local runs)

Local run:
    cd sdk && pip install -e ".[server]" && cd ..
    python server.py

Deployment (Render/Railway, see DEPLOY.md): build with `pip install -r requirements.txt`,
start with `uvicorn server:app --host 0.0.0.0 --port $PORT`.

Exposes three reference agents: `customer-support-ticket`, `research-report`, and
`expense-approval`. The research agent's `fetch_top_source` tool needs a real URL to fetch from
— this app serves its own source index at `/source/{doc_id}` (matching examples/research_agent/
run.py's local stub server's exact path and response shape — agent.py hardcodes the path suffix,
only the base URL is actually configurable) and points the flow's `source_base_url` at its own
loopback address, so the fetch is still a genuine HTTP round-trip through a real socket, just to
itself rather than an external stub process. `expense-approval` genuinely pauses (RunStatus.
WAITING) on every submission until a human calls `POST /v1/runs/{run_id}/approve` — see
fastapi_ingress.py's docstring for how create_run detects this and schedules the run in the
background instead of blocking the HTTP request on it. All three run on SEPARATE
AsyncOrchestrator instances (same shared state_store, different tool_registry each) — see
fastapi_ingress.py's docstring for why merging their tools into one registry isn't safe (all
three register tools named "search" and/or "llm_call" backed by different documents;
ToolRegistry silently overwrites on name collision).

Known limitation: long-term memory (customer ticket history) always uses
`InMemoryLongTermMemory`, even when `DATABASE_URL` gives real Postgres for run state — so a
customer's ticket history resets on every server restart/redeploy, while run records themselves
persist. `integrations/vector_memory.py` (Chroma) is the real long-term memory backend but
needs its own persistence volume/service; not wired in here. Flagged, not silently glossed over.

Serves `frontend/index.html` (a plain HTML/CSS/JS page, no build step, no framework) at `/` —
submits a ticket to `POST /v1/runs` and renders each pipeline stage's real result. Machine-
readable service info moved to `/api` (previously lived at `/`).
"""
from __future__ import annotations

import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(_ROOT / "sdk"))
sys.path.insert(0, str(_ROOT))

from agentframework.core.orchestrator import AsyncOrchestrator
from agentframework.core.registry import FlowRegistry
from agentframework.core.state_store import InMemoryStateStore
from agentframework.integrations.fastapi_ingress import build_app
from agentframework.memory.in_memory import InMemoryLongTermMemory
from agentframework.tools.llm_tool import MockLLMProvider

from examples.customer_support_agent.agent import build_flow as build_support_flow
from examples.customer_support_agent.agent import build_tool_registry as build_support_tools
from examples.expense_approval_agent.agent import build_flow as build_expense_flow
from examples.expense_approval_agent.agent import build_tool_registry as build_expense_tools
from examples.research_agent.agent import SOURCES as RESEARCH_SOURCES
from examples.research_agent.agent import build_flow as build_research_flow
from examples.research_agent.agent import build_tool_registry as build_research_tools


def _get_llm_provider():
    if os.environ.get("GEMINI_API_KEY"):
        from agentframework.integrations.gemini_llm_provider import GeminiLLMProvider
        print("LLM: using real Gemini API")
        return GeminiLLMProvider()
    if os.environ.get("ANTHROPIC_API_KEY"):
        from agentframework.integrations.anthropic_llm_provider import AnthropicLLMProvider
        print("LLM: using real Anthropic API")
        return AnthropicLLMProvider()
    print("LLM: no API key found (GEMINI_API_KEY / ANTHROPIC_API_KEY) — using MockLLMProvider")
    return MockLLMProvider()


def _get_state_store():
    dsn = os.environ.get("DATABASE_URL")
    if dsn:
        from agentframework.integrations.postgres_state_store import PostgresStateStore
        print("State store: using real Postgres")
        return PostgresStateStore(dsn)
    print("State store: no DATABASE_URL found — using InMemoryStateStore (data lost on restart)")
    return InMemoryStateStore()


llm_provider = _get_llm_provider()
state_store = _get_state_store()
long_term_memory = InMemoryLongTermMemory()

# All three agents' tools can't share one ToolRegistry (see fastapi_ingress.py docstring: all
# three register "search" and/or "llm_call" tools backed by different documents, and
# ToolRegistry silently overwrites on name collision) — so each gets its own registry and
# orchestrator, sharing the same state_store/long_term_memory so GET /v1/runs/{id} works for
# any flow's runs regardless of which orchestrator actually executed it.
support_tools = build_support_tools(llm_provider=llm_provider)
research_tools = build_research_tools(llm_provider=llm_provider)
expense_tools = build_expense_tools(llm_provider=llm_provider)

port = int(os.environ.get("PORT", 8000))
_source_base_url = f"http://127.0.0.1:{port}"

flow_registry = FlowRegistry()
flow_registry.register("customer-support-ticket", build_support_flow)
flow_registry.register("research-report", lambda: build_research_flow(_source_base_url))
flow_registry.register("expense-approval", build_expense_flow)

orchestrator = AsyncOrchestrator(
    state_store=state_store,
    tool_registry=support_tools,
    long_term_memory=long_term_memory,
)
research_orchestrator = AsyncOrchestrator(
    state_store=state_store,
    tool_registry=research_tools,
    long_term_memory=long_term_memory,
)
expense_orchestrator = AsyncOrchestrator(
    state_store=state_store,
    tool_registry=expense_tools,
    long_term_memory=long_term_memory,
)


@asynccontextmanager
async def _lifespan(app):
    # FastAPI's current recommended startup/shutdown pattern (replaced an earlier
    # @app.on_event("startup"), deprecated — see docs/Memory.md). Everything before yield runs
    # on startup; this app has no shutdown-side cleanup, so nothing runs after it.
    if hasattr(state_store, "init_schema"):
        await state_store.init_schema()
    yield


app = build_app(orchestrator, flow_registry,
                 orchestrators_by_flow={"research-report": research_orchestrator,
                                        "expense-approval": expense_orchestrator},
                 lifespan=_lifespan)


@app.get("/source/{doc_id}")
async def internal_source(doc_id: str):
    """Backs the research agent's fetch_top_source tool — same path and response shape as
    examples/research_agent/run.py's local stub HTTP server (agent.py hardcodes the URL as
    f"{source_base_url}/source/{doc_id}"; only the base URL is actually configurable), so
    build_research_flow doesn't need to know or care whether it's talking to that stub or
    this route."""
    from fastapi import HTTPException
    content = RESEARCH_SOURCES.get(doc_id, "")
    if not content:
        raise HTTPException(status_code=404, detail=f"no source found for '{doc_id}'")
    return {"doc_id": doc_id, "content": content}


@app.get("/")
async def frontend():
    from fastapi.responses import FileResponse
    return FileResponse(_ROOT / "frontend" / "index.html")


@app.get("/api")
async def api_info():
    return {
        "service": "agentframework",
        "flows_available": flow_registry.names(),
        "docs": "/docs",
        "frontend": "/",
        "example_request": {
            "method": "POST",
            "url": "/v1/runs",
            "body": {
                "flow_name": "customer-support-ticket",
                "inputs": {"ticket_text": "My package arrived damaged, I need a refund."},
                "session_id": "same customer's own id — reuse it across their tickets to get "
                               "long-term memory recall of their history; omit it and each "
                               "request gets its own isolated memory scope",
            },
        },
        "research_example_request": {
            "method": "POST",
            "url": "/v1/runs",
            "body": {
                "flow_name": "research-report",
                "inputs": {"question": "renewable energy adoption trends"},
            },
        },
        "expense_example_request": {
            "method": "POST",
            "url": "/v1/runs",
            "body": {
                "flow_name": "expense-approval",
                "inputs": {"employee_id": "emp-alice", "amount": 45.00, "category": "meals",
                           "description": "Team lunch"},
                "session_id": "same employee's id — reuse it to build expense history",
            },
            "note": "this flow always pauses (status: 'waiting') until a human calls "
                    "POST /v1/runs/{run_id}/approve with {\"task_name\": \"request_approval\", "
                    "\"approved\": true/false}",
        },
    }


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=port)
