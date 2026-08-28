"""Production REST ingress using FastAPI (docs/Architecture.md > Ingress). Same routes/response
shapes as `io.rest_ingress.RestIngress` (the stdlib reference server) — swap this in once
FastAPI/uvicorn are installed:

    pip install -e ".[server]"
    uvicorn agentframework.integrations.fastapi_ingress:build_app --factory

`build_app` takes the same (orchestrator, flow_registry) the stdlib server takes, so callers
don't need to change anything else in how they wire the framework together. An optional third
argument, `orchestrators_by_flow`, lets specific flow names run against a DIFFERENT orchestrator
instance than the default — needed when two agents' tool registries can't be merged into one
(see docs/Memory.md: the customer support and research reference agents both register tools
named "search" and "llm_call", with different underlying data; ToolRegistry.register() silently
overwrites on name collision, so naively sharing one registry between them would make one
agent's "search" quietly return the other agent's documents). Every orchestrator passed this way
should share the same state_store as the default orchestrator — get_run/get_audit below always
read through the default orchestrator's state_store, so a run created by a different
orchestrator is only findable afterward if they're actually the same underlying store.

IMPORTANT (see docs/Memory.md for the incident this fixed): `RunRequestBody` must be defined at
true module level, NOT inside `build_app()`. A Pydantic `BaseModel` defined inside a function
breaks FastAPI's OpenAPI schema generation with a "class not fully defined" error — Pydantic
resolves the model by name through the module's global namespace, and a locally-scoped class is
never bound there. This fails silently until the first request to `/docs`/`/openapi.json`
(schema generation is lazy), while normal request handling (`POST /v1/runs`) works fine — which
is exactly the confusing symptom that made this bug hard to diagnose the first time around.
The module-level `try/except` below preserves the "importing this file doesn't require fastapi/
pydantic to be installed unless build_app() is actually called" contract, while still giving the
model a real top-level name.

Error responses (docs/Design.md > API Design Principles): every error, regardless of source,
comes back as {"error_type": str, "message": str, "retryable": bool} via two global exception
handlers below — one for the AgentFrameworkError hierarchy (core/errors.py), one for
HTTPException (FastAPI's own, e.g. the 404 in get_run). Before this, only FlowValidationError
was explicitly caught in create_run; every other AgentFrameworkError an orchestrator run could
raise (GuardrailViolation, ApprovalRejected, TaskTimeoutError, ToolError, MemoryError_) would
have propagated as a raw, unhandled 500 with none of the caller-useful `retryable` signal this
project's own error hierarchy was designed to carry. The global handlers fix that uniformly
instead of requiring every route to remember to catch every possible error type itself.
"""
from __future__ import annotations

from typing import Any, Optional

from agentframework.core.orchestrator import AsyncOrchestrator
from agentframework.core.registry import FlowRegistry
from agentframework.core.errors import AgentFrameworkError

try:
    from fastapi import FastAPI, HTTPException, Request
    from fastapi.responses import JSONResponse
    from pydantic import BaseModel, Field
    _IMPORT_ERROR: Optional[ImportError] = None
except ImportError as exc:  # pragma: no cover - exercised only when the extra isn't installed
    _IMPORT_ERROR = exc

if _IMPORT_ERROR is None:

    class RunRequestBody(BaseModel):
        flow_name: str
        inputs: dict[str, Any] = Field(default_factory=dict)
        session_id: Optional[str] = None


# Status code per AgentFrameworkError subclass name — anything not listed falls back to 500.
# Kept as a plain name->code dict (not isinstance chains) so adding a new error type in
# core/errors.py doesn't require touching this file unless a non-500 code is actually wanted.
_STATUS_CODE_BY_ERROR_NAME = {
    "FlowValidationError": 400,
    "GuardrailViolation": 422,
    "ApprovalRejected": 422,
    "TaskTimeoutError": 504,
    "ToolError": 502,
}


def build_app(orchestrator: AsyncOrchestrator, flow_registry: FlowRegistry,
              orchestrators_by_flow: Optional[dict[str, AsyncOrchestrator]] = None):
    if _IMPORT_ERROR is not None:
        raise ImportError(
            "fastapi_ingress requires the 'server' extra: pip install -e '.[server]'"
        ) from _IMPORT_ERROR

    orchestrators_by_flow = orchestrators_by_flow or {}

    app = FastAPI(title="agentframework ingress")

    @app.exception_handler(AgentFrameworkError)
    async def agentframework_error_handler(request: Request, exc: AgentFrameworkError):
        error_type = type(exc).__name__
        status_code = _STATUS_CODE_BY_ERROR_NAME.get(error_type, 500)
        return JSONResponse(status_code=status_code, content={
            "error_type": error_type,
            "message": str(exc),
            "retryable": exc.retryable,
        })

    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException):
        return JSONResponse(status_code=exc.status_code, content={
            "error_type": "HTTPException",
            "message": exc.detail,
            "retryable": False,
        })

    @app.post("/v1/runs", status_code=202)
    async def create_run(body: RunRequestBody):
        # No manual try/except needed: FlowValidationError (unknown flow name) and any
        # AgentFrameworkError orchestrator.run() raises are both caught by the global handler
        # above, uniformly, without every route needing to remember every error type.
        flow = flow_registry.build(body.flow_name)
        active_orchestrator = orchestrators_by_flow.get(body.flow_name, orchestrator)
        run = await active_orchestrator.run(flow, body.inputs, session_id=body.session_id)
        return {"run_id": run.run_id, "status": run.status.value}

    @app.get("/v1/runs/{run_id}")
    async def get_run(run_id: str):
        run = await orchestrator.state_store.get_run(run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="run not found")
        return {
            "run_id": run.run_id,
            "flow_name": run.flow_name,
            "status": run.status.value,
            "tasks": {
                name: {"status": s.status.value, "attempt": s.attempt, "result": s.result,
                       "error": s.error}
                for name, s in run.tasks.items()
            },
        }

    @app.get("/v1/runs/{run_id}/audit")
    async def get_audit(run_id: str):
        trail = await orchestrator.state_store.audit_trail(run_id)
        return {"run_id": run_id, "audit_trail": [
            {"task": s.name, "status": s.status.value, "attempt": s.attempt, "error": s.error}
            for s in trail
        ]}

    return app
