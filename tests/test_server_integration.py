"""Real (non-stubbed) FastAPI integration tests for server.py.

Unlike tests/smoke_test.py's stub-based `test_server_wiring_against_stub_fastapi`, this uses the
REAL fastapi/pydantic packages via a genuinely live uvicorn server (not FastAPI's TestClient's
default in-process ASGI transport). This matters twice over:
- It exercises Pydantic's actual OpenAPI schema generator — exactly where this project's real
  production bug lived (RunRequestBody originally defined inside build_app(), breaking real
  schema generation with a "class not fully defined" error that only surfaced on a live Render
  deploy — see docs/Memory.md). test_openapi_schema_generates_successfully is a direct
  regression test for that exact bug.
- The research-report flow makes a genuine HTTP call back to the server's OWN /source/{doc_id}
  route (via HttpTool -> real urllib, a real socket connection, not mocked) to prove its fetch
  is real. TestClient's default in-process transport wouldn't have anything actually LISTENING
  on the port that call targets — only a real, live server does. This is not a hypothetical
  concern: writing this test caught a real bug before it shipped (server.py's route was first
  registered at the wrong path, `/internal/source/{doc_id}`, while the flow itself hardcodes
  `/source/{doc_id}` — see docs/Memory.md).

Requires the `server` extra plus httpx:
    cd sdk && pip install -e ".[server]" && pip install httpx pytest && cd ..
    pytest tests/test_server_integration.py -v

Not part of tests/smoke_test.py's dependency-free suite — this needs real installs, which is
exactly why it lives here instead: CI (see .github/workflows/tests.yml) has real network access
this project's own development sandbox never had, so this is the first place these real
FastAPI/Pydantic/uvicorn code paths get automated, non-stubbed coverage at all.
"""
from __future__ import annotations

import os
import socket
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "sdk"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")
pytest.importorskip("uvicorn")


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def client():
    # No API keys/DB configured -> server.py falls back to MockLLMProvider + InMemoryStateStore.
    # That's deliberate: this test suite verifies the REAL FastAPI/Pydantic/uvicorn wiring is
    # correct, not that a real LLM/database call succeeds (run_demo_real_llm.py /
    # run_demo_postgres.py cover that, manually, with real credentials).
    for key in ["GEMINI_API_KEY", "ANTHROPIC_API_KEY", "DATABASE_URL"]:
        os.environ.pop(key, None)

    port = _free_port()
    os.environ["PORT"] = str(port)  # must be set BEFORE importing server.py: it reads PORT at
    # module import time to build source_base_url for the research-report flow.
    sys.modules.pop("server", None)

    import httpx
    import uvicorn

    import server as server_module

    config = uvicorn.Config(server_module.app, host="127.0.0.1", port=port, log_level="warning")
    uv_server = uvicorn.Server(config)
    thread = threading.Thread(target=uv_server.run, daemon=True)
    thread.start()

    base_url = f"http://127.0.0.1:{port}"
    for _ in range(50):
        try:
            httpx.get(f"{base_url}/health", timeout=0.2)
            break
        except httpx.TransportError:
            time.sleep(0.1)
    else:
        raise RuntimeError("live test server did not start accepting connections in time")

    with httpx.Client(base_url=base_url, timeout=15.0) as http_client:
        yield http_client

    uv_server.should_exit = True
    thread.join(timeout=5)


def test_openapi_schema_generates_successfully(client):
    """Direct regression test for this project's real production bug: RunRequestBody defined
    inside build_app() broke FastAPI's actual OpenAPI schema generation. Only a real (not
    stubbed) Pydantic schema generator can catch this."""
    resp = client.get("/openapi.json")
    assert resp.status_code == 200
    schema = resp.json()
    assert "RunRequestBody" in schema.get("components", {}).get("schemas", {})


def test_docs_page_loads(client):
    resp = client.get("/docs")
    assert resp.status_code == 200


def test_frontend_root_serves_html(client):
    resp = client.get("/")
    assert resp.status_code == 200
    assert "Dispatch Desk" in resp.text


def test_api_info_route_reports_registered_flow(client):
    resp = client.get("/api")
    assert resp.status_code == 200
    flows = resp.json()["flows_available"]
    assert "customer-support-ticket" in flows
    assert "research-report" in flows


def test_internal_source_route_serves_research_documents(client):
    resp = client.get("/source/report-2026-solar")
    assert resp.status_code == 200
    body = resp.json()
    assert body["doc_id"] == "report-2026-solar"
    assert "solar" in body["content"].lower()


def test_internal_source_route_404s_for_unknown_doc(client):
    resp = client.get("/source/does-not-exist")
    assert resp.status_code == 404


def test_full_run_via_real_http_client(client):
    resp = client.post("/v1/runs", json={
        "flow_name": "customer-support-ticket",
        "inputs": {"ticket_text": "My billing was overcharged, please refund."},
    })
    assert resp.status_code == 202
    body = resp.json()
    assert body["status"] == "succeeded"

    detail_resp = client.get(f"/v1/runs/{body['run_id']}")
    assert detail_resp.status_code == 200
    detail = detail_resp.json()
    assert detail["tasks"]["draft_reply"]["result"] is not None


def test_unknown_run_id_returns_404_with_typed_error_shape(client):
    """Regression test for the typed error response global exception handlers: every error,
    regardless of source, should come back as {"error_type", "message", "retryable"} — not
    FastAPI's raw default {"detail": ...} shape. This one goes through the HTTPException
    handler (the 404 in get_run is a plain HTTPException, not an AgentFrameworkError)."""
    resp = client.get("/v1/runs/does-not-exist")
    assert resp.status_code == 404
    body = resp.json()
    assert body["error_type"] == "HTTPException"
    assert body["message"] == "run not found"
    assert body["retryable"] is False


def test_unknown_flow_name_returns_400_with_typed_error_shape(client):
    """Regression test for the AgentFrameworkError global exception handler: an unknown
    flow_name raises FlowValidationError inside flow_registry.build(), with no manual
    try/except in create_run anymore — the global handler is what's actually responsible for
    turning that into a typed 400, not route-local error handling."""
    resp = client.post("/v1/runs", json={
        "flow_name": "this-flow-does-not-exist",
        "inputs": {},
    })
    assert resp.status_code == 400
    body = resp.json()
    assert body["error_type"] == "FlowValidationError"
    assert body["retryable"] is False


def test_research_report_flow_completes_via_real_self_referential_http_call(client):
    """The real point of the `client` fixture running a genuinely live uvicorn server (not
    TestClient's default in-process transport): the research-report flow's fetch_top_source
    task makes a real urllib HTTP call back to this SAME server's own /source/{doc_id} route.
    Nothing would be listening on that port under the default TestClient transport, so this is
    the only test in this file that actually proves the self-referential fetch design works at
    all — and it's exactly the design that had a real path-mismatch bug during development
    (server.py first registered the route at /internal/source/{doc_id} while the flow itself
    hardcodes /source/{doc_id} — see docs/Memory.md)."""
    resp = client.post("/v1/runs", json={
        "flow_name": "research-report",
        "inputs": {"question": "renewable energy adoption trends"},
    })
    assert resp.status_code == 202
    body = resp.json()
    assert body["status"] == "succeeded"

    detail = client.get(f"/v1/runs/{body['run_id']}").json()
    fetch_result = detail["tasks"]["fetch_top_source"]["result"]
    assert fetch_result["status"] == 200
    assert "content" in fetch_result["body"]

    final_report = detail["tasks"]["finalize_report"]["result"]["response"]
    assert "report-2026" in final_report  # the reflection pass adds the citation back in


def test_customer_support_and_research_tools_do_not_cross_contaminate(client):
    """Regression test for the real bug this whole multi-orchestrator design exists to avoid:
    both reference agents register a "search" tool under the same name, backed by different
    documents. If they ever ended up sharing one ToolRegistry again, one flow's search results
    would silently come from the other agent's documents."""
    support_resp = client.post("/v1/runs", json={
        "flow_name": "customer-support-ticket",
        "inputs": {"ticket_text": "My billing was overcharged this month."},
    })
    research_resp = client.post("/v1/runs", json={
        "flow_name": "research-report",
        "inputs": {"question": "renewable energy adoption trends"},
    })

    support_detail = client.get(f"/v1/runs/{support_resp.json()['run_id']}").json()
    research_detail = client.get(f"/v1/runs/{research_resp.json()['run_id']}").json()

    support_docs = [r["doc_id"] for r in support_detail["tasks"]["search_kb"]["result"]["results"]]
    research_docs = [r["doc_id"] for r in
                      research_detail["tasks"]["search_sources"]["result"]["results"]]

    assert all("faq" in d for d in support_docs)
    assert all("report" in d for d in research_docs)


def test_session_id_enables_cross_request_memory_recall(client):
    """Direct regression test for the second real production bug found: RunRequestBody had no
    session_id field, so every request got an isolated memory scope and cross-ticket recall
    silently never worked over HTTP (see docs/Memory.md)."""
    r1 = client.post("/v1/runs", json={
        "flow_name": "customer-support-ticket",
        "inputs": {"ticket_text": "My billing was overcharged this month."},
        "session_id": "ci-test-customer",
    })
    assert r1.status_code == 202

    r2 = client.post("/v1/runs", json={
        "flow_name": "customer-support-ticket",
        "inputs": {"ticket_text": "Following up on my billing issue."},
        "session_id": "ci-test-customer",
    })
    assert r2.status_code == 202
    run_id2 = r2.json()["run_id"]

    detail2 = client.get(f"/v1/runs/{run_id2}").json()
    recall = detail2["tasks"]["recall_history"]["result"]
    assert len(recall) >= 1
    assert "overcharged" in recall[0].lower()
