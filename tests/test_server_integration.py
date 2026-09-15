"""Real (non-stubbed) FastAPI integration tests for server.py.

Unlike tests/smoke_test.py's stub-based `test_server_wiring_against_stub_fastapi`, this uses the
REAL fastapi/pydantic packages via a genuinely live uvicorn server (not FastAPI's TestClient's
default in-process ASGI transport). This matters in several ways:
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
- The expense-approval flow's create_run schedules a REAL asyncio background task and returns
  before it completes — a stub that calls route functions directly can exercise the logic, but
  only a real live server + real concurrent HTTP requests can prove the actual timing/scheduling
  genuinely works end to end (POST returns "queued" immediately, a concurrent poll sees
  "waiting", POST /approve unblocks it for real).

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
    assert "expense-approval" in flows


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
    fetched_doc_id = fetch_result["body"]["doc_id"]
    assert fetched_doc_id in {"report-2026-renewables", "report-2026-solar"}
    assert fetch_result["body"]["content"]  # real source text, non-empty either way

    # NOTE: does NOT assert on finalize_report's actual text content. CI has no
    # GEMINI_API_KEY/ANTHROPIC_API_KEY configured, so server.py falls back to MockLLMProvider,
    # which returns a fixed "(mock response)" string regardless of prompt — asserting real
    # LLM-quality output (e.g. a citation) here would be asserting something only true with a
    # real API key, which this test doesn't have. What's actually being verified is structural:
    # the flow completes successfully end to end, including the real self-referential HTTP
    # fetch above — that's true regardless of which LLM provider is behind it.
    assert detail["tasks"]["finalize_report"]["status"] == "succeeded"
    assert detail["tasks"]["finalize_report"]["result"]["response"]


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


def test_expense_approval_flow_does_not_block_and_completes_via_approve(client):
    """Real, live-server proof of the background-task design: POST /v1/runs for
    expense-approval must return immediately with status "queued" — NOT block the HTTP request
    until a human acts. This is exactly the scenario the `on_created` callback (added to
    AsyncOrchestrator.run()) and create_run's background-scheduling logic exist for; only a
    real live server (not the stub, which calls route functions directly with no actual
    concurrency) can prove the timing genuinely works: the POST truly returns before the flow
    reaches the approval gate, a separate poll genuinely sees "waiting", and POST /approve
    genuinely unblocks the still-suspended background task."""
    resp = client.post("/v1/runs", json={
        "flow_name": "expense-approval",
        "inputs": {"employee_id": "emp-live-test", "amount": 55.0, "category": "meals",
                   "description": "Team lunch during a live CI test run"},
        "session_id": "emp-live-test",
    })
    assert resp.status_code == 202
    body = resp.json()
    assert body["status"] == "queued"  # the real point of this test: NOT "succeeded"
    run_id = body["run_id"]

    waiting = None
    for _ in range(100):
        detail = client.get(f"/v1/runs/{run_id}").json()
        if detail["status"] == "waiting":
            waiting = detail
            break
        time.sleep(0.05)
    assert waiting is not None, "run never reached WAITING — background scheduling is broken"
    assert waiting["tasks"]["assess_expense"]["result"]["response"]

    approve_resp = client.post(f"/v1/runs/{run_id}/approve",
                                json={"task_name": "request_approval", "approved": True})
    assert approve_resp.status_code == 200
    assert approve_resp.json() == {"run_id": run_id, "task_name": "request_approval",
                                    "approved": True}

    final = None
    for _ in range(100):
        final = client.get(f"/v1/runs/{run_id}").json()
        if final["status"] in ("succeeded", "failed"):
            break
        time.sleep(0.05)
    assert final["status"] == "succeeded"
    assert final["tasks"]["record_decision"]["result"] == "recorded"


def test_expense_approval_flow_rejected_via_approve_endpoint(client):
    """Same real live-server path as above, but for the rejection branch — confirms
    ApprovalRejected (raised inside orchestrator.run()'s background task when resumed with
    approved=False) correctly lands the run in "failed" status as seen through a real,
    independent GET request, not just observed from inside the same process/coroutine."""
    resp = client.post("/v1/runs", json={
        "flow_name": "expense-approval",
        "inputs": {"employee_id": "emp-live-reject", "amount": 9000.0, "category": "travel",
                   "description": "Unapproved last-minute business class upgrade"},
        "session_id": "emp-live-reject",
    })
    assert resp.status_code == 202
    run_id = resp.json()["run_id"]

    for _ in range(100):
        detail = client.get(f"/v1/runs/{run_id}").json()
        if detail["status"] == "waiting":
            break
        time.sleep(0.05)
    else:
        raise AssertionError("run never reached WAITING")

    approve_resp = client.post(f"/v1/runs/{run_id}/approve",
                                json={"task_name": "request_approval", "approved": False})
    assert approve_resp.status_code == 200

    final = None
    for _ in range(100):
        final = client.get(f"/v1/runs/{run_id}").json()
        if final["status"] in ("succeeded", "failed"):
            break
        time.sleep(0.05)
    assert final["status"] == "failed"
    assert final["tasks"]["request_approval"]["error"] == \
        "rejected by human-in-the-loop approval"


def test_approve_unknown_run_id_returns_404(client):
    resp = client.post("/v1/runs/does-not-exist/approve",
                        json={"task_name": "request_approval", "approved": True})
    assert resp.status_code == 404
    assert resp.json()["error_type"] == "HTTPException"
