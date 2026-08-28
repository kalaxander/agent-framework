"""Dependency-free smoke tests for Phase 1 + Phase 2 + Phase 3 + Phase 4 + Phase 5 + Phase 6 +
Phase 7. Run with:
    python3 tests/smoke_test.py   (from the repo root)

Mirrors tests/test_flow_and_executor.py (the pytest version, for CI once network/pip access is
available) but uses plain asserts so it also runs in offline/no-pip environments.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import threading
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer

sys.path.insert(0, "sdk")
sys.path.insert(0, ".")  # for examples/shared_flows.py, used by the Phase 7 tests

from agentframework import Flow, Task, FlowValidationError
from agentframework.core.flow import RetryPolicy
from agentframework.core.executor import SyncExecutor
from agentframework.core.orchestrator import AsyncOrchestrator
from agentframework.core.registry import FlowRegistry
from agentframework.core.state_store import RunStatus, TaskStatus
from agentframework.core.errors import TaskTimeoutError, GuardrailViolation, ApprovalRejected
from agentframework.io.rest_ingress import RestIngress
from agentframework.io.message_queue import InMemoryMessageQueue
from agentframework.io.worker import ExecutorWorker, RunRequest
from agentframework.io.output_actions import OutputActionRegistry, LogOutputAction
from agentframework.tools.registry import ToolRegistry
from agentframework.tools.http_tool import HttpTool
from agentframework.tools.llm_tool import LlmTool, MockLLMProvider
from agentframework.tools.search_tool import SimpleSearchTool
from agentframework.memory.in_memory import InMemoryShortTermMemory, InMemoryLongTermMemory
from agentframework.guardrails.builtin import (
    RequiredKeysGuardrail, RateLimitGuardrail, BudgetGuardrail, ContentFilterGuardrail,
)
from agentframework.observability.logger import JsonLineLogger
from agentframework.observability.metrics import InMemoryMetrics
from agentframework.integrations.airflow_adapter import compile_to_airflow_dag, run_task_for_airflow

passed = 0


def check(condition: bool, label: str) -> None:
    global passed
    assert condition, f"FAILED: {label}"
    passed += 1
    print(f"  ok: {label}")


def _make_fastapi_pydantic_stubs():
    """Build fresh stub `fastapi` / `fastapi.responses` / `pydantic` modules, shared by every
    stub-based FastAPI test below (rather than each test hand-rolling its own near-duplicate
    stub, which is exactly the kind of duplication that made these three drift out of sync when
    fastapi_ingress.py started using Request/exception_handler/JSONResponse). Call fresh per
    test — `.routes`/`._startup_handlers` must not leak between tests.

    These stubs only support calling route handlers DIRECTLY (as every test here does) — they
    do NOT implement real request dispatch or exception-handler invocation, so they can't
    verify the actual typed-error-response behavior `@app.exception_handler(...)` registers.
    That's deliberate, not a gap: `tests/test_server_integration.py` (real FastAPI, real
    TestClient, run in CI where real network access exists) is where that's actually verified,
    the same way it's the source of truth for the real OpenAPI-schema-generation checks these
    stubs also can't do.

    Returns (fake_fastapi, fake_pydantic, FakeHTTPException, FakeBaseModel).
    """
    import types

    class FakeHTTPException(Exception):
        def __init__(self, status_code, detail):
            self.status_code = status_code
            self.detail = detail

    class FakeRequest:
        pass

    class FakeJSONResponse:
        def __init__(self, status_code=200, content=None):
            self.status_code = status_code
            self.content = content or {}

    class FakeFastAPI:
        def __init__(self, title=None):
            self.title = title
            self.routes = {}
            self._startup_handlers = []
            self._exception_handlers = {}

        def post(self, path, status_code=200):
            def deco(fn):
                self.routes[("POST", path)] = fn
                return fn
            return deco

        def get(self, path):
            def deco(fn):
                self.routes[("GET", path)] = fn
                return fn
            return deco

        def on_event(self, name):
            def deco(fn):
                if name == "startup":
                    self._startup_handlers.append(fn)
                return fn
            return deco

        def exception_handler(self, exc_type):
            def deco(fn):
                self._exception_handlers[exc_type] = fn
                return fn
            return deco

    fake_fastapi = types.ModuleType("fastapi")
    fake_fastapi.FastAPI = FakeFastAPI
    fake_fastapi.HTTPException = FakeHTTPException
    fake_fastapi.Request = FakeRequest

    fake_fastapi_responses = types.ModuleType("fastapi.responses")

    class FakeFileResponse:
        def __init__(self, path):
            self.path = path

    fake_fastapi_responses.FileResponse = FakeFileResponse
    fake_fastapi_responses.JSONResponse = FakeJSONResponse
    fake_fastapi.responses = fake_fastapi_responses

    fake_pydantic = types.ModuleType("pydantic")

    class FakeBaseModel:
        session_id = None  # mirrors real Pydantic: unset Optional fields still resolve

        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)

    def fake_field(default=None, default_factory=None, **kwargs):
        return default_factory() if default_factory is not None else default

    fake_pydantic.BaseModel = FakeBaseModel
    fake_pydantic.Field = fake_field

    return fake_fastapi, fake_pydantic, FakeHTTPException, FakeBaseModel


def _install_fastapi_pydantic_stubs(fake_fastapi, fake_pydantic):
    """Registers the stub modules in sys.modules and returns a restore() callback. Always call
    restore() in a finally block — leaving stubs installed breaks every later test that needs
    the real (or a different fake) fastapi/pydantic."""
    originals = {
        "fastapi": sys.modules.get("fastapi"),
        "fastapi.responses": sys.modules.get("fastapi.responses"),
        "pydantic": sys.modules.get("pydantic"),
    }
    sys.modules["fastapi"] = fake_fastapi
    sys.modules["fastapi.responses"] = fake_fastapi.responses
    sys.modules["pydantic"] = fake_pydantic
    sys.modules.pop("agentframework.integrations.fastapi_ingress", None)

    def restore():
        for name, original in originals.items():
            if original is not None:
                sys.modules[name] = original
            else:
                sys.modules.pop(name, None)

    return restore


def test_topological_order():
    flow = Flow(name="chain")
    flow.add_task(Task(name="a", fn=lambda ctx: 1))
    flow.add_task(Task(name="b", fn=lambda ctx: 2, depends_on=["a"]))
    flow.add_task(Task(name="c", fn=lambda ctx: 3, depends_on=["b"]))
    check(flow.topological_order() == ["a", "b", "c"], "topological order is a,b,c")


def test_missing_dependency():
    flow = Flow(name="broken")
    flow.add_task(Task(name="a", fn=lambda ctx: 1, depends_on=["ghost"]))
    try:
        flow.validate()
        check(False, "missing dependency should raise")
    except FlowValidationError:
        check(True, "missing dependency raises FlowValidationError")


def test_cycle_detection():
    flow = Flow(name="cyclic")
    flow.add_task(Task(name="a", fn=lambda ctx: 1, depends_on=["b"]))
    flow.add_task(Task(name="b", fn=lambda ctx: 2, depends_on=["a"]))
    try:
        flow.validate()
        check(False, "cycle should raise")
    except FlowValidationError:
        check(True, "cycle raises FlowValidationError")


def test_phase1_executor():
    flow = Flow(name="demo")
    flow.add_task(Task(name="classify", fn=lambda ctx: {"category": "billing"}))
    flow.add_task(Task(name="draft", fn=lambda ctx: f"Re: {ctx['classify']['category']}",
                        depends_on=["classify"]))
    result = SyncExecutor().run(flow, inputs={"ticket_id": 42})
    check(result["draft"] == "Re: billing", "Phase 1 executor produces expected output")
    check(result["__inputs__"] == {"ticket_id": 42}, "Phase 1 preserves original inputs")


def test_phase1_retry():
    attempts = {"count": 0}

    def flaky(ctx):
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise RuntimeError("transient")
        return "ok"

    flow = Flow(name="retry-demo")
    flow.add_task(Task(name="flaky", fn=flaky,
                        retry_policy=RetryPolicy(max_attempts=5, backoff_seconds=0.01,
                                                  backoff_multiplier=1.0)))
    result = SyncExecutor().run(flow, inputs={})
    check(result["flaky"] == "ok" and attempts["count"] == 3, "Phase 1 retries then succeeds")


def test_phase2_orchestrator():
    flow = Flow(name="demo2")
    flow.add_task(Task(name="classify", fn=lambda ctx: {"category": "billing"}))
    flow.add_task(Task(name="fetch", fn=lambda ctx: ["doc1"], depends_on=["classify"]))
    flow.add_task(Task(name="priority", fn=lambda ctx: "normal", depends_on=["classify"]))
    flow.add_task(Task(name="draft",
                        fn=lambda ctx: f"[{ctx['priority']}] {ctx['fetch']}",
                        depends_on=["fetch", "priority"]))
    run = asyncio.run(AsyncOrchestrator().run(flow, inputs={}))
    check(run.status == RunStatus.SUCCEEDED, "Phase 2 run status is succeeded")
    check(run.tasks["draft"].result == "[normal] ['doc1']", "Phase 2 draft output correct")
    check(run.tasks["classify"].status == TaskStatus.SUCCEEDED, "Phase 2 task states persisted")


def test_phase2_timeout():
    import time

    def slow(ctx):
        time.sleep(0.2)
        return "late"

    flow = Flow(name="timeout-demo")
    flow.add_task(Task(name="slow", fn=slow, timeout_seconds=0.05,
                        retry_policy=RetryPolicy(max_attempts=1)))
    try:
        asyncio.run(AsyncOrchestrator().run(flow, inputs={}))
        check(False, "timeout should raise")
    except TaskTimeoutError:
        check(True, "Phase 2 enforces per-task timeout")


def test_phase1_and_phase2_agree():
    flow_defs = lambda: (
        Flow(name="agree")
        .add_task(Task(name="a", fn=lambda ctx: 10))
        .add_task(Task(name="b", fn=lambda ctx: ctx["a"] * 2, depends_on=["a"]))
    )
    phase1_result = SyncExecutor().run(flow_defs(), inputs={})
    phase2_run = asyncio.run(AsyncOrchestrator().run(flow_defs(), inputs={}))
    check(phase1_result["b"] == phase2_run.tasks["b"].result == 20,
          "Phase 1 and Phase 2 agree on output for the same flow")


def _demo_flow() -> Flow:
    flow = Flow(name="demo3")
    flow.add_task(Task(name="a", fn=lambda ctx: 10))
    flow.add_task(Task(name="b", fn=lambda ctx: ctx["a"] * 2, depends_on=["a"]))
    return flow


def test_phase3_rest_ingress():
    registry = FlowRegistry()
    registry.register("demo3", _demo_flow)
    ingress = RestIngress(AsyncOrchestrator(), registry)
    port = ingress.start()
    try:
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/v1/runs",
            data=json.dumps({"flow_name": "demo3", "inputs": {}}).encode(),
            headers={"Content-Type": "application/json"}, method="POST",
        )
        with urllib.request.urlopen(req) as resp:
            body = json.loads(resp.read())
            status_code = resp.status
        check(status_code == 202 and body["status"] == "succeeded",
              "REST POST /v1/runs starts and completes a run")

        run_id = body["run_id"]
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/v1/runs/{run_id}") as resp:
            status_body = json.loads(resp.read())
        check(status_body["tasks"]["b"]["result"] == 20,
              "REST GET /v1/runs/{id} returns correct task result")

        with urllib.request.urlopen(f"http://127.0.0.1:{port}/v1/runs/{run_id}/audit") as resp:
            audit_body = json.loads(resp.read())
        check(len(audit_body["audit_trail"]) == 2,
              "REST GET /v1/runs/{id}/audit returns both task states")

        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/v1/runs/nope")
            check(False, "unknown run_id should 404")
        except urllib.error.HTTPError as exc:
            check(exc.code == 404, "REST returns 404 for unknown run_id")
    finally:
        ingress.stop()


def test_phase3_queue_worker_and_output_actions():
    async def _inner():
        registry = FlowRegistry()
        registry.register("demo3", _demo_flow)
        queue = InMemoryMessageQueue()
        outputs = OutputActionRegistry()
        log_action = LogOutputAction()
        outputs.register(log_action)

        worker = ExecutorWorker(queue, AsyncOrchestrator(), registry, outputs)
        worker.start()
        results_iter = queue.consume("flow.run_results")

        await worker.submit(RunRequest(flow_name="demo3", inputs={}, output_actions=["log"]))
        result_message = await asyncio.wait_for(results_iter.__anext__(), timeout=5)
        await asyncio.sleep(0.05)  # let output-action dispatch (after publish) complete
        await worker.stop()

        check(result_message.payload["status"] == "succeeded",
              "queue worker publishes a succeeded result message")
        check(len(log_action.records) == 1 and
              log_action.records[0]["result"]["b"] == 20,
              "queue worker dispatches output action with correct result")

    asyncio.run(_inner())


# --- Phase 4: tools registry ---

class _KbHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def do_GET(self):
        body = json.dumps({"article": "refund policy text"}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def _start_kb_server() -> int:
    server = HTTPServer(("127.0.0.1", 0), _KbHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server.server_address[1]


def test_phase4_http_tool_real_request():
    port = _start_kb_server()
    tools = ToolRegistry()
    tools.register(HttpTool())

    async def _inner():
        return await tools.invoke("http_call", {"url": f"http://127.0.0.1:{port}/kb"})

    result = asyncio.run(_inner())
    check(result["status"] == 200 and result["body"]["article"] == "refund policy text",
          "HttpTool makes a real HTTP request and parses the JSON response")


def test_phase4_search_tool():
    tools = ToolRegistry()
    tools.register(SimpleSearchTool(documents={
        "a": "billing refund overcharge",
        "b": "shipping delivery times",
    }))

    async def _inner():
        return await tools.invoke("search", {"query": "billing refund", "top_k": 1})

    result = asyncio.run(_inner())
    check(result["results"][0]["doc_id"] == "a",
          "SimpleSearchTool ranks the matching document first")


def test_phase4_llm_tool_mock_provider():
    provider = MockLLMProvider(responses={"billing": "here is your billing answer"})
    tools = ToolRegistry()
    tools.register(LlmTool(provider=provider))

    async def _inner():
        return await tools.invoke("llm_call", {"prompt": "a billing question"})

    result = asyncio.run(_inner())
    check(result["response"] == "here is your billing answer",
          "LlmTool + MockLLMProvider returns the matched canned response")
    check(provider.calls == ["a billing question"],
          "MockLLMProvider records the prompt it was called with")


def test_phase4_tool_input_validation_guardrail():
    tools = ToolRegistry()
    tools.register(SimpleSearchTool(documents={}))

    async def _inner():
        try:
            await tools.invoke("search", {"top_k": 3})  # missing required 'query'
            return False
        except GuardrailViolation:
            return True

    check(asyncio.run(_inner()), "ToolRegistry.invoke rejects invalid input via validate_input")


def test_phase4_task_resolves_tool_in_orchestrator():
    tools = ToolRegistry()
    tools.register(SimpleSearchTool(documents={"doc1": "billing refund"}))

    flow = Flow(name="tool-flow")
    flow.add_task(Task(name="search_step", tool="search",
                        tool_input=lambda ctx: {"query": "billing"}))

    run = asyncio.run(AsyncOrchestrator(tool_registry=tools).run(flow, inputs={}))
    check(run.status == RunStatus.SUCCEEDED and
          run.tasks["search_step"].result["results"][0]["doc_id"] == "doc1",
          "AsyncOrchestrator resolves Task(tool=...) via the ToolRegistry end to end")


def test_phase4_missing_tool_registry_raises():
    flow = Flow(name="no-registry")
    flow.add_task(Task(name="t", tool="search", tool_input=lambda ctx: {"query": "x"}))
    try:
        asyncio.run(AsyncOrchestrator().run(flow, inputs={}))  # no tool_registry passed
        check(False, "should raise when tool is referenced with no tool_registry configured")
    except NotImplementedError:
        check(True, "orchestrator raises a clear error for an unconfigured tool_registry")


# --- Phase 5: memory ---

def test_phase5_short_term_scratchpad():
    async def _inner():
        async def writer(ctx):
            await ctx["__memory__"].remember_short("tone", "friendly")
            return "ok"

        async def reader(ctx):
            return await ctx["__memory__"].recall_short("tone")

        flow = Flow(name="scratchpad")
        flow.add_task(Task(name="writer", fn=writer))
        flow.add_task(Task(name="reader", fn=reader, depends_on=["writer"]))

        orchestrator = AsyncOrchestrator(short_term_memory=InMemoryShortTermMemory())
        run = await orchestrator.run(flow, inputs={})
        return run.tasks["reader"].result

    check(asyncio.run(_inner()) == "friendly",
          "short-term memory: a later task recalls what an earlier task remembered")


def test_phase5_short_term_isolated_per_run():
    async def _inner():
        async def writer(ctx):
            await ctx["__memory__"].remember_short("k", "v")
            return "ok"

        async def reader(ctx):
            return await ctx["__memory__"].recall_short("k")

        write_flow = Flow(name="w")
        write_flow.add_task(Task(name="writer", fn=writer))
        read_flow = Flow(name="r")
        read_flow.add_task(Task(name="reader", fn=reader))

        short_term = InMemoryShortTermMemory()
        orchestrator = AsyncOrchestrator(short_term_memory=short_term)
        await orchestrator.run(write_flow, inputs={})
        # a *different* run never wrote "k" -> should recall None, not leak the other run's value
        run2 = await orchestrator.run(read_flow, inputs={})
        return run2.tasks["reader"].result

    check(asyncio.run(_inner()) is None,
          "short-term memory is isolated per run_id (no cross-run leakage)")


def test_phase5_long_term_recall_across_runs():
    async def _inner():
        long_term = InMemoryLongTermMemory()
        orchestrator = AsyncOrchestrator(long_term_memory=long_term)
        session = "session-a"

        async def remember(ctx):
            await ctx["__memory__"].remember_long("billing overcharge refunded")
            return "ok"

        async def recall(ctx):
            records = await ctx["__memory__"].recall_long("billing refund", top_k=1)
            return records[0].text if records else None

        flow1 = Flow(name="f1")
        flow1.add_task(Task(name="remember", fn=remember))
        await orchestrator.run(flow1, inputs={}, session_id=session)

        flow2 = Flow(name="f2")  # a separate run
        flow2.add_task(Task(name="recall", fn=recall))
        run2 = await orchestrator.run(flow2, inputs={}, session_id=session)
        return run2.tasks["recall"].result

    check(asyncio.run(_inner()) == "billing overcharge refunded",
          "long-term memory persists across separate runs sharing the same session_id")


def test_phase5_long_term_isolated_per_session():
    async def _inner():
        long_term = InMemoryLongTermMemory()
        orchestrator = AsyncOrchestrator(long_term_memory=long_term)

        async def remember(ctx):
            await ctx["__memory__"].remember_long("billing overcharge refunded")
            return "ok"

        async def recall(ctx):
            records = await ctx["__memory__"].recall_long("billing refund", top_k=1)
            return records

        flow1 = Flow(name="f1")
        flow1.add_task(Task(name="remember", fn=remember))
        await orchestrator.run(flow1, inputs={}, session_id="session-a")

        flow2 = Flow(name="f2")
        flow2.add_task(Task(name="recall", fn=recall))
        run2 = await orchestrator.run(flow2, inputs={}, session_id="session-b")
        return run2.tasks["recall"].result

    check(asyncio.run(_inner()) == [],
          "long-term memory is isolated per session_id (no cross-session leakage)")


def test_phase5_missing_memory_raises_clear_error():
    async def _inner():
        async def reader(ctx):
            return await ctx["__memory__"].recall_short("k")

        flow = Flow(name="no-memory-configured")
        flow.add_task(Task(name="reader", fn=reader))
        # no short_term_memory/long_term_memory passed -> __memory__ never gets injected
        try:
            await AsyncOrchestrator().run(flow, inputs={})
            return False
        except KeyError:
            return True

    check(asyncio.run(_inner()),
          "context has no __memory__ key when no memory store is configured")


# --- Phase 6: guardrails + observability ---

def test_phase6_required_keys_guardrail_rejects_and_no_retry():
    async def _inner():
        metrics = InMemoryMetrics()
        flow = Flow(name="bad-input")
        flow.add_task(Task(
            name="t", fn=lambda ctx: "should not run",
            guardrails=[RequiredKeysGuardrail(["required_field"])],
            retry_policy=RetryPolicy(max_attempts=3, backoff_seconds=0.01),
        ))
        orchestrator = AsyncOrchestrator(metrics=metrics)
        try:
            await orchestrator.run(flow, inputs={})
            return None
        except GuardrailViolation:
            return len(metrics.records)  # how many attempts got recorded

    attempts = asyncio.run(_inner())
    check(attempts == 1,
          "GuardrailViolation fails closed: only 1 attempt recorded despite max_attempts=3")


def test_phase6_flow_level_guardrail_applies_to_every_task():
    async def _inner():
        shared_budget = BudgetGuardrail(max_calls=2)
        flow = Flow(name="budget-flow", guardrails=[shared_budget])
        for i in range(3):
            flow.add_task(Task(name=f"t{i}", fn=lambda ctx: "ok"))
        try:
            await AsyncOrchestrator().run(flow, inputs={})
            return False
        except GuardrailViolation:
            return True

    check(asyncio.run(_inner()),
          "a Flow-level guardrail applies to every task and rejects once its budget is spent")


def test_phase6_content_filter_rejects_bad_output():
    async def _inner():
        flow = Flow(name="content-filter")
        flow.add_task(Task(
            name="draft", fn=lambda ctx: "please contact our lawsuit department",
            guardrails=[ContentFilterGuardrail(["lawsuit"])],
        ))
        try:
            await AsyncOrchestrator().run(flow, inputs={})
            return False
        except GuardrailViolation:
            return True

    check(asyncio.run(_inner()),
          "post-execution ContentFilterGuardrail rejects output containing a banned term")


def test_phase6_metrics_and_logger_record_successful_run():
    async def _inner():
        metrics = InMemoryMetrics()
        logger = JsonLineLogger()
        flow = Flow(name="observed-flow")
        flow.add_task(Task(name="a", fn=lambda ctx: {"usage": {"tokens": 10, "cost": 0.001}}))
        orchestrator = AsyncOrchestrator(metrics=metrics, logger=logger)
        await orchestrator.run(flow, inputs={})
        return metrics, logger

    metrics, logger = asyncio.run(_inner())
    check(metrics.success_rate() == 1.0 and metrics.total_tokens() == 10,
          "metrics collector records success rate and token usage from task output")
    events = [e["event"] for e in logger.records]
    check("run_started" in events and "run_succeeded" in events and "task_succeeded" in events,
          "structured logger captures run_started/task_succeeded/run_succeeded events")


def test_phase6_retryable_error_is_still_retried():
    async def _inner():
        attempts = {"count": 0}

        def flaky(ctx):
            attempts["count"] += 1
            if attempts["count"] < 2:
                raise RuntimeError("transient, not a guardrail violation")
            return "ok"

        flow = Flow(name="still-retries")
        flow.add_task(Task(
            name="flaky", fn=flaky,
            guardrails=[RequiredKeysGuardrail([])],  # a guardrail present, but input is valid
            retry_policy=RetryPolicy(max_attempts=3, backoff_seconds=0.01, backoff_multiplier=1.0),
        ))
        run = await AsyncOrchestrator().run(flow, inputs={})
        return run.tasks["flaky"].result, attempts["count"]

    result, attempts = asyncio.run(_inner())
    check(result == "ok" and attempts == 2,
          "an ordinary retryable error still retries normally even with a guardrail attached")


# --- Phase 7: Airflow adapter ---

class _StubTaskInstance:
    def __init__(self, xcom: dict):
        self.xcom = xcom

    def xcom_pull(self, task_ids: str):
        return self.xcom.get(task_ids)


def test_phase7_compile_to_airflow_dag_is_valid_python():
    src = compile_to_airflow_dag("examples.shared_flows", "simple_pipeline_flow",
                                  dag_id="test-dag")
    try:
        compile(src, "<generated_dag>", "exec")
        valid = True
    except SyntaxError:
        valid = False
    check(valid, "compile_to_airflow_dag produces syntactically valid Python")


def test_phase7_compile_to_airflow_dag_has_correct_structure():
    src = compile_to_airflow_dag("examples.shared_flows", "simple_pipeline_flow",
                                  dag_id="test-dag")
    has_all_tasks = all(f"{name} = PythonOperator(" in src
                         for name in ["classify", "fetch_docs", "draft"])
    has_edges = "classify >> fetch_docs" in src and "fetch_docs >> draft" in src
    check(has_all_tasks and has_edges,
          "generated DAG has a PythonOperator per task and correct >> dependency edges")


def test_phase7_airflow_execution_matches_sync_executor():
    from examples.shared_flows import simple_pipeline_flow

    flow = simple_pipeline_flow()
    order = flow.topological_order()
    xcom: dict = {}
    ti = _StubTaskInstance(xcom)
    for task_name in order:
        xcom[task_name] = run_task_for_airflow(
            flow_module="examples.shared_flows", flow_factory="simple_pipeline_flow",
            task_name=task_name, ti=ti,
        )

    direct = SyncExecutor().run(simple_pipeline_flow(), inputs={})
    check(xcom == {k: direct[k] for k in order},
          "Airflow-compiled task execution (via simulated XCom) matches SyncExecutor exactly")


# --- Phase 8: reference agents ---

def test_phase8_support_agent_memory_recall_across_tickets():
    async def _inner():
        from agentframework.memory.in_memory import InMemoryLongTermMemory
        from examples.customer_support_agent.agent import build_flow, build_tool_registry

        tools = build_tool_registry()
        long_term = InMemoryLongTermMemory()
        orchestrator = AsyncOrchestrator(tool_registry=tools, long_term_memory=long_term)

        await orchestrator.run(
            build_flow(), {"ticket_text": "billing overcharge refund request"},
            session_id="cust-x",
        )
        run2 = await orchestrator.run(
            build_flow(), {"ticket_text": "follow up on my billing overcharge"},
            session_id="cust-x",
        )
        return run2.tasks["recall_history"].result

    history = asyncio.run(_inner())
    check(len(history) >= 1 and "billing" in history[0].lower(),
          "support agent recalls a customer's prior ticket via long-term memory")


def test_phase8_support_agent_guardrail_blocks_bad_reply():
    async def _inner():
        from agentframework import Flow, Task
        from agentframework.guardrails.builtin import ContentFilterGuardrail
        from agentframework.tools.llm_tool import LlmTool, MockLLMProvider
        from agentframework.tools.registry import ToolRegistry

        tools = ToolRegistry()
        tools.register(LlmTool(provider=MockLLMProvider(
            responses={"trigger": "We recommend you consider a lawsuit."},
        )))
        flow = Flow(name="bad-reply-check")
        flow.add_task(Task(
            name="draft", tool="llm_call",
            tool_input=lambda ctx: {"prompt": "trigger"},
            guardrails=[ContentFilterGuardrail(["lawsuit"])],
        ))
        try:
            await AsyncOrchestrator(tool_registry=tools).run(flow, inputs={})
            return False
        except GuardrailViolation:
            return True

    check(asyncio.run(_inner()),
          "support agent's content filter guardrail would block a reply mentioning lawsuits")


def test_phase8_research_agent_reflection_pass_adds_missing_citations():
    async def _inner():
        from examples.research_agent.agent import SOURCES, build_flow, build_tool_registry

        # No real HTTP server needed for this check — fetch_top_source is exercised for real in
        # run_demo_phase8.py; here we isolate the critique/finalize logic by stubbing the fetch.
        import agentframework.tools.http_tool as http_tool_module

        class _StubHttpTool(http_tool_module.HttpTool):
            async def run(self, input):
                doc_id = input["url"].rsplit("/", 1)[-1]
                return {"status": 200, "body": {"doc_id": doc_id, "content": SOURCES[doc_id]}}

        tools = build_tool_registry()
        tools.register(_StubHttpTool())  # overwrite the real HttpTool registration by name

        flow = build_flow(source_base_url="http://unused")
        run = await AsyncOrchestrator(tool_registry=tools).run(
            flow, inputs={"question": "renewable energy adoption trends"}
        )
        missing = run.tasks["critique_report"].result["missing_citations"]
        final_text = run.tasks["finalize_report"].result["response"]
        return missing, final_text

    missing, final_text = asyncio.run(_inner())
    check(len(missing) > 0, "research agent's critique step finds the draft's citation gap")
    check(all(doc_id in final_text for doc_id in missing),
          "research agent's reflection pass (finalize_report) closes the gap the critique found")


# --- Phase 9: human-in-the-loop pause/resume ---

def test_phase9_approval_pauses_run_and_resume_approved_continues():
    async def _inner():
        flow = Flow(name="approval-ok")
        flow.add_task(Task(name="a", fn=lambda ctx: "draft"))
        flow.add_task(Task(name="b", fn=lambda ctx: f"sent: {ctx['a']}",
                            depends_on=["a"], requires_approval=True))

        orchestrator = AsyncOrchestrator()
        run_task = asyncio.create_task(orchestrator.run(flow, inputs={}))

        run_id = None
        saw_waiting = False
        for _ in range(50):
            await asyncio.sleep(0.02)
            if orchestrator.state_store._runs:
                run_id = next(iter(orchestrator.state_store._runs))
                run = await orchestrator.state_store.get_run(run_id)
                if run.status == RunStatus.WAITING:
                    saw_waiting = True
                    break

        await orchestrator.resume(run_id, "b", approved=True)
        final_run = await run_task
        return saw_waiting, final_run.status, final_run.tasks["b"].result

    saw_waiting, status, result = asyncio.run(_inner())
    check(saw_waiting, "a run with requires_approval=True actually reaches RunStatus.WAITING")
    check(status == RunStatus.SUCCEEDED and result == "sent: draft",
          "resume(approved=True) unblocks the run and it completes normally")


def test_phase9_approval_rejected_fails_the_run():
    async def _inner():
        flow = Flow(name="approval-rejected")
        flow.add_task(Task(name="a", fn=lambda ctx: "draft"))
        flow.add_task(Task(name="b", fn=lambda ctx: "should not run",
                            depends_on=["a"], requires_approval=True))

        orchestrator = AsyncOrchestrator()
        run_task = asyncio.create_task(orchestrator.run(flow, inputs={}))
        await asyncio.sleep(0.1)

        run_id = next(iter(orchestrator.state_store._runs))
        await orchestrator.resume(run_id, "b", approved=False)

        try:
            await run_task
            return False
        except ApprovalRejected:
            return True

    check(asyncio.run(_inner()),
          "resume(approved=False) rejects the task and fails the run (fail-closed)")


# --- Real LLM provider (verified against a stub client, not the live API — see docs/Memory.md) ---

def test_anthropic_provider_wiring_against_stub_client():
    """Verifies AnthropicLLMProvider's request-building/response-parsing against a stub client
    shaped like the real Anthropic SDK. Does NOT call the live API — that's what
    run_demo_real_llm.py (run manually, with a real ANTHROPIC_API_KEY) is for."""
    import types

    fake_anthropic = types.ModuleType("anthropic")

    class _FakeUsage:
        def __init__(self, input_tokens, output_tokens):
            self.input_tokens = input_tokens
            self.output_tokens = output_tokens

    class _FakeTextBlock:
        def __init__(self, text):
            self.type = "text"
            self.text = text

    class _FakeResponse:
        def __init__(self, text):
            self.content = [_FakeTextBlock(text)]
            self.usage = _FakeUsage(12, 8)

    captured = []

    class _FakeMessages:
        async def create(self, **kwargs):
            captured.append(kwargs)
            return _FakeResponse(f"echo: {kwargs['messages'][0]['content']}")

    class _FakeAsyncAnthropic:
        def __init__(self, api_key):
            self.messages = _FakeMessages()

    fake_anthropic.AsyncAnthropic = _FakeAsyncAnthropic
    original_module = sys.modules.get("anthropic")
    sys.modules["anthropic"] = fake_anthropic
    # force a fresh import of the provider module in case it (or "anthropic") was cached
    sys.modules.pop("agentframework.integrations.anthropic_llm_provider", None)

    try:
        from agentframework.integrations.anthropic_llm_provider import AnthropicLLMProvider

        async def _inner():
            provider = AnthropicLLMProvider(api_key="fake-key", model="claude-sonnet-4-5")
            response = await provider.complete("What is 2+2?")
            tool = LlmTool(provider=provider, cost_per_1k_tokens=0.003)
            output = await tool.run({"prompt": "What is 2+2?"})
            return response, captured[-1], provider.last_usage, output

        response, call_kwargs, last_usage, output = asyncio.run(_inner())
    finally:
        if original_module is not None:
            sys.modules["anthropic"] = original_module
        else:
            sys.modules.pop("anthropic", None)

    check(response == "echo: What is 2+2?",
          "AnthropicLLMProvider.complete() parses the stub response correctly")
    check(call_kwargs["model"] == "claude-sonnet-4-5" and
          call_kwargs["messages"] == [{"role": "user", "content": "What is 2+2?"}],
          "AnthropicLLMProvider.complete() builds the API request correctly")
    check(last_usage == {"input_tokens": 12, "output_tokens": 8},
          "AnthropicLLMProvider captures real token usage from the response")
    check(output["usage"]["tokens"] == 20,
          "LlmTool uses the provider's real usage instead of falling back to a word-count estimate")


def test_gemini_provider_wiring_against_stub_client():
    """Verifies GeminiLLMProvider's request-building/response-parsing against a stub client
    shaped like the real google-genai SDK. Does NOT call the live API."""
    import types

    fake_google = types.ModuleType("google")
    fake_genai = types.ModuleType("google.genai")

    class _FakeUsageMetadata:
        def __init__(self, prompt_token_count, candidates_token_count):
            self.prompt_token_count = prompt_token_count
            self.candidates_token_count = candidates_token_count

    class _FakeResponse:
        def __init__(self, text):
            self.text = text
            self.usage_metadata = _FakeUsageMetadata(15, 10)

    captured = []

    class _FakeAioModels:
        async def generate_content(self, model, contents):
            captured.append({"model": model, "contents": contents})
            return _FakeResponse(f"gemini says: {contents}")

    class _FakeAio:
        def __init__(self):
            self.models = _FakeAioModels()

    class _FakeClient:
        def __init__(self, api_key):
            self.aio = _FakeAio()

    fake_genai.Client = _FakeClient
    fake_google.genai = fake_genai
    original_google = sys.modules.get("google")
    original_genai = sys.modules.get("google.genai")
    sys.modules["google"] = fake_google
    sys.modules["google.genai"] = fake_genai
    sys.modules.pop("agentframework.integrations.gemini_llm_provider", None)

    try:
        from agentframework.integrations.gemini_llm_provider import GeminiLLMProvider

        async def _inner():
            provider = GeminiLLMProvider(api_key="fake-key", model="gemini-2.5-flash")
            response = await provider.complete("What is 2+2?")
            tool = LlmTool(provider=provider, cost_per_1k_tokens=0.0)
            output = await tool.run({"prompt": "What is 2+2?"})
            return response, captured[-1], provider.last_usage, output

        response, call_kwargs, last_usage, output = asyncio.run(_inner())
    finally:
        for name, original in [("google", original_google), ("google.genai", original_genai)]:
            if original is not None:
                sys.modules[name] = original
            else:
                sys.modules.pop(name, None)

    check(response == "gemini says: What is 2+2?",
          "GeminiLLMProvider.complete() parses the stub response correctly")
    check(call_kwargs["model"] == "gemini-2.5-flash" and call_kwargs["contents"] == "What is 2+2?",
          "GeminiLLMProvider.complete() builds the API request correctly")
    check(last_usage == {"input_tokens": 15, "output_tokens": 10},
          "GeminiLLMProvider captures real token usage from the response")
    check(output["usage"]["tokens"] == 25,
          "LlmTool uses Gemini's real usage instead of falling back to a word-count estimate")


# --- server.py (deployment entrypoint) wiring, verified against a stub FastAPI/pydantic ---

def test_server_wiring_against_stub_fastapi():
    """Verifies server.py's provider/store selection + route wiring end to end, including
    actually running a request through the real support-agent flow, WITHOUT needing fastapi,
    uvicorn, or any real API key/database installed. Does NOT verify a real deployment — see
    DEPLOY.md for that (run manually against Render or similar)."""
    fake_fastapi, fake_pydantic, _FakeHTTPException, _FakeBaseModel = _make_fastapi_pydantic_stubs()
    restore = _install_fastapi_pydantic_stubs(fake_fastapi, fake_pydantic)
    original_server = sys.modules.get("server")
    sys.modules.pop("server", None)

    saved_env = {k: os.environ.pop(k, None) for k in
                 ["GEMINI_API_KEY", "ANTHROPIC_API_KEY", "DATABASE_URL"]}

    try:
        import server as server_module

        async def _inner():
            create_run = server_module.app.routes[("POST", "/v1/runs")]
            body = _FakeBaseModel(
                flow_name="customer-support-ticket",
                inputs={"ticket_text": "My billing was overcharged, please refund."},
            )
            result = await create_run(body)
            get_run = server_module.app.routes[("GET", "/v1/runs/{run_id}")]
            detail = await get_run(result["run_id"])
            api_info = await server_module.app.routes[("GET", "/api")]()
            frontend_response = await server_module.app.routes[("GET", "/")]()

            # Checks the route is registered at the EXACT path the research flow's
            # fetch_top_source task hardcodes (f"{source_base_url}/source/{doc_id}") — this
            # dict lookup is what actually caught a real bug before it shipped: the route was
            # first registered at "/internal/source/{doc_id}" instead. Deliberately does NOT
            # run the research-report flow end to end here: fetch_top_source makes a REAL
            # urllib HTTP call, and nothing is actually listening on a real socket in this
            # stub-based test (routes are called as plain Python functions, no live server) —
            # that real round trip is what test_server_integration.py's live-uvicorn-backed
            # `client` fixture is specifically for.
            source_route = server_module.app.routes.get(("GET", "/source/{doc_id}"))
            source_result = await source_route("report-2026-solar") if source_route else None

            return (result, detail, api_info, frontend_response, source_route, source_result)

        (result, detail, api_info, frontend_response,
         source_route, source_result) = asyncio.run(_inner())
    finally:
        restore()
        if original_server is not None:
            sys.modules["server"] = original_server
        else:
            sys.modules.pop("server", None)
        for k, v in saved_env.items():
            if v is not None:
                os.environ[k] = v

    check(result["status"] == "succeeded",
          "server.py: POST /v1/runs completes a real run through the support agent flow")
    check(detail["tasks"]["draft_reply"]["result"] is not None,
          "server.py: GET /v1/runs/{id} returns the drafted reply")
    check(sorted(api_info["flows_available"]) == ["customer-support-ticket", "research-report"],
          "server.py: /api route reports both registered flows")
    check(getattr(frontend_response, "path", None) is not None and
          str(frontend_response.path).endswith("frontend/index.html") or
          str(frontend_response.path).endswith("frontend\\index.html"),
          "server.py: / route serves frontend/index.html")
    check(source_route is not None,
          "server.py: a route is registered at the exact path /source/{doc_id} the research "
          "flow's fetch_top_source task hardcodes (catches the real path-mismatch bug found "
          "during development — see docs/Memory.md)")
    check(source_result is not None and source_result["doc_id"] == "report-2026-solar"
          and "solar" in source_result["content"].lower(),
          "server.py: /source/{doc_id} returns the correct research document content")
    check(len(server_module.app._startup_handlers) == 1,
          "server.py: a startup handler is registered for state-store schema init")


def test_fastapi_ingress_session_id_enables_cross_request_memory_recall():
    """Regression test for a real gap found when the user tested the deployed server: the REST
    API's RunRequestBody had no session_id field, so every POST /v1/runs got its own isolated
    memory scope (defaulting to that run's own run_id) — meaning the customer-history-recall
    feature silently never worked over HTTP, even though it worked in the local Python demos
    that pass session_id explicitly. Verifies the fix: two separate REST requests sharing the
    same session_id let the second one recall the first's ticket via long-term memory."""
    fake_fastapi, fake_pydantic, _FakeHTTPException, _FakeBaseModel = _make_fastapi_pydantic_stubs()
    restore = _install_fastapi_pydantic_stubs(fake_fastapi, fake_pydantic)

    try:
        from agentframework.integrations.fastapi_ingress import build_app
        from agentframework.memory.in_memory import InMemoryLongTermMemory

        async def _inner():
            long_term = InMemoryLongTermMemory()
            orchestrator = AsyncOrchestrator(long_term_memory=long_term)
            registry = FlowRegistry()

            # Deliberately SEPARATE remember-only and recall-only flows/runs — a single run
            # that both remembers and recalls would trivially find its own just-stored memory
            # under its own default session regardless of whether cross-request sharing works
            # at all, which is exactly the mistake this test's first draft made.
            async def remember(ctx):
                return await ctx["__memory__"].remember_long(ctx["__inputs__"]["text"])

            async def recall(ctx):
                records = await ctx["__memory__"].recall_long("billing", top_k=2)
                return [r.text for r in records]

            def make_remember_flow():
                flow = Flow(name="session-remember")
                flow.add_task(Task(name="remember", fn=remember))
                return flow

            def make_recall_flow():
                flow = Flow(name="session-recall")
                flow.add_task(Task(name="recall", fn=recall))
                return flow

            registry.register("session-remember", make_remember_flow)
            registry.register("session-recall", make_recall_flow)
            app = build_app(orchestrator, registry)
            create_run = app.routes[("POST", "/v1/runs")]
            get_run = app.routes[("GET", "/v1/runs/{run_id}")]

            remember_body = _FakeBaseModel(flow_name="session-remember",
                                            inputs={"text": "billing overcharge refund"},
                                            session_id="cust-shared")
            await create_run(remember_body)

            recall_body = _FakeBaseModel(flow_name="session-recall", inputs={},
                                          session_id="cust-shared")
            recall_result_run = await create_run(recall_body)
            detail = await get_run(recall_result_run["run_id"])

            # control: a DIFFERENT session_id must see nothing from the first run
            isolated_body = _FakeBaseModel(flow_name="session-recall", inputs={},
                                            session_id="someone-else")
            isolated_run = await create_run(isolated_body)
            isolated_detail = await get_run(isolated_run["run_id"])

            return detail, isolated_detail

        detail, isolated_detail = asyncio.run(_inner())
    finally:
        restore()

    recall_result = detail["tasks"]["recall"]["result"]
    isolated_result = isolated_detail["tasks"]["recall"]["result"]
    check(len(recall_result) >= 1 and "billing" in recall_result[0].lower(),
          "POST /v1/runs with a shared session_id lets a LATER, SEPARATE request recall an "
          "earlier one's memory (fixes the gap found in the deployed server)")
    check(isolated_result == [],
          "a different session_id via the REST API correctly sees nothing (no leakage)")


def test_fastapi_ingress_run_request_body_is_module_level():
    """Regression check for a real production bug: RunRequestBody was originally defined
    INSIDE build_app(), which breaks FastAPI's OpenAPI schema generation with a Pydantic
    "class not fully defined" error (Pydantic resolves models by name through the module's
    global namespace; a locally-scoped class is never bound there). It only surfaced on a real
    deploy hitting /openapi.json — the stub-based test above can't catch it, since the fake
    BaseModel doesn't do real JSON-schema generation. This test at least confirms the
    module-level structural fix stays in place: RunRequestBody must be a true attribute of the
    fastapi_ingress module, not hidden inside build_app's closure."""
    fake_fastapi, fake_pydantic, _FakeHTTPException, _FakeBaseModel = _make_fastapi_pydantic_stubs()
    restore = _install_fastapi_pydantic_stubs(fake_fastapi, fake_pydantic)

    try:
        import agentframework.integrations.fastapi_ingress as ingress_module

        is_module_level = "RunRequestBody" in vars(ingress_module)
        # the class the route actually annotates with must be the SAME object as the
        # module-level one — i.e. not shadowed by a second, locally-scoped definition.
        source = ingress_module.build_app.__code__.co_consts
        no_nested_class_code = not any(
            getattr(c, "co_name", None) == "RunRequestBody" for c in source if hasattr(c, "co_name")
        )
    finally:
        restore()

    check(is_module_level,
          "RunRequestBody is defined at fastapi_ingress module level, not inside build_app()")
    check(no_nested_class_code,
          "build_app()'s compiled code contains no nested RunRequestBody class definition")


if __name__ == "__main__":
    for fn in [
        test_topological_order,
        test_missing_dependency,
        test_cycle_detection,
        test_phase1_executor,
        test_phase1_retry,
        test_phase2_orchestrator,
        test_phase2_timeout,
        test_phase1_and_phase2_agree,
        test_phase3_rest_ingress,
        test_phase3_queue_worker_and_output_actions,
        test_phase4_http_tool_real_request,
        test_phase4_search_tool,
        test_phase4_llm_tool_mock_provider,
        test_phase4_tool_input_validation_guardrail,
        test_phase4_task_resolves_tool_in_orchestrator,
        test_phase4_missing_tool_registry_raises,
        test_phase5_short_term_scratchpad,
        test_phase5_short_term_isolated_per_run,
        test_phase5_long_term_recall_across_runs,
        test_phase5_long_term_isolated_per_session,
        test_phase5_missing_memory_raises_clear_error,
        test_phase6_required_keys_guardrail_rejects_and_no_retry,
        test_phase6_flow_level_guardrail_applies_to_every_task,
        test_phase6_content_filter_rejects_bad_output,
        test_phase6_metrics_and_logger_record_successful_run,
        test_phase6_retryable_error_is_still_retried,
        test_phase7_compile_to_airflow_dag_is_valid_python,
        test_phase7_compile_to_airflow_dag_has_correct_structure,
        test_phase7_airflow_execution_matches_sync_executor,
        test_phase8_support_agent_memory_recall_across_tickets,
        test_phase8_support_agent_guardrail_blocks_bad_reply,
        test_phase8_research_agent_reflection_pass_adds_missing_citations,
        test_phase9_approval_pauses_run_and_resume_approved_continues,
        test_phase9_approval_rejected_fails_the_run,
        test_anthropic_provider_wiring_against_stub_client,
        test_gemini_provider_wiring_against_stub_client,
        test_server_wiring_against_stub_fastapi,
        test_fastapi_ingress_session_id_enables_cross_request_memory_recall,
        test_fastapi_ingress_run_request_body_is_module_level,
    ]:
        print(f"{fn.__name__}:")
        fn()
    print(f"\n{passed} checks passed.")
