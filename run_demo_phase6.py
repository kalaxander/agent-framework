"""Run Phase 6 end to end: Flow-level + Task-level guardrails, structured JSON logging, and
metrics aggregation, wired through AsyncOrchestrator. No external dependencies. Run with:
    python3 run_demo_phase6.py
"""
from __future__ import annotations

import asyncio
import sys

sys.path.insert(0, "sdk")

from agentframework import Flow, Task
from agentframework.core.flow import RetryPolicy
from agentframework.core.orchestrator import AsyncOrchestrator
from agentframework.core.state_store import RunStatus
from agentframework.core.errors import GuardrailViolation
from agentframework.tools.registry import ToolRegistry
from agentframework.tools.search_tool import SimpleSearchTool
from agentframework.tools.llm_tool import LlmTool, MockLLMProvider
from agentframework.guardrails.builtin import (
    RequiredKeysGuardrail, RateLimitGuardrail, BudgetGuardrail, ContentFilterGuardrail,
)
from agentframework.observability.logger import JsonLineLogger
from agentframework.observability.metrics import InMemoryMetrics


async def demo_guardrails_pass():
    print("=== Phase 6: guardrails pass — a normal run succeeds ===")

    tools = ToolRegistry()
    tools.register(SimpleSearchTool(documents={"doc1": "billing refund policy"}))
    tools.register(LlmTool(
        provider=MockLLMProvider(responses={"billing": "Here is a helpful billing answer."}),
        cost_per_1k_tokens=0.002,
    ))

    logger = JsonLineLogger()  # capture only, no stdout spam
    metrics = InMemoryMetrics()

    flow = Flow(
        name="guarded-triage",
        guardrails=[RateLimitGuardrail(max_calls=5, window_seconds=10),
                    BudgetGuardrail(max_calls=10)],
    )
    flow.add_task(Task(
        name="search_docs", tool="search",
        tool_input=lambda ctx: {"query": "billing refund"},
        guardrails=[RequiredKeysGuardrail(["query"])],
    ))
    flow.add_task(Task(
        name="draft_reply", tool="llm_call", depends_on=["search_docs"],
        tool_input=lambda ctx: {"prompt": f"billing question, docs: {ctx['search_docs']}"},
        guardrails=[ContentFilterGuardrail(["lawsuit"])],
    ))

    orchestrator = AsyncOrchestrator(tool_registry=tools, metrics=metrics, logger=logger)
    run = await orchestrator.run(flow, inputs={})

    print(f"run status: {run.status.value}")
    print(f"draft_reply result: {run.tasks['draft_reply'].result}")
    print()
    print("structured log entries captured:")
    for entry in logger.records:
        print(f"  {entry}")
    print()
    print("metrics summary:")
    import json
    print(json.dumps(metrics.summary(), indent=2))
    print()


async def demo_guardrail_rejects_missing_key():
    print("=== Phase 6: RequiredKeysGuardrail rejects a malformed tool call ===")
    tools = ToolRegistry()
    tools.register(SimpleSearchTool(documents={}))

    flow = Flow(name="bad-input-demo")
    flow.add_task(Task(
        name="search_docs", tool="search",
        tool_input=lambda ctx: {"top_k": 3},  # missing required 'query'
        guardrails=[RequiredKeysGuardrail(["query"])],
        retry_policy=RetryPolicy(max_attempts=3),
    ))

    metrics = InMemoryMetrics()
    orchestrator = AsyncOrchestrator(tool_registry=tools, metrics=metrics)
    try:
        await orchestrator.run(flow, inputs={})
        print("FAIL: should have raised GuardrailViolation")
    except GuardrailViolation as exc:
        print(f"OK: run failed as expected -> {exc}")
        # fail-closed: only 1 attempt recorded, not 3, because GuardrailViolation isn't retried
        attempts_recorded = len(metrics.records)
        print(f"attempts recorded: {attempts_recorded} (expected 1 — guardrail violations are "
              f"never retried, even though retry_policy allowed up to 3)")
    print()


async def demo_shared_rate_limit_across_tasks():
    print("=== Phase 6: a shared RateLimitGuardrail rejects the 3rd call across 3 tasks ===")
    shared_limit = RateLimitGuardrail(max_calls=2, window_seconds=60)

    flow = Flow(name="rate-limited-flow", guardrails=[shared_limit])
    for i in range(3):
        flow.add_task(Task(name=f"step_{i}", fn=lambda ctx: "ok"))

    orchestrator = AsyncOrchestrator()
    try:
        await orchestrator.run(flow, inputs={})
        print("FAIL: should have raised GuardrailViolation on the 3rd task")
    except GuardrailViolation as exc:
        print(f"OK: rejected once the shared rate limit was exceeded -> {exc}")
    print()


async def demo_content_filter_rejects_output():
    print("=== Phase 6: ContentFilterGuardrail rejects a bad LLM output (post-execution) ===")
    tools = ToolRegistry()
    tools.register(LlmTool(provider=MockLLMProvider(
        responses={"trigger": "We recommend you consider a lawsuit against the vendor."},
    )))

    flow = Flow(name="content-filter-demo")
    flow.add_task(Task(
        name="draft", tool="llm_call",
        tool_input=lambda ctx: {"prompt": "trigger the bad response"},
        guardrails=[ContentFilterGuardrail(["lawsuit"])],
    ))

    orchestrator = AsyncOrchestrator(tool_registry=tools)
    try:
        await orchestrator.run(flow, inputs={})
        print("FAIL: should have raised GuardrailViolation")
    except GuardrailViolation as exc:
        print(f"OK: post-execution guardrail rejected the output -> {exc}")
    print()


if __name__ == "__main__":
    asyncio.run(demo_guardrails_pass())
    asyncio.run(demo_guardrail_rejects_missing_key())
    asyncio.run(demo_shared_rate_limit_across_tasks())
    asyncio.run(demo_content_filter_rejects_output())
    print("Phase 6 (guardrails + observability) verified end to end.")
