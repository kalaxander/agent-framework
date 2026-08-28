"""Reference Agent #2 — Research Agent (docs/Phases.md Phase 8).

Real workflow: ingest a research question -> search an internal source index -> fetch the top
source's full content (real HTTP call) -> summarize findings -> draft a report -> critique the
draft against what was actually searched -> produce a revised, cited final report.

The critique -> revise step is a bounded, single-pass stand-in for the "reflection loops" stretch
goal (docs/Phases.md > Stretch Goals). The Task/Flow model is a DAG, not a loop construct — there
is no "repeat until satisfied" primitive in this framework yet — so this demonstrates one fixed
critique-and-revise pass rather than an open-ended loop. See docs/Memory.md.

Exercises: ToolRegistry (Phase 4: search + http_call + llm_call), a flow-level guardrail shared
across every llm_call (Phase 6: RateLimitGuardrail), a post-execution content filter on the
final report, and metrics/logging.
"""
from __future__ import annotations

from typing import Optional

from agentframework import Flow, Task
from agentframework.guardrails.builtin import ContentFilterGuardrail, RateLimitGuardrail, \
    RequiredKeysGuardrail
from agentframework.tools.http_tool import HttpTool
from agentframework.tools.llm_tool import LLMProvider, LlmTool, MockLLMProvider
from agentframework.tools.registry import ToolRegistry
from agentframework.tools.search_tool import SimpleSearchTool

SOURCES = {
    "report-2026-renewables": "Renewable energy adoption grew 12% year over year according to "
                               "the latest industry report.",
    "report-2026-solar": "Solar capacity additions outpaced wind installations for the third "
                          "consecutive year.",
}


def build_tool_registry(llm_provider: Optional[LLMProvider] = None) -> ToolRegistry:
    tools = ToolRegistry()
    tools.register(SimpleSearchTool(documents=SOURCES))
    tools.register(HttpTool())
    tools.register(LlmTool(
        provider=llm_provider or MockLLMProvider(
            responses={
                "summarize findings": "Renewable energy is growing significantly, driven "
                                       "largely by solar capacity gains.",
                "draft report": "Report: renewable adoption is rising, led by strong growth "
                                 "in solar over the past year.",
                # the draft response above deliberately omits citations — the critique step
                # (plain Python, not an LLM call) catches that, and this revise response shows
                # the correction:
                "revise report": "Report: renewable adoption is rising, led by strong growth "
                                  "in solar over the past year. Sources: "
                                  "[report-2026-renewables] [report-2026-solar]",
            },
            default="(no response configured for this prompt)",
        ),
        cost_per_1k_tokens=0.002,
    ))
    return tools


def build_flow(source_base_url: str) -> Flow:
    """`__inputs__` must contain: {"question": str}."""
    shared_llm_rate_limit = RateLimitGuardrail(max_calls=10, window_seconds=60)

    flow = Flow(name="research-report", guardrails=[shared_llm_rate_limit])

    flow.add_task(Task(
        name="search_sources", tool="search",
        tool_input=lambda ctx: {"query": ctx["__inputs__"]["question"], "top_k": 2},
        guardrails=[RequiredKeysGuardrail(["query"])],
    ))

    flow.add_task(Task(
        name="fetch_top_source", tool="http_call", depends_on=["search_sources"],
        tool_input=lambda ctx: {
            "url": f"{source_base_url}/source/{ctx['search_sources']['results'][0]['doc_id']}"
        },
    ))

    flow.add_task(Task(
        name="summarize_findings", tool="llm_call",
        depends_on=["search_sources", "fetch_top_source"],
        tool_input=lambda ctx: {
            "prompt": f"summarize findings: {ctx['fetch_top_source']['body']}",
        },
    ))

    flow.add_task(Task(
        name="draft_report", tool="llm_call", depends_on=["summarize_findings"],
        tool_input=lambda ctx: {
            "prompt": f"draft report: {ctx['summarize_findings']['response']}",
        },
    ))

    def critique_report(ctx):
        """Plain-Python critique (not an LLM call, deliberately — a cheap, deterministic check
        is more trustworthy for "did we cite our sources" than asking another model)."""
        searched_ids = [r["doc_id"] for r in ctx["search_sources"]["results"]]
        draft_text = ctx["draft_report"]["response"]
        missing = [doc_id for doc_id in searched_ids if doc_id not in draft_text]
        return {"missing_citations": missing, "passed": len(missing) == 0}

    flow.add_task(Task(
        name="critique_report", fn=critique_report,
        depends_on=["draft_report", "search_sources"],
    ))

    flow.add_task(Task(
        name="finalize_report", tool="llm_call",
        depends_on=["draft_report", "critique_report"],
        tool_input=lambda ctx: {
            "prompt": f"revise report: {ctx['draft_report']['response']} "
                      f"missing citations: {ctx['critique_report']['missing_citations']}",
        },
        guardrails=[ContentFilterGuardrail(["plagiar"])],
    ))

    return flow
