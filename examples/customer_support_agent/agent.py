"""Reference Agent #1 — Customer Support Ticket Agent (docs/Phases.md Phase 8).

Real workflow: ingest a ticket -> search a KB for relevant docs -> recall this customer's past
tickets (long-term memory, keyed by customer_id) -> draft a reply -> guardrail-check the reply
-> remember this ticket for next time.

Exercises: ToolRegistry (Phase 4: search + llm_call), long-term memory (Phase 5, session_id =
customer_id so recall works across separate tickets from the same customer), guardrails
(Phase 6: required input keys + a content filter on the drafted reply).
"""
from __future__ import annotations

from typing import Optional

from agentframework import Flow, Task
from agentframework.guardrails.builtin import ContentFilterGuardrail, RequiredKeysGuardrail
from agentframework.tools.llm_tool import LLMProvider, LlmTool, MockLLMProvider
from agentframework.tools.registry import ToolRegistry
from agentframework.tools.search_tool import SimpleSearchTool

KB_DOCS = {
    "billing-faq#12": "How to request a refund for a billing overcharge on your account.",
    "refund-policy#3": "Our refund policy covers items returned within 30 days.",
    "shipping-faq#7": "Shipping usually takes 3-5 business days domestically.",
    "password-reset#2": "Reset your password from the account settings page.",
}


def build_tool_registry(llm_provider: Optional[LLMProvider] = None) -> ToolRegistry:
    tools = ToolRegistry()
    tools.register(SimpleSearchTool(documents=KB_DOCS))
    tools.register(LlmTool(
        provider=llm_provider or MockLLMProvider(
            responses={
                "billing": "Thanks for reaching out about your billing question. Based on our "
                           "refund policy, you're eligible for a refund and we've processed it.",
                "shipping": "Thanks for asking about shipping — most orders arrive within 3-5 "
                            "business days domestically.",
                "password": "You can reset your password from the account settings page at "
                             "any time.",
            },
            default="Thanks for contacting support — we're looking into this for you.",
        ),
        cost_per_1k_tokens=0.002,
    ))
    return tools


def build_flow() -> Flow:
    """`__inputs__` must contain: {"ticket_text": str}. Call orchestrator.run(flow, inputs,
    session_id=customer_id) — the session_id is what scopes long-term memory recall to one
    customer's history across separate tickets/runs."""
    flow = Flow(name="customer-support-ticket")

    flow.add_task(Task(
        name="search_kb", tool="search",
        tool_input=lambda ctx: {"query": ctx["__inputs__"]["ticket_text"], "top_k": 2},
        guardrails=[RequiredKeysGuardrail(["query"])],
    ))

    async def recall_customer_history(ctx):
        records = await ctx["__memory__"].recall_long(ctx["__inputs__"]["ticket_text"], top_k=2)
        return [r.text for r in records]

    flow.add_task(Task(name="recall_history", fn=recall_customer_history))

    flow.add_task(Task(
        name="draft_reply", tool="llm_call",
        depends_on=["search_kb", "recall_history"],
        tool_input=lambda ctx: {
            "prompt": f"{ctx['__inputs__']['ticket_text']} | relevant docs: "
                      f"{[r['doc_id'] for r in ctx['search_kb']['results']]} | "
                      f"customer history: {ctx['recall_history']}",
        },
        guardrails=[ContentFilterGuardrail(["lawsuit", "we are not liable"])],
    ))

    async def remember_ticket(ctx):
        await ctx["__memory__"].remember_long(
            ctx["__inputs__"]["ticket_text"],
            metadata={"reply": ctx["draft_reply"]["response"]},
        )
        return "remembered"

    flow.add_task(Task(name="remember_ticket", fn=remember_ticket, depends_on=["draft_reply"]))

    return flow
