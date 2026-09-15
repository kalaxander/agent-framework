"""Reference Agent #3 — Expense Approval Agent (Phase C).

Real workflow: an employee submits an expense -> search company expense policy for relevant
rules -> recall this employee's past expense history (long-term memory, keyed by employee_id)
-> an LLM writes an advisory assessment against policy + history -> a human reviewer must
approve or reject EVERY submission (Phase 9's human-in-the-loop: Task(requires_approval=True) +
AsyncOrchestrator.resume() — a genuine asyncio.Event suspend, not a poll loop) -> on approval,
record the decision to memory for future recall.

Exercises: ToolRegistry (search + llm_call), long-term memory (session_id = employee_id, same
pattern as the support agent's customer_id), guardrails (required input keys) — and, the actual
reason this agent exists, Phase 9's human-in-the-loop approval, which neither of the first two
reference agents demonstrates at all despite it being built, tested, and documented since Phase 9.

Design note: the approval gate is unconditional (every expense needs a human sign-off, not just
large ones) rather than conditional on amount. This isn't a simplification forced by the
framework's limits — Task.requires_approval is set once at flow-definition time, not evaluated
per-run, so "auto-approve under $100, require approval above it" would need two separate flows
(or a runtime decision to pick between them) rather than one flow with a conditional gate. It's
also a completely normal real expense-policy design on its own — plenty of real organizations
require manager sign-off on every expense regardless of size. Kept simple deliberately.
"""
from __future__ import annotations

from typing import Optional

from agentframework import Flow, Task
from agentframework.guardrails.builtin import RequiredKeysGuardrail
from agentframework.tools.llm_tool import LLMProvider, LlmTool, MockLLMProvider
from agentframework.tools.registry import ToolRegistry
from agentframework.tools.search_tool import SimpleSearchTool

POLICY_DOCS = {
    "policy-travel#1": "Travel expenses over $500 require pre-approval from the traveler's "
                        "manager before booking.",
    "policy-meals#2": "Meal expenses are capped at $75 per day when traveling for business.",
    "policy-software#3": "Software purchases under $200 can be self-approved by team leads; "
                          "purchases above that threshold require IT review before submission.",
    "policy-general#4": "All expenses must include an itemized description and be submitted "
                         "within 30 days of the purchase date to be eligible for reimbursement.",
}


def build_tool_registry(llm_provider: Optional[LLMProvider] = None) -> ToolRegistry:
    tools = ToolRegistry()
    tools.register(SimpleSearchTool(documents=POLICY_DOCS))
    tools.register(LlmTool(
        provider=llm_provider or MockLLMProvider(
            responses={
                "travel": "This travel expense appears consistent with policy. If the amount "
                          "exceeds $500, please confirm manager pre-approval was obtained.",
                "meals": "This meal expense falls within the standard per-day cap under "
                         "company policy.",
                "software": "This software purchase should be checked against the "
                            "self-approval threshold for team leads.",
            },
            default="This expense has been reviewed against company policy; no issues "
                    "flagged, but final sign-off is still required.",
        ),
        cost_per_1k_tokens=0.002,
    ))
    return tools


def build_flow() -> Flow:
    """`__inputs__` must contain: {"employee_id": str, "amount": float, "category": str,
    "description": str}. Call orchestrator.run(flow, inputs, session_id=employee_id) — the
    session_id is what scopes long-term memory recall to one employee's expense history across
    separate submissions/runs, same convention as the support agent's customer_id.

    Every run pauses at "request_approval" (RunStatus.WAITING) until
    AsyncOrchestrator.resume(run_id, "request_approval", approved=...) is called — see run.py
    for the full pattern, including how to learn run_id immediately via the `on_created`
    callback rather than waiting for the (necessarily long-blocked) run() coroutine to return.
    """
    flow = Flow(name="expense-approval")

    flow.add_task(Task(
        name="search_policy", tool="search",
        tool_input=lambda ctx: {
            "query": f"{ctx['__inputs__']['category']} {ctx['__inputs__']['description']}",
            "top_k": 2,
        },
        guardrails=[RequiredKeysGuardrail(["query"])],
    ))

    async def recall_employee_history(ctx):
        records = await ctx["__memory__"].recall_long(
            f"{ctx['__inputs__']['category']} expense", top_k=3)
        return [r.text for r in records]

    flow.add_task(Task(name="recall_history", fn=recall_employee_history))

    flow.add_task(Task(
        name="assess_expense", tool="llm_call",
        depends_on=["search_policy", "recall_history"],
        tool_input=lambda ctx: {
            "prompt": f"Assess this expense against company policy: "
                      f"${ctx['__inputs__']['amount']} for {ctx['__inputs__']['category']} — "
                      f"{ctx['__inputs__']['description']}. Relevant policy sections: "
                      f"{[r['doc_id'] for r in ctx['search_policy']['results']]}. "
                      f"Employee's recent expense history: {ctx['recall_history']}",
        },
    ))

    flow.add_task(Task(
        name="request_approval",
        fn=lambda ctx: {
            "employee_id": ctx["__inputs__"]["employee_id"],
            "amount": ctx["__inputs__"]["amount"],
            "category": ctx["__inputs__"]["category"],
            "assessment": ctx["assess_expense"]["response"],
        },
        depends_on=["assess_expense"],
        requires_approval=True,
    ))

    async def record_decision(ctx):
        await ctx["__memory__"].remember_long(
            f"{ctx['__inputs__']['category']} expense: ${ctx['__inputs__']['amount']} — "
            f"{ctx['__inputs__']['description']}",
            metadata={"decision": "approved", "amount": ctx["__inputs__"]["amount"]},
        )
        return "recorded"

    flow.add_task(Task(name="record_decision", fn=record_decision,
                        depends_on=["request_approval"]))

    return flow
