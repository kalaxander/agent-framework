"""Reusable Flow factories, kept in an importable module (not a closure inside a demo script).
Needed by integrations/airflow_adapter.py: Airflow re-imports DAG files in a separate process,
so a live Flow object can't be handed to it directly — only an import path + factory function
name survive that boundary.
"""
from __future__ import annotations

from agentframework import Flow, Task


def simple_pipeline_flow() -> Flow:
    """classify -> fetch_docs -> draft, all plain fn tasks (no tool/network dependency) so this
    stays runnable anywhere, including inside a generated Airflow DAG with no other agentframework
    extras installed."""

    def classify(ctx):
        return {"category": "billing"}

    def fetch_docs(ctx):
        return ["billing-faq#12", "refund-policy#3"]

    def draft(ctx):
        category = ctx["classify"]["category"]
        docs = ctx["fetch_docs"]
        return f"Re: {category} — see {', '.join(docs)}"

    flow = Flow(name="simple-pipeline")
    flow.add_task(Task(name="classify", fn=classify))
    flow.add_task(Task(name="fetch_docs", fn=fetch_docs, depends_on=["classify"]))
    flow.add_task(Task(name="draft", fn=draft, depends_on=["classify", "fetch_docs"]))
    return flow
