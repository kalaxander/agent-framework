"""Run Phase 4 end to end: ToolRegistry + built-in HttpTool/LlmTool/SimpleSearchTool, resolved
by name inside a Flow and executed through the Phase 2 AsyncOrchestrator. No external
dependencies — HttpTool talks to a real local stdlib HTTP server over loopback. Run with:
    python3 run_demo_phase4.py
"""
from __future__ import annotations

import asyncio
import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

sys.path.insert(0, "sdk")

from agentframework import Flow, Task
from agentframework.core.orchestrator import AsyncOrchestrator
from agentframework.tools.registry import ToolRegistry
from agentframework.tools.http_tool import HttpTool
from agentframework.tools.llm_tool import LlmTool, MockLLMProvider
from agentframework.tools.search_tool import SimpleSearchTool
from agentframework.core.errors import GuardrailViolation


# --- a tiny local API for HttpTool to call, so the demo is fully self-contained/offline ---
class _KbHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def do_GET(self):
        body = json.dumps({"article": "Refunds are processed within 5-7 business days."}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def start_kb_server() -> int:
    server = HTTPServer(("127.0.0.1", 0), _KbHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server.server_address[1]


async def main():
    print("=== Phase 4: ToolRegistry + built-in tools, resolved inside a Flow ===")

    kb_port = start_kb_server()

    documents = {
        "billing-faq#12": "How to request a refund for a billing overcharge on your account.",
        "refund-policy#3": "Our refund policy covers items returned within 30 days.",
        "shipping-faq#7": "Shipping usually takes 3-5 business days domestically.",
    }

    tools = ToolRegistry()
    tools.register(HttpTool())
    tools.register(SimpleSearchTool(documents=documents))
    tools.register(LlmTool(provider=MockLLMProvider(
        responses={"billing": "Thanks for reaching out about your billing question — "
                               "here's a summary of relevant docs and policy."},
        default="Thanks for reaching out.",
    )))

    print(f"registered tools: {tools.names()}")

    # A support-ticket-triage flow where every step is a named tool, not a raw fn.
    flow = Flow(name="tool-based-triage")
    flow.add_task(Task(
        name="search_docs",
        tool="search",
        tool_input=lambda ctx: {"query": "billing refund"},
    ))
    flow.add_task(Task(
        name="fetch_policy_article",
        tool="http_call",
        tool_input=lambda ctx: {"url": f"http://127.0.0.1:{kb_port}/kb/refund-policy"},
    ))
    flow.add_task(Task(
        name="draft_reply",
        tool="llm_call",
        depends_on=["search_docs", "fetch_policy_article"],
        tool_input=lambda ctx: {
            "prompt": f"billing ticket. docs found: {ctx['search_docs']['results']}. "
                      f"policy article: {ctx['fetch_policy_article']['body']}",
        },
    ))

    orchestrator = AsyncOrchestrator(tool_registry=tools)
    run = await orchestrator.run(flow, inputs={"ticket_id": 5})

    print(f"run status: {run.status.value}")
    print(f"search_docs result:  {run.tasks['search_docs'].result}")
    print(f"http_call result:    {run.tasks['fetch_policy_article'].result}")
    print(f"llm_call result:     {run.tasks['draft_reply'].result}")
    print()

    print("=== Tool-level input validation (guardrail hook) ===")
    try:
        await tools.invoke("search", {"top_k": 3})  # missing required 'query'
        print("FAIL: should have raised GuardrailViolation")
    except GuardrailViolation as exc:
        print(f"OK: rejected invalid input -> {exc}")
    print()

    print("Phase 4 (tools registry + built-in tools) verified end to end.")


if __name__ == "__main__":
    asyncio.run(main())
