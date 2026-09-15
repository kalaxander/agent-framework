"""Run the research reference agent end to end: real HTTP fetch against a local source server,
a genuine critique-and-revise pass (the draft is deliberately uncited; the critique step catches
it; the revised report includes citations), plus metrics/logging and the shared rate-limit
guardrail. No external dependencies. Run with:
    python3 run.py     (from this directory)  OR  python3 examples/research_agent/run.py
"""
from __future__ import annotations

import asyncio
import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "sdk"))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from agentframework.core.orchestrator import AsyncOrchestrator
from agentframework.observability.logger import JsonLineLogger
from agentframework.observability.metrics import InMemoryMetrics

from examples.research_agent.agent import SOURCES, build_flow, build_tool_registry


class _SourceHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def do_GET(self):
        path = urlparse(self.path).path  # "/source/<doc_id>"
        doc_id = path.rsplit("/", 1)[-1]
        content = SOURCES.get(doc_id, "")
        body = json.dumps({"doc_id": doc_id, "content": content}).encode()
        self.send_response(200 if content else 404)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def start_source_server() -> int:
    server = HTTPServer(("127.0.0.1", 0), _SourceHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server.server_address[1]


async def main():
    print("=== Reference Agent 2: Research Agent (with a real reflection pass) ===\n")

    port = start_source_server()
    source_base_url = f"http://127.0.0.1:{port}"

    tools = build_tool_registry()
    flow = build_flow(source_base_url)
    metrics = InMemoryMetrics()
    logger = JsonLineLogger()

    orchestrator = AsyncOrchestrator(tool_registry=tools, metrics=metrics, logger=logger)
    run = await orchestrator.run(flow, inputs={"question": "renewable energy adoption trends"})

    print(f"run status: {run.status.value}\n")
    print(f"searched sources: "
          f"{[r['doc_id'] for r in run.tasks['search_sources'].result['results']]}")
    print(f"fetched (real HTTP): {run.tasks['fetch_top_source'].result}")
    print(f"summary: {run.tasks['summarize_findings'].result['response']}")
    print()
    print(f"draft report (before critique): {run.tasks['draft_report'].result['response']}")
    print(f"critique found missing citations: "
          f"{run.tasks['critique_report'].result['missing_citations']}")
    print(f"final report (after reflection pass): "
          f"{run.tasks['finalize_report'].result['response']}")
    print()

    citations_added = (
        run.tasks["critique_report"].result["missing_citations"]
        and all(
            doc_id in run.tasks["finalize_report"].result["response"]
            for doc_id in run.tasks["critique_report"].result["missing_citations"]
        )
    )
    print(f"reflection pass actually fixed the gap the critique found: {bool(citations_added)}")
    print()

    print("metrics summary:")
    print(json.dumps(metrics.summary(), indent=2))


if __name__ == "__main__":
    asyncio.run(main())
