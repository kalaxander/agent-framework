"""Run the customer support reference agent end to end: two customers, two tickets each — the
second ticket per customer demonstrates long-term memory recall of their first ticket. Output is
dispatched through the Phase 3 queue-driven ExecutorWorker to log + webhook output actions, and
Phase 6 guardrails/metrics/logging are all wired in. No external dependencies. Run with:
    python3 run.py     (from this directory)  OR  python3 examples/customer_support_agent/run.py
"""
from __future__ import annotations

import asyncio
import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "sdk"))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from agentframework.core.orchestrator import AsyncOrchestrator
from agentframework.core.registry import FlowRegistry
from agentframework.io.message_queue import InMemoryMessageQueue
from agentframework.io.output_actions import LogOutputAction, OutputActionRegistry, WebhookOutputAction
from agentframework.io.worker import ExecutorWorker, RunRequest
from agentframework.memory.in_memory import InMemoryLongTermMemory
from agentframework.observability.logger import JsonLineLogger
from agentframework.observability.metrics import InMemoryMetrics

from examples.customer_support_agent.agent import build_flow, build_tool_registry

received_webhooks: list[dict] = []


class _WebhookHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        received_webhooks.append(json.loads(self.rfile.read(length)))
        self.send_response(200)
        self.end_headers()


def start_webhook_receiver() -> int:
    server = HTTPServer(("127.0.0.1", 0), _WebhookHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server.server_address[1]


async def main():
    print("=== Reference Agent 1: Customer Support Ticket Agent ===\n")

    webhook_port = start_webhook_receiver()
    tools = build_tool_registry()
    long_term_memory = InMemoryLongTermMemory()
    metrics = InMemoryMetrics()
    logger = JsonLineLogger()

    registry = FlowRegistry()
    registry.register("customer-support-ticket", build_flow)

    orchestrator = AsyncOrchestrator(
        tool_registry=tools,
        long_term_memory=long_term_memory,
        metrics=metrics,
        logger=logger,
    )

    queue = InMemoryMessageQueue()
    outputs = OutputActionRegistry()
    log_action = LogOutputAction()
    outputs.register(log_action)
    outputs.register(WebhookOutputAction(url=f"http://127.0.0.1:{webhook_port}/hook"))

    worker = ExecutorWorker(queue, orchestrator, registry, outputs)
    worker.start()
    results_iter = queue.consume("flow.run_results")

    tickets = [
        ("cust-alice", "My billing was overcharged this month, I'd like a refund please."),
        ("cust-bob", "When will my shipping order arrive?"),
        ("cust-alice", "Following up on my billing overcharge from before — any update?"),
    ]

    for customer_id, ticket_text in tickets:
        print(f"--- Ticket for {customer_id} ---")
        print(f"customer says: {ticket_text!r}")

        await worker.submit(RunRequest(
            flow_name="customer-support-ticket",
            inputs={"ticket_text": ticket_text},
            output_actions=["log", "webhook"],
        ))
        result_message = await asyncio.wait_for(results_iter.__anext__(), timeout=5)
        await asyncio.sleep(0.05)  # let output-action dispatch (after publish) finish

        print(f"run status: {result_message.payload['status']}")
        print()

    await worker.stop()

    print(f"log output action recorded {len(log_action.records)} tickets total")
    print(f"webhook receiver got {len(received_webhooks)} POST(s)")
    print()

    # Demonstrate memory recall explicitly, with a stable session_id per customer (the queue
    # path above used default per-run session scoping, so it didn't exercise cross-run recall).
    print("--- Long-term memory recall demo (stable session_id per customer) ---")
    for customer_id, ticket_text in tickets:
        await orchestrator.run(build_flow(), {"ticket_text": ticket_text}, session_id=customer_id)
    alice_run = await orchestrator.run(
        build_flow(), {"ticket_text": "billing refund status"}, session_id="cust-alice",
    )
    print(f"cust-alice's recall_history sees prior tickets: "
          f"{alice_run.tasks['recall_history'].result}")
    print()

    print("metrics summary:")
    print(json.dumps(metrics.summary(), indent=2))


if __name__ == "__main__":
    asyncio.run(main())
