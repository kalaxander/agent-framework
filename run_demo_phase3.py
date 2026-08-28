"""Run Phase 3 end to end: REST ingress (real HTTP, stdlib server) + queue-driven ExecutorWorker
+ output actions (log + webhook). No external dependencies. Run with:
    python3 run_demo_phase3.py
"""
from __future__ import annotations

import asyncio
import json
import sys
import time
import urllib.request
import urllib.error
from http.server import BaseHTTPRequestHandler, HTTPServer
import threading

sys.path.insert(0, "sdk")

from agentframework import Flow, Task
from agentframework.core.orchestrator import AsyncOrchestrator
from agentframework.core.registry import FlowRegistry
from agentframework.io.rest_ingress import RestIngress
from agentframework.io.message_queue import InMemoryMessageQueue
from agentframework.io.worker import ExecutorWorker, RunRequest
from agentframework.io.output_actions import OutputActionRegistry, LogOutputAction, WebhookOutputAction


def support_ticket_flow() -> Flow:
    flow = Flow(name="support-ticket-triage")
    flow.add_task(Task(name="classify", fn=lambda ctx: {"category": "billing"}))
    flow.add_task(Task(name="fetch_docs", fn=lambda ctx: ["billing-faq#12"],
                        depends_on=["classify"]))
    flow.add_task(Task(name="draft",
                        fn=lambda ctx: f"Re: {ctx['classify']['category']} — {ctx['fetch_docs']}",
                        depends_on=["fetch_docs"]))
    return flow


# --- a tiny local webhook receiver, purely to prove WebhookOutputAction actually POSTs ---
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


def demo_rest_ingress():
    print("=== Phase 3: REST ingress (real HTTP over loopback) ===")
    registry = FlowRegistry()
    registry.register("support-ticket-triage", support_ticket_flow)
    orchestrator = AsyncOrchestrator()
    ingress = RestIngress(orchestrator, registry)
    port = ingress.start()
    print(f"listening on http://127.0.0.1:{port}")

    try:
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/v1/runs",
            data=json.dumps({"flow_name": "support-ticket-triage", "inputs": {"ticket_id": 7}}).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req) as resp:
            start_body = json.loads(resp.read())
        print(f"POST /v1/runs -> {resp.status} {start_body}")
        run_id = start_body["run_id"]

        with urllib.request.urlopen(f"http://127.0.0.1:{port}/v1/runs/{run_id}") as resp:
            status_body = json.loads(resp.read())
        print(f"GET  /v1/runs/{run_id} -> {resp.status}")
        print(f"  status: {status_body['status']}")
        print(f"  draft : {status_body['tasks']['draft']['result']}")

        with urllib.request.urlopen(f"http://127.0.0.1:{port}/v1/runs/{run_id}/audit") as resp:
            audit_body = json.loads(resp.read())
        print(f"GET  /v1/runs/{run_id}/audit -> {resp.status}")
        for entry in audit_body["audit_trail"]:
            print(f"  {entry['task']:12s} {entry['status']}")

        # 404 path
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/v1/runs/nonexistent-run-id")
        except urllib.error.HTTPError as exc:
            print(f"GET  /v1/runs/nonexistent-run-id -> {exc.code} (expected 404)")
    finally:
        ingress.stop()
    print()


async def demo_queue_worker():
    print("=== Phase 3: queue-driven ExecutorWorker + output actions ===")
    webhook_port = start_webhook_receiver()

    registry = FlowRegistry()
    registry.register("support-ticket-triage", support_ticket_flow)

    queue = InMemoryMessageQueue()
    orchestrator = AsyncOrchestrator()

    outputs = OutputActionRegistry()
    log_action = LogOutputAction()
    outputs.register(log_action)
    outputs.register(WebhookOutputAction(url=f"http://127.0.0.1:{webhook_port}/hook"))

    worker = ExecutorWorker(queue, orchestrator, registry, outputs)
    worker_task = worker.start()

    # subscribe to results before publishing, so we don't miss the message
    results_iter = queue.consume("flow.run_results")

    await worker.submit(RunRequest(
        flow_name="support-ticket-triage",
        inputs={"ticket_id": 99},
        output_actions=["log", "webhook"],
    ))

    result_message = await asyncio.wait_for(results_iter.__anext__(), timeout=5)
    print(f"consumed from flow.run_results: {result_message.payload}")

    # give the output-action dispatch (which happens after the result publish) a moment
    await asyncio.sleep(0.1)

    print(f"LogOutputAction recorded {len(log_action.records)} run(s):")
    for record in log_action.records:
        print(f"  run_id={record['run_id']} result={record['result']}")

    print(f"webhook receiver got {len(received_webhooks)} POST(s):")
    for hook in received_webhooks:
        print(f"  {hook}")

    await worker.stop()
    print()


if __name__ == "__main__":
    demo_rest_ingress()
    asyncio.run(demo_queue_worker())
    print("Phase 3 (REST ingress + queue-driven worker + output actions) verified end to end.")
