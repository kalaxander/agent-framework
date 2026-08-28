"""REST ingress, stdlib-only (docs/PRD.md > Input Handlers: REST path).

Uses `http.server` rather than FastAPI so this runs with zero pip installs, consistent with the
rest of core/io (see the pydantic note in core/flow.py and the Kafka/Postgres split pattern).
`integrations/fastapi_ingress.py` is the production drop-in with the same routes, for when
FastAPI/uvicorn are available — swap one for the other; nothing else in the stack changes.

Routes:
    POST /v1/runs              {"flow_name": ..., "inputs": {...}, "output_actions": [...]}
                                -> 202 {"run_id": ...}   (runs synchronously today; see note below)
    GET  /v1/runs/{run_id}      -> run status + per-task states
    GET  /v1/runs/{run_id}/audit -> ordered task audit trail

Note: this reference server runs each flow to completion *within* the request (via a background
asyncio loop, see below) rather than handing off to a queue — i.e. it exercises the REST ingress
+ Orchestrator + StateStore path end to end. For the async queue-handoff path (ingress doesn't
block on flow completion), see io/worker.py + message_queue.py, wired together in run_demo.py.
"""
from __future__ import annotations

import asyncio
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Optional
from urllib.parse import urlparse

from agentframework.core.orchestrator import AsyncOrchestrator
from agentframework.core.registry import FlowRegistry
from agentframework.core.errors import FlowValidationError
from agentframework.core.state_store import RunStatus


class RestIngress:
    """Owns a background asyncio event loop (Orchestrator.run is async) and an HTTP server
    thread that submits work to it via `asyncio.run_coroutine_threadsafe`."""

    def __init__(self, orchestrator: AsyncOrchestrator, flow_registry: FlowRegistry, port: int = 0):
        self.orchestrator = orchestrator
        self.flow_registry = flow_registry
        self.port = port
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._loop_thread: Optional[threading.Thread] = None
        self._httpd: Optional[ThreadingHTTPServer] = None
        self._http_thread: Optional[threading.Thread] = None

    def start(self) -> int:
        self._loop = asyncio.new_event_loop()
        self._loop_thread = threading.Thread(target=self._loop.run_forever, daemon=True)
        self._loop_thread.start()

        ingress = self
        orchestrator = self.orchestrator
        flow_registry = self.flow_registry

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, fmt, *args):  # silence default access logging
                pass

            def _json(self, status: int, payload: dict) -> None:
                body = json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _run_coro(self, coro):
                future = asyncio.run_coroutine_threadsafe(coro, ingress._loop)
                return future.result()

            def do_POST(self):
                path = urlparse(self.path).path
                if path != "/v1/runs":
                    return self._json(404, {"error": "not found"})
                length = int(self.headers.get("Content-Length", 0))
                try:
                    body = json.loads(self.rfile.read(length) or b"{}")
                    flow = flow_registry.build(body["flow_name"])
                except (FlowValidationError, KeyError, json.JSONDecodeError) as exc:
                    return self._json(400, {"error": str(exc)})

                try:
                    run = self._run_coro(orchestrator.run(flow, body.get("inputs", {})))
                except Exception as exc:  # noqa: BLE001
                    return self._json(500, {"error": str(exc)})
                return self._json(202, {"run_id": run.run_id, "status": run.status.value})

            def do_GET(self):
                path = urlparse(self.path).path
                parts = path.strip("/").split("/")
                if len(parts) == 3 and parts[0] == "v1" and parts[1] == "runs":
                    run_id = parts[2]
                    run = self._run_coro(orchestrator.state_store.get_run(run_id))
                    if run is None:
                        return self._json(404, {"error": "run not found"})
                    return self._json(200, {
                        "run_id": run.run_id,
                        "flow_name": run.flow_name,
                        "status": run.status.value,
                        "tasks": {
                            name: {"status": s.status.value, "attempt": s.attempt,
                                   "result": s.result, "error": s.error}
                            for name, s in run.tasks.items()
                        },
                    })
                if len(parts) == 4 and parts[0] == "v1" and parts[1] == "runs" and parts[3] == "audit":
                    run_id = parts[2]
                    trail = self._run_coro(orchestrator.state_store.audit_trail(run_id))
                    return self._json(200, {"run_id": run_id, "audit_trail": [
                        {"task": s.name, "status": s.status.value, "attempt": s.attempt,
                         "error": s.error}
                        for s in trail
                    ]})
                return self._json(404, {"error": "not found"})

        self._httpd = ThreadingHTTPServer(("127.0.0.1", self.port), Handler)
        self.port = self._httpd.server_address[1]
        self._http_thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)
        self._http_thread.start()
        return self.port

    def stop(self) -> None:
        if self._httpd:
            self._httpd.shutdown()
        if self._loop:
            self._loop.call_soon_threadsafe(self._loop.stop)
