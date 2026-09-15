"""Queue-driven executor worker (docs/PRD.md > Input Handlers: "queue consumer" path;
docs/Architecture.md > Executors).

Consumes `RunRequest` messages from a topic (e.g. published by a Kafka ingress consumer),
executes the named flow through the same `AsyncOrchestrator` the REST path uses, and dispatches
configured output actions with the result. This is what makes ingress genuinely async/
event-driven instead of "REST handler blocks until the flow finishes."

Scope note (see docs/Memory.md): this worker decouples *flow-level* scheduling from ingress via
the queue. Decomposing execution further — each individual task hopping across Kafka
task-assigned/task-completed topics to physically separate executor processes — is deferred to
Phase 7 (Apache integration depth); today all tasks in a run execute inside one worker process,
which already satisfies "Ingress (queue) -> Orchestrator -> Executors -> State/Memory" as long as
you run 1+ of these workers as separate processes/pods talking to a shared queue + state store.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field

from agentframework.core.orchestrator import AsyncOrchestrator
from agentframework.core.registry import FlowRegistry
from agentframework.core.state_store import RunStatus
from agentframework.io.message_queue import MessageQueue
from agentframework.io.output_actions import OutputActionRegistry

logger = logging.getLogger("agentframework.worker")

RUN_REQUESTS_TOPIC = "flow.run_requests"
RUN_RESULTS_TOPIC = "flow.run_results"


@dataclass
class RunRequest:
    flow_name: str
    inputs: dict
    output_actions: list[str] = field(default_factory=list)
    run_id: str | None = None  # caller-supplied idempotency key, optional


class ExecutorWorker:
    def __init__(
        self,
        queue: MessageQueue,
        orchestrator: AsyncOrchestrator,
        flow_registry: FlowRegistry,
        output_actions: OutputActionRegistry,
    ):
        self.queue = queue
        self.orchestrator = orchestrator
        self.flow_registry = flow_registry
        self.output_actions = output_actions
        self._task: asyncio.Task | None = None

    async def submit(self, request: RunRequest) -> None:
        await self.queue.publish(RUN_REQUESTS_TOPIC, request.__dict__)

    async def _handle_one(self, payload: dict) -> None:
        request = RunRequest(**payload)
        try:
            flow = self.flow_registry.build(request.flow_name)
            run = await self.orchestrator.run(flow, request.inputs)
            result = {name: state.result for name, state in run.tasks.items()}
            await self.queue.publish(
                RUN_RESULTS_TOPIC,
                {"run_id": run.run_id, "flow_name": flow.name, "status": run.status.value},
            )
            if request.output_actions:
                await self.output_actions.dispatch(
                    request.output_actions, run.run_id, flow.name, result
                )
        except Exception:
            logger.exception("Run failed for flow '%s'", request.flow_name)
            await self.queue.publish(
                RUN_RESULTS_TOPIC,
                {"flow_name": request.flow_name, "status": RunStatus.FAILED.value},
            )
            # Swallow here rather than re-raise: run_forever() must keep consuming subsequent
            # messages even if one run fails terminally (failure is already recorded above).

    async def run_forever(self) -> None:
        async for message in self.queue.consume(RUN_REQUESTS_TOPIC):
            await self._handle_one(message.payload)

    def start(self) -> asyncio.Task:
        """Start consuming in the background; returns the asyncio.Task (cancel to stop)."""
        self._task = asyncio.create_task(self.run_forever())
        return self._task

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
