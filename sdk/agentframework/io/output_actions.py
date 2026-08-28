"""Output actions (docs/PRD.md > Output Actions): pluggable emitters for a run's final result.
Interface is deliberately tiny so adding a new one never touches Orchestrator/worker code
(docs/Rules.md > Coding Conventions).
"""
from __future__ import annotations

import json
import urllib.error
import urllib.request
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


class OutputAction(ABC):
    name: str

    @abstractmethod
    async def send(self, run_id: str, flow_name: str, result: dict[str, Any]) -> None: ...


@dataclass
class LogOutputAction(OutputAction):
    """Default/fallback output action: records results in memory. Useful in tests and as a
    safe default when no external sink is configured."""

    name: str = "log"
    records: list[dict[str, Any]] = field(default_factory=list)

    async def send(self, run_id: str, flow_name: str, result: dict[str, Any]) -> None:
        self.records.append({"run_id": run_id, "flow_name": flow_name, "result": result})


@dataclass
class WebhookOutputAction(OutputAction):
    """POSTs the run result as JSON to a configured URL. Uses stdlib urllib (no `requests`
    dependency) so it works without any install step, consistent with the rest of core/io."""

    url: str
    name: str = "webhook"
    timeout_seconds: float = 10.0

    async def send(self, run_id: str, flow_name: str, result: dict[str, Any]) -> None:
        import asyncio

        body = json.dumps({"run_id": run_id, "flow_name": flow_name, "result": result}).encode()
        req = urllib.request.Request(
            self.url, data=body, headers={"Content-Type": "application/json"}, method="POST"
        )

        def _post() -> None:
            try:
                urllib.request.urlopen(req, timeout=self.timeout_seconds).read()
            except urllib.error.URLError as exc:
                raise ConnectionError(f"Webhook POST to {self.url} failed: {exc}") from exc

        # urllib is blocking; run it off the event loop like any other sync I/O call.
        await asyncio.get_running_loop().run_in_executor(None, _post)


class OutputActionRegistry:
    def __init__(self) -> None:
        self._actions: dict[str, OutputAction] = {}

    def register(self, action: OutputAction) -> None:
        self._actions[action.name] = action

    def get(self, name: str) -> OutputAction:
        if name not in self._actions:
            raise KeyError(f"No output action registered under name '{name}'")
        return self._actions[name]

    async def dispatch(
        self, names: list[str], run_id: str, flow_name: str, result: dict[str, Any]
    ) -> None:
        for name in names:
            await self.get(name).send(run_id, flow_name, result)
