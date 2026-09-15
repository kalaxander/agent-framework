"""Registers named Flow factories so ingress (REST/queue) can resolve "run flow X" requests
without importing every flow module directly. A factory (not a Flow instance) is stored because
Task.fn closures often capture per-run state; each request should get a fresh Flow.
"""
from __future__ import annotations

from typing import Callable

from agentframework.core.errors import FlowValidationError
from agentframework.core.flow import Flow

FlowFactory = Callable[[], Flow]


class FlowRegistry:
    def __init__(self) -> None:
        self._factories: dict[str, FlowFactory] = {}

    def register(self, name: str, factory: FlowFactory) -> None:
        self._factories[name] = factory

    def build(self, name: str) -> Flow:
        if name not in self._factories:
            raise FlowValidationError(f"No flow registered under name '{name}'")
        return self._factories[name]()

    def names(self) -> list[str]:
        return sorted(self._factories)
