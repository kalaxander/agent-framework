"""Built-in guardrails. Each is a small, self-contained, in-process check — no external
dependency. Attach the same instance to multiple tasks or to a whole Flow to share state
(e.g. one RateLimitGuardrail shared across every LLM-calling task in a flow).
"""
from __future__ import annotations

import time
from typing import Any

from agentframework.core.errors import GuardrailViolation
from agentframework.guardrails.base import Guardrail


class RequiredKeysGuardrail(Guardrail):
    """Pre-execution: rejects if `input` is missing any of `keys`."""

    name = "required_keys"

    def __init__(self, keys: list[str]):
        self.keys = keys

    def pre_execute(self, task_name: str, input: dict[str, Any]) -> None:
        missing = [k for k in self.keys if k not in input]
        if missing:
            raise GuardrailViolation(
                f"Task '{task_name}' missing required input key(s): {missing}"
            )


class RateLimitGuardrail(Guardrail):
    """Pre-execution: rejects once more than `max_calls` have gone through in the trailing
    `window_seconds`. Counts across every task/flow this instance is attached to — attach one
    shared instance wherever the limit should apply jointly (e.g. one LLM provider's quota)."""

    name = "rate_limit"

    def __init__(self, max_calls: int, window_seconds: float):
        self.max_calls = max_calls
        self.window_seconds = window_seconds
        self._call_times: list[float] = []

    def pre_execute(self, task_name: str, input: dict[str, Any]) -> None:
        now = time.monotonic()
        cutoff = now - self.window_seconds
        self._call_times = [t for t in self._call_times if t >= cutoff]
        if len(self._call_times) >= self.max_calls:
            raise GuardrailViolation(
                f"Task '{task_name}' rejected: rate limit of {self.max_calls} calls per "
                f"{self.window_seconds}s exceeded"
            )
        self._call_times.append(now)


class BudgetGuardrail(Guardrail):
    """Pre-execution: rejects once more than `max_calls` total have gone through this instance
    (no time window — a hard lifetime cap, e.g. a per-run or per-session budget)."""

    name = "budget"

    def __init__(self, max_calls: int):
        self.max_calls = max_calls
        self.calls_used = 0

    def pre_execute(self, task_name: str, input: dict[str, Any]) -> None:
        if self.calls_used >= self.max_calls:
            raise GuardrailViolation(
                f"Task '{task_name}' rejected: budget of {self.max_calls} calls exhausted"
            )
        self.calls_used += 1


class ContentFilterGuardrail(Guardrail):
    """Post-execution: rejects if the stringified output contains any banned substring
    (case-insensitive). Stand-in for a real PII/content-safety filter — swap the match logic
    for a real classifier/service without changing the Guardrail interface."""

    name = "content_filter"

    def __init__(self, banned_substrings: list[str]):
        self.banned_substrings = banned_substrings

    def post_execute(self, task_name: str, output: Any) -> None:
        text = str(output).lower()
        for term in self.banned_substrings:
            if term.lower() in text:
                raise GuardrailViolation(
                    f"Task '{task_name}' output rejected by content filter (matched '{term}')"
                )
