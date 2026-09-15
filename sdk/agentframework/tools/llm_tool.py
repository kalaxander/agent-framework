"""Built-in LLM call tool, with a pluggable `LLMProvider` interface (docs/PRD.md > Target Users:
"pluggable interface for calling external systems ... LLMs").

`MockLLMProvider` is the reference/test provider — deterministic canned responses, no network or
API key required (this sandbox has neither). A real provider (Anthropic, OpenAI, a local model
server, etc.) implements the same `LLMProvider.complete()` interface and swaps in without
touching `LlmTool` or any Task/Flow that uses it — see docs/Memory.md for the open question on
which provider(s) to support first.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Optional

from agentframework.core.errors import GuardrailViolation
from agentframework.tools.base import Tool


class LLMProvider(ABC):
    @abstractmethod
    async def complete(self, prompt: str, **kwargs: Any) -> str: ...


class MockLLMProvider(LLMProvider):
    """Returns a canned response if any key in `responses` appears in the prompt, else
    `default`. Deterministic and offline — for tests, demos, and local development without an
    API key."""

    def __init__(self, responses: Optional[dict[str, str]] = None, default: str = "(mock response)"):
        self.responses = responses or {}
        self.default = default
        self.calls: list[str] = []  # prompts seen, for test assertions

    async def complete(self, prompt: str, **kwargs: Any) -> str:
        self.calls.append(prompt)
        for key, response in self.responses.items():
            if key in prompt:
                return response
        return self.default


class LlmTool(Tool):
    """Input: {"prompt": str, ...extra kwargs passed through to the provider}
    Output: {"response": str, "usage": {"tokens": int, "cost": float}}

    `usage` uses the provider's real token count when available (a provider can set
    `self.last_usage = {"input_tokens": int, "output_tokens": int}` after `complete()` — see
    `integrations/anthropic_llm_provider.py`); otherwise falls back to a word-count estimate
    (what `MockLLMProvider` gets, since it has no real tokenizer).
    """

    name = "llm_call"

    def __init__(self, provider: LLMProvider, cost_per_1k_tokens: float = 0.0):
        self.provider = provider
        self.cost_per_1k_tokens = cost_per_1k_tokens

    def validate_input(self, input: dict[str, Any]) -> None:
        if "prompt" not in input or not isinstance(input["prompt"], str):
            raise GuardrailViolation("llm_call requires a string 'prompt' field")

    async def run(self, input: dict[str, Any]) -> dict[str, Any]:
        extra = {k: v for k, v in input.items() if k != "prompt"}
        prompt = input["prompt"]
        response = await self.provider.complete(prompt, **extra)

        real_usage = getattr(self.provider, "last_usage", None)
        if real_usage:
            tokens = real_usage.get("input_tokens", 0) + real_usage.get("output_tokens", 0)
        else:
            tokens = len(prompt.split()) + len(response.split())  # estimate, no real tokenizer
        cost = (tokens / 1000) * self.cost_per_1k_tokens

        return {"response": response, "usage": {"tokens": tokens, "cost": cost}}
