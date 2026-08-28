"""Production LLMProvider backed by the Anthropic API (tools/llm_tool.py > LLMProvider).
Same interface as MockLLMProvider — swap it into any LlmTool(provider=...) without changing
any Flow/Task that uses it. Requires the `llm` extra:

    pip install -e ".[llm]"   # anthropic

and an API key: pass api_key=... explicitly, or set the ANTHROPIC_API_KEY environment variable
(checked automatically if api_key isn't given).

Honesty note (see docs/Memory.md): this file was written and its request/response-parsing logic
verified against a stub client (this sandbox has no network access or API key to call the real
API) — the actual live API call has not been executed from within this repo. Run
run_demo_real_llm.py with a real key to do that verification yourself.
"""
from __future__ import annotations

import os
from typing import Any, Optional

from agentframework.tools.llm_tool import LLMProvider


class AnthropicLLMProvider(LLMProvider):
    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "claude-sonnet-4-5",
        max_tokens: int = 1024,
    ):
        try:
            import anthropic
        except ImportError as exc:
            raise ImportError(
                "AnthropicLLMProvider requires the 'llm' extra: pip install -e '.[llm]'"
            ) from exc

        key = api_key or os.environ.get("ANTHROPIC_API_KEY")
        if not key:
            raise ValueError(
                "AnthropicLLMProvider needs an API key: pass api_key=..., or set the "
                "ANTHROPIC_API_KEY environment variable."
            )

        self._client = anthropic.AsyncAnthropic(api_key=key)
        self.model = model
        self.max_tokens = max_tokens
        # Set after each complete() call — real token counts from the API response, used by
        # LlmTool in place of its word-count estimate when this attribute is present.
        self.last_usage: Optional[dict[str, int]] = None

    async def complete(self, prompt: str, **kwargs: Any) -> str:
        model = kwargs.pop("model", self.model)
        max_tokens = kwargs.pop("max_tokens", self.max_tokens)
        system = kwargs.pop("system", None)

        create_kwargs: dict[str, Any] = {
            "model": model,
            "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": prompt}],
        }
        if system:
            create_kwargs["system"] = system

        response = await self._client.messages.create(**create_kwargs)

        self.last_usage = {
            "input_tokens": response.usage.input_tokens,
            "output_tokens": response.usage.output_tokens,
        }

        return "".join(
            block.text for block in response.content if getattr(block, "type", None) == "text"
        )
