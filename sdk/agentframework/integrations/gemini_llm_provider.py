"""Production LLMProvider backed by the Gemini API (tools/llm_tool.py > LLMProvider). Same
interface as MockLLMProvider/AnthropicLLMProvider — swap into any LlmTool(provider=...) without
changing any Flow/Task that uses it. Requires the `gemini` extra:

    pip install -e ".[gemini]"   # google-genai

and an API key: pass api_key=... explicitly, or set the GEMINI_API_KEY environment variable
(the SDK's Client() picks it up automatically if api_key isn't given). Get a free key at
https://aistudio.google.com/app/apikey — no billing required for the free tier.

Honesty note (see docs/Memory.md): this file was written and its request/response-parsing logic
verified against a stub client (this sandbox has no network access or API key to call the real
API) — the actual live API call has not been executed from within this repo. Run
run_demo_real_llm.py with a real key to do that verification yourself.
"""
from __future__ import annotations

import os
from typing import Any, Optional

from agentframework.tools.llm_tool import LLMProvider


class GeminiLLMProvider(LLMProvider):
    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "gemini-2.5-flash",
    ):
        try:
            from google import genai
        except ImportError as exc:
            raise ImportError(
                "GeminiLLMProvider requires the 'gemini' extra: pip install -e '.[gemini]'"
            ) from exc

        key = api_key or os.environ.get("GEMINI_API_KEY")
        if not key:
            raise ValueError(
                "GeminiLLMProvider needs an API key: pass api_key=..., or set the "
                "GEMINI_API_KEY environment variable. Get a free one at "
                "https://aistudio.google.com/app/apikey"
            )

        self._client = genai.Client(api_key=key)
        self.model = model
        # Set after each complete() call — real token counts from the API response, used by
        # LlmTool in place of its word-count estimate when this attribute is present.
        self.last_usage: Optional[dict[str, int]] = None

    async def complete(self, prompt: str, **kwargs: Any) -> str:
        model = kwargs.pop("model", self.model)

        response = await self._client.aio.models.generate_content(
            model=model,
            contents=prompt,
        )

        usage = response.usage_metadata
        if usage is not None:
            self.last_usage = {
                "input_tokens": usage.prompt_token_count or 0,
                "output_tokens": usage.candidates_token_count or 0,
            }

        return response.text or ""
