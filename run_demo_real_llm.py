"""Run the customer support reference agent against a REAL LLM instead of MockLLMProvider —
the "does it actually work with a real model" verification.

Checks for GEMINI_API_KEY first (free tier, no billing needed — get one at
https://aistudio.google.com/app/apikey), then ANTHROPIC_API_KEY as an alternative.

Setup (Gemini):
    cd sdk && pip install -e ".[gemini]" && cd ..
    export GEMINI_API_KEY=...

Setup (Anthropic):
    cd sdk && pip install -e ".[llm]" && cd ..
    export ANTHROPIC_API_KEY=sk-ant-...

Then:
    python3 run_demo_real_llm.py

Honesty note: I (Claude, building this) have no network access or API key in my own sandbox, so
this script and both providers' request/response handling were verified against stub clients
mimicking each real SDK, NOT against the live API — see docs/Memory.md. Running this file
yourself with a real key is the actual first live-API verification of this integration.
"""
from __future__ import annotations

import asyncio
import os
import sys

sys.path.insert(0, "sdk")
sys.path.insert(0, ".")


def pick_provider():
    """Returns (provider_instance, label) for whichever key is available, or (None, None)."""
    if os.environ.get("GEMINI_API_KEY"):
        try:
            from agentframework.integrations.gemini_llm_provider import GeminiLLMProvider
        except ImportError:
            print("GEMINI_API_KEY is set, but the 'google-genai' package isn't installed.\n"
                  "Run: cd sdk && pip install -e \".[gemini]\"")
            return None, None
        return GeminiLLMProvider(), "Gemini (gemini-2.5-flash)"

    if os.environ.get("ANTHROPIC_API_KEY"):
        try:
            from agentframework.integrations.anthropic_llm_provider import AnthropicLLMProvider
        except ImportError:
            print("ANTHROPIC_API_KEY is set, but the 'anthropic' package isn't installed.\n"
                  "Run: cd sdk && pip install -e \".[llm]\"")
            return None, None
        return AnthropicLLMProvider(), "Anthropic (claude-sonnet-4-5)"

    return None, None


def main():
    provider, label = pick_provider()
    if provider is None:
        print(
            "No API key found (checked GEMINI_API_KEY, then ANTHROPIC_API_KEY).\n\n"
            "To run this demo against a real LLM (Gemini's free tier is the easiest path):\n"
            "  1. Get a free key: https://aistudio.google.com/app/apikey\n"
            "  2. cd sdk && pip install -e \".[gemini]\" && cd ..\n"
            "  3. export GEMINI_API_KEY=...   (or `set` on Windows)\n"
            "  4. python3 run_demo_real_llm.py\n\n"
            "Without a key, every other demo in this repo (run_demo_phase4.py, the two "
            "reference agents, etc.) still works fine using MockLLMProvider — this script is "
            "specifically for verifying a real API integration."
        )
        return

    asyncio.run(run(provider, label))


async def run(real_provider, label: str):
    from agentframework.core.orchestrator import AsyncOrchestrator
    from agentframework.memory.in_memory import InMemoryLongTermMemory
    from agentframework.observability.metrics import InMemoryMetrics

    from examples.customer_support_agent.agent import build_flow, build_tool_registry

    print(f"=== Customer support agent, running against the REAL {label} API ===\n")

    tools = build_tool_registry(llm_provider=real_provider)
    metrics = InMemoryMetrics()
    orchestrator = AsyncOrchestrator(
        tool_registry=tools,
        long_term_memory=InMemoryLongTermMemory(),  # required: the flow's recall_history task
        metrics=metrics,                            # reads ctx["__memory__"]
    )

    ticket = "My package arrived damaged and I'd like a replacement or refund."
    print(f"customer says: {ticket!r}\n")

    run = await orchestrator.run(build_flow(), {"ticket_text": ticket}, session_id="cust-demo")

    print(f"run status: {run.status.value}")
    print(f"REAL LLM-drafted reply:\n  {run.tasks['draft_reply'].result['response']}\n")
    print(f"real token usage: {run.tasks['draft_reply'].result['usage']}")
    print()
    print(f"This response came from the live {label} API — the exact same Flow/Task/"
          "AsyncOrchestrator code that ran with MockLLMProvider throughout the rest of this "
          "repo, with only the provider swapped.")


if __name__ == "__main__":
    main()
