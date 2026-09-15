# Reference Agent 1 — Customer Support Ticket Agent

Ingests a support ticket, searches a KB for relevant docs, recalls the customer's past tickets
(long-term memory), drafts a reply, guardrail-checks it, and remembers the ticket for next time.

## Run it
```bash
python3 run.py
```
No external dependencies — everything (KB search, the LLM call, output delivery) runs against
in-process reference implementations, including a real local webhook receiver over loopback.

## What it demonstrates
| Framework piece | Where |
|---|---|
| Tools (Phase 4) | `search` (KB lookup), `llm_call` (draft the reply) |
| Long-term memory (Phase 5) | `recall_history`/`remember_ticket`, scoped by `session_id=customer_id` — a second ticket from the same customer recalls the first |
| Guardrails (Phase 6) | `RequiredKeysGuardrail` on the search input, `ContentFilterGuardrail` on the drafted reply |
| Metrics/logging (Phase 6) | printed summary at the end |
| Queue-driven ingress + output actions (Phase 3) | tickets submitted via `ExecutorWorker`, results dispatched to a log action and a real webhook |

## Swapping in a real LLM
`agent.py`'s `build_tool_registry()` takes an optional `llm_provider` — pass
`GeminiLLMProvider()` or `AnthropicLLMProvider()` (both in `agentframework.integrations`,
`../../run_demo_real_llm.py` shows both wired up) instead of the default `MockLLMProvider`.
Nothing else in the flow changes.
