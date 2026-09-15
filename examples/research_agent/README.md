# Reference Agent 2 — Research Agent

Ingests a research question, searches an internal source index, fetches the top source's full
content over real HTTP, summarizes it, drafts a report, critiques the draft against what was
actually searched, and produces a revised, cited final report.

## Run it
```bash
python3 run.py
```
Starts a real local HTTP server (loopback) serving the "source" documents `fetch_top_source`
calls over genuine HTTP — this isn't mocked at the network layer, only the LLM responses are
mocked (see below).

## What it demonstrates
| Framework piece | Where |
|---|---|
| Tools (Phase 4) | `search`, `http_call` (real request), `llm_call` |
| Reflection (stretch goal) | `critique_report` (plain Python, not an LLM call — deliberately, since "did we cite our sources" is a cheap deterministic check) finds a citation gap in the draft; `finalize_report` is a second LLM pass that fixes it. `run.py` verifies programmatically that the gap was actually closed, not just eyeballed. |
| Guardrails (Phase 6) | a `RateLimitGuardrail` shared across the whole flow (`Flow(guardrails=[...])`) caps total calls; `ContentFilterGuardrail` on the final report |
| Metrics/logging (Phase 6) | printed summary at the end, including `fetch_top_source`'s real network latency |

## On the "reflection loop"
This is a **bounded, single critique-and-revise pass**, not an open-ended loop — the underlying
Flow/Task model is a DAG, so there's no "repeat until satisfied" primitive in the framework yet.
See `docs/Memory.md` for that as an open question for a future phase.

## Swapping in a real LLM
Same pattern as the support agent: `build_tool_registry()` takes an optional `llm_provider`.
