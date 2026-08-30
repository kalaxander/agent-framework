# Memory.md — running build context

Purpose: keep whoever (human or AI) picks this project up next from having to re-read the whole
codebase or guess at decisions. Update this after each meaningful step.

## Status
- **Phase 1 — Core Flow Model**: done. `Task`/`Flow` dataclasses, DAG validation (missing-dep +
  cycle detection), `Flow.topological_order()`, `SyncExecutor` (retry/backoff, per-task timeout,
  in-memory audit log). Verified with `tests/smoke_test.py` and `tests/test_flow_and_executor.py`.
- **Phase 2 — Orchestrator & State Store**: done. `AsyncOrchestrator` runs the *same* `Flow`
  model, but executes independent tasks concurrently via `Flow.levels()` (grouped by dependency
  depth) instead of Phase 1's strictly-sequential order. Run lifecycle
  (`queued→running→succeeded/failed/cancelled`) persisted through the `StateStore` interface.
  `InMemoryStateStore` is the reference/test backend; `integrations/postgres_state_store.py` is
  the production Postgres backend (same interface, lazy-imports asyncpg so its absence doesn't
  break the core package).
- **Phase 3 — Ingress & Executors**: done. `io/rest_ingress.py` (stdlib `http.server` REST
  reference server) + `integrations/fastapi_ingress.py` (production, same routes, lazy-import).
  `io/message_queue.py` (`MessageQueue` interface + `InMemoryMessageQueue`) +
  `integrations/kafka_message_queue.py` (production Kafka, lazy-import). `io/worker.py`
  (`ExecutorWorker`) consumes `RunRequest`s off the queue and runs them through the same
  `AsyncOrchestrator` from Phase 2. `io/output_actions.py` (`LogOutputAction`,
  `WebhookOutputAction` via stdlib `urllib`). Verified end to end with real HTTP requests and a
  real local webhook POST — see `run_demo_phase3.py` and `tests/smoke_test.py`.
- **Phase 4 — Tools Registry**: done. `Tool` ABC (`tools/base.py`, `validate_input`/
  `validate_output` hooks raising `GuardrailViolation`) + `ToolRegistry` (`tools/registry.py`).
  `Task` gained `tool: Optional[str]` + `tool_input: Optional[Callable]` fields (already present
  from Phase 1, now actually wired up); both `SyncExecutor` and `AsyncOrchestrator` accept an
  optional `tool_registry=` and resolve `Task(tool=...)` through it — `Task(fn=...)` still works
  unchanged, so nothing from Phases 1-3 broke. Built-in tools: `HttpTool` (real `urllib` calls,
  tested against a real local server), `LlmTool` + `LLMProvider`/`MockLLMProvider`
  (deterministic, no API key), `SimpleSearchTool` (in-memory keyword search). Verified end to
  end in `run_demo_phase4.py` and `smoke_test.py`.
- **Phase 5 — Memory**: done. `ShortTermMemory`/`LongTermMemory` interfaces (`memory/base.py`)
  + `MemoryHandle` (injected into task context as `context["__memory__"]` whenever either store
  is configured). Reference impls: `InMemoryShortTermMemory`, `InMemoryLongTermMemory` (keyword-
  scored recall, same approach as `SimpleSearchTool`). Production: `RedisShortTermMemory`
  (`integrations/redis_memory.py`) and `ChromaLongTermMemory` (`integrations/vector_memory.py`,
  real embedding-based semantic recall) — both lazy-import, same pattern as Postgres/Kafka.
  `AsyncOrchestrator.run()`/`SyncExecutor.run()` take an optional `session_id` (defaults to the
  run's own id — long-term recall across runs only happens if a stable `session_id` is passed
  explicitly). Verified end to end in `run_demo_phase5.py` (including a session-isolation check)
  and `smoke_test.py` (29 checks total).
- **Phase 6 — Guardrails & Observability**: done. `Guardrail` interface (`guardrails/base.py`) —
  sync `pre_execute`/`post_execute`, attachable per `Task` and/or per `Flow` (flow-level applies
  to every task in it). Built-ins in `guardrails/builtin.py`: `RequiredKeysGuardrail`,
  `RateLimitGuardrail` (shared state across every task/flow it's attached to, so one instance can
  cap a whole flow's calls to something), `BudgetGuardrail`, `ContentFilterGuardrail`. Both
  executors now treat any `AgentFrameworkError` with `retryable=False` (which is
  `GuardrailViolation`'s default) as fail-closed — one attempt only, `retry_policy.max_attempts`
  is ignored for those. `observability/logger.py` (`JsonLineLogger`, matches `Design.md`'s
  `{ts, event, ...}` format) and `observability/metrics.py` (`InMemoryMetrics`/`TaskMetric`,
  success rate / avg latency / retries / token+cost) are optional constructor args on both
  `SyncExecutor` and `AsyncOrchestrator`. `LlmTool` now returns a `"usage": {"tokens", "cost"}`
  key (word-count-based estimate) so metrics have something real to aggregate without a real
  provider. Verified end to end in `run_demo_phase6.py` (4 scenarios: guardrails pass, a missing-
  key rejection with fail-closed retry counting, a shared rate limit across 3 separate tasks, and
  a post-execution content-filter rejection) and `smoke_test.py` (35 checks total).
- **Phase 7 — Apache Integration Depth**: done. `integrations/airflow_adapter.py`:
  `compile_to_airflow_dag()` generates Airflow DAG source (Flow -> one `PythonOperator` per
  task, `>>` for each dependency edge); `run_task_for_airflow()` is the function every generated
  task calls, rebuilding the Flow by import path (Airflow re-imports DAG files in separate
  processes, so a live Flow object can't cross that boundary — see `examples/shared_flows.py`,
  the first module that needed to exist outside `sdk/` for real, since it has to be importable
  the same way from both a demo script and a generated DAG file) and running the task through
  `SyncExecutor._run_task_with_retry` so retries/guardrails/audit-log behavior matches every
  other executor. Verified: generated DAG source checked for Python syntax validity and correct
  structure (task ids, dependency edges); `run_task_for_airflow` run against a stub XCom object
  and its output compared exactly against `SyncExecutor` running the same Flow directly (see
  `run_demo_phase7.py`, `smoke_test.py`, now 38 checks). `integrations/camel/route.yaml` is a
  real Camel YAML-DSL route (file-drop -> REST ingress bridge) — documented, not executed, since
  Camel is JVM-only and this sandbox has no JVM/network access; see
  `integrations/camel/README.md` and the deviation note below.
- **Phase 8 — Reference Agents**: done. `examples/customer_support_agent/` (ticket -> KB search
  -> recall customer history via long-term memory keyed by `session_id=customer_id` -> draft
  reply -> content-filter guardrail -> remember ticket; output through the Phase 3
  `ExecutorWorker` to log + real webhook) and `examples/research_agent/` (question -> search ->
  real HTTP fetch -> summarize -> draft -> plain-Python critique (deliberately not an LLM call)
  -> LLM revise pass that closes the gap the critique found — the "reflection" stretch goal,
  bounded to one pass since the Flow/Task model is a DAG with no loop primitive). Both have a
  `run.py` (verified) and a `README.md`. Building these surfaced the Phase 7 dependency bug
  described below (same root cause, same fix pattern), and 3 targeted `smoke_test.py` checks
  (42 total) verify actual agent behavior — memory recall content, guardrail rejection, and that
  the reflection pass closes the *specific* gap its own critique found — not just "ran without
  erroring."
- **Phase 9 — Design Doc & Wrap-up**: done. `Architecture.md`/`Design.md` finalized with
  explicit "as-built deviations" sections (see those files' final sections for the summary; full
  reasoning is in this file's Key Decisions below). `benchmarks/run_benchmarks.py` produces real
  measured numbers in `benchmarks/results.md` (not hand-written) — notably a ~4.86x speedup from
  `AsyncOrchestrator`'s concurrent execution vs `SyncExecutor`'s sequential execution on 10
  independent tasks, plus per-run overhead and retry-path cost, all with an explicit caveat that
  these exclude real Kafka/Postgres/Redis latency (in-memory reference stores only). Stretch
  goal closed: human-in-the-loop pause/resume — `Task(requires_approval=True)` +
  `AsyncOrchestrator.resume(run_id, task_name, approved)`, a genuine `asyncio.Event` suspend
  (verified the run actually reaches `RunStatus.WAITING`, not just that it eventually
  completes), finally implementing the `WAITING` status that had been stubbed unused since
  Phase 2. `ApprovalRejected` (new error, `retryable=False`) added to `core/errors.py`. Verified
  in `run_demo_phase9.py` (both approved/rejected paths) and 2 `smoke_test.py` checks (45 total,
  stable across repeated runs). Stretch goals NOT done: multi-agent collaboration (no primitive
  for one flow to spawn/await another flow) and open-ended reflection loops (the research
  agent's critique/revise is a bounded single pass, not "repeat until satisfied" — no loop
  construct exists in the Flow/Task DAG model).
- **All 9 phases from the original brief are now complete.** This project has an actual
  design-doc pass remaining only in the sense that a human reviewer might want to read
  `Architecture.md`/`Design.md`'s deviation sections and decide whether any of those gaps
  (YAML flow loader, `/v1/flows` endpoint, schema-first tool validation, typed REST error
  shape, env-var configuration convention) are worth closing before a real deployment — none of
  them block anything currently in this repo from running.
- **Post-completion: real LLM providers added** (`integrations/anthropic_llm_provider.py` +
  `integrations/gemini_llm_provider.py`) — the user asked specifically to move from
  "MockLLMProvider everywhere" toward something they could call genuinely deployed/useful for
  their minor project, and the mocked LLM was flagged (by me, unprompted, when asked directly
  for an honest assessment) as the single biggest gap between "looks like an agent framework"
  and "is one." Anthropic was built first; the user then said they actually have a **Gemini**
  key (free tier, no billing) rather than Anthropic's (paid), so `GeminiLLMProvider` was added
  alongside it — same `LLMProvider.complete()` interface as `MockLLMProvider`/
  `AnthropicLLMProvider`, so it swaps into any `LlmTool(provider=...)` — including both
  reference agents — with zero other changes. `run_demo_real_llm.py` now checks for
  `GEMINI_API_KEY` first (the key the user actually has), falling back to `ANTHROPIC_API_KEY`.
  `LlmTool` uses a provider's *real* token usage (`provider.last_usage`) when present, falling
  back to the word-count estimate only for providers that don't supply one (i.e. still
  `MockLLMProvider`). Current Gemini SDK shape (`google-genai` package, `from google import
  genai`, async via `client.aio.models.generate_content()`, usage via
  `response.usage_metadata.prompt_token_count`/`candidates_token_count`) was confirmed via web
  search before writing this, not assumed from training data, since these SDKs change fairly
  often. **Neither provider's actual API call logic has been verified against its live API** —
  no network/API key in this sandbox — both verified instead against stub clients mimicking
  each real SDK's shape (same technique as the Airflow adapter's stub-XCom verification),
  covering request-building and response-parsing logic (`smoke_test.py`, 8 new checks total
  across both providers, 53 total).
- **Real bug found when the user actually ran `run_demo_real_llm.py` with a real Gemini key**:
  `KeyError: '__memory__'` — the script built `AsyncOrchestrator` without `long_term_memory=...`,
  but the support agent's `recall_customer_history` task reads `ctx["__memory__"]`
  unconditionally. This is exactly the kind of gap stub-based verification can't catch (the stub
  test never exercised the full agent flow, only `AnthropicLLMProvider`/`GeminiLLMProvider` in
  isolation) — real end-to-end runs by the person with the actual API key found a bug my mocked
  verification missed. Fixed by passing `long_term_memory=InMemoryLongTermMemory()` in
  `run_demo_real_llm.py`, matching what `examples/customer_support_agent/run.py` already did.
  **Confirmed fixed** — the user re-ran `run_demo_real_llm.py` with their real `GEMINI_API_KEY`
  and it completed successfully end to end: real Gemini-generated reply (contextually
  appropriate — asked for order number + photo, referenced the correct KB doc), real token
  usage (130 tokens) and cost figures flowed through `InMemoryMetrics`. This is the first fully
  live-API-verified run in this project — everything before this was verified against stubs or
  in-memory reference implementations only. `GeminiLLMProvider` is now genuinely proven, not
  just plausible.
- **Project scope expanded: user has months of runway, wants this genuinely resume/mentor-ready,
  not just phase-complete.** Agreed roadmap (their words: "proper working project"): (A) real
  persistent backend + actually hosted somewhere reachable + a minimal frontend, (B) real
  integration tests against real services (not just stubs) + CI + closing 1-2 of the honest
  design-doc gaps, (C) a third, more ambitious agent built on top of the framework + presenting
  the docs as if for an audience, not just internal notes. This reframes "Not started"/"Open
  Questions" below from "maybe someday" to "the actual next work," roughly in that order.
- **Postgres work started (Phase A, item 1)**: reviewed `PostgresStateStore` (written back in
  Phase 2, never tested against real Postgres) and fixed a real latent bug before connecting
  anything real to it — asyncpg does NOT auto-encode Python `str` into `jsonb` columns without
  either a custom type codec or an explicit `::jsonb` cast in the SQL; added the casts to every
  INSERT/UPDATE touching a jsonb column (`create_run`, `update_task_state`). Also added a
  `close()` method (the pool was never released before) and removed an unused `sqlalchemy`
  dependency from the `storage` extra — nothing in the codebase actually imports it, only
  `asyncpg` is used. `run_demo_postgres.py` is the verification script: writes a run with one
  `PostgresStateStore` instance, fully closes its connection pool (simulating the process
  exiting), then reads the same run back with a second, independent instance — proving actual
  persistence, not just "the query didn't error."
- **Confirmed working against a real (Neon) Postgres database.** The user ran
  `run_demo_postgres.py` against a real free-tier Neon instance: run written, pool fully closed,
  a second independent `PostgresStateStore` connection read the exact same run + full audit
  trail back correctly — "data survived the simulated restart intact: True". The `::jsonb` cast
  fix above was never actually exercised against real Postgres before this, so this is the
  first proof it was necessary/correct, not just defensive. Second real external dependency now
  fully verified (after Gemini) — the framework has a genuine, checkable persistence story, not
  just an in-memory one. Same dual-Python-install issue as before (their `pip install` and
  `python3` point at different interpreters) — resolved by using `python` instead of `python3`
  for this project on their machine; not a bug in the codebase.
- **Phase A, item 2 started: actual deployment scaffolding.** `server.py` (repo root) is the
  deployment entrypoint — wires `_get_llm_provider()` (Gemini if `GEMINI_API_KEY` set, else
  Anthropic if `ANTHROPIC_API_KEY` set, else `MockLLMProvider`) and `_get_state_store()`
  (`PostgresStateStore` if `DATABASE_URL` set, else `InMemoryStateStore`) into the existing
  `integrations/fastapi_ingress.py`, plus a `/` info route, a `/health` route, and an
  `on_event("startup")` hook that calls `init_schema()` only if the store has that method (so it
  no-ops cleanly for `InMemoryStateStore`). Only the customer support agent's flow is registered
  — the research agent's `fetch_top_source` needs a real URL to fetch, which isn't wired up for
  a deployed context yet (flagged in `server.py`'s docstring, not silently dropped). Also
  flagged: long-term memory (customer ticket history) is always `InMemoryLongTermMemory` even
  with real Postgres configured for run state, so ticket history specifically resets on
  restart/redeploy while run records persist — a real gap, not glossed over.
  `requirements.txt` + `Procfile` + `DEPLOY.md` (step-by-step GitHub -> Render guide, written at
  the same explicit level as the API-key setup guides that already worked well for this user)
  added for actual hosting. `.gitignore` + `.env.example` added since this is the first point in
  the project where secrets could plausibly end up committed to a public GitHub repo — `.env` is
  excluded, `.env.example` documents the shape without real values.
  **Verified**: the full route-wiring and an actual request through the real support-agent flow,
  against a stub `fastapi`/`pydantic` (same technique as every other integration in this
  project) — `smoke_test.py`, 4 new checks, 57 total.
- **Third real deployment milestone, and third real bug caught by an actual platform.** The
  user pushed to GitHub and deployed on Render: build succeeded, `LLM: using real Gemini API`
  and `State store: using real Postgres` both confirmed in the live deploy logs, server started
  cleanly. But `/docs` (and `/openapi.json` underneath it) returned 500 — Render defaulted to
  Python 3.14 (visible in the build log: "Using Python version 3.14.3 (default)"), a version
  this project has never been built or tested against (everything targets 3.11+). Added
  `.python-version` (pinned to `3.11.11`). **User set `PYTHON_VERSION=3.11.11` in Render's
  dashboard and redeployed — same 500 error persisted, meaning the Python-version theory alone
  was insufficient (or the pin didn't take effect the way expected).** Re-examined
  `fastapi_ingress.py` for a version-independent cause and found a real (if usually-harmless)
  Pydantic anti-pattern: `inputs: dict[str, Any] = {}` — a mutable dict literal as a field
  default, rather than `Field(default_factory=dict)`. Fixed. Separately, and probably the more
  load-bearing fix: `requirements.txt` used loose `>=` ranges for fastapi/pydantic, which could
  let pip resolve an untested version combination on a fresh build — replaced with exact,
  verified-compatible pins (`fastapi==0.115.6`, specifically released for "compatibility with
  Pydantic >=2.10" per its own changelog — confirmed via web search rather than assumed;
  `pydantic==2.10.4`; `asyncpg==0.30.0`; `google-genai==2.19.0`, version-bumped to the actual
  current release after confirming via web search that the `genai.Client`/
  `client.aio.models.generate_content`/`response.usage_metadata.prompt_token_count` interface
  this project's `GeminiLLMProvider` uses is unchanged in the current major version). Caught a
  real bug in my own test stub while verifying this: `smoke_test.py`'s stub `pydantic` module
  didn't define `Field`, so importing `fastapi_ingress` inside the test raised and got silently
  swallowed by the module's own `except ImportError` handler — fixed by adding a `_fake_field`
  helper to the stub. Same fix pattern as the `PostgresStateStore` jsonb cast and the
  `server.py` `__memory__` bug: a real external environment surfaced something no amount of
  local/stub verification could have caught alone, because the sandbox this project was built
  in never had network access to hit any real platform, database, or API in the first place.
  **The exact-pin fix backfired**: user redeployed and got a NEW, worse failure —
  `ResolutionImpossible`, pip refusing to even install `requirements.txt` because
  `fastapi==0.115.6` + `pydantic==2.10.4` + `google-genai==2.19.0` don't actually satisfy each
  other's real dependency constraints (build never completed, let alone reaching the original
  bug). I had exact-pinned a version triple across 3 unrelated packages based on partial,
  separately-checked evidence for each pair, without ever running the real resolver against all
  of them together — that was overreach, not a verified fix. Found evidence (a HuggingFace Space
  commit) that `google-genai==1.5.0` + `pydantic==2.10.6` install together fine, meaning the
  conflict is specific to the *exact triple* I picked, not an inherent incompatibility — but
  rather than hand-pick yet another unverified triple, reverted `requirements.txt` to
  minimum-version floors (`fastapi>=0.115`, `pydantic>=2.10`, etc.) and let pip's own resolver
  solve the graph, which is what it exists for.
- **Root cause of `/openapi.json` 500, finally confirmed via a real runtime traceback from the
  user's Render deploy** — and it was neither of the two earlier guesses. The actual bug:
  `RunRequestBody` (the Pydantic model for `POST /v1/runs`'s body) was defined **inside**
  `build_app()`, a factory function — not at module level. Pydantic's OpenAPI schema generator
  resolves model classes by name through the module's global namespace (`sys.modules[...].
  __dict__`); a class defined inside a function is never bound there, so schema generation
  raises `PydanticUserError: ... is not fully defined`. This is a genuinely well-documented
  FastAPI/Pydantic v2 gotcha, not a version-mismatch issue at all — the two earlier fix
  attempts (Python-version pin, exact dependency pins) were both barking up the wrong tree,
  though neither was harmful in itself (the second one actively backfired with a
  `ResolutionImpossible` build failure, documented above). The bug explains every observed
  symptom precisely: the server started fine and `POST /v1/runs` worked normally (Pydantic can
  still validate/parse a locally-scoped model at runtime — schema generation is a separate,
  *lazy* code path that only runs on the first request to `/docs`/`/openapi.json`), which is
  exactly the misleading combination of symptoms that made this hard to diagnose without a real
  traceback. Fixed by moving `RunRequestBody` to true module level, guarded by a module-level
  `try/except ImportError` (setting `_IMPORT_ERROR`) so the file still doesn't hard-require
  fastapi/pydantic just to be imported — same "lazy dependency, no hard import cost" contract as
  every other `integrations/` file, just restructured to also give Pydantic a resolvable name.
  **Added a regression test that actually catches this class of bug** —
  `test_fastapi_ingress_run_request_body_is_module_level` in `smoke_test.py` — checks
  `RunRequestBody` is a real module attribute and that `build_app`'s compiled bytecode contains
  no nested class definition with that name. Deliberately verified this test is not
  theater: reverted the fix, confirmed the test fails with exactly the expected message,
  restored the fix, confirmed 59/59 pass again (stable across 3 repeated runs). This is the
  final entry in this saga — the earlier stub-based `test_server_wiring_against_stub_fastapi`
  could never have caught this, because the stub `BaseModel` doesn't do real JSON-schema
  generation; the actual bug lived specifically in a code path this project's stub-verification
  approach couldn't reach until the user provided a real Render traceback. Worth remembering as
  the whole point of this multi-round debugging arc: for anything touching a real external
  platform, database, or API, this project's own testing — however thorough — is not a
  substitute for the user actually running it and reporting back what happens.
- **Deployment fully confirmed working, and one more real gap found in the process.** User
  tested the live deployed server end-to-end via `/docs`: `POST /v1/runs` and
  `GET /v1/runs/{id}` both worked correctly, with a genuinely good real Gemini response (asked
  for order number/damage description/photos, correctly cited both KB docs, real token/cost
  tracking). But `recall_history` came back empty even though the ticket was clearly about
  billing. Root cause: `RunRequestBody` never had a `session_id` field, and `create_run()` never
  forwarded one to `orchestrator.run()` — so every REST request got its own isolated memory
  scope (defaulting to that request's own `run_id`), meaning the customer-history-recall
  feature — one of the framework's actual demonstrated capabilities (Phase 5, and the whole
  point of the support agent's `recall_history` task) — silently never worked over HTTP, even
  though the local Python demos worked perfectly (they pass `session_id` explicitly in code).
  Fixed: added an optional `session_id: Optional[str] = None` field to `RunRequestBody`,
  forwarded to `orchestrator.run(..., session_id=body.session_id)`. `server.py`'s root route's
  `example_request` updated to document the field so external callers actually know it exists.
  **Caught a real flaw in my own first regression test while writing it**: the initial version
  had both "remember" and "recall" in the SAME flow/run, so recall trivially found what remember
  had *just* stored in that same run's own default session — passing regardless of whether
  cross-request session sharing worked at all. Caught this by deliberately reverting the fix and
  finding the test still passed (should have failed) — redesigned with separate remember-only
  and recall-only flows/runs, plus an isolation control (a different `session_id` must see
  nothing). Re-verified both ways: reverted the fix again, confirmed the redesigned test now
  correctly fails; restored the fix, confirmed it passes (61 checks total, stable across 3
  runs). This is the second time in this same deployment arc that a test only became trustworthy
  after being deliberately proven to fail against the bug it claims to catch — worth continuing
  as standard practice for any fix going forward, not just here.
- **Deployment fully, finally confirmed end-to-end in production.** User pushed the session_id
  fix, redeployed, sent two separate `POST /v1/runs` requests with a shared `session_id`, and
  the second one's `recall_history` correctly returned the first ticket's exact text — proof
  that cross-request memory recall genuinely works live, not just in local/stub tests. Combined
  with the earlier confirmations (real Gemini, real Postgres surviving a restart), every
  capability this framework demonstrates has now actually been shown working in a real
  deployment: real LLM, real persistent database, real cross-request session memory, all
  reachable at a public URL. This closes out Phase A item 2 (deployment) from the roadmap
  agreed when the user said they wanted this genuinely resume/mentor-ready.
- **Phase A item 3 (minimal frontend) done.** `frontend/index.html` — plain HTML/CSS/JS, no
  build step, no framework. Design brief grounded in the actual subject (a ticket moving through
  a real staged pipeline — search → recall → draft → remember) via a postal dispatch/routing-
  slip metaphor: each stage renders as a card that gets visually "stamped" as it resolves,
  connected by a perforated tear-line. Deliberately steered away from the three generic-AI-
  design defaults per the frontend-design skill (no cream+terracotta, no near-black+neon, no
  broadsheet) — palette is slate/parchment/cobalt/stamp-red/mustard, type is slab-serif headline
  + monospace data. `server.py`'s `/` route now serves this file (`FileResponse`); the old
  JSON service-info moved to `/api`. Verified for real, not just eyeballed: extracted and
  syntax-checked the embedded JS with `node --check`, then ran the actual stage-rendering
  functions against a realistic API response shape in Node — including deliberately injecting a
  `<script>` tag into the mocked LLM response text to confirm HTML-escaping actually prevents
  XSS from untrusted LLM output before assuming it worked. Updated `test_server_wiring_against_
  stub_fastapi` (which broke when `/` stopped returning JSON) to check `/api` for the JSON
  contract and confirm `/` returns a `FileResponse` pointing at `frontend/index.html` — required
  adding a fake `fastapi.responses` submodule to the stub, registered in `sys.modules` under its
  own dotted name (not just as an attribute on the fake `fastapi` module), since Python's import
  system resolves `from fastapi.responses import FileResponse` as a real submodule lookup, not
  attribute access. 62 checks total, stable across 3 runs.
- **Frontend deployed and confirmed working, with one real (and instructive) bug found and
  fixed.** User pushed and tested it live — the dispatch-desk styling, form, and all four stage
  cards rendered and processed correctly (real Gemini replies, real KB search including a
  correct "No matching articles found" for an unrelated query, real token/cost). But the drafted
  reply text itself was essentially invisible — visible only as extremely faint text in the
  screenshots. Root cause: `.reply-box` (dark `--slate-deep` background) never set an explicit
  `color`, so `.reply-text` inherited `--ink` (near-black) from the parent `.stage` card's own
  color rule (stage cards sit on light parchment, so dark ink text is correct *there* — it just
  doesn't get overridden going into the nested dark box). Computed the actual WCAG contrast
  ratio to quantify it rather than eyeball the fix: the bug was 1.07:1 (WCAG AA minimum is
  4.5:1) — essentially the exact reason it read as "no draft reply" even though the text was
  technically present in the DOM. Fixed by setting `color: var(--paper)` explicitly on both
  `.reply-box` and `.reply-text`; new ratio is 14.10:1. While fixing it, audited every other
  text/background color pairing in the stylesheet the same way (computed contrast ratios for
  all 7 real pairings used) rather than assuming the rest of the design was fine because this
  one instance wasn't caught in review — all 7 pass comfortably (5.6:1 to 13.2:1). This is a
  category of bug pure code-logic testing (the render-function tests written earlier) structurally
  cannot catch — those verified the right *text* reaches the DOM, not that it's visually
  readable once styled; needed an actual screenshot from the user to surface it.
- **Phase B, CI piece done: GitHub Actions wired up** (`.github/workflows/tests.yml`, 3 jobs).
  Notable: GitHub Actions runners have real internet access this project's own dev sandbox never
  had, so `tests/test_server_integration.py` (new) is the first place the REAL fastapi/pydantic
  packages get exercised via FastAPI's actual `TestClient` — not the `sys.modules` stub
  technique used everywhere else in `smoke_test.py`. Both real production bugs found during
  deployment (`RunRequestBody` defined inside a function breaking real OpenAPI schema
  generation; missing `session_id` breaking cross-request memory recall) have direct regression
  tests here using the real machinery that actually broke, not a stub that couldn't have caught
  either. Repo confirmed as `kalaxander/agent-framework` from earlier Render deploy logs, used
  to add a real (not placeholder) CI badge to `README.md`. Validated `.github/workflows/tests.yml`
  is syntactically correct YAML via PyYAML before considering it done (noting PyYAML's YAML-1.1
  quirk of parsing bare `on:` as boolean `True` — a false-flag for local validation only,
  standard and correct as written; GitHub's own parser always treats it as the reserved trigger
  keyword). **Confirmed green on GitHub** — all 3 jobs passed on first push (smoke-test 12s,
  pytest-suite 14s, server-integration 17s, total run 21s). This closes out the CI piece of
  Phase B entirely — every real production bug found during this project's deployment now has
  an automated regression test using the real (non-stubbed) library that actually broke,
  running on every push.
- **Phase B, first documented gap closed: typed REST error responses** in
  `integrations/fastapi_ingress.py`. Two global exception handlers — one for the
  `AgentFrameworkError` hierarchy (name->status-code map: FlowValidationError 400,
  GuardrailViolation/ApprovalRejected 422, TaskTimeoutError 504, ToolError 502, default 500),
  one for FastAPI's own `HTTPException` — both return `{error_type, message, retryable}`
  uniformly. This closed a real gap beyond the typed shape itself: previously only
  `FlowValidationError` was explicitly caught in `create_run`; any other `AgentFrameworkError`
  (GuardrailViolation, ApprovalRejected, TaskTimeoutError, ToolError) would have propagated as a
  raw unhandled 500. `docs/Design.md`'s deviation note updated to be precise about scope: this
  is fixed in the FastAPI production ingress, NOT the stdlib reference `RestIngress` (still
  returns the older `{"error": ...}` shape) — flagged as accurate rather than overclaiming full
  closure. **Refactored the 3 separate hand-rolled fastapi/pydantic stubs in `smoke_test.py`**
  (which had already drifted out of sync twice before, each time this file's imports changed)
  into shared `_make_fastapi_pydantic_stubs()` / `_install_fastapi_pydantic_stubs()` helpers —
  fixes the duplication at its root instead of patching each copy again. Added 2 new real
  regression tests to `tests/test_server_integration.py` (real TestClient): unknown flow_name
  returns 400 with the typed FlowValidationError shape, unknown run_id returns 404 with the
  typed HTTPException shape. **Important limitation, stated plainly rather than glossed over**:
  I could not actually execute `test_server_integration.py` to verify these two new tests
  fail-then-pass around the fix, the way every other fix in this project has been double-checked
  — fastapi isn't installed in this sandbox. Reverted the fix locally and confirmed the
  stub-based `smoke_test.py` suite still passes either way (as expected — the stubs don't
  exercise real exception dispatch, which is exactly why the real-TestClient tests exist).
  **Confirmed green on GitHub Actions** — both new tests (`test_unknown_run_id_returns_404_
  with_typed_error_shape`, `test_unknown_flow_name_returns_400_with_typed_error_shape`) passed
  for real against real FastAPI. This closes the typed-error-response gap with actual proof, not
  just local reasoning. No guardrail is currently wired into the customer-support flow, so a
  live 422 case isn't reachable without adding one artificially — skipped rather than faked;
  noted as a known gap, not silently dropped.
- **CI maintenance: bumped `actions/checkout@v4`→`v7` and `actions/setup-python@v5`→`v7`** in
  `.github/workflows/tests.yml` after GitHub flagged both as running on deprecated Node.js 20 —
  time-sensitive, not cosmetic: GitHub removes Node.js 20 from all runners on September 16,
  2026, after which unpinned/old-major-version actions using it would start failing outright.
  Checked v7's one breaking change (restricts fork PR checkouts on `pull_request_target`/
  `workflow_run` triggers) doesn't apply — this workflow only triggers on `push`/`pull_request`
  to `main`. Validated YAML via PyYAML post-edit, confirmed via `sed` that all 6 occurrences
  (2 actions × 3 jobs) updated. **Not yet confirmed pushed/green** — the user asked what the
  warning meant and moved directly into the research-agent work below without confirming this
  was pushed; don't assume it's live without checking.
- **Phase B, last documented gap closed: research agent exposed on the deployed server.**
  Turned into more real engineering than a simple "add a route," with two real bugs caught
  before shipping:
  1. **Tool-name collision discovered before implementation, not after.** Both reference agents
     register tools named "search" and "llm_call" — backed by DIFFERENT documents (KB_DOCS vs
     SOURCES). `ToolRegistry.register()` silently overwrites on name collision (no error, no
     warning) — naively sharing one `ToolRegistry`/`AsyncOrchestrator` between both agents would
     have made one flow silently return the OTHER agent's search results, the kind of bug that
     would only surface as visibly wrong output during an actual demo. Caught by checking
     `search_tool.py`/`llm_tool.py`'s `name = ...` class attributes before writing any wiring
     code, not by writing the wiring first and debugging it after.
  2. **Fix: extended `build_app()`** (`fastapi_ingress.py`) with an optional
     `orchestrators_by_flow: dict[str, AsyncOrchestrator]` param — `create_run` looks up
     `orchestrators_by_flow.get(flow_name, orchestrator)` per request, defaulting to the
     original single-orchestrator behavior when not provided (fully backward compatible — every
     existing caller/test needed zero changes). `server.py` now builds two `AsyncOrchestrator`s
     (support + research), each with its own `ToolRegistry`, both sharing the SAME `state_store`
     (and `long_term_memory`) — critical detail: `get_run`/`get_audit` always read through the
     DEFAULT orchestrator's `state_store`, so a run created by the research orchestrator is only
     findable afterward because it's genuinely the same underlying store object, not two
     separate ones that happen to look similar.
  3. **A second real bug, caught by ME while WRITING the wiring, before it ever ran**: first
     registered the internal source route at `/internal/source/{doc_id}`, but
     `examples/research_agent/agent.py`'s `fetch_top_source` task hardcodes the URL as
     `f"{source_base_url}/source/{doc_id}"` — only the base URL is actually configurable, not
     the path suffix. Would have made the research flow fail with a 404 immediately in
     production. Found by re-reading agent.py's exact `tool_input` lambda instead of assuming
     the path I'd chosen would just work, and confirmed the fix by grepping the source for the
     literal `source_base_url}/source` string rather than trusting memory of it.
  4. **A third, structural testing gap found while writing the regression test for bug #3**: the
     research flow's `fetch_top_source` makes a REAL `urllib` HTTP call back to the server's own
     route — a genuine socket connection. Neither `smoke_test.py`'s stub tests (routes are
     called as plain Python functions, nothing ever listens on a real port) NOR FastAPI's
     default `TestClient` (in-process ASGI transport, also no real socket) can actually prove
     this self-referential round trip works. Fixed by rewriting `test_server_integration.py`'s
     `client` fixture to run a genuinely live `uvicorn.Server` on a real OS-assigned free port
     (found via a throwaway socket bind), waiting for it to actually accept connections before
     yielding a real `httpx.Client` pointed at it. This upgraded EVERY test in that file to run
     against a real live server, not just the new one — strictly more realistic than before.
  5. **Verified honestly, with the same discipline as every other fix in this project**:
     wrote a real, non-mocked end-to-end test locally (a real local HTTP stub server, real async
     orchestrator execution, real tool registries) proving the multi-orchestrator design has no
     cross-contamination between the two agents' search tools and that shared-state_store lookup
     works — ran and passed BEFORE writing any of the FastAPI-layer code, to validate the
     underlying design first. Then, for the `smoke_test.py` stub coverage: reintroduced bug #3
     (`/internal/source/` typo) deliberately and confirmed the new stub check
     ("a route is registered at the exact path /source/{doc_id}...") genuinely fails, then
     restored the fix and confirmed 64/64 checks pass, stable across 3 runs. Could NOT locally
     verify `test_server_integration.py`'s new live-server-based tests the same way (fastapi/
     uvicorn aren't installed in this sandbox) — stated as a real limitation, not glossed over;
     the actual proof is the next CI run.
  6. Two new real tests in `test_server_integration.py`:
     `test_research_report_flow_completes_via_real_self_referential_http_call` (the actual
     self-fetch round trip, end to end, through a live server) and
     `test_customer_support_and_research_tools_do_not_cross_contaminate` (the actual regression
     test for the bug this whole design change exists to prevent). Two new checks in
     `smoke_test.py`'s stub test verify the route is registered at the right path and returns
     correct content, without attempting the real network call the stub environment can't
     support.
  7. `server.py`'s own module docstring updated to describe the actual final design (two
     orchestrators, shared state_store, `/source/{doc_id}` route) rather than the previous
     "not done yet" note — this was the only place this specific gap was formally documented
     (Architecture.md's deviations section covers the research agent's reflection-loop
     deviation, but never separately called out deployment exposure, so no change needed there).
  This closes every item on the original Phase B "closing 1-2 documented design gaps" list.
- Remaining roadmap (as of the entry immediately above): Phase C — a third, more ambitious agent
  built using this project's own framework, plus presenting the docs for an audience (the user's
  mentor). Given everything in Phase A/B is done and verified live, Phase C is the natural next
  step whenever the user is ready — no other loose ends outstanding as of that entry.
- **Phase C started: third reference agent — Expense Approval Agent.** User picked this over an
  Incident Triage alternative after I recommended it specifically because it's the only way to
  demonstrate Phase 9's human-in-the-loop approval (`Task(requires_approval=True)` +
  `AsyncOrchestrator.resume()`), which has sat built and tested since Phase 9 without either
  original reference agent ever actually using it.
  - **Note: the sandbox this work happens in reset mid-session** (all files under
    `/home/claude/agent-framework` disappeared between one turn and the next). Recovered cleanly
    by `git clone`-ing the user's own GitHub repo directly (`github.com`/`codeload.github.com`
    are reachable) rather than asking the user to re-upload anything — confirmed the clone's
    commit history and local test results matched exactly where things stood before the reset,
    then redid the (already-designed) Expense Approval Agent work from scratch against the
    freshly-cloned repo. Worth remembering: if this happens again, git clone the user's repo
    first before assuming anything is lost — everything already pushed is safe there regardless
    of what happens to this sandbox.
  - **Real design problem found and solved before writing any code**: both existing reference
    agents already register tools named "search"/"llm_call"; a third agent doing the same would
    hit the exact ToolRegistry name-collision problem the research-agent work fixed earlier —
    solved the same way, a third `AsyncOrchestrator`/`ToolRegistry` pair sharing the common
    `state_store`.
  - **A genuinely new, non-trivial problem this agent surfaced**: `AsyncOrchestrator.run()`
    suspends on a REAL `asyncio.Event` when it hits an approval-gated task (confirmed by reading
    `run_demo_phase9.py`, the original Phase 9 demo, which schedules `run()` via
    `asyncio.create_task` and learns `run_id` by directly peeking at the state_store — a
    demo-only shortcut that doesn't work for a real REST API, since there's no "the only run in
    the store" assumption to lean on there). This means `POST /v1/runs` cannot simply `await`
    an approval-gated flow to completion the way it does every other flow, or the HTTP request
    would hang indefinitely.
  - **Fix, made as a small additive change to core/orchestrator.py** (not worked around at the
    REST layer): `run()` gained an optional `on_created: Optional[Callable] = None` parameter,
    called with the new run_id immediately after the RunRecord is created — before any task
    executes, before any blocking. Default `None`, zero behavior change for every existing
    caller; confirmed via `run_demo_phase9.py` still passing unchanged and the full smoke suite
    still green before touching anything else.
  - **`fastapi_ingress.py`**: added a module-level `ApproveRequestBody` (learned the RunRequestBody
    lesson — module level from the start, not inside a function). `create_run` now checks
    `any(t.requires_approval for t in flow.tasks.values())`; non-approval flows keep the
    original synchronous behavior completely unchanged, approval flows get scheduled as a
    background `asyncio.Task` and the handler returns `{"run_id", "status": "queued"}` as soon
    as `on_created` fires (via an `asyncio.Event`, not a sleep-polling loop). New
    `POST /v1/runs/{run_id}/approve` route looks up which orchestrator actually owns the run via
    `orchestrators_by_flow[run.flow_name]` (same mechanism `create_run` itself already used) and
    calls `.resume()` on it. The background task's own exception (if any) is deliberately
    swallowed with a comment explaining why: the failure is already correctly recorded in
    state_store by `run()` itself before re-raising, and nothing else observes this task's
    result — this just avoids a noisy, misleading "Task exception was never retrieved" warning
    for something that isn't actually a problem.
  - **Verified the REST-layer design for real, twice, before trusting it**: once as a pure-Python
    simulation of `create_run`/`approve_run`'s exact logic (no FastAPI at all) against a real
    orchestrator/flow, and again after the sandbox reset against the freshly recreated files —
    both times confirmed the full real sequence (queued -> waiting -> approve -> succeeded)
    actually works, not just "looks correct on read-through."
  - **New agent**: `examples/expense_approval_agent/` (`agent.py`, `run.py`, `README.md`).
    Flow: search company expense policy -> recall employee's expense history (long-term memory,
    `session_id = employee_id`, same convention as the support agent's `customer_id`) -> LLM
    writes an advisory assessment -> human approval gate (unconditional — every submission
    requires sign-off, not just large ones; documented explicitly as a deliberate design choice
    given `Task.requires_approval` is set once at flow-definition time, not evaluated per run,
    plus a genuinely normal real expense-policy pattern on its own) -> record decision to
    memory. Ran `run.py` for real (not just read through it) after every recreation — three
    passes (approved, rejected, second approval showing cross-run memory recall of the first) —
    confirmed working both before and after the sandbox reset.
  - **New tests, real regression coverage**: `smoke_test.py` gained
    `test_expense_approval_flow_queues_and_completes_via_approve_route` (stub-based, calls the
    real route functions directly) — deliberately removed the `approve_run` route and confirmed
    this test genuinely fails before restoring it and confirming 69/69 pass, stable across 3
    runs. `test_server_integration.py` gained 3 new tests using the real live-uvicorn `client`
    fixture (approve/complete, reject/fail, unknown-run-id 404) — could not execute these myself
    (no fastapi/uvicorn in this sandbox, same limitation as always), but hand-verified the exact
    response shapes each assertion depends on (the rejection error string, in particular) by
    running the real orchestrator logic standalone and printing the actual `get_run`-equivalent
    dict before trusting the assertion against it.
  - **Docs updated**: `server.py`/`fastapi_ingress.py` docstrings describe the final 3-agent,
    background-task design (not a "not done yet" note). `Architecture.md`'s as-built deviations
    section gained a new entry (#8) for the third agent — did NOT edit `PRD.md`'s "at least two
    reference agents" line, since that's the original requirement text, accurately describing
    what was asked for; three exceeding two doesn't make the original line wrong. Fixed one
    stale claim found along the way in `DEPLOY.md` ("research agent isn't exposed on the
    deployed server yet") — that gap was actually closed in an earlier session; DEPLOY.md just
    hadn't been updated to reflect it. Added a full "testing the expense approval agent over the
    API" walkthrough to DEPLOY.md matching the format of the existing memory-recall walkthrough.
  - **Not yet done**: the dispatch-desk frontend (`frontend/index.html`) has no UI for the
    expense-approval flow's queued/waiting/approve interaction pattern yet — it's reachable via
    `/docs` or direct API calls, just not through the visual frontend. Flagged to the user as an
    open question rather than built without asking, since it's a real scope/design decision
    (the queued->waiting->approve pattern is meaningfully different from the other two agents'
    single-shot submit-and-see-result interaction, and deserves its own design thought rather
    than a quick bolt-on). Not yet pushed to GitHub as of this entry — user has the zip and file
    list but hasn't confirmed a push or run against a live deployment yet.

## Key Decisions / Deviations From Original Docs
- **Removed all Intel-specific scope (Intel DevCloud hosting, Intel OpenVINO model
  optimization)** at the user's request. The original brief's "Intel Tech" section and
  Phase 9/"performance benchmarks pre/post Intel optimization" deliverable are gone from
  `PRD.md`, `Architecture.md`, `Phases.md`, and `Rules.md`. Performance benchmarking is still a
  deliverable (framework throughput/latency, retry/timeout behavior under load) — it's just no
  longer tied to a specific hosting platform or ML runtime. No code depended on
  Intel/OpenVINO, so nothing in `sdk/` changed for this.
- **Dropped pydantic from the core engine, using stdlib `dataclasses` instead**, because this
  build environment has no network/pip access and pydantic wasn't installable. This was flagged
  in `Rules.md`'s spirit (don't silently change architecture) — noted at the top of
  `core/flow.py` and in `Architecture.md`. Pydantic is still the intended choice for the FastAPI
  ingress layer (Phase 3) where request/response schema validation is genuinely useful; it's now
  in the `server` extra, not a core dependency. If/when this project moves to an environment with
  package access, this can be revisited, but there's no strong reason to reintroduce it to the
  core — the dataclass version is simpler and dependency-free.
- **`Flow.levels()`** was added beyond what `Architecture.md` originally specified: groups task
  names into dependency "levels" so the orchestrator can run independent tasks concurrently
  (`asyncio.gather`) rather than one at a time. Phase 1's `SyncExecutor` still runs strictly
  sequentially by design (it's the simple reference implementation); Phase 2 is where concurrency
  was introduced.
- Task `fn` may be sync or async (`AsyncOrchestrator` detects via `inspect.iscoroutinefunction`
  and runs sync fns in a thread pool executor) — neither `Architecture.md` nor `Design.md`
  specified this, noting it here so Phase 4 (tools registry) builds tools compatible with both.
- **REST ingress is stdlib (`http.server`), not FastAPI, for the reference implementation** —
  same reasoning as the pydantic drop (no network/pip access here). `RestIngress` runs each flow
  to completion within the HTTP request (via a background asyncio loop + a helper thread) rather
  than handing off to a queue, so it exercises ingress→orchestrator→state-store directly.
  `integrations/fastapi_ingress.py` is the production drop-in with identical routes/response
  shapes for when FastAPI is installed.
- **Executor scope**: `ExecutorWorker` decouples *flow-level* scheduling from ingress via the
  queue (ingress publishes a `RunRequest`, a worker process consumes it and runs the whole flow).
  It does **not** yet decompose individual *tasks* across separate Kafka
  task-assigned/task-completed topics to physically separate executor processes —
  `Architecture.md`'s Executors section describes that finer-grained split, which is deferred to
  Phase 7. Running 1+ `ExecutorWorker`s as separate processes against a shared
  `InMemoryMessageQueue`-alike (i.e. `KafkaMessageQueue` in prod) + shared `StateStore` already
  gives horizontal scaling at the flow-run level, which is enough for Phase 3's own goals.
- **`LlmTool`/`SimpleSearchTool` are reference implementations with no network dependency** —
  `MockLLMProvider` gives deterministic canned responses (keyed by substring match on the
  prompt) instead of calling a real model, and `SimpleSearchTool` does in-memory keyword scoring
  over a caller-supplied `documents` dict instead of hitting a real search API/vector DB. Both
  implement the same interface a real provider/backend would (`LLMProvider.complete()` /
  `Tool.run()`), so swapping in a real one later doesn't change any Flow that uses them by name.
- **`InMemoryLongTermMemory` reuses `SimpleSearchTool`'s keyword-scoring approach** rather than
  real embeddings, for the same no-network reason. `ChromaLongTermMemory` is the production
  swap-in with genuine semantic (embedding-based) recall — same `LongTermMemory` interface, so a
  Flow using `context["__memory__"].recall_long(...)` doesn't change either way.
- **`SyncExecutor` (Phase 1) also got memory wiring for parity with `AsyncOrchestrator`**, even
  though Phase 1 has no `RunRecord`/`run_id` of its own — it now generates a throwaway UUID per
  `run()` call purely to scope short-term memory. This wasn't asked for by any phase doc; done so
  a sync task written against `SyncExecutor` and later moved to `AsyncOrchestrator` doesn't need
  its memory-handling code rewritten. The same "give SyncExecutor parity" choice was repeated for
  Phase 6's guardrails/metrics/logger.
- **Guardrails (Phase 6) stayed separate from Phase 4's `Tool.validate_input`/`validate_output`**
  rather than merging them — resolves the open question from the Phase 5 notes. Reasoning: tool
  I/O validation is about one tool's own contract (owned by whoever wrote that tool) and always
  applies; a `Guardrail` is a *policy* attached externally by whoever assembles the `Task`/`Flow`
  (rate limits, budgets, content filtering) and varies per deployment. Keeping them separate
  means a tool author's validation can't be silently bypassed by flow composition, and a flow
  author's policy doesn't require touching tool code. Both still raise the same
  `GuardrailViolation` and get the same fail-closed retry treatment.
- **Guardrail methods are sync, not async** — unlike `Tool.run` (async, since tools may do real
  I/O). Guardrails are meant to be fast local checks; making them sync avoids needing the same
  async/sync bridging `SyncExecutor` already does for tools (`asyncio.run(...)` inside a worker
  thread). If a future guardrail genuinely needs I/O (e.g. calling an external moderation API),
  it can still do so synchronously (blocking urllib call) — same tradeoff `HttpTool` makes today.
- **The Camel piece of Phase 7 is documentation, not executed code** — the one deliberate
  exception to "everything in this repo has actually been run." Camel is a JVM framework; there
  is no legitimate Python library integration to build (fabricating one would violate
  `Rules.md`'s "don't invent Apache product capabilities"). `integrations/camel/route.yaml` is a
  real, correctly-structured Camel YAML-DSL route — just not one this sandbox could execute
  (no JVM, no network to install a Camel distribution).
- **`examples/shared_flows.py` exists because the Airflow adapter forced it to** — every prior
  demo defined its Flow inline as a closure in the demo script, which works fine for every
  executor except the Airflow one: Airflow re-imports DAG files in separate scheduler/worker
  processes, so the Flow has to be reachable by `module:factory_function` import path, not passed
  as a live object. This uncovered a real bug in the flow definition itself — `draft` read
  `classify`'s output but only declared `fetch_docs` as a dependency, which every other executor
  papered over by giving tasks the *whole* accumulated context regardless of declared
  dependencies. Airflow-style XCom only pulls declared dependencies, so the bug surfaced
  immediately; fixed by declaring `depends_on=["classify", "fetch_docs"]` explicitly. Worth
  noting for Phase 8's reference agents: a Flow that only works because of the loose
  "whole-context" convenience should be treated as under-specified, not correct.

## How to Run Things (no pip install required, except run_demo_real_llm.py)
```bash
cd agent-framework
python3 run_demo.py            # Phase 1 + Phase 2 run the same Flow, side by side
python3 run_demo_phase3.py     # Phase 3: real HTTP against RestIngress + queue-driven worker
python3 run_demo_phase4.py     # Phase 4: ToolRegistry + built-in tools inside a Flow
python3 run_demo_phase5.py     # Phase 5: short-term scratchpad + cross-run long-term recall
python3 run_demo_phase6.py     # Phase 6: guardrails (pass + 3 rejection scenarios) + metrics/logs
python3 run_demo_phase7.py     # Phase 7: Flow -> Airflow DAG compile + simulated-XCom execution
cd examples/customer_support_agent && python3 run.py && cd ../..   # Phase 8, agent 1
cd examples/research_agent && python3 run.py && cd ../..           # Phase 8, agent 2
python3 run_demo_phase9.py     # Phase 9: human-in-the-loop pause/resume (approved + rejected)
python3 benchmarks/run_benchmarks.py   # Phase 9: real measured perf numbers -> benchmarks/results.md
python3 tests/smoke_test.py    # dependency-free assert-based test suite (53 checks)

# Needs an API key — checks GEMINI_API_KEY first (free tier), then ANTHROPIC_API_KEY:
python3 run_demo_real_llm.py   # the only demo in this repo that calls a real external API
```
With network/pip access, the pytest suite (`tests/test_flow_and_executor.py`) and
`pip install -e ".[dev]"` also work and cover the same Phase 1 behavior — Phase 2-9 pytest
coverage (via `pytest-asyncio`) hasn't been added yet since it couldn't be run/verified here;
`tests/smoke_test.py` is the verified source of truth for Phase 2-9 until then.

## Open Questions (post-Phase-9, for whoever picks this up next)
- **Answered**: which real LLM provider(s) should `LlmTool` support first — Anthropic
  (`AnthropicLLMProvider`) and Gemini (`GeminiLLMProvider`, the one the user actually has a free
  key for). Still open: whether to add a third (OpenAI, a local model server via an
  OpenAI-compatible endpoint, etc.) — the `LLMProvider` interface makes this a new file, not a
  redesign.
- Whether `run_demo_real_llm.py` has actually been run successfully against a live API yet — as
  of this note, it hasn't (from my side); that's the next concrete verification step, owned by
  whoever has the API key.
- Kafka topic naming convention beyond `flow.run_requests` / `flow.run_results` (not yet decided
  for a real deployment — these two names are the reference implementation's choice).
- Whether task-level Kafka decoupling (Architecture.md's original Executors design) is actually
  needed, or whether flow-level worker scaling (what Phase 3 built, and what both Phase 8 agents
  use) is sufficient long-term.
- Whether `SimpleSearchTool`'s in-memory index and `InMemoryLongTermMemory`'s keyword recall
  should both be replaced by one shared real search/vector backend, since they're doing
  functionally the same thing (keyword scoring) for two different framework concerns.
- Whether a real loop construct ("repeat until a guardrail/critique passes, up to N times")
  should be added to the Flow/Task model, or whether hand-rolled bounded passes (like the
  research agent's) are fine indefinitely.
- Whether the design-doc gaps noted in `Architecture.md`/`Design.md` §6 (YAML flow loader,
  `/v1/flows` endpoint, schema-first tool validation, typed REST error responses, env-var
  configuration convention) are worth closing before a real deployment.
- Whether `InMemoryMetrics`/`JsonLineLogger` need a real Prometheus/log-shipper export path, or
  whether that's fine to defer further (e.g. to whenever this actually gets deployed somewhere).
- Whether the reference agents should be demoed running as compiled Airflow DAGs (the adapter
  exists and is verified, but nothing has used it end-to-end for a "real" agent yet).
