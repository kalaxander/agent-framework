# PRD.md — Project Requirements Document

## 1. What We're Building
A **build-your-own AI Agent Framework** (an SDK, not an application) that lets developers define,
execute, monitor, and audit agentic workflows. It is a foundational layer other teams build agents
on top of — comparable in *purpose* to crew.ai / AutoGen / n8n, but built from scratch, without
depending on any of them. Apache projects (Kafka, Airflow, Camel, etc.) are allowed as
infrastructure for messaging, orchestration, and storage.

## 2. Target Users
- **Agent developers**: engineers who want to compose LLM-powered task flows (tools, memory,
  guardrails) without hand-rolling orchestration/retry/observability plumbing every time.
- **Platform/SRE teams**: need to monitor, audit, and safely operate agents running in production.

## 3. Problem Statement
Existing agent frameworks are either too opinionated, too heavyweight, or black boxes when it
comes to auditability. Teams need a minimal, transparent, composable core: define a workflow as a
graph of tasks, run it reliably (retries/timeouts), see exactly what happened (logs/metrics/traces),
and enforce guardrails — all without adopting a monolithic third-party framework.

## 4. Core Features (MVP)
1. **Flow Definition API** — define agentic workflows as a composition of task flows (DAG or state
   machine), in code or declarative YAML.
2. **Execution Engine (Orchestrator)** — schedules and runs flows, manages task state, retries,
   timeouts, backoff, and failure handling.
3. **Input Handlers** — REST endpoint and/or queue consumer that turns external events into a flow
   run.
4. **Tools / Actions** — pluggable interface for calling external systems (APIs, DBs, LLMs, search).
5. **Output Actions** — pluggable interface for emitting results (webhook, queue, DB write, message).
6. **Memory** — short-term (per-run context) and long-term (cross-run/session) memory abstraction.
7. **Guardrails** — pre/post execution policy checks (input validation, output filtering, budget/
   rate limits, PII redaction hooks).
8. **Observability** — structured logs, metrics (latency, success/error rate, token/cost usage),
   and an audit trail per run (who/what/when/inputs/outputs/tool calls).

## 5. Non-Functional Requirements
- Reliable execution: automatic retries with backoff, per-task and per-flow timeouts.
- Auditable: every run must be reconstructable from stored state/logs.
- Extensible: adding a new tool, input handler, or output action must not require touching the
  orchestrator core.
- No dependency on crew.ai, AutoGen, n8n, or similar all-in-one agent frameworks.
- Apache components (Kafka, Airflow, Camel, etc.) may be used for messaging/orchestration/storage.

## 6. Deliverables
- Framework SDK with APIs for **flows, tools, and policies**.
- At least **two reference agents** demonstrating real workflows end to end.
- **Design doc** (this repo's `/docs`) + **performance benchmarks** for the framework itself
  (throughput/latency under load, retry/timeout behavior).

## 7. Performance Targets
- Reliable execution under induced failures (tool timeouts, transient errors) via retries/timeouts.
- Framework overhead (orchestration/state-store round-trip) stays low relative to task execution
  time, with before/after numbers for any optimization work done.

## 8. Stretch Goals
- Multi-agent collaboration (flows that spawn/coordinate sub-agents).
- Reflection loops (self-critique / retry-with-feedback steps).
- Human-in-the-loop steps (pause a flow for approval, then resume).

## 9. Out of Scope (MVP)
- A hosted UI builder (CLI/SDK + minimal dashboard is enough for MVP).
- Multi-tenant billing/auth beyond a basic API key check.
