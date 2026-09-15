# Expense Approval Agent (Reference Agent #3)

The other two reference agents (customer support, research) both showcase search + memory +
LLM drafting. This one exists for a different reason: to actually demonstrate **Phase 9's
human-in-the-loop approval** — `Task(requires_approval=True)` + `AsyncOrchestrator.resume()` —
which was built, tested, and documented back in Phase 9 but never used by either existing agent.

## The workflow

An employee submits an expense (amount, category, description). The agent:

1. **Searches company expense policy** for rules relevant to this category/description.
2. **Recalls this employee's past expense history** (long-term memory, keyed by `employee_id`
   as the `session_id` — same pattern the support agent uses for `customer_id`).
3. **An LLM writes an advisory assessment** against the policy + history it just found.
4. **Pauses and waits for a real human** to approve or reject — every submission, not just large
   ones (see "Why every expense requires approval" below).
5. On approval, **records the decision** to memory, so it shows up in that employee's history
   the next time they submit something.

## Why every expense requires approval, not just large ones

`Task.requires_approval` is set once when the flow is *defined*, not evaluated per run — so
there's no built-in way to say "pause only if amount > $500" without either a second flow or a
runtime branch picking between two flows. Rather than force that complexity in for a reference
agent, this one requires approval on every submission. That's also a completely normal real
expense-policy design on its own, not a workaround.

## Running it

```bash
python3 run.py
```

Runs three passes: an approved expense, a rejected one, and a second approved expense from the
same employee (`emp-alice`) — the third pass's `recall_history` should show the first one, proving
memory recall works across separate, independently-approved runs.

## The tricky bit: `run()` genuinely blocks

`AsyncOrchestrator.run()` suspends on a real `asyncio.Event` when it hits an approval-gated task
— not a poll loop, an actual suspend. That means a caller can't just `await orchestrator.run(...)`
directly if they need to know the `run_id` (to later call `resume()`) before the human has acted.

This agent's `run.py` (and the REST layer, in `server.py`) solve it the same way: schedule
`run()` as a background `asyncio.Task`, and use the `on_created` callback added to
`AsyncOrchestrator.run()` for this exact purpose — it fires with the `run_id` immediately after
the run record is created, well before any task (let alone the approval-gated one) executes.

## Try it over the deployed REST API

```
POST /v1/runs
{"flow_name": "expense-approval", "inputs": {"employee_id": "emp-alice", "amount": 45.00, "category": "meals", "description": "Team lunch"}, "session_id": "emp-alice"}
```

Returns immediately with `{"run_id": ..., "status": "queued"}` (not `"succeeded"` — this flow
can't complete until someone approves it). Then:

```
GET /v1/runs/{run_id}
```

Shows `"status": "waiting"` and the LLM's assessment. Approve or reject it:

```
POST /v1/runs/{run_id}/approve
{"task_name": "request_approval", "approved": true}
```

Then `GET /v1/runs/{run_id}` again to see the final result.
