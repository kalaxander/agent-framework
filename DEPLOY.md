# Deploying this project — step by step

This gets you a real, public URL your mentor can visit directly. Two parts: (1) put the code on
GitHub, (2) point a free hosting platform (Render) at it.

## Part 1 — Push to GitHub

If you don't already have this project in a GitHub repo:

1. Go to [github.com/new](https://github.com/new), create a new repository (any name, e.g.
   `agent-framework`). Don't check "Add a README" — you already have one.
2. In your project folder (PowerShell):
   ```powershell
   git init
   git add .
   git commit -m "Initial commit"
   git branch -M main
   git remote add origin https://github.com/YOUR-USERNAME/YOUR-REPO-NAME.git
   git push -u origin main
   ```
   If `git` isn't installed, get it from [git-scm.com](https://git-scm.com/download/win) first.
3. **Double-check `.env` isn't in the repo** (it shouldn't be — `.gitignore` already excludes
   it) — go to your repo on github.com and confirm you don't see a `.env` file listed with real
   keys in it. If you never created a `.env` file locally, there's nothing to worry about.

## Part 2 — Deploy on Render

1. Go to [render.com](https://render.com), sign up (free, GitHub login is easiest).
2. Click **New +** → **Web Service**.
3. Connect your GitHub account if prompted, then select your repo.
4. Render should auto-detect it's a Python app. Fill in:
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `uvicorn server:app --host 0.0.0.0 --port $PORT`
     (this matches the `Procfile`, but setting it explicitly is more reliable)
   - **Instance Type**: Free
5. Before clicking "Create Web Service," add your environment variables — scroll to
   **Environment Variables** and add:
   - `GEMINI_API_KEY` = your real key (optional — server falls back to a mock LLM without it)
   - `DATABASE_URL` = your real Neon/Supabase connection string (optional — falls back to
     in-memory storage without it)
6. Click **Create Web Service**. Render will build and deploy — takes a few minutes the first
   time. Watch the logs; you should see lines like `LLM: using real Gemini API` and `State
   store: using real Postgres` if you set those env vars (or the fallback messages if you
   didn't — both are fine, just different capability levels).
7. Once it says "Live," Render gives you a URL like `https://your-app-name.onrender.com`.

## Verify it's actually working

**The easy way**: just visit `https://your-app-name.onrender.com` — that's now the dispatch-desk
frontend (`frontend/index.html`), a real form. Type a ticket, click "Route Ticket," watch each
pipeline stage (search KB → recall history → draft reply → remember ticket) resolve with real
data. This is what to actually show your mentor — a live demo, not a JSON blob.

**The API way**, if you want to see the raw requests: visit
`https://your-app-name.onrender.com/docs` — FastAPI's interactive API page.
- Expand `POST /v1/runs`, click "Try it out," paste:
  ```json
  {"flow_name": "customer-support-ticket", "inputs": {"ticket_text": "My package arrived damaged, I need a refund."}}
  ```
- Click Execute. You should get back a `run_id` and `status: "succeeded"`.
- Copy the `run_id`, then try `GET /v1/runs/{run_id}` to see the full result, including the
  drafted reply.

Or from PowerShell:
```powershell
curl.exe -X POST https://your-app-name.onrender.com/v1/runs -H "Content-Type: application/json" -d "{\"flow_name\": \"customer-support-ticket\", \"inputs\": {\"ticket_text\": \"My package arrived damaged, I need a refund.\"}}"
```

### Testing long-term memory (customer ticket history) over the API

Add `"session_id": "some-customer-id"` to the request body. Send two requests using the SAME
`session_id` (representing the same customer's two different tickets) — the second one's
`recall_history` task result should include the first ticket's text. Omit `session_id` (or use
different ones) and each request gets its own isolated memory scope — that's the correct default
behavior, not a bug, but it means you need to explicitly pass a shared `session_id` to see
cross-ticket recall in action.

### Testing the expense approval agent (human-in-the-loop) over the API

This flow is different from the other two: `POST /v1/runs` returns immediately with
`"status": "queued"`, not `"succeeded"` — the flow genuinely pauses partway through and waits
for a real approval before it can finish.

1. `POST /v1/runs`:
   ```json
   {"flow_name": "expense-approval", "inputs": {"employee_id": "emp-1", "amount": 45.00, "category": "meals", "description": "Team lunch"}, "session_id": "emp-1"}
   ```
   Copy the returned `run_id`.
2. `GET /v1/runs/{run_id}` — should show `"status": "waiting"` and an LLM-written assessment
   under the `assess_expense` task.
3. `POST /v1/runs/{run_id}/approve`:
   ```json
   {"task_name": "request_approval", "approved": true}
   ```
4. `GET /v1/runs/{run_id}` again — should now show `"status": "succeeded"`.

Try `"approved": false` on a second submission too — status should come back `"failed"`, with
`request_approval`'s task error reading `"rejected by human-in-the-loop approval"`.

## Worth knowing before demo day

- **Render's free tier spins down after ~15 minutes of no traffic**, and the first request after
  that takes 30-60 seconds to wake back up. If you're demoing live, hit the URL yourself a
  minute or two before your mentor looks at it, so it's already warm.
- If you set `DATABASE_URL`, runs really do persist in your real Postgres — you can restart the
  Render service and a previous `run_id` will still be retrievable via `GET /v1/runs/{run_id}`.
  That's a good thing to actually demonstrate, not just claim.
- All three reference agents (`customer-support-ticket`, `research-report`, `expense-approval`)
  are exposed on the deployed server. The dispatch-desk frontend (`/`) currently only has a form
  for the support agent — the other two are reachable via `/docs` or direct API calls.

## Troubleshooting

**`/docs` shows "Failed to load API definition" / `/openapi.json` returns 500, but the server
otherwise starts and other endpoints work.** This happened during this project's own first
deploy. The **actual, confirmed root cause** (found via a real Render runtime traceback, after
two earlier wrong guesses — see docs/Memory.md for the full story): `RunRequestBody` was
defined as a Pydantic model **inside** the `build_app()` function instead of at module level.
Pydantic's OpenAPI schema generator resolves model classes by name through the module's global
namespace — a class defined inside a function is never bound there, so schema generation fails
with `PydanticUserError: ... is not fully defined`. This only surfaces on the first request to
`/docs`/`/openapi.json` (schema generation is lazy), which is why the server started fine and
`POST /v1/runs` worked normally — a very misleading set of symptoms. Fixed by moving
`RunRequestBody` to true module level in `fastapi_ingress.py`. If you're on an older copy of
this repo, pulling the current `sdk/agentframework/integrations/fastapi_ingress.py` fixes this.

(Two earlier, incorrect guesses along the way — pinning the Python version, and exact-pinning
dependency versions — are also reflected in this repo's history/`.python-version`/
`requirements.txt`. Neither was the actual cause, though version-pinning in general remains
reasonable practice and hasn't been reverted.)
