"""Structured logging (docs/PRD.md > Observability, docs/Design.md > Logging/Audit Format:
"one JSON line per event: {ts, run_id, task_name, event, payload, duration_ms}").

Kept separate from the audit trail (core.state_store.StateStore.audit_trail) — the audit trail
is the durable, queryable "what happened" record; this is the human/log-aggregator-facing
stream, and is fine to lose (stdout, a log shipper) since the audit trail is the source of truth.
"""
from __future__ import annotations

import json
import sys
import time
from typing import Any, Optional, TextIO


class JsonLineLogger:
    def __init__(self, stream: Optional[TextIO] = None, capture: bool = True):
        """`stream`: where to write JSON lines (e.g. sys.stdout); None writes nowhere.
        `capture`: also keep every entry in `self.records` (in-memory) — convenient for tests/
        demos; disable for long-running processes where unbounded memory growth matters."""
        self.stream = stream
        self.capture = capture
        self.records: list[dict[str, Any]] = []

    def log(self, event: str, **fields: Any) -> None:
        entry = {"ts": time.time(), "event": event, **fields}
        if self.capture:
            self.records.append(entry)
        if self.stream is not None:
            self.stream.write(json.dumps(entry) + "\n")
            self.stream.flush()


def stdout_logger(capture: bool = False) -> JsonLineLogger:
    return JsonLineLogger(stream=sys.stdout, capture=capture)
