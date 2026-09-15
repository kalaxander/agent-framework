"""Phase 5 — Memory (docs/PRD.md > Memory: "short-term (per-run context) and long-term
(cross-run/session) memory abstraction").

Two separate stores, each with its own interface (so Redis and a vector DB can be swapped in
independently — docs/Architecture.md > State & Memory):

- `ShortTermMemory`: a per-run scratchpad. Keyed by `run_id`; cleared/irrelevant once the run
  ends. Reference impl: `InMemoryShortTermMemory`. Production: `integrations/redis_memory.py`.
- `LongTermMemory`: cross-run/session memory with a simple remember/recall API (semantic-ish
  search over remembered text). Keyed by `session_id` (a caller-chosen scope — e.g. "same user
  across many ticket runs"). Reference impl: `InMemoryLongTermMemory` (keyword scoring, no
  embeddings). Production: `integrations/vector_memory.py` (Chroma).

`MemoryHandle` is what actually lands in a task's `context["__memory__"]` — a thin facade over
whichever store(s) the orchestrator/executor was configured with, scoped to the current run/
session so task code doesn't need to know or pass around IDs.
"""
from __future__ import annotations

import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class MemoryRecord:
    id: str
    text: str
    metadata: dict[str, Any] = field(default_factory=dict)
    ts: float = field(default_factory=time.time)
    score: float = 0.0  # relevance score, set by recall() — meaningless outside a recall result


class ShortTermMemory(ABC):
    @abstractmethod
    async def set(self, run_id: str, key: str, value: Any) -> None: ...

    @abstractmethod
    async def get(self, run_id: str, key: str) -> Any: ...

    @abstractmethod
    async def all(self, run_id: str) -> dict[str, Any]: ...

    @abstractmethod
    async def clear(self, run_id: str) -> None: ...


class LongTermMemory(ABC):
    @abstractmethod
    async def remember(self, session_id: str, text: str, metadata: Optional[dict[str, Any]] = None) -> str:
        """Store `text` under `session_id`; returns a record id."""
        ...

    @abstractmethod
    async def recall(self, session_id: str, query: str, top_k: int = 5) -> list[MemoryRecord]:
        """Return up to `top_k` records for `session_id` most relevant to `query`, best first."""
        ...

    @abstractmethod
    async def forget(self, session_id: str, record_id: str) -> None: ...


class MemoryHandle:
    """Facade injected into task context as `context["__memory__"]`. All methods are async —
    for a sync `Task(fn=...)` (as used by SyncExecutor), wrap calls in `asyncio.run(...)`
    (matches how sync tasks already invoke async tools — see core/executor.py)."""

    def __init__(
        self,
        run_id: str,
        session_id: str,
        short_term: Optional[ShortTermMemory] = None,
        long_term: Optional[LongTermMemory] = None,
    ):
        self.run_id = run_id
        self.session_id = session_id
        self._short = short_term
        self._long = long_term

    def _require_short(self) -> ShortTermMemory:
        if self._short is None:
            raise RuntimeError(
                "No ShortTermMemory configured — pass short_term_memory=... to the "
                "executor/orchestrator."
            )
        return self._short

    def _require_long(self) -> LongTermMemory:
        if self._long is None:
            raise RuntimeError(
                "No LongTermMemory configured — pass long_term_memory=... to the "
                "executor/orchestrator."
            )
        return self._long

    async def remember_short(self, key: str, value: Any) -> None:
        await self._require_short().set(self.run_id, key, value)

    async def recall_short(self, key: str) -> Any:
        return await self._require_short().get(self.run_id, key)

    async def all_short(self) -> dict[str, Any]:
        return await self._require_short().all(self.run_id)

    async def remember_long(self, text: str, metadata: Optional[dict[str, Any]] = None) -> str:
        return await self._require_long().remember(self.session_id, text, metadata)

    async def recall_long(self, query: str, top_k: int = 5) -> list[MemoryRecord]:
        return await self._require_long().recall(self.session_id, query, top_k)

    async def forget_long(self, record_id: str) -> None:
        await self._require_long().forget(self.session_id, record_id)


def new_record_id() -> str:
    return str(uuid.uuid4())
