"""Reference/test implementations — no external service required. Production swaps:
`integrations/redis_memory.py` (short-term) and `integrations/vector_memory.py` (long-term).
"""
from __future__ import annotations

from typing import Any, Optional

from agentframework.memory.base import LongTermMemory, MemoryRecord, ShortTermMemory, new_record_id


class InMemoryShortTermMemory(ShortTermMemory):
    def __init__(self) -> None:
        self._runs: dict[str, dict[str, Any]] = {}

    async def set(self, run_id: str, key: str, value: Any) -> None:
        self._runs.setdefault(run_id, {})[key] = value

    async def get(self, run_id: str, key: str) -> Any:
        return self._runs.get(run_id, {}).get(key)

    async def all(self, run_id: str) -> dict[str, Any]:
        return dict(self._runs.get(run_id, {}))

    async def clear(self, run_id: str) -> None:
        self._runs.pop(run_id, None)


class InMemoryLongTermMemory(LongTermMemory):
    """Keyword-scored recall (same approach as tools.search_tool.SimpleSearchTool) — no
    embeddings/vector index, so relevance is term-overlap, not semantic similarity. Good enough
    for tests/demos and small deployments; swap in `integrations/vector_memory.py`'s
    ChromaLongTermMemory for real semantic recall.
    """

    def __init__(self) -> None:
        self._sessions: dict[str, dict[str, MemoryRecord]] = {}

    async def remember(self, session_id: str, text: str, metadata: Optional[dict[str, Any]] = None) -> str:
        record = MemoryRecord(id=new_record_id(), text=text, metadata=metadata or {})
        self._sessions.setdefault(session_id, {})[record.id] = record
        return record.id

    async def recall(self, session_id: str, query: str, top_k: int = 5) -> list[MemoryRecord]:
        terms = query.lower().split()
        records = list(self._sessions.get(session_id, {}).values())
        scored: list[MemoryRecord] = []
        for record in records:
            text_lower = record.text.lower()
            score = sum(text_lower.count(term) for term in terms)
            if score > 0:
                scored.append(MemoryRecord(id=record.id, text=record.text,
                                            metadata=record.metadata, ts=record.ts, score=score))
        scored.sort(key=lambda r: r.score, reverse=True)
        return scored[:top_k]

    async def forget(self, session_id: str, record_id: str) -> None:
        self._sessions.get(session_id, {}).pop(record_id, None)
