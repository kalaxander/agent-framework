"""Production LongTermMemory backed by Chroma (docs/Architecture.md > Memory: "long-term
(cross-run/session, vector DB e.g. Chroma/pgvector)"). Same interface as
memory.in_memory.InMemoryLongTermMemory, but with real embedding-based semantic recall instead
of keyword scoring. Requires the `memory` extra:

    pip install -e ".[memory]"   # chromadb

Each `session_id` gets its own Chroma collection, so recall never crosses sessions.
"""
from __future__ import annotations

from typing import Any, Optional

from agentframework.memory.base import LongTermMemory, MemoryRecord, new_record_id


class ChromaLongTermMemory(LongTermMemory):
    def __init__(self, persist_directory: Optional[str] = None):
        try:
            import chromadb
        except ImportError as exc:
            raise ImportError(
                "ChromaLongTermMemory requires the 'memory' extra: pip install -e '.[memory]'"
            ) from exc

        self._client = (
            chromadb.PersistentClient(path=persist_directory)
            if persist_directory else chromadb.Client()
        )

    def _collection(self, session_id: str):
        return self._client.get_or_create_collection(name=f"session_{session_id}")

    async def remember(self, session_id: str, text: str, metadata: Optional[dict[str, Any]] = None) -> str:
        record_id = new_record_id()
        self._collection(session_id).add(
            ids=[record_id], documents=[text], metadatas=[metadata or {}]
        )
        return record_id

    async def recall(self, session_id: str, query: str, top_k: int = 5) -> list[MemoryRecord]:
        result = self._collection(session_id).query(query_texts=[query], n_results=top_k)
        records = []
        ids = result.get("ids", [[]])[0]
        docs = result.get("documents", [[]])[0]
        metas = result.get("metadatas", [[]])[0]
        distances = result.get("distances", [[]])[0]
        for rid, doc, meta, dist in zip(ids, docs, metas, distances):
            records.append(MemoryRecord(id=rid, text=doc, metadata=meta or {}, score=1 - dist))
        return records

    async def forget(self, session_id: str, record_id: str) -> None:
        self._collection(session_id).delete(ids=[record_id])
