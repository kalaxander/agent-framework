"""Production ShortTermMemory backed by Redis (docs/Architecture.md > Memory: "short-term
(per-run scratchpad, in Redis)"). Same interface as memory.in_memory.InMemoryShortTermMemory.
Requires the `memory` extra:

    pip install -e ".[memory]"   # redis
"""
from __future__ import annotations

import json
from typing import Any

from agentframework.memory.base import ShortTermMemory

_KEY_PREFIX = "agentfw:run:"


class RedisShortTermMemory(ShortTermMemory):
    def __init__(self, redis_url: str, ttl_seconds: int = 3600):
        try:
            import redis.asyncio  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "RedisShortTermMemory requires the 'memory' extra: pip install -e '.[memory]'"
            ) from exc
        self._redis_url = redis_url
        self._ttl_seconds = ttl_seconds
        self._client = None

    async def _get_client(self):
        import redis.asyncio as aioredis

        if self._client is None:
            self._client = aioredis.from_url(self._redis_url, decode_responses=True)
        return self._client

    @staticmethod
    def _hash_key(run_id: str) -> str:
        return f"{_KEY_PREFIX}{run_id}"

    async def set(self, run_id: str, key: str, value: Any) -> None:
        client = await self._get_client()
        hash_key = self._hash_key(run_id)
        await client.hset(hash_key, key, json.dumps(value))
        await client.expire(hash_key, self._ttl_seconds)

    async def get(self, run_id: str, key: str) -> Any:
        client = await self._get_client()
        raw = await client.hget(self._hash_key(run_id), key)
        return json.loads(raw) if raw is not None else None

    async def all(self, run_id: str) -> dict[str, Any]:
        client = await self._get_client()
        raw = await client.hgetall(self._hash_key(run_id))
        return {k: json.loads(v) for k, v in raw.items()}

    async def clear(self, run_id: str) -> None:
        client = await self._get_client()
        await client.delete(self._hash_key(run_id))
