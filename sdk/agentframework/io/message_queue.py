"""Messaging abstraction (docs/Architecture.md > Orchestrator/Executors are decoupled via
Kafka). `InMemoryMessageQueue` is the reference/test backend (asyncio.Queue per topic);
`integrations/kafka_message_queue.py` implements the same interface over Apache Kafka for
production — same pattern as core/state_store.py's StateStore / PostgresStateStore split.
"""
from __future__ import annotations

import asyncio
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, AsyncIterator


@dataclass
class Message:
    topic: str
    payload: dict[str, Any]


class MessageQueue(ABC):
    @abstractmethod
    async def publish(self, topic: str, payload: dict[str, Any]) -> None: ...

    @abstractmethod
    async def consume(self, topic: str) -> AsyncIterator[Message]:
        """Yield messages published to `topic`, forever (until the consumer stops iterating)."""
        ...


class InMemoryMessageQueue(MessageQueue):
    """Reference MessageQueue. Topics are created lazily on first publish/consume."""

    def __init__(self) -> None:
        self._topics: dict[str, asyncio.Queue] = {}

    def _queue_for(self, topic: str) -> asyncio.Queue:
        if topic not in self._topics:
            self._topics[topic] = asyncio.Queue()
        return self._topics[topic]

    async def publish(self, topic: str, payload: dict[str, Any]) -> None:
        # round-trip through JSON so this behaves like a real broker (no shared mutable objects
        # leaking between publisher/consumer, and it catches non-serializable payloads early).
        json.dumps(payload)
        await self._queue_for(topic).put(Message(topic=topic, payload=payload))

    async def consume(self, topic: str) -> AsyncIterator[Message]:
        queue = self._queue_for(topic)
        while True:
            message = await queue.get()
            yield message
