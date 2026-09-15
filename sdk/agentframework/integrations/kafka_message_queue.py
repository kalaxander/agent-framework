"""Production MessageQueue backed by Apache Kafka (docs/Architecture.md > messaging).
Implements the same interface as io.message_queue.InMemoryMessageQueue. Requires the `kafka`
extra:

    pip install -e ".[kafka]"   # aiokafka
"""
from __future__ import annotations

import json
from typing import Any, AsyncIterator

from agentframework.io.message_queue import Message, MessageQueue


class KafkaMessageQueue(MessageQueue):
    def __init__(self, bootstrap_servers: str):
        try:
            import aiokafka  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "KafkaMessageQueue requires the 'kafka' extra: pip install -e '.[kafka]'"
            ) from exc
        self._bootstrap_servers = bootstrap_servers
        self._producer = None

    async def _get_producer(self):
        from aiokafka import AIOKafkaProducer

        if self._producer is None:
            self._producer = AIOKafkaProducer(bootstrap_servers=self._bootstrap_servers)
            await self._producer.start()
        return self._producer

    async def publish(self, topic: str, payload: dict[str, Any]) -> None:
        producer = await self._get_producer()
        await producer.send_and_wait(topic, json.dumps(payload).encode())

    async def consume(self, topic: str) -> AsyncIterator[Message]:
        from aiokafka import AIOKafkaConsumer

        consumer = AIOKafkaConsumer(topic, bootstrap_servers=self._bootstrap_servers)
        await consumer.start()
        try:
            async for record in consumer:
                yield Message(topic=topic, payload=json.loads(record.value))
        finally:
            await consumer.stop()
