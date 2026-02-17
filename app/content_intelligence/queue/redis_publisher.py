from __future__ import annotations

import json
import logging
from typing import Iterable

import redis.asyncio as redis

from app.content_intelligence.models.topic import Topic
from app.content_intelligence.queue.base import BaseQueuePublisher

logger = logging.getLogger(__name__)


class RedisQueuePublisher(BaseQueuePublisher):
    def __init__(self, config):
        self.config = config or {}
        self.host = self.config.get("host", "localhost")
        self.port = self.config.get("port", 6379)
        self.stream = self.config.get("topic_stream", "content_topics")
        self._client: redis.Redis | None = None

    async def _client_instance(self) -> redis.Redis:
        if not self._client:
            self._client = redis.Redis(host=self.host, port=self.port, decode_responses=True)
        return self._client

    async def publish(self, topics: Iterable[Topic]) -> None:
        client = await self._client_instance()
        for topic in topics:
            payload = json.dumps(topic.to_dict())
            await client.xadd(self.stream, {"payload": payload}, maxlen=1000, approximate=True)
            logger.info("Published topic %s to stream %s", topic.topic_id, self.stream)
