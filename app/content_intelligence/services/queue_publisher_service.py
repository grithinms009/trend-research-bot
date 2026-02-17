from __future__ import annotations

import logging
from typing import Iterable, Set

from app.content_intelligence.models.topic import Topic
from app.content_intelligence.queue.base import BaseQueuePublisher

logger = logging.getLogger(__name__)


class QueuePublisherService:
    """Publishes topics into downstream queue, ensuring idempotency."""

    def __init__(self, publisher: BaseQueuePublisher) -> None:
        self.publisher = publisher
        self._seen_ids: Set[str] = set()

    async def publish(self, topics: Iterable[Topic]) -> int:
        fresh_topics = [topic for topic in topics if topic.topic_id not in self._seen_ids]
        if not fresh_topics:
            logger.info("No new topics to publish")
            return 0

        await self.publisher.publish(fresh_topics)
        for topic in fresh_topics:
            self._seen_ids.add(topic.topic_id)
        logger.info("Published %d topics to downstream queue", len(fresh_topics))
        return len(fresh_topics)
