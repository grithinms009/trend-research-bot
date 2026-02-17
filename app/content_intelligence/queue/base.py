from __future__ import annotations

import abc
from typing import Iterable

from app.content_intelligence.models.topic import Topic


class BaseQueuePublisher(abc.ABC):
    """Abstract publisher that pushes topics into downstream queues."""

    @abc.abstractmethod
    async def publish(self, topics: Iterable[Topic]) -> None:
        raise NotImplementedError
