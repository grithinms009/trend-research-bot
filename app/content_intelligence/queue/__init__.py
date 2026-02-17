from __future__ import annotations

from typing import Dict

from app.content_intelligence.queue.base import BaseQueuePublisher
from app.content_intelligence.queue.redis_publisher import RedisQueuePublisher


def build_queue_publisher(config: Dict) -> BaseQueuePublisher:
    backend = (config or {}).get("backend", "redis").lower()
    if backend == "redis":
        return RedisQueuePublisher((config or {}).get("redis", {}))
    raise ValueError(f"Unsupported queue backend: {backend}")


__all__ = ["build_queue_publisher", "BaseQueuePublisher", "RedisQueuePublisher"]
