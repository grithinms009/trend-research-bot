from __future__ import annotations

import asyncio
import logging
from typing import List

from app.content_intelligence.connectors.base import BaseConnector
from app.content_intelligence.models.topic import RawTopic

logger = logging.getLogger(__name__)


class TrendDiscoveryService:
    def __init__(self, connectors: List[BaseConnector]) -> None:
        self.connectors = connectors

    async def discover(self) -> List[RawTopic]:
        tasks = [connector.fetch() for connector in self.connectors]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        topics: List[RawTopic] = []
        for connector, result in zip(self.connectors, results, strict=False):
            if isinstance(result, Exception):
                logger.warning("Connector %s returned error: %s", connector.name, result)
                continue
            topics.extend(result)
            logger.info("Connector %s produced %d topics", connector.name, len(result))
        return topics
