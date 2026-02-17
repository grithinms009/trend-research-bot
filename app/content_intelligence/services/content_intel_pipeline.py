from __future__ import annotations

import asyncio
import logging
from typing import List

from app.content_intelligence.config import load_config
from app.content_intelligence.connectors import load_connectors
from app.content_intelligence.models.topic import Topic
from app.content_intelligence.queue import build_queue_publisher
from app.content_intelligence.services.content_score_service import ContentScoreService
from app.content_intelligence.services.quality_filter_service import QualityFilterService
from app.content_intelligence.services.queue_publisher_service import QueuePublisherService
from app.content_intelligence.services.source_fetch_service import SourceFetchService
from app.content_intelligence.services.topic_cluster_service import TopicClusterService
from app.content_intelligence.services.trend_discovery_service import TrendDiscoveryService
from app.content_intelligence.utils.category import CategoryRouter

logger = logging.getLogger(__name__)


class ContentIntelPipeline:
    """Co-ordinates the content-first scraping pipeline."""

    def __init__(self, config_path: str | None = None):
        self.config = load_config(config_path)
        connector_config = self.config.get("connectors", {})
        self.connectors = load_connectors(connector_config)
        category_router = CategoryRouter(self.config.get("categories", {}))
        self.discovery = TrendDiscoveryService(self.connectors)
        self.source_fetch = SourceFetchService(category_router)
        self.cluster = TopicClusterService()
        self.quality = QualityFilterService()
        scoring_cfg = self.config.get("scoring", {})
        self.scoring = ContentScoreService(
            freshness_half_life_hours=scoring_cfg.get("freshness_half_life_hours", 12),
            monetization_baseline=scoring_cfg.get("monetization_baseline", 0.4),
        )
        queue_config = self.config.get("queue", {})
        publisher = build_queue_publisher(queue_config)
        self.queue = QueuePublisherService(publisher)

    async def run(self) -> List[Topic]:
        raw_topics = await self.discovery.discover()
        candidates = self.source_fetch.build_candidates(raw_topics)
        clustered = self.cluster.cluster(candidates)
        filtered = self.quality.filter(clustered)
        scored = self.scoring.score(filtered)
        await self.queue.publish(scored)
        return scored


def run_sync(config_path: str | None = None) -> List[Topic]:
    pipeline = ContentIntelPipeline(config_path)
    return asyncio.run(pipeline.run())
