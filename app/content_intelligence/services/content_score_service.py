from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import List

from app.content_intelligence.models.topic import Topic, TopicCandidate, TopicScores

logger = logging.getLogger(__name__)


class ContentScoreService:
    def __init__(self, freshness_half_life_hours: int = 12, monetization_baseline: float = 0.4):
        self.freshness_half_life_hours = freshness_half_life_hours
        self.monetization_baseline = monetization_baseline

    def score(self, candidates: List[TopicCandidate]) -> List[Topic]:
        scored: List[Topic] = []
        for candidate in candidates:
            scores = self._compute_scores(candidate)
            now = datetime.now(timezone.utc)
            topic = Topic(
                topic_id=candidate.topic_id,
                topic_title=candidate.topic_title,
                keywords=candidate.keywords,
                source_urls=candidate.source_urls,
                category=candidate.category,
                trend_score=scores.trend_velocity,
                freshness_score=scores.news_frequency,
                monetization_score=scores.monetization_value,
                global_score=scores.global_score(),
                created_at=now,
                diagnostics={
                    "raw_metadata": candidate.metadata,
                    "score_components": scores.__dict__,
                },
            )
            scored.append(topic)
        logger.info("ContentScoreService scored %d topics", len(scored))
        return scored

    def _compute_scores(self, candidate: TopicCandidate) -> TopicScores:
        metadata = candidate.metadata.get("raw_metadata", {})
        source_counts = candidate.metadata.get("source_counts", {})

        trend_velocity = min(1.0, (source_counts.get("youtube", 0) + source_counts.get("reddit", 0)) / 10.0)
        search_volume = min(1.0, len(candidate.keywords) / 12.0)
        news_frequency = min(1.0, len(candidate.source_urls) / 10.0)
        youtube_activity = min(1.0, source_counts.get("youtube", 0) / 5.0)
        monetization_value = max(self.monetization_baseline, min(1.0, metadata.get("estimated_cpm", 0.5)))

        return TopicScores(
            trend_velocity=trend_velocity,
            search_volume=search_volume,
            news_frequency=news_frequency,
            youtube_activity=youtube_activity,
            monetization_value=monetization_value,
        )
