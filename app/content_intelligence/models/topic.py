from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, Any


@dataclass(slots=True)
class RawTopic:
    """Topic payload as provided by connector plugins before normalization."""

    title: str
    summary: str
    source_urls: List[str]
    category_hint: str
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class TopicScores:
    trend_velocity: float
    search_volume: float
    news_frequency: float
    youtube_activity: float
    monetization_value: float

    def global_score(self) -> float:
        return round(
            (self.trend_velocity * 0.35)
            + (self.search_volume * 0.25)
            + (self.news_frequency * 0.15)
            + (self.youtube_activity * 0.15)
            + (self.monetization_value * 0.10),
            4,
        )


@dataclass(slots=True)
class TopicCandidate:
    topic_id: str
    topic_title: str
    keywords: List[str]
    source_urls: List[str]
    category: str
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class Topic:
    topic_id: str
    topic_title: str
    keywords: List[str]
    source_urls: List[str]
    category: str
    trend_score: float
    freshness_score: float
    monetization_score: float
    global_score: float
    created_at: datetime
    diagnostics: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "topic_id": self.topic_id,
            "topic_title": self.topic_title,
            "keywords": self.keywords,
            "source_urls": self.source_urls,
            "category": self.category,
            "trend_score": self.trend_score,
            "freshness_score": self.freshness_score,
            "monetization_score": self.monetization_score,
            "global_score": self.global_score,
            "created_at": self.created_at.isoformat(),
            "diagnostics": self.diagnostics,
        }
