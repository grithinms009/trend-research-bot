from __future__ import annotations

from typing import Dict, List

from app.content_intelligence.connectors.base import BaseConnector
from app.content_intelligence.connectors.youtube_trending import YouTubeTrendingConnector
from app.content_intelligence.connectors.google_trends import GoogleTrendsConnector
from app.content_intelligence.connectors.news_rss import NewsRssConnector
from app.content_intelligence.connectors.reddit_topics import RedditTopicsConnector
from app.content_intelligence.connectors.x_social import XSocialConnector

CONNECTOR_REGISTRY = {
    "youtube_trending": YouTubeTrendingConnector,
    "google_trends": GoogleTrendsConnector,
    "news_rss": NewsRssConnector,
    "reddit_topics": RedditTopicsConnector,
    "x_social": XSocialConnector,
}


def load_connectors(connector_config: Dict[str, Dict]) -> List[BaseConnector]:
    connectors: List[BaseConnector] = []
    for name, params in connector_config.items():
        connector_cls = CONNECTOR_REGISTRY.get(name)
        if not connector_cls:
            continue
        connector = connector_cls(params or {})
        connectors.append(connector)
    return connectors
