from __future__ import annotations

import asyncio
import logging
from typing import List

import feedparser
import httpx

from app.content_intelligence.connectors.base import BaseConnector
from app.content_intelligence.models.topic import RawTopic

logger = logging.getLogger(__name__)


class YouTubeTrendingConnector(BaseConnector):
    name = "youtube_trending"
    FEED_TEMPLATE = "https://www.youtube.com/feeds/videos.xml?chart=mostPopular&hl=en&gl={region}"

    async def _fetch_impl(self) -> List[RawTopic]:
        regions = self.config.get("regions", ["US"])
        limit = self.config.get("limit", 10)
        topics: List[RawTopic] = []

        async with httpx.AsyncClient(timeout=10) as client:
            tasks = [self._fetch_region(client, region, limit) for region in regions]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, list):
                topics.extend(result)
            else:
                logger.warning("YouTubeTrendingConnector region fetch failed: %s", result)
        return topics

    async def _fetch_region(self, client: httpx.AsyncClient, region: str, limit: int) -> List[RawTopic]:
        url = self.FEED_TEMPLATE.format(region=region)
        resp = await client.get(url, headers={"User-Agent": "ContentIntelBot/1.0"})
        resp.raise_for_status()

        feed = feedparser.parse(resp.text)
        topics: List[RawTopic] = []

        for entry in feed.entries[:limit]:
            title = getattr(entry, "title", "Untitled")
            summary = getattr(entry, "summary", title)
            video_url = getattr(entry, "link", "")
            metadata = {
                "youtube_views": getattr(entry, "views", 0),
                "region": region,
                "published": getattr(entry, "published", ""),
                "source_type": "youtube",
            }
            topics.append(
                RawTopic(
                    title=title,
                    summary=summary,
                    source_urls=[video_url] if video_url else [],
                    category_hint="ai_news",  # will be rerouted later
                    metadata=metadata,
                )
            )
        await asyncio.sleep(0.2)
        return topics
