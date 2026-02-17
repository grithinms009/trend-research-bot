from __future__ import annotations

import asyncio
import logging
from typing import List

import feedparser
import httpx

from app.content_intelligence.connectors.base import BaseConnector
from app.content_intelligence.models.topic import RawTopic

logger = logging.getLogger(__name__)


class NewsRssConnector(BaseConnector):
    name = "news_rss"

    async def _fetch_impl(self) -> List[RawTopic]:
        feeds = self.config.get("feeds", [])
        limit = self.config.get("limit", 15)
        if not feeds:
            logger.warning("NewsRssConnector has no feeds configured")
            return []

        async with httpx.AsyncClient(timeout=10) as client:
            tasks = [self._fetch_feed(client, url, limit) for url in feeds]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        topics: List[RawTopic] = []
        for result in results:
            if isinstance(result, list):
                topics.extend(result)
            else:
                logger.warning("NewsRssConnector feed failed: %s", result)
        return topics

    async def _fetch_feed(self, client: httpx.AsyncClient, feed_url: str, limit: int) -> List[RawTopic]:
        resp = await client.get(feed_url, headers={"User-Agent": "ContentIntelBot/1.0"})
        resp.raise_for_status()
        feed = feedparser.parse(resp.text)

        topics: List[RawTopic] = []
        for entry in feed.entries[:limit]:
            title = getattr(entry, "title", "Untitled")
            summary = getattr(entry, "summary", title)
            link = getattr(entry, "link", "")
            topics.append(
                RawTopic(
                    title=title,
                    summary=summary,
                    source_urls=[link] if link else [],
                    category_hint="ai_news",
                    metadata={"feed": feed.feed.get("title", ""), "published": getattr(entry, "published", "")},
                )
            )
        await asyncio.sleep(0.05)
        return topics
