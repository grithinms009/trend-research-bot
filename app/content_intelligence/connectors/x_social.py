from __future__ import annotations

import asyncio
import logging
from typing import List

import httpx
import feedparser

from app.content_intelligence.connectors.base import BaseConnector
from app.content_intelligence.models.topic import RawTopic

logger = logging.getLogger(__name__)


class XSocialConnector(BaseConnector):
    name = "x_social"
    SEARCH_URL = "https://nitter.net/search/rss"

    async def _fetch_impl(self) -> List[RawTopic]:
        if not self.config.get("enabled", False):
            return []
        search_terms = self.config.get("search_terms", [])
        limit = self.config.get("limit", 10)
        if not search_terms:
            return []

        async with httpx.AsyncClient(timeout=10) as client:
            tasks = [self._fetch_term(client, term, limit) for term in search_terms]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        topics: List[RawTopic] = []
        for result in results:
            if isinstance(result, list):
                topics.extend(result)
            else:
                logger.warning("XSocialConnector term fetch failed: %s", result)
        return topics

    async def _fetch_term(self, client: httpx.AsyncClient, term: str, limit: int) -> List[RawTopic]:
        params = {"q": term}
        resp = await client.get(self.SEARCH_URL, params=params, headers={"User-Agent": "ContentIntelBot/1.0"})
        resp.raise_for_status()
        feed = feedparser.parse(resp.text)

        topics: List[RawTopic] = []
        for entry in feed.entries[:limit]:
            text = getattr(entry, "title", term)
            topics.append(
                RawTopic(
                    title=text,
                    summary=getattr(entry, "summary", text),
                    source_urls=[getattr(entry, "link", "")],
                    category_hint="ai_news",
                    metadata={"term": term},
                )
            )
        await asyncio.sleep(0.05)
        return topics
