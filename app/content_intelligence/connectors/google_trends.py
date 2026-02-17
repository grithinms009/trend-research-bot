from __future__ import annotations

import asyncio
import json
import logging
from typing import List

import httpx

from app.content_intelligence.connectors.base import BaseConnector
from app.content_intelligence.models.topic import RawTopic

logger = logging.getLogger(__name__)


class GoogleTrendsConnector(BaseConnector):
    name = "google_trends"
    API_URL = "https://trends.google.com/trends/api/dailytrends"

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
                logger.warning("GoogleTrendsConnector region fetch failed: %s", result)
        return topics

    async def _fetch_region(self, client: httpx.AsyncClient, region: str, limit: int) -> List[RawTopic]:
        params = {"hl": "en-US", "tz": 0, "geo": region}
        resp = await client.get(self.API_URL, params=params, headers={"User-Agent": "ContentIntelBot/1.0"})
        resp.raise_for_status()

        raw_text = resp.text
        payload = raw_text[raw_text.find("{") :]
        data = json.loads(payload)

        searches = data.get("default", {}).get("trendingSearchesDays", [])
        topics: List[RawTopic] = []
        for day in searches:
            for search in day.get("trendingSearches", [])[:limit]:
                title = search.get("title", {}).get("query", "Untitled trend")
                articles = search.get("articles", [])
                summary = search.get("snippet", title)
                urls = [article.get("url") for article in articles if article.get("url")]
                metadata = {
                    "search_volume": search.get("formattedTraffic", "0"),
                    "region": region,
                    "source_type": "search",
                }
                topics.append(
                    RawTopic(
                        title=title,
                        summary=summary,
                        source_urls=urls,
                        category_hint="ai_news",
                        metadata=metadata,
                    )
                )
        await asyncio.sleep(0.1)
        return topics
