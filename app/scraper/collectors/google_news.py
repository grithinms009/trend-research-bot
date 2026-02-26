"""
Google News RSS collector — fetches trending articles per channel category.

Provides high-quality articles with real URLs for all 5 channels by
querying category-specific search terms via Google News RSS.
Resolves Google News redirect URLs to actual article URLs.
"""

import logging
from typing import Dict, List
from urllib.parse import quote_plus

import feedparser
import requests
from dateutil import parser as dateparser

from .base_collector import BaseCollector

logger = logging.getLogger(__name__)

MAX_ENTRIES_PER_QUERY = 8
GOOGLE_NEWS_RSS = "https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
REQUEST_TIMEOUT = 10

# Category-specific search queries mapped to channels
CHANNEL_QUERIES = {
    "C1": [
        "artificial intelligence news today",
        "tech startup news",
        "new AI tool released",
        "cybersecurity news",
        "robotics breakthrough",
    ],
    "C2": [
        "stock market news today",
        "cryptocurrency news",
        "federal reserve interest rate",
        "wall street earnings report",
        "global economy news",
    ],
    "C3": [
        "scientific discovery news",
        "space exploration news NASA",
        "archaeology ancient discovery",
        "physics breakthrough research",
        "history documentary facts",
    ],
    "C4": [
        "luxury travel destination",
        "world best hotels resorts",
        "luxury lifestyle billionaire",
        "exotic vacation island paradise",
        "supercar luxury brand news",
    ],
    "C5": [
        "productivity tips habits",
        "morning routine successful people",
        "self improvement life hacks",
        "time management work life balance",
        "mental health mindfulness tips",
    ],
}


class GoogleNewsCollector(BaseCollector):
    """Collects trending articles from Google News RSS per channel category."""

    def collect_topics(self) -> List[Dict]:
        all_topics = []

        for channel_id, queries in CHANNEL_QUERIES.items():
            for query in queries:
                try:
                    topics = self._fetch_query(query, channel_id)
                    all_topics.extend(topics)
                except Exception as e:
                    logger.warning(
                        "Google News fetch error (query=%s, channel=%s): %s",
                        query, channel_id, e,
                    )

        return all_topics

    def _fetch_query(self, query: str, channel_id: str) -> List[Dict]:
        encoded_query = quote_plus(query)
        feed_url = GOOGLE_NEWS_RSS.format(query=encoded_query)
        feed = feedparser.parse(feed_url)
        topics = []

        for i, entry in enumerate(feed.entries[:MAX_ENTRIES_PER_QUERY]):
            # Parse published date
            published_at = ""
            pub_field = getattr(entry, "published", "")
            if pub_field:
                try:
                    published_at = dateparser.parse(pub_field).isoformat()
                except Exception:
                    published_at = pub_field

            # Google News URLs are redirects — resolve to actual article URL
            raw_url = getattr(entry, "link", "")
            url = self._resolve_google_url(raw_url) if raw_url else ""
            title = getattr(entry, "title", "")

            if not title or not url:
                continue

            raw = {
                "title": title,
                "url": url,
                "source": "google_news",
                "score": 1.5,
                "rank": i + 1,
                "published_at": published_at,
                "channel_hint": channel_id,  # Pre-assigned channel
            }

            enriched = self.enrich_topic(raw)

            # Override channel with the intended channel if classifier
            # defaults to C1 but we fetched from a specific category
            if enriched.get("channel") == "C1" and channel_id != "C1":
                enriched["channel"] = channel_id

            topics.append(enriched)

        return topics

    @staticmethod
    def _resolve_google_url(google_url: str) -> str:
        """Follow Google News redirect to get the actual article URL."""
        if "news.google.com" not in google_url:
            return google_url
        try:
            resp = requests.head(
                google_url,
                allow_redirects=True,
                timeout=REQUEST_TIMEOUT,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            final_url = resp.url
            # If we still end up on Google, try GET with redirect
            if "google.com" in final_url:
                resp = requests.get(
                    google_url,
                    allow_redirects=True,
                    timeout=REQUEST_TIMEOUT,
                    headers={"User-Agent": "Mozilla/5.0"},
                )
                final_url = resp.url
            return final_url if "google.com" not in final_url else google_url
        except Exception as exc:
            logger.debug("Failed to resolve Google URL %s: %s", google_url[:60], exc)
            return google_url
