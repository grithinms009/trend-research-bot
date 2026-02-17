"""
YouTube collector — fetches trending/popular videos via RSS feed.
"""

import logging
from typing import Dict, List

import feedparser
from dateutil import parser as dateparser

from .base_collector import BaseCollector

logger = logging.getLogger(__name__)

YOUTUBE_FEEDS = [
    "https://www.youtube.com/feeds/videos.xml?chart=mostPopular",
]

MAX_ENTRIES = 20


class YouTubeCollector(BaseCollector):
    """Collects trending video topics from YouTube RSS feeds."""

    def collect_topics(self) -> List[Dict]:
        all_topics = []

        for feed_url in YOUTUBE_FEEDS:
            try:
                topics = self._fetch_feed(feed_url)
                all_topics.extend(topics)
            except Exception as e:
                logger.warning(f"YouTube feed error ({feed_url}): {e}")

        return all_topics

    def _fetch_feed(self, feed_url: str) -> List[Dict]:
        feed = feedparser.parse(feed_url)
        topics = []

        for i, entry in enumerate(feed.entries[:MAX_ENTRIES]):
            # Parse published date
            published_at = ""
            pub_field = getattr(entry, "published", "") or getattr(entry, "updated", "")
            if pub_field:
                try:
                    published_at = dateparser.parse(pub_field).isoformat()
                except Exception:
                    published_at = pub_field

            # Build video URL
            url = getattr(entry, "link", "")

            raw = {
                "title": entry.title,
                "url": url,
                "source": "youtube",
                "score": 2.0,
                "rank": i + 1,
                "published_at": published_at,
            }

            enriched = self.enrich_topic(raw)
            topics.append(enriched)

        return topics
