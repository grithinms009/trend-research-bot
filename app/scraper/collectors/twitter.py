"""
Twitter / X trends collector — scrapes trending topics from trends24.in.

For each trending topic, searches Google News RSS to find a real article
URL, so article extraction succeeds downstream.
"""

import logging
from typing import Dict, List
from urllib.parse import quote_plus

import feedparser
import requests
from bs4 import BeautifulSoup

from .base_collector import BaseCollector

logger = logging.getLogger(__name__)

TRENDS_URL = "https://trends24.in/united-states/"
GOOGLE_NEWS_RSS = "https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
REQUEST_TIMEOUT = 15


class TwitterCollector(BaseCollector):
    """Collects trending topics from trends24.in (Twitter/X trends)."""

    def collect_topics(self) -> List[Dict]:
        try:
            response = requests.get(
                TRENDS_URL,
                timeout=REQUEST_TIMEOUT,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            response.raise_for_status()
        except Exception as e:
            logger.warning(f"Twitter trends fetch error: {e}")
            return []

        soup = BeautifulSoup(response.text, "html.parser")

        # Extract trend names from anchor tags inside trend cards
        trend_links = soup.select("ol li a")
        trend_names = []
        for a in trend_links:
            text = a.get_text(strip=True)
            if text and text not in trend_names:
                trend_names.append(text)
            if len(trend_names) >= 20:
                break

        topics = []
        for i, name in enumerate(trend_names):
            # Search Google News for a real article URL for this trending topic
            article_url = self._find_article_url(name)

            raw = {
                "title": name,
                "url": article_url,
                "source": "twitter",
                "score": 2.5,
                "rank": i + 1,
                "published_at": "",
            }
            enriched = self.enrich_topic(raw)
            topics.append(enriched)

        return topics

    @staticmethod
    def _find_article_url(topic_name: str) -> str:
        """Search Google News RSS for a real article URL matching the topic."""
        try:
            query = quote_plus(topic_name)
            feed_url = GOOGLE_NEWS_RSS.format(query=query)
            feed = feedparser.parse(feed_url)
            for entry in feed.entries[:3]:
                link = getattr(entry, "link", "")
                if link:
                    return link
        except Exception as exc:
            logger.debug("Google News search failed for '%s': %s", topic_name, exc)
        return ""
