"""
Twitter / X trends collector — scrapes trending topics from trends24.in.
"""

import logging
from typing import Dict, List

import requests
from bs4 import BeautifulSoup

from .base_collector import BaseCollector

logger = logging.getLogger(__name__)

TRENDS_URL = "https://trends24.in/united-states/"
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
            raw = {
                "title": name,
                "url": "",
                "source": "twitter",
                "score": 2.5,
                "rank": i + 1,
                "published_at": "",
            }
            enriched = self.enrich_topic(raw)
            topics.append(enriched)

        return topics
