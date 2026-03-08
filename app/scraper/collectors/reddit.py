"""
Reddit collector — fetches top posts from r/worldnews RSS feed.
"""

import logging
from typing import Dict, List

import feedparser
from dateutil import parser as dateparser

from .base_collector import BaseCollector

logger = logging.getLogger(__name__)

# Each feed mapped to its intended channel (active: C1 + C5 only)
SUBREDDIT_FEEDS = [
    # C1 — AI / Tech News
    ("https://www.reddit.com/r/worldnews/.rss", "C1"),
    ("https://www.reddit.com/r/technology/.rss", "C1"),
    ("https://www.reddit.com/r/artificial/.rss", "C1"),
    ("https://www.reddit.com/r/machinelearning/.rss", "C1"),
    ("https://www.reddit.com/r/science/.rss", "C1"),
    ("https://www.reddit.com/r/Futurology/.rss", "C1"),
    ("https://www.reddit.com/r/singularity/.rss", "C1"),
    ("https://www.reddit.com/r/ChatGPT/.rss", "C1"),
    # C5 — Productivity / Life Hacks
    ("https://www.reddit.com/r/productivity/.rss", "C5"),
    ("https://www.reddit.com/r/getdisciplined/.rss", "C5"),
    ("https://www.reddit.com/r/selfimprovement/.rss", "C5"),
    ("https://www.reddit.com/r/LifeProTips/.rss", "C5"),
    ("https://www.reddit.com/r/Entrepreneur/.rss", "C5"),
    ("https://www.reddit.com/r/DecidingToBeBetter/.rss", "C5"),
    ("https://www.reddit.com/r/ZenHabits/.rss", "C5"),
    ("https://www.reddit.com/r/simpleliving/.rss", "C5"),
]

MAX_ENTRIES_PER_FEED = 15


class RedditCollector(BaseCollector):
    """Collects trending topics from Reddit RSS feeds."""

    def collect_topics(self) -> List[Dict]:
        all_topics = []

        for feed_url, channel_hint in SUBREDDIT_FEEDS:
            try:
                topics = self._fetch_feed(feed_url, channel_hint)
                all_topics.extend(topics)
            except Exception as e:
                logger.warning(f"Reddit feed error ({feed_url}): {e}")

        return all_topics

    def _fetch_feed(self, feed_url: str, channel_hint: str = "C1") -> List[Dict]:
        feed = feedparser.parse(feed_url)
        topics = []

        for i, entry in enumerate(feed.entries[:MAX_ENTRIES_PER_FEED]):
            # Parse published date
            published_at = ""
            if hasattr(entry, "published"):
                try:
                    published_at = dateparser.parse(entry.published).isoformat()
                except Exception:
                    published_at = entry.published

            summary_html = getattr(entry, "summary", "") or getattr(entry, "description", "")
            if not summary_html:
                content_list = getattr(entry, "content", [])
                if content_list:
                    summary_html = "\n".join(
                        c.value for c in content_list if hasattr(c, "value") and c.value
                    )

            raw = {
                "title": entry.title,
                "url": entry.link,
                "source": "reddit",
                "score": 1.2,
                "rank": i + 1,
                "published_at": published_at,
                "summary_html": summary_html,
            }

            enriched = self.enrich_topic(raw)

            # Respect channel hint from subreddit mapping
            if enriched.get("channel") == "C1" and channel_hint != "C1":
                enriched["channel"] = channel_hint

            topics.append(enriched)

        return topics
