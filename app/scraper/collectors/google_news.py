"""
Google News RSS collector — fetches trending articles per channel category.

Provides high-quality articles with real URLs for all 5 channels by
querying category-specific search terms via Google News RSS.
Resolves Google News redirect URLs to actual article URLs.
"""

import logging
import re
from typing import Dict, List
from urllib.parse import quote_plus, urlparse

import feedparser
import requests
from bs4 import BeautifulSoup
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
            # Try extracting real URL from RSS entry first (most reliable)
            url = self._extract_url_from_entry(entry) or ""
            if not url or "google" in urlparse(url).netloc.lower():
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
    def _extract_url_from_entry(entry) -> str:
        """Extract real article URL from RSS entry fields.
        
        Google News RSS entries contain the actual article URL in:
        1. entry.source.href — publisher link
        2. entry.summary / entry.description — HTML with <a href> to real article
        3. entry.links — list of alternate links
        """
        # Try entry.source.href (feedparser provides this for some feeds)
        source = getattr(entry, "source", None)
        if source:
            href = getattr(source, "href", "") or ""
            if href and "google" not in href.lower():
                return href

        # Try extracting from summary/description HTML
        for field in ["summary", "description"]:
            html = getattr(entry, field, "") or ""
            if html and "<a" in html.lower():
                soup = BeautifulSoup(html, "html.parser")
                for a_tag in soup.find_all("a", href=True):
                    href = a_tag["href"]
                    if isinstance(href, str) and href.startswith("http"):
                        parsed = urlparse(href)
                        domain = parsed.netloc.lower()
                        if "google" not in domain and "feedburner" not in domain:
                            return href

        # Try entry.links list
        links = getattr(entry, "links", []) or []
        for link in links:
            href = link.get("href", "") if isinstance(link, dict) else ""
            rel = link.get("rel", "") if isinstance(link, dict) else ""
            if href and rel != "self" and "google" not in href.lower():
                return href

        return ""

    @staticmethod
    def _resolve_google_url(google_url: str) -> str:
        """Follow Google News redirect to get the actual article URL.
        
        Google News uses JS-based redirects, so requests.head() won't work.
        We try requests.get() first, then parse HTML for canonical/og:url.
        """
        if "news.google.com" not in google_url:
            return google_url
        try:
            resp = requests.get(
                google_url,
                allow_redirects=True,
                timeout=REQUEST_TIMEOUT,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                },
            )
            final_url = resp.url

            # If we ended up at a real article, use it
            if "google.com" not in final_url and "google." not in final_url:
                return final_url

            # Still on Google — try parsing the page for the real article URL
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(resp.text, "html.parser")

            # Try canonical link
            canonical = soup.find("link", {"rel": "canonical"})
            if canonical and canonical.get("href"):
                href = canonical["href"]
                if "google.com" not in href:
                    return href

            # Try og:url meta tag
            og_url = soup.find("meta", {"property": "og:url"})
            if og_url and og_url.get("content"):
                content = og_url["content"]
                if "google.com" not in content:
                    return content

            # Try data-url or href attributes in article links
            for a_tag in soup.find_all("a", href=True):
                href = a_tag["href"]
                if href.startswith("http") and "google.com" not in href and "google." not in href:
                    return href

            # Last resort: try extracting from the URL path itself
            # Google News URLs sometimes embed the target: /articles/...
            if "/articles/" in google_url:
                # Can't extract from this format, return as-is
                pass

        except Exception as exc:
            logger.debug("Failed to resolve Google URL %s: %s", google_url[:60], exc)

        return google_url
