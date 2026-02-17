"""
Base collector with shared article enrichment logic.

Uses newspaper3k for article extraction, yake for keyword extraction,
and the channel classifier for automatic channel tagging.
"""

import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Dict, List, Optional

import yake
from newspaper import Article

from .channel_classifier import classify_channel

logger = logging.getLogger(__name__)

# YAKE keyword extractor (configured once, reused)
_kw_extractor = yake.KeywordExtractor(
    lan="en",
    n=2,           # max n-gram size
    top=12,        # extract up to 12 keywords
    dedupLim=0.5,  # dedup threshold
)


class BaseCollector(ABC):
    """Abstract base class for all topic collectors."""

    @abstractmethod
    def collect_topics(self) -> List[Dict]:
        """Fetch and return a list of enriched topic dicts."""
        ...

    # ------------------------------------------------------------------ #
    #  Enrichment helpers                                                  #
    # ------------------------------------------------------------------ #

    def enrich_topic(self, raw: dict) -> dict:
        """
        Take a raw topic dict (must have 'title', 'source', 'score', 'rank')
        and enrich it with article text, summary, keywords, and channel tag.
        """
        url = raw.get("url", "")
        title = raw.get("title", "")
        article_text = ""
        published_at = raw.get("published_at", "")
        summary = ""

        # --- Attempt full article download via newspaper3k ---
        if url:
            try:
                article = Article(url)
                article.download()
                article.parse()
                article.nlp()  # enables summary + keywords

                article_text = article.text or ""
                summary = article.summary or ""
                if article.publish_date:
                    published_at = article.publish_date.isoformat()
            except Exception as e:
                logger.debug(f"Article fetch failed for {url}: {e}")

        # --- Fallback: generate summary from title if article fetch failed ---
        if not summary:
            summary = title

        # --- Keyword extraction ---
        text_for_keywords = article_text if article_text else title
        extracted_keywords = self._extract_keywords(text_for_keywords)

        # --- Channel classification ---
        channel_tag = classify_channel(title, extracted_keywords)

        return {
            "title": title,
            "source": raw.get("source", ""),
            "url": url,
            "published_at": published_at,
            "article_text": article_text,
            "summary": self._trim_summary(summary),
            "keywords": extracted_keywords,
            "channel": channel_tag,
            "collected_at": datetime.now(timezone.utc).isoformat(),
            "score": float(raw.get("score", 1.0)),
            "rank": raw.get("rank", 0),
        }

    @staticmethod
    def _extract_keywords(text: str) -> List[str]:
        """Extract 8–12 keywords using YAKE."""
        if not text or len(text.strip()) < 10:
            return []
        try:
            kw_pairs = _kw_extractor.extract_keywords(text)
            # yake returns list of (keyword, score); lower score = more relevant
            keywords = [kw for kw, _score in kw_pairs]
            return keywords[:12]
        except Exception:
            return []

    @staticmethod
    def _trim_summary(text: str, max_sentences: int = 4) -> str:
        """Trim summary to at most max_sentences sentences."""
        if not text:
            return ""
        sentences = text.replace("\n", " ").split(". ")
        trimmed = ". ".join(sentences[:max_sentences])
        if not trimmed.endswith("."):
            trimmed += "."
        return trimmed
