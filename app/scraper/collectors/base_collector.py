"""
Base collector with shared article enrichment logic.

Uses newspaper3k for article extraction, yake for keyword extraction,
and the channel classifier for automatic channel tagging.
"""

import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone, date
from typing import Dict, List, Optional, Tuple

from bs4 import BeautifulSoup
import yake
from newspaper import Article

try:  # Optional heavy dependency for fallback extraction
    import trafilatura  # type: ignore[import]
except ImportError:  # pragma: no cover - optional
    trafilatura = None  # type: ignore[assignment]

from .channel_classifier import classify_channel

logger = logging.getLogger(__name__)

# YAKE keyword extractor (configured once, reused)
_kw_extractor = yake.KeywordExtractor(
    lan="en",
    n=2,
    top=12,
    dedupLim=0.5,
)


def clean_html(html_text: str) -> str:
    if not html_text:
        return ""
    soup = BeautifulSoup(html_text, "html.parser")
    text = soup.get_text(separator=" ")
    return " ".join(text.split())


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
        published_at = raw.get("published_at", "")

        summary_html = raw.get("summary_html") or raw.get("description_html")
        content = ""
        has_article = False
        summary_text = ""

        if summary_html:
            content = clean_html(summary_html)
            summary_text = content
            has_article = bool(content)
        elif url:
            content, summary_text, published_at = self._extract_article(url, published_at)
            has_article = bool(content)

        if not summary_text:
            summary_text = title

        text_for_keywords = content if content else title
        extracted_keywords = self._extract_keywords(text_for_keywords)

        # --- Channel classification ---
        channel_tag = classify_channel(title, extracted_keywords)

        return {
            "title": title,
            "source": raw.get("source", ""),
            "url": url,
            "published_at": published_at,
            "content": content,
            "article_text": content,
            "summary": self._trim_summary(summary_text),
            "keywords": extracted_keywords,
            "has_article": has_article,
            "channel": channel_tag,
            "collected_at": datetime.now(timezone.utc).isoformat(),
            "score": float(raw.get("score", 1.0)),
            "rank": raw.get("rank", 0),
        }

    @staticmethod
    def _extract_article(url: str, published_at: str) -> Tuple[str, str, str]:
        article_text = ""
        summary = ""
        updated_published = published_at

        try:
            article = Article(url)
            article.download()
            article.parse()
            article_text = (article.text or "").strip()
            if article.publish_date:
                updated_published = BaseCollector._safe_isoformat(article.publish_date)
            try:
                article.nlp()
                summary = (article.summary or "").strip()
            except Exception:
                summary = summary
        except Exception as exc:
            logger.debug(f"Article fetch failed for {url}: {exc}")

        if len(article_text) < 120 and trafilatura:
            try:
                downloaded = trafilatura.fetch_url(url)
                extracted = trafilatura.extract(downloaded) if downloaded else ""
                if extracted and len(extracted) > len(article_text):
                    article_text = extracted.strip()
            except Exception as exc:
                logger.debug(f"Trafilatura extraction failed for {url}: {exc}")

        return article_text, summary, updated_published

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

    @staticmethod
    def _safe_isoformat(value) -> str:
        if not value:
            return ""
        if isinstance(value, (datetime, date)):
            try:
                return value.isoformat()
            except Exception:
                return str(value)
        return str(value)
