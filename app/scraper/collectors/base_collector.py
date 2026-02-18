"""
Base collector with shared article enrichment logic.

Uses a multi-tier extraction fallback chain:
  1. newspaper3k (primary)
  2. trafilatura (if available)
  3. requests + BeautifulSoup paragraph aggregation
  4. Reddit: extract linked article URL from summary HTML

Also provides keyword extraction (via YAKE) and channel classification.
"""

import logging
import re
from abc import ABC, abstractmethod
from datetime import datetime, timezone, date
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests as http_requests
from bs4 import BeautifulSoup
import yake
from newspaper import Article

try:  # Optional heavy dependency for fallback extraction
    import trafilatura  # type: ignore[import]
except ImportError:  # pragma: no cover - optional
    trafilatura = None  # type: ignore[assignment]

from .channel_classifier import classify_channel

logger = logging.getLogger(__name__)

# Minimum article length to consider valid
MIN_ARTICLE_CHARS = 300
REQUEST_TIMEOUT = 15

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


def extract_urls_from_html(html_text: str) -> List[str]:
    """Extract all href URLs from HTML content (e.g. Reddit summary_html)."""
    if not html_text:
        return []
    soup = BeautifulSoup(html_text, "html.parser")
    urls = []
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        if href and not href.startswith("#"):
            parsed = urlparse(href)
            if parsed.scheme in ("http", "https"):
                domain = parsed.netloc.lower()
                if not any(skip in domain for skip in ["reddit.com", "imgur.com", "i.redd.it", "redd.it"]):
                    urls.append(href)
    return urls


class BaseCollector(ABC):
    """Abstract base class for all topic collectors."""

    @abstractmethod
    def collect_topics(self) -> List[Dict]:
        """Fetch and return a list of enriched topic dicts."""
        ...

    def enrich_topic(self, raw: dict) -> dict:
        """
        Take a raw topic dict (must have 'title', 'source', 'score', 'rank')
        and enrich it with article text, summary, keywords, and channel tag.
        """
        url = raw.get("url", "")
        title = raw.get("title", "")
        source = raw.get("source", "")
        published_at = raw.get("published_at", "")

        summary_html = raw.get("summary_html") or raw.get("description_html")
        content = ""
        has_article = False
        summary_text = ""
        extraction_method = "none"

        # Step 1: If Reddit, try extracting real article URL from HTML
        extra_urls: List[str] = []
        if summary_html:
            plain_text = clean_html(summary_html)
            if source == "reddit":
                extra_urls = extract_urls_from_html(summary_html)
                if len(plain_text) >= MIN_ARTICLE_CHARS:
                    content = plain_text
                    summary_text = plain_text
                    extraction_method = "html_content"
            else:
                content = plain_text
                summary_text = plain_text
                if len(plain_text) >= MIN_ARTICLE_CHARS:
                    extraction_method = "html_content"

        # Step 2: Try extracting from primary URL
        if len(content) < MIN_ARTICLE_CHARS and url:
            extracted_text, extracted_summary, extracted_published, method = self._extract_article_chain(url)
            if extracted_text and len(extracted_text) > len(content):
                content = extracted_text
                summary_text = extracted_summary or summary_text
                published_at = extracted_published or published_at
                extraction_method = method

        # Step 3: Try Reddit linked URLs
        if len(content) < MIN_ARTICLE_CHARS and extra_urls:
            for linked_url in extra_urls[:3]:
                extracted_text, extracted_summary, extracted_published, method = self._extract_article_chain(linked_url)
                if extracted_text and len(extracted_text) > len(content):
                    content = extracted_text
                    summary_text = extracted_summary or summary_text
                    published_at = extracted_published or published_at
                    url = linked_url
                    extraction_method = method
                    if len(content) >= MIN_ARTICLE_CHARS:
                        break

        has_article = len(content) >= MIN_ARTICLE_CHARS

        if not summary_text:
            summary_text = title

        text_for_keywords = content if content else title
        extracted_keywords = self._extract_keywords(text_for_keywords)

        channel_tag = classify_channel(title, extracted_keywords)

        logger.info(
            "Enriched topic '%s': %d chars, method=%s, has_article=%s",
            title[:60], len(content), extraction_method, has_article,
        )

        return {
            "title": title,
            "source": source,
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
            "extraction_method": extraction_method,
            "article_length": len(content),
        }

    @staticmethod
    def _extract_article_chain(url: str) -> Tuple[str, str, str, str]:
        """
        Multi-tier extraction fallback chain:
          1. newspaper3k
          2. trafilatura
          3. requests + BeautifulSoup <p> aggregation
        
        Returns: (article_text, summary, published_at, method_name)
        """
        article_text = ""
        summary = ""
        published_at = ""
        method = "none"

        # Tier 1: newspaper3k
        try:
            article = Article(url)
            article.download()
            article.parse()
            article_text = (article.text or "").strip()
            if article.publish_date:
                published_at = BaseCollector._safe_isoformat(article.publish_date)
            try:
                article.nlp()
                summary = (article.summary or "").strip()
            except Exception:
                pass
            if len(article_text) >= MIN_ARTICLE_CHARS:
                method = "newspaper3k"
        except Exception as exc:
            logger.debug("newspaper3k failed for %s: %s", url, exc)

        # Tier 2: trafilatura
        if len(article_text) < MIN_ARTICLE_CHARS and trafilatura:
            try:
                downloaded = trafilatura.fetch_url(url)
                extracted = trafilatura.extract(downloaded) if downloaded else ""
                if extracted and len(extracted) > len(article_text):
                    article_text = extracted.strip()
                    if len(article_text) >= MIN_ARTICLE_CHARS:
                        method = "trafilatura"
            except Exception as exc:
                logger.debug("trafilatura failed for %s: %s", url, exc)

        # Tier 3: requests + BeautifulSoup paragraph aggregation
        if len(article_text) < MIN_ARTICLE_CHARS:
            try:
                resp = http_requests.get(
                    url,
                    timeout=REQUEST_TIMEOUT,
                    headers={"User-Agent": "Mozilla/5.0 (compatible; TrendBot/1.0)"},
                )
                resp.raise_for_status()
                soup = BeautifulSoup(resp.text, "html.parser")
                
                for tag in soup(["script", "style", "nav", "footer", "header", "aside"]):
                    tag.decompose()
                
                paragraphs = soup.find_all("p")
                p_texts = []
                for p in paragraphs:
                    text = p.get_text(strip=True)
                    if len(text) > 40:
                        p_texts.append(text)
                
                bs_text = "\n\n".join(p_texts)
                if len(bs_text) > len(article_text):
                    article_text = bs_text.strip()
                    if len(article_text) >= MIN_ARTICLE_CHARS:
                        method = "bs4_paragraphs"
            except Exception as exc:
                logger.debug("BS4 paragraph extraction failed for %s: %s", url, exc)

        return article_text, summary, published_at, method

    @staticmethod
    def _extract_keywords(text: str) -> List[str]:
        """Extract 8-12 keywords using YAKE."""
        if not text or len(text.strip()) < 10:
            return []
        try:
            kw_pairs = _kw_extractor.extract_keywords(text)
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
