import hashlib
import logging
import re
from dataclasses import dataclass
from typing import Dict, Optional, Tuple
from urllib.parse import parse_qs, urlparse

import requests
from bs4 import BeautifulSoup

try:  # readability-lxml is preferred but optional during tests
    from readability import Document as ReadabilityDocument
except ImportError:  # pragma: no cover
    ReadabilityDocument = None  # type: ignore

logger = logging.getLogger(__name__)

USER_AGENT = "Mozilla/5.0 (compatible; TrendResearchBot/1.0; +https://trendbot.ai)"
REQUEST_TIMEOUT = 20

BANNED_PATTERNS = [
    "accept all",
    "reject all",
    "privacytools",
    "cookies and data",
    "g.co/privacytools",
]


@dataclass
class ArticleExtractionResult:
    url: str
    text: str
    summary: str
    word_count: int
    published_at: Optional[str] = None


class ArticleExtractor:
    MIN_WORD_COUNT = 400
    DUPLICATE_LINE_THRESHOLD = 0.3
    MIN_UNIQUE_WORD_RATIO = 0.5

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self.metrics: Dict[str, float] = {
            "articles_attempted": 0,
            "articles_fetched": 0,
            "articles_rejected_short": 0,
            "articles_rejected_banned": 0,
            "articles_rejected_duplicate": 0,
            "articles_rejected_repetition": 0,
            "articles_rejected_quality": 0,
            "articles_valid": 0,
            "total_word_count": 0,
        }
        self._seen_fingerprints: set[str] = set()

    def extract(self, url: str) -> Optional[ArticleExtractionResult]:
        if not url:
            return None

        self.metrics["articles_attempted"] += 1

        resolved_url = self._resolve_url(url)
        html = self._fetch_html(resolved_url)
        if not html:
            self.metrics["articles_rejected_quality"] += 1
            return None

        self.metrics["articles_fetched"] += 1

        text, summary = self._extract_text(html)
        if not text:
            self.metrics["articles_rejected_quality"] += 1
            return None

        text = self._clean_text(text)
        if not text:
            self.metrics["articles_rejected_quality"] += 1
            return None

        word_count = self._word_count(text)
        if word_count < self.MIN_WORD_COUNT:
            self.metrics["articles_rejected_short"] += 1
            logger.debug("Article rejected (short) %s — %d words", resolved_url, word_count)
            return None

        lowered = text.lower()
        if any(pattern in lowered for pattern in BANNED_PATTERNS):
            self.metrics["articles_rejected_banned"] += 1
            logger.debug("Article rejected (banned pattern) %s", resolved_url)
            return None

        if self._has_repeated_lines(text):
            self.metrics["articles_rejected_repetition"] += 1
            logger.debug("Article rejected (repeated lines) %s", resolved_url)
            return None

        if self._unique_word_ratio(text) < self.MIN_UNIQUE_WORD_RATIO:
            self.metrics["articles_rejected_repetition"] += 1
            logger.debug("Article rejected (low unique word ratio) %s", resolved_url)
            return None

        fingerprint = self._fingerprint(text)
        if fingerprint in self._seen_fingerprints:
            self.metrics["articles_rejected_duplicate"] += 1
            logger.debug("Article rejected (duplicate content) %s", resolved_url)
            return None
        self._seen_fingerprints.add(fingerprint)

        summary_text = summary or self._build_summary(text)
        self.metrics["articles_valid"] += 1
        self.metrics["total_word_count"] += word_count

        return ArticleExtractionResult(
            url=resolved_url,
            text=text,
            summary=summary_text,
            word_count=word_count,
        )

    def get_metrics(self) -> Dict[str, float]:
        return dict(self.metrics)

    def log_metrics(self) -> None:
        print("\n--- Article Extraction Metrics ---")
        for key, value in self.metrics.items():
            if key == "total_word_count":
                continue
            print(f"{key}: {int(value) if isinstance(value, (int, float)) else value}")
        valid = self.metrics.get("articles_valid", 0)
        if valid:
            avg_len = self.metrics["total_word_count"] / max(valid, 1)
            print(f"avg_article_word_count: {avg_len:.1f}")
        print("---------------------------------\n")

    # ------------------------
    # Internal helpers

    def _resolve_url(self, url: str) -> str:
        parsed = urlparse(url)
        if "news.google" not in parsed.netloc:
            return url

        try:
            resp = self.session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            if resp.url and "news.google" not in urlparse(resp.url).netloc:
                return resp.url

            redirected = self._extract_redirect_from_html(resp.text)
            return redirected or url
        except requests.RequestException:
            return url

    def _extract_redirect_from_html(self, html: str) -> Optional[str]:
        soup = BeautifulSoup(html, "html.parser")
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            target = self._parse_google_redirect(str(href))
            if target:
                return target
        for meta in soup.find_all("meta"):
            content = str(meta.get("content") or "")
            if content and "url=" in content:
                target = self._parse_google_redirect(content)
                if target:
                    return target
        return None

    @staticmethod
    def _parse_google_redirect(value: str) -> Optional[str]:
        parsed = urlparse(value)
        if parsed.netloc and "google" not in parsed.netloc:
            return value
        params = parse_qs(parsed.query)
        if "url" in params:
            return params["url"][0]
        return None

    def _fetch_html(self, url: str) -> Optional[str]:
        try:
            resp = self.session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as exc:
            logger.debug("Failed to fetch %s: %s", url, exc)
            return None

    def _extract_text(self, html: str) -> Tuple[str, str]:
        if ReadabilityDocument:
            try:
                doc = ReadabilityDocument(html)
                summary_html = doc.summary(html_partial=True)
                text = self._html_to_text(summary_html)
                summary = doc.short_title() or ""
                if text and len(text.split()) >= self.MIN_WORD_COUNT:
                    return text, summary
            except Exception:
                pass

        # Fallback: manual paragraph aggregation
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "noscript", "header", "footer", "form", "nav", "aside"]):
            tag.decompose()
        paragraphs = [p.get_text(" ", strip=True) for p in soup.find_all("p")]
        filtered = [p for p in paragraphs if len(p.split()) >= 6]
        text = "\n\n".join(filtered)
        return text, ""

    @staticmethod
    def _html_to_text(html: str) -> str:
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "noscript", "header", "footer", "form", "nav", "aside"]):
            tag.decompose()
        paragraphs = [p.get_text(" ", strip=True) for p in soup.find_all("p")]
        return "\n\n".join(paragraphs)

    @staticmethod
    def _clean_text(text: str) -> str:
        lines = []
        for raw_line in text.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            lower = line.lower()
            if any(pattern in lower for pattern in BANNED_PATTERNS):
                continue
            lines.append(line)
        cleaned = "\n\n".join(lines)
        return re.sub(r"\s+", " ", cleaned).strip()

    @staticmethod
    def _word_count(text: str) -> int:
        return len(re.findall(r"\w+", text))

    def _has_repeated_lines(self, text: str) -> bool:
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        if not lines:
            return True
        unique = len(set(lines))
        repetition_ratio = 1 - (unique / len(lines))
        return repetition_ratio >= self.DUPLICATE_LINE_THRESHOLD

    @staticmethod
    def _unique_word_ratio(text: str) -> float:
        words = [w.lower() for w in re.findall(r"\w+", text)]
        if not words:
            return 0.0
        return len(set(words)) / len(words)

    @staticmethod
    def _fingerprint(text: str) -> str:
        normalized = text[:5000].encode("utf-8", "ignore")
        return hashlib.sha1(normalized).hexdigest()

    @staticmethod
    def _build_summary(text: str, max_sentences: int = 3) -> str:
        sentences = re.split(r"(?<=[.!?])\s+", text)
        snippet = " ".join(sentences[:max_sentences])
        return snippet.strip()
