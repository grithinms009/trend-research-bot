from __future__ import annotations

import hashlib
import logging
from typing import Iterable, List

from app.content_intelligence.models.topic import RawTopic, TopicCandidate
from app.content_intelligence.utils.category import CategoryRouter
from app.content_intelligence.utils.text import extract_keywords, normalize_urls, slugify

logger = logging.getLogger(__name__)


class SourceFetchService:
    """Normalizes raw connector payloads into TopicCandidate objects."""

    def __init__(self, category_router: CategoryRouter, keyword_limit: int = 12) -> None:
        self.category_router = category_router
        self.keyword_limit = keyword_limit

    def build_candidates(self, raw_topics: Iterable[RawTopic]) -> List[TopicCandidate]:
        seen = set()
        candidates: List[TopicCandidate] = []

        for raw in raw_topics:
            text_blob = f"{raw.title}\n{raw.summary}"
            keywords = extract_keywords(text_blob, limit=self.keyword_limit)
            category = self.category_router.route(text_blob, fallback=raw.category_hint)
            urls = normalize_urls(raw.source_urls)
            topic_hash = hashlib.sha1(text_blob.encode("utf-8")).hexdigest()[:10]
            slug = slugify(raw.title, allow_unicode=False)
            topic_id = f"{slug}-{topic_hash}"

            if topic_id in seen:
                continue
            seen.add(topic_id)

            candidate = TopicCandidate(
                topic_id=topic_id,
                topic_title=raw.title,
                keywords=keywords,
                source_urls=urls,
                category=category,
                metadata={
                    "summary": raw.summary,
                    "raw_metadata": raw.metadata,
                },
            )
            candidates.append(candidate)
        logger.info("SourceFetchService normalized %d topics", len(candidates))
        return candidates
