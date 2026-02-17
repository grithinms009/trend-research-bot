from __future__ import annotations

import logging
from typing import List

from app.content_intelligence.models.topic import TopicCandidate

logger = logging.getLogger(__name__)


class QualityFilterService:
    def __init__(self, min_keyword_count: int = 3, min_source_urls: int = 1):
        self.min_keyword_count = min_keyword_count
        self.min_source_urls = min_source_urls

    def filter(self, candidates: List[TopicCandidate]) -> List[TopicCandidate]:
        filtered: List[TopicCandidate] = []
        for candidate in candidates:
            if len(candidate.keywords) < self.min_keyword_count:
                continue
            if len(candidate.source_urls) < self.min_source_urls:
                continue
            filtered.append(candidate)
        logger.info("Quality filter kept %d/%d candidates", len(filtered), len(candidates))
        return filtered
