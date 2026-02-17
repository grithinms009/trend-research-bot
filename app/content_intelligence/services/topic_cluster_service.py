from __future__ import annotations

import itertools
import logging
from collections import defaultdict
from typing import Any, Dict, List

from app.content_intelligence.models.topic import TopicCandidate

logger = logging.getLogger(__name__)


def _cluster_key(keywords: List[str]) -> str:
    return "-".join(sorted(keywords[:6]))


class TopicClusterService:
    """Groups candidates into merged topics to prevent duplicates."""

    def __init__(self, max_candidates: int = 250) -> None:
        self.max_candidates = max_candidates

    def cluster(self, candidates: List[TopicCandidate]) -> List[TopicCandidate]:
        clusters: Dict[str, Dict[str, Any]] = defaultdict(
            lambda: {
                "keywords": [],
                "source_urls": set(),
                "titles": [],
                "metadata": {
                    "raw_metadata": [],
                    "summaries": [],
                    "source_counts": defaultdict(int),
                    "latest_published": None,
                },
            }
        )

        for candidate in itertools.islice(candidates, self.max_candidates):
            key = _cluster_key(candidate.keywords)
            cluster = clusters[key]
            cluster["keywords"].extend(candidate.keywords)
            cluster["source_urls"].update(candidate.source_urls)
            cluster["titles"].append(candidate.topic_title)
            cluster["metadata"]["summaries"].append(candidate.metadata.get("summary", ""))
            raw_meta = candidate.metadata.get("raw_metadata")
            if isinstance(raw_meta, dict):
                cluster["metadata"]["raw_metadata"].append(raw_meta)
                source_type = raw_meta.get("source_type")
                if source_type:
                    cluster["metadata"]["source_counts"][source_type] += 1
                published = raw_meta.get("published")
                if published:
                    latest = cluster["metadata"].get("latest_published")
                    cluster["metadata"]["latest_published"] = max(latest or published, published)
            cluster["metadata"]["category_hint"] = candidate.category
            cluster["metadata"]["topic_ids"] = cluster["metadata"].get("topic_ids", []) + [candidate.topic_id]
            cluster["category"] = candidate.category
            cluster["topic_id"] = candidate.topic_id

        merged: List[TopicCandidate] = []
        for key, data in clusters.items():
            keywords = list(dict.fromkeys(data["keywords"]))[:12]
            source_urls = sorted(data["source_urls"])
            title = data["titles"][0]
            merged.append(
                TopicCandidate(
                    topic_id=f"{key}-{len(merged)}",
                    topic_title=title,
                    keywords=keywords,
                    source_urls=source_urls,
                    category=data["metadata"].get("category_hint", "general"),
                    metadata=data["metadata"],
                )
            )

        logger.info("Clustered %d candidates into %d merged topics", len(candidates), len(merged))
        return merged
