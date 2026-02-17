from __future__ import annotations

import hashlib
from typing import Dict, List


class CategoryRouter:
    def __init__(self, category_config: Dict[str, Dict[str, List[str]]]):
        self.category_config = category_config or {}
        self._normalized = {
            key: [kw.lower() for kw in values.get("keywords", [])]
            for key, values in self.category_config.items()
        }

    def route(self, text: str, fallback: str = "general") -> str:
        blob = text.lower()
        best_category = fallback
        best_score = -1

        for category, keywords in self._normalized.items():
            score = sum(1 for kw in keywords if kw in blob)
            if score > best_score:
                best_score = score
                best_category = category

        if best_score <= 0 and self.category_config:
            # Provide deterministic fallback using hash of text
            categories = sorted(self.category_config.keys())
            idx = int(hashlib.sha1(blob.encode("utf-8")).hexdigest(), 16) % len(categories)
            best_category = categories[idx]

        return best_category
