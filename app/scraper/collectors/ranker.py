"""
Topic ranker — merges duplicate topics and ranks by score.
"""

from typing import Any, Dict, List


def rank_topics(topics: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Merge topics by normalized title, sum scores, collect sources,
    preserve enriched fields from the highest-scoring entry, and
    return the top 50 ranked topics.
    """
    merged: Dict[str, Dict[str, Any]] = {}

    for t in topics:
        title = t.get("title", "")
        if not title:
            continue

        key = title.lower().strip()
        score = float(t.get("score", 1.0))

        if key not in merged:
            # First occurrence — store the full enriched dict
            merged[key] = {
                **t,
                "score": 0.0,
                "sources": set(),
            }

        entry = merged[key]
        entry["score"] += score
        entry["sources"].add(t.get("source", ""))

        # Keep the richer version (longer article text wins)
        existing_text = len(entry.get("article_text", "") or "")
        new_text = len(t.get("article_text", "") or "")
        if new_text > existing_text:
            # Preserve accumulated score and sources
            saved_score = entry["score"]
            saved_sources = entry["sources"]
            entry.update(t)
            entry["score"] = saved_score
            entry["sources"] = saved_sources

    # Sort by descending score
    ranked: List[Dict[str, Any]] = sorted(
        merged.values(), key=lambda x: x["score"], reverse=True
    )

    # Convert source sets to lists for JSON serialization
    for r in ranked:
        r["sources"] = list(r["sources"])

    # Return top 50
    result: List[Dict[str, Any]] = ranked[:50]  # pyre-ignore[16]
    return result
