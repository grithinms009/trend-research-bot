"""
Topic Content Validator — balanced validation for production pipeline.

Rejects only:
- Articles with < 400 characters
- Cookie-wall/boilerplate content (banned phrases repeated 3+ times)
- Duplicate content

Does NOT reject normal news content with political names or keywords.
"""

import json
import glob
import logging
import os
from datetime import datetime
from typing import List, Dict

logger = logging.getLogger(__name__)

MIN_ARTICLE_LENGTH = 400

# Only reject if these phrases appear 3+ times (cookie walls repeat, real articles don't)
COOKIE_WALL_PHRASES = [
    "accept all",
    "reject all",
    "cookie policy",
    "privacy policy",
    "g.co/privacytools",
    "cookies and data",
    "manage preferences",
    "consent to cookies",
]


class TopicContentValidator:
    def __init__(self):
        self.metrics = {
            "topics_received": 0,
            "topics_retained": 0,
            "topics_rejected_short": 0,
            "topics_rejected_cookie": 0,
            "topics_rejected_duplicate": 0,
        }
        self._seen_titles = set()

    def validate(self, topics: List[Dict]) -> List[Dict]:
        valid = []
        for topic in topics or []:
            self.metrics["topics_received"] += 1
            article = (topic.get("article_text") or topic.get("content") or "").strip()
            title = (topic.get("title") or "unknown").strip()
            lower_article = article.lower()

            # 1. Article length check
            if len(article) < MIN_ARTICLE_LENGTH:
                self.metrics["topics_rejected_short"] += 1
                logger.warning(
                    "Validator: '%s' rejected — %d chars (min %d)",
                    title[:60], len(article), MIN_ARTICLE_LENGTH,
                )
                continue

            # 2. Cookie-wall check — only reject if phrases appear 3+ times total
            cookie_hits = sum(
                lower_article.count(phrase)
                for phrase in COOKIE_WALL_PHRASES
            )
            if cookie_hits >= 3:
                self.metrics["topics_rejected_cookie"] += 1
                logger.warning(
                    "Validator: '%s' rejected — cookie/boilerplate content (%d hits)",
                    title[:60], cookie_hits,
                )
                continue

            # 3. Duplicate title check
            title_key = title.lower().strip()
            if title_key in self._seen_titles:
                self.metrics["topics_rejected_duplicate"] += 1
                logger.warning("Validator: '%s' rejected — duplicate title", title[:60])
                continue
            self._seen_titles.add(title_key)

            valid.append(topic)

        self.metrics["topics_retained"] = len(valid)
        return valid

    def log_metrics(self):
        print("\n--- Content Validator Metrics ---")
        for key, value in self.metrics.items():
            print(f"{key}: {value}")

        received = self.metrics["topics_received"]
        retained = self.metrics["topics_retained"]
        if received > 0:
            rate = (retained / received) * 100
            print(f"validation_success_rate: {rate:.1f}%")
        print("-------------------------------\n")


def _load_latest_topics(input_dir: str) -> List[Dict]:
    files = sorted(glob.glob(f"{input_dir}/*.json"))
    if not files:
        print("No analyzed topics found")
        return []

    latest_file = files[-1]
    with open(latest_file) as f:
        try:
            topics = json.load(f)
        except json.JSONDecodeError:
            print(f"Invalid JSON in {latest_file}")
            return []

    print(f"Validating topics from: {latest_file}")
    return topics


def _write_valid_topics(output_dir: str, topics: List[Dict]):
    os.makedirs(output_dir, exist_ok=True)
    outfile = os.path.join(output_dir, f"{datetime.now().strftime('%Y%m%d_%H%M')}.json")
    with open(outfile, "w") as f:
        json.dump(topics, f, indent=2)
    print(f"Validated topics saved to: {outfile}")


def main():
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    input_dir = os.path.join(base_dir, "data", "topics_analyzed")
    output_dir = os.path.join(base_dir, "data", "topics_validated")

    topics = _load_latest_topics(input_dir)
    if not topics:
        return

    validator = TopicContentValidator()
    valid_topics = validator.validate(topics)
    validator.log_metrics()

    _write_valid_topics(output_dir, valid_topics)
    print(f"Validated topics: {len(valid_topics)} / {len(topics)}")


if __name__ == "__main__":
    main()
