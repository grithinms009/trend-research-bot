import json
import glob
import os
from datetime import datetime
from typing import List, Dict

MIN_ARTICLE_LENGTH = 200
BANNED_PATTERNS = [
    "accept all",
    "reject all",
    "privacytools",
    "cookies and data",
]


class TopicContentValidator:
    def __init__(self):
        self.metrics = {
            "topics_received": 0,
            "topics_retained": 0,
            "topics_rejected_short": 0,
            "topics_rejected_banned": 0,
        }

    def validate(self, topics: List[Dict]) -> List[Dict]:
        valid = []
        for topic in topics or []:
            self.metrics["topics_received"] += 1
            article = (topic.get("article_text") or topic.get("content") or "").strip()
            lower_article = article.lower()

            if len(lower_article) < MIN_ARTICLE_LENGTH:
                self.metrics["topics_rejected_short"] += 1
                continue

            if any(pattern in lower_article for pattern in BANNED_PATTERNS):
                self.metrics["topics_rejected_banned"] += 1
                continue

            valid.append(topic)

        self.metrics["topics_retained"] = len(valid)
        return valid

    def log_metrics(self):
        print("\n--- Content Validator Metrics ---")
        for key, value in self.metrics.items():
            print(f"{key}: {value}")
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
