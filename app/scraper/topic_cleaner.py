import os
import json
import glob
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class TopicCleaner:
    REQUIRED_FIELDS = ("url", "summary")

    def __init__(self):
        self.metrics = {
            "topics_received": 0,
            "topics_retained": 0,
            "topics_dropped_missing_content": 0,
        }

    def clean(self, topics):
        print("Cleaning topics...")
        cleaned = []
        seen_titles = set()

        for topic in topics or []:
            self.metrics["topics_received"] += 1
            if not self._is_valid(topic):
                self.metrics["topics_dropped_missing_content"] += 1
                logger.warning(
                    "Cleaner dropped topic '%s' — missing article_text/url/summary",
                    topic.get("title", "unknown"),
                )
                continue

            title = topic.get("title")
            if title in seen_titles:
                continue

            topic["cleaned_at"] = datetime.now().isoformat()
            cleaned.append(topic)
            seen_titles.add(title)

        self.metrics["topics_retained"] = len(cleaned)
        self._log_metrics()
        return cleaned

    def _is_valid(self, topic):
        if not isinstance(topic, dict):
            return False

        for field in self.REQUIRED_FIELDS:
            value = topic.get(field)
            if not value or not isinstance(value, str) or not value.strip():
                return False

        article = topic.get("article_text", "") or topic.get("content", "")
        if len(article.strip()) < 80:
            return False

        return True

    def _log_metrics(self):
        print("\n--- Cleaner Metrics ---")
        for key, value in self.metrics.items():
            print(f"{key}: {value}")
        print("----------------------\n")

if __name__ == "__main__":
    # Determine paths
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    DATA_DIR = os.path.join(BASE_DIR, "data", "topics")
    CLEAN_DIR = os.path.join(BASE_DIR, "data", "topics_clean")
    os.makedirs(CLEAN_DIR, exist_ok=True)
    
    # Load latest
    files = sorted(glob.glob(f"{DATA_DIR}/*.json"))
    if not files:
        print("No raw topics found.")
        exit(0)
        
    latest_file = files[-1]
    with open(latest_file) as f:
        topics = json.load(f)
        
    cleaner = TopicCleaner()
    cleaned_topics = cleaner.clean(topics)
    
    outfile = os.path.join(CLEAN_DIR, f"{datetime.now().strftime('%Y%m%d_%H%M')}.json")
    with open(outfile, "w") as f:
        json.dump(cleaned_topics, f, indent=2)
    print(f"Cleaned topics saved to: {outfile}")