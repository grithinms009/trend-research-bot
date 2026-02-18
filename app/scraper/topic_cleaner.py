import os
import json
import glob
import logging
import statistics
from datetime import datetime

logger = logging.getLogger(__name__)

MIN_ARTICLE_CHARS = 300


class TopicCleaner:
    REQUIRED_FIELDS = ("title",)  # url checked separately; summary can be derived from title

    def __init__(self):
        self.metrics = {
            "topics_received": 0,
            "topics_retained": 0,
            "topics_dropped_no_title": 0,
            "topics_dropped_no_url": 0,
            "topics_dropped_short_article": 0,
            "topics_dropped_duplicate": 0,
        }
        self.article_lengths: list = []

    def clean(self, topics):
        print("Cleaning topics...")
        cleaned = []
        seen_titles = set()

        for topic in topics or []:
            self.metrics["topics_received"] += 1
            title = (topic.get("title") or "").strip()
            url = (topic.get("url") or topic.get("source_url") or "").strip()
            article_text = (topic.get("article_text") or topic.get("content") or "").strip()

            # Track article lengths for distribution logging
            self.article_lengths.append(len(article_text))

            # Validation gate 1: title must exist
            if not title:
                self.metrics["topics_dropped_no_title"] += 1
                logger.warning("Cleaner dropped topic — missing title")
                continue

            # Validation gate 2: URL must exist
            if not url:
                self.metrics["topics_dropped_no_url"] += 1
                logger.warning("Cleaner dropped topic '%s' — missing URL", title[:60])
                continue

            # Validation gate 3: article_text >= 300 chars
            if len(article_text) < MIN_ARTICLE_CHARS:
                self.metrics["topics_dropped_short_article"] += 1
                logger.warning(
                    "Cleaner dropped topic '%s' — article_text only %d chars (min %d)",
                    title[:60], len(article_text), MIN_ARTICLE_CHARS,
                )
                continue

            # Validation gate 4: deduplication
            title_key = title.lower()
            if title_key in seen_titles:
                self.metrics["topics_dropped_duplicate"] += 1
                continue

            topic["cleaned_at"] = datetime.now().isoformat()
            cleaned.append(topic)
            seen_titles.add(title_key)

        self.metrics["topics_retained"] = len(cleaned)
        self._log_metrics()
        return cleaned

    def _log_metrics(self):
        print("\n--- Cleaner Metrics ---")
        for key, value in self.metrics.items():
            print(f"{key}: {value}")

        # Article length distribution
        if self.article_lengths:
            print(f"\n--- Article Length Distribution ---")
            print(f"  total_articles: {len(self.article_lengths)}")
            print(f"  min_length: {min(self.article_lengths)}")
            print(f"  max_length: {max(self.article_lengths)}")
            print(f"  average_length: {statistics.mean(self.article_lengths):.0f}")
            print(f"  median_length: {statistics.median(self.article_lengths):.0f}")
            above_threshold = sum(1 for l in self.article_lengths if l >= MIN_ARTICLE_CHARS)
            print(f"  above_{MIN_ARTICLE_CHARS}_chars: {above_threshold}/{len(self.article_lengths)}")

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
    print(f"Reading topics from: {latest_file}")
    with open(latest_file) as f:
        topics = json.load(f)
    
    print(f"Loaded {len(topics)} topics")
    
    cleaner = TopicCleaner()
    cleaned_topics = cleaner.clean(topics)
    
    if not cleaned_topics:
        print("WARNING: Cleaner produced 0 topics! Check article extraction.")
    
    outfile = os.path.join(CLEAN_DIR, f"{datetime.now().strftime('%Y%m%d_%H%M')}.json")
    with open(outfile, "w") as f:
        json.dump(cleaned_topics, f, indent=2)
    print(f"Cleaned topics saved to: {outfile} ({len(cleaned_topics)} topics)")