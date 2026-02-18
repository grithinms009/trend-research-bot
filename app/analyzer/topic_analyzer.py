import os
import json
import glob
import time
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class TopicAnalyzer:
    def __init__(self):
        self.metrics = {
            "input_count": 0,
            "output_count": 0,
            "execution_time": 0.0,
        }

    def analyze(self, topics):
        start = time.time()
        print("Analyzing topics...")
        self.metrics["input_count"] = len(topics)

        if not topics:
            print("WARNING: No topics to analyze!")
            return []

        analyzed = []
        for t in topics:
            t["analyzed_at"] = datetime.now().isoformat()
            # Tag based on source and existing keywords
            tags = set()
            source = t.get("source", "")
            if source:
                tags.add(source)
            keywords = t.get("keywords", [])
            if keywords:
                tags.add("has_keywords")
            if t.get("has_article"):
                tags.add("has_article")
            channel = t.get("channel", "")
            if channel:
                tags.add(channel)
            t["tags"] = list(tags)
            analyzed.append(t)

        self.metrics["output_count"] = len(analyzed)
        self.metrics["execution_time"] = round(time.time() - start, 2)
        self._log_metrics()
        return analyzed

    def _log_metrics(self):
        print("\n--- Analyzer Metrics ---")
        for key, value in self.metrics.items():
            print(f"{key}: {value}")
        print("-----------------------\n")


if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    DATA_DIR = os.path.join(BASE_DIR, "data", "topics_clean")
    ANALYZED_DIR = os.path.join(BASE_DIR, "data", "topics_analyzed")
    os.makedirs(ANALYZED_DIR, exist_ok=True)
    
    files = sorted(glob.glob(f"{DATA_DIR}/*.json"))
    if not files:
        print("ERROR: No cleaned topics found. Run topic_cleaner.py first.")
        exit(1)
        
    latest_file = files[-1]
    print(f"Reading cleaned topics from: {latest_file}")
    with open(latest_file) as f:
        topics = json.load(f)
    
    print(f"Loaded {len(topics)} cleaned topics for analysis")
    
    analyzer = TopicAnalyzer()
    results = analyzer.analyze(topics)
    
    if not results:
        print("WARNING: Analyzer produced 0 results!")
    
    outfile = os.path.join(ANALYZED_DIR, f"{datetime.now().strftime('%Y%m%d_%H%M')}.json")
    with open(outfile, "w") as f:
        json.dump(results, f, indent=2)
    print(f"Analyzed topics saved to: {outfile} ({len(results)} topics)")