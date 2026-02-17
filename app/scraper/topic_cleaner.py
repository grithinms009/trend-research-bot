import os
import json
import glob
from datetime import datetime

class TopicCleaner:
    def __init__(self):
        pass

    def clean(self, topics):
        print("Cleaning topics...")
        # Placeholder: deduplicate or format
        cleaned = []
        seen_titles = set()
        for t in topics:
            if t["title"] not in seen_titles:
                t["cleaned_at"] = datetime.now().isoformat()
                cleaned.append(t)
                seen_titles.add(t["title"])
        return cleaned

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