import os
import json
import glob
from datetime import datetime

class TopicAnalyzer:
    def __init__(self):
        pass

    def analyze(self, topics):
        print("Analyzing topics...")
        analyzed = []
        for t in topics:
            t["analyzed_at"] = datetime.now().isoformat()
            t["tags"] = ["news"] # Placeholder
            analyzed.append(t)
        return analyzed

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    DATA_DIR = os.path.join(BASE_DIR, "data", "topics_clean")
    ANALYZED_DIR = os.path.join(BASE_DIR, "data", "topics_analyzed")
    os.makedirs(ANALYZED_DIR, exist_ok=True)
    
    files = sorted(glob.glob(f"{DATA_DIR}/*.json"))
    if not files:
        print("No cleaned topics found.")
        exit(0)
        
    latest_file = files[-1]
    with open(latest_file) as f:
        topics = json.load(f)
        
    analyzer = TopicAnalyzer()
    results = analyzer.analyze(topics)
    
    outfile = os.path.join(ANALYZED_DIR, f"{datetime.now().strftime('%Y%m%d_%H%M')}.json")
    with open(outfile, "w") as f:
        json.dump(results, f, indent=2)
    print(f"Analyzed topics saved to: {outfile}")