import os
import json
import glob
from datetime import datetime

class TopicCluster:
    def __init__(self):
        pass

    def cluster(self, topics):
        print("Clustering topics...")
        # Simple clustering: group by source for now, or just make one big cluster for testing
        clusters = []
        
        # Create a dummy cluster with all topics
        cluster = {
            "id": "cluster_001",
            "keywords": ["general", "news"],
            "trend_score": sum(t.get("score", 1.0) for t in topics) / max(1, len(topics)) * 10,
            "size": len(topics),
            "topics": topics
        }
        clusters.append(cluster)
        return clusters

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    DATA_DIR = os.path.join(BASE_DIR, "data", "topics_analyzed")
    CLUSTER_DIR = os.path.join(BASE_DIR, "data", "topic_clusters")
    os.makedirs(CLUSTER_DIR, exist_ok=True)
    
    files = sorted(glob.glob(f"{DATA_DIR}/*.json"))
    if not files:
        print("No analyzed topics found.")
        exit(0)
        
    latest_file = files[-1]
    with open(latest_file) as f:
        topics = json.load(f)
        
    clusterer = TopicCluster()
    results = clusterer.cluster(topics)
    
    outfile = os.path.join(CLUSTER_DIR, f"{datetime.now().strftime('%Y%m%d_%H%M')}.json")
    with open(outfile, "w") as f:
        json.dump(results, f, indent=2)
    print(f"Topic clusters saved to: {outfile}")