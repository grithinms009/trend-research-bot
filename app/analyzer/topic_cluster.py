import os
import json
import glob
from datetime import datetime
from typing import List, Dict, Any

from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import TfidfVectorizer


class TopicCluster:
    def __init__(self):
        self.vectorizer = TfidfVectorizer(stop_words="english")

    def cluster(self, topics: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        print("Clustering topics...")
        titles = [t.get("title", "") for t in topics if t.get("title")]
        clusters: Dict[int, List[Dict[str, Any]]] = {}
        orphans: List[Dict[str, Any]] = []

        if titles:
            matrix = self.vectorizer.fit_transform(titles)

            k = min(5, max(1, len(titles) // 3))
            model = KMeans(n_clusters=k, n_init=10)
            labels = model.fit_predict(matrix)

            title_idx = 0
            for topic in topics:
                if not topic.get("title"):
                    orphans.append(topic)
                    continue
                label = int(labels[title_idx])
                clusters.setdefault(label, []).append(topic)
                title_idx += 1
        else:
            orphans = topics[:]

        if orphans:
            clusters.setdefault(-1, []).extend(orphans)

        results: List[Dict[str, Any]] = []
        for label, grouped in clusters.items():
            trend_score = sum(t.get("score", 1.0) for t in grouped)
            cluster_id = "cluster_orphan" if label == -1 else f"cluster_{label}"
            results.append({
                "id": cluster_id,
                "keywords": [],
                "trend_score": trend_score,
                "size": len(grouped),
                "topics": grouped,
            })

        return results

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