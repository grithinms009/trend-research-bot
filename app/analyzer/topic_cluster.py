import os
import json
import glob
import logging
import time
from datetime import datetime
from typing import List, Dict, Any

import numpy as np
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

logger = logging.getLogger(__name__)


class TopicCluster:
    def __init__(self):
        self.vectorizer = TfidfVectorizer(stop_words="english")
        self.metrics = {
            "input_count": 0,
            "output_clusters": 0,
            "total_topics_clustered": 0,
            "orphan_count": 0,
            "execution_time": 0.0,
        }

    def cluster(self, topics: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        start = time.time()
        print("Clustering topics...")
        self.metrics["input_count"] = len(topics)

        titles = [t.get("title", "") for t in topics if t.get("title")]
        clusters: Dict[int, List[Dict[str, Any]]] = {}
        orphans: List[Dict[str, Any]] = []
        cluster_similarities: Dict[int, float] = {}

        if len(titles) < 3:
            # Edge case: too few topics for meaningful clustering
            print(f"Only {len(titles)} topics — skipping KMeans, creating single cluster")
            topics_with_titles = [t for t in topics if t.get("title")]
            orphans_list = [t for t in topics if not t.get("title")]
            if topics_with_titles:
                clusters[0] = topics_with_titles
                cluster_similarities[0] = 1.0  # trivial similarity
            orphans = orphans_list
        elif titles:
            matrix = self.vectorizer.fit_transform(titles)

            k = min(5, max(2, len(titles) // 3))
            model = KMeans(n_clusters=k, n_init=10, random_state=42)
            labels = model.fit_predict(matrix)

            # Compute intra-cluster similarity
            sim_matrix = cosine_similarity(matrix)

            title_idx = 0
            for topic in topics:
                if not topic.get("title"):
                    orphans.append(topic)
                    continue
                label = int(labels[title_idx])
                clusters.setdefault(label, []).append(topic)
                title_idx += 1

            # Compute average similarity per cluster
            for label in clusters:
                indices = [i for i, l in enumerate(labels) if l == label]
                if len(indices) > 1:
                    sims = []
                    for i in range(len(indices)):
                        for j in range(i + 1, len(indices)):
                            sims.append(sim_matrix[indices[i], indices[j]])
                    cluster_similarities[label] = float(np.mean(sims)) if sims else 0.0
                else:
                    cluster_similarities[label] = 1.0
        else:
            orphans = topics[:]

        if orphans:
            clusters.setdefault(-1, []).extend(orphans)
            self.metrics["orphan_count"] = len(orphans)

        # Build results with stable unique IDs
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results: List[Dict[str, Any]] = []
        
        print("\n--- Cluster Report ---")
        for label, grouped in sorted(clusters.items()):
            trend_score = sum(t.get("score", 1.0) for t in grouped)
            cluster_id = f"cluster_orphan_{timestamp}" if label == -1 else f"cluster_{label}_{timestamp}"
            avg_sim = cluster_similarities.get(label, 0.0)

            results.append({
                "id": cluster_id,
                "keywords": [],
                "trend_score": trend_score,
                "size": len(grouped),
                "avg_similarity": round(avg_sim, 4),
                "topics": grouped,
            })

            print(f"  {cluster_id}: {len(grouped)} topics, trend_score={trend_score:.2f}, avg_similarity={avg_sim:.4f}")
        
        print("---------------------\n")

        self.metrics["output_clusters"] = len(results)
        self.metrics["total_topics_clustered"] = sum(len(c.get("topics", [])) for c in results)
        self.metrics["execution_time"] = round(time.time() - start, 2)
        self._log_metrics()

        return results

    def _log_metrics(self):
        print("--- Cluster Metrics ---")
        for key, value in self.metrics.items():
            print(f"{key}: {value}")
        print("----------------------\n")

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    DATA_DIR = os.path.join(BASE_DIR, "data", "topics_analyzed")
    CLUSTER_DIR = os.path.join(BASE_DIR, "data", "topic_clusters")
    os.makedirs(CLUSTER_DIR, exist_ok=True)
    
    files = sorted(glob.glob(f"{DATA_DIR}/*.json"))
    if not files:
        print("ERROR: No analyzed topics found. Run topic_analyzer.py first.")
        exit(1)
        
    latest_file = files[-1]
    print(f"Reading analyzed topics from: {latest_file}")
    with open(latest_file) as f:
        topics = json.load(f)
    
    print(f"Loaded {len(topics)} topics for clustering")
    
    clusterer = TopicCluster()
    results = clusterer.cluster(topics)
    
    if not results:
        print("WARNING: Clustering produced 0 clusters!")
    
    outfile = os.path.join(CLUSTER_DIR, f"{datetime.now().strftime('%Y%m%d_%H%M')}.json")
    with open(outfile, "w") as f:
        json.dump(results, f, indent=2)
    print(f"Topic clusters saved to: {outfile}")