import json
import glob
import os
from datetime import datetime, timezone

# ---------------- CONFIG ----------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(BASE_DIR, "data")
CLUSTER_DIR = os.path.join(DATA_DIR, "topic_clusters")
QUEUE_DIR = os.path.join(DATA_DIR, "topic_queue")

# weights for priority scoring
TREND_WEIGHT = 1.0
FRESHNESS_WEIGHT = 0.5  # newer topics get a boost
COMPETITION_WEIGHT = -0.3  # optional placeholder if you add competition scoring

# ensure output folder exists
os.makedirs(QUEUE_DIR, exist_ok=True)


def compute_priority(cluster):
    # Base: trend_score
    score = cluster.get("trend_score", 0) * TREND_WEIGHT

    # Freshness: newest topic in cluster
    topic_times = []
    for t in cluster.get("topics", []):
        if "cleaned_at" in t:
            try:
                dt = datetime.fromisoformat(t["cleaned_at"])
                # Ensure timezone-aware (assume UTC if naive)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                topic_times.append(dt)
            except (ValueError, TypeError):
                pass

    if topic_times:
        newest = max(topic_times)
        age_hours = (datetime.now(timezone.utc) - newest).total_seconds() / 3600
        freshness_score = max(0.0, 24 - age_hours) / 24  # 0-1 scale, more recent = higher
        score += freshness_score * FRESHNESS_WEIGHT

    # Placeholder for competition scoring (if you have external data)
    # score += cluster.get("competition_score", 0) * COMPETITION_WEIGHT

    return score


if __name__ == "__main__":
    # ---------------- LOAD LATEST CLUSTERS ----------------
    files = sorted(glob.glob(f"{CLUSTER_DIR}/*.json"))
    if not files:
        print("No clusters found. Run topic_cluster.py first.")
        exit(1)

    latest_file = files[-1]
    with open(latest_file) as f:
        clusters = json.load(f)

    # ---------------- COMPUTE PRIORITIES ----------------
    for c in clusters:
        c["priority_score"] = compute_priority(c)

    # ---------------- SORT AND QUEUE ----------------
    clusters_sorted = sorted(clusters, key=lambda x: x["priority_score"], reverse=True)

    # Save queue
    outfile = os.path.join(QUEUE_DIR, f"{datetime.now().strftime('%Y%m%d_%H%M')}.json")
    with open(outfile, "w") as f:
        json.dump(clusters_sorted, f, indent=2)

    print("topic queue saved:", outfile)