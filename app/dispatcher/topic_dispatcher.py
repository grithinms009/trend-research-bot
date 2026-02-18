import os
import json
import yaml
import glob
import logging
from datetime import datetime
from pathlib import Path

MIN_ARTICLE_CHARS = 300


class TopicDispatcher:
    """Reads prioritized topics from topic_queue/ and dispatches to channel-specific generation queues."""

    def __init__(self):
        # Load channel configuration
        base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        config_path = os.path.join(base_path, "app", "config", "channels.yaml")
        with open(config_path, "r") as f:
            config_data = yaml.safe_load(f) or {}

        if not isinstance(config_data, dict):
            raise ValueError("channels.yaml must contain a mapping at the root level")

        channels = config_data.get("channels")
        if not isinstance(channels, dict) or not channels:
            raise ValueError("Channel configuration missing 'channels' section in channels.yaml")

        self.channel_config = channels
        self.metrics = {
            "input_count": 0,
            "topics_received": 0,
            "topics_dispatched": 0,
            "topics_skipped_invalid": 0,
            "topics_skipped_no_channel": 0,
            "failure_reasons": [],
        }
        self.logger = logging.getLogger(__name__)

    def dispatch_by_channel(self, topics):
        """
        Group topics by channel and save to channel-specific generation queues.
        """
        base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        generated_base = os.path.join(base_path, "data", "topic_generated")
        
        counts = {cid: 0 for cid in self.channel_config.keys()}
        
        for topic in topics or []:
            self.metrics["topics_received"] += 1
            cid = topic.get("channel")
            
            if cid not in self.channel_config:
                self.metrics["topics_skipped_no_channel"] += 1
                self.logger.warning(
                    "Dispatcher skipped topic '%s' — channel '%s' not in config",
                    topic.get("title", "unknown")[:60], cid,
                )
                self.metrics["failure_reasons"].append(f"unknown_channel:{cid}")
                continue

            if not self._is_valid(topic):
                self.metrics["topics_skipped_invalid"] += 1
                reason = self._get_invalid_reason(topic)
                self.logger.warning(
                    "Dispatcher skipped topic '%s' — %s",
                    topic.get("title", "unknown")[:60], reason,
                )
                self.metrics["failure_reasons"].append(reason)
                continue

            channel_dir = os.path.join(generated_base, cid)
            os.makedirs(channel_dir, exist_ok=True)
            
            # Create generation request
            request = {
                "channel_id": cid,
                "channel_name": self.channel_config[cid]["name"],
                "topic": topic,
                "model": self.channel_config[cid]["model"],
                "tone": self.channel_config[cid]["tone"],
                "status": "pending_generation",
                "dispatched_at": datetime.now().isoformat()
            }
            
            # Unique filename for this topic
            timestamp = datetime.now().strftime("%H%M%S_%f")
            filename = f"req_{timestamp}.json"
            filepath = os.path.join(channel_dir, filename)
            
            with open(filepath, "w") as f:
                json.dump(request, f, indent=2)

            counts[cid] += 1
            self.metrics["topics_dispatched"] += 1

        self._log_metrics()
        return counts

    def _is_valid(self, topic):
        if not isinstance(topic, dict):
            return False

        title = (topic.get("title") or "").strip()
        if not title:
            return False

        url = (topic.get("url") or "").strip()
        if not url:
            return False

        article = (topic.get("article_text") or topic.get("content") or "").strip()
        if len(article) < MIN_ARTICLE_CHARS:
            return False

        return True

    def _get_invalid_reason(self, topic):
        """Return a human-readable reason why a topic is invalid."""
        if not isinstance(topic, dict):
            return "not_a_dict"
        if not (topic.get("title") or "").strip():
            return "missing_title"
        if not (topic.get("url") or "").strip():
            return "missing_url"
        article = (topic.get("article_text") or topic.get("content") or "").strip()
        if len(article) < MIN_ARTICLE_CHARS:
            return f"short_article:{len(article)}_chars"
        return "unknown"

    def _log_metrics(self):
        print("\n--- Dispatcher Metrics ---")
        for key, value in self.metrics.items():
            if key == "failure_reasons":
                if value:
                    # Summarize failure reasons
                    from collections import Counter
                    reason_counts = Counter(value)
                    print(f"failure_reasons:")
                    for reason, count in reason_counts.most_common():
                        print(f"  {reason}: {count}")
            else:
                print(f"{key}: {value}")
        print("-------------------------\n")


if __name__ == "__main__":
    import time
    start = time.time()
    
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    # ✅ FIXED: Read from topic_queue/ (output of prioritizer), NOT topics_analyzed/
    QUEUE_DIR = os.path.join(BASE_DIR, "data", "topic_queue")
    files = sorted(glob.glob(f"{QUEUE_DIR}/*.json"))
    
    if not files:
        print("ERROR: No prioritized topics found in topic_queue/. Run topic_prioritizer.py first.")
        exit(1)
        
    latest_file = files[-1]
    print(f"Reading prioritized topics from: {latest_file}")
    with open(latest_file) as f:
        clusters = json.load(f)
    
    # Flatten clusters → individual topics
    all_topics = []
    if isinstance(clusters, list):
        for item in clusters:
            if isinstance(item, dict) and "topics" in item:
                # Cluster format: {"id": ..., "topics": [...]}
                all_topics.extend(item.get("topics", []))
            elif isinstance(item, dict) and "title" in item:
                # Already a flat topic
                all_topics.append(item)
    
    print(f"Total topics extracted from queue: {len(all_topics)}")
    
    dispatcher = TopicDispatcher()
    dispatcher.metrics["input_count"] = len(all_topics)
    stats = dispatcher.dispatch_by_channel(all_topics)
    
    duration = round(time.time() - start, 2)
    
    print(f"\n--- Dispatch Stats (completed in {duration}s) ---")
    for cid, count in stats.items():
        print(f"Channel {cid}: {count} topics queued")
    total = sum(stats.values())
    print(f"Total dispatched: {total}")
    print("----------------------\n")

    if total == 0:
        print("WARNING: 0 topics dispatched! Check if topics have valid channels and article text.")