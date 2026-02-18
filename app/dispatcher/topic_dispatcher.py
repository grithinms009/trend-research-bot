import os
import json
import yaml
import glob
import logging
from datetime import datetime
from pathlib import Path


class TopicDispatcher:
    REQUIRED_FIELDS = ("url", "summary")

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
            "topics_received": 0,
            "topics_dispatched": 0,
            "topics_skipped_invalid": 0,
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
                continue

            if not self._is_valid(topic):
                self.metrics["topics_skipped_invalid"] += 1
                self.logger.warning(
                    "Dispatcher skipped topic '%s' — insufficient article content",
                    topic.get("title", "unknown"),
                )
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

        for field in self.REQUIRED_FIELDS:
            value = topic.get(field)
            if not value or not isinstance(value, str) or not value.strip():
                return False

        article = topic.get("article_text", "") or topic.get("content", "")
        if len(article.strip()) < 80:
            return False

        return True

    def _log_metrics(self):
        print("\n--- Dispatcher Metrics ---")
        for key, value in self.metrics.items():
            print(f"{key}: {value}")
        print("-------------------------\n")


if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    # Check for latest analyzed topics file
    TOPICS_DIR = os.path.join(BASE_DIR, "data", "topics_analyzed")
    files = sorted(glob.glob(f"{TOPICS_DIR}/*.json"))
    
    if not files:
        print("No topics found to dispatch.")
        exit(0)
        
    latest_file = files[-1]
    print(f"Reading topics from: {latest_file}")
    with open(latest_file) as f:
        topics = json.load(f)
    
    dispatcher = TopicDispatcher()
    stats = dispatcher.dispatch_by_channel(topics)
    
    print("\n--- Dispatch Stats ---")
    for cid, count in stats.items():
        print(f"Channel {cid}: {count} topics queued")
    print("----------------------\n")