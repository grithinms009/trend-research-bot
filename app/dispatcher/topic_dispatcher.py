import os
import json
import yaml
import glob
from datetime import datetime
from pathlib import Path

class TopicDispatcher:
    def __init__(self):
        # Load channel configuration
        base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        config_path = os.path.join(base_path, "app", "config", "channels.yaml")
        with open(config_path, "r") as f:
            self.channel_config = yaml.safe_load(f)["channels"]

    def dispatch_by_channel(self, topics):
        """
        Group topics by channel and save to channel-specific generation queues.
        """
        base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        generated_base = os.path.join(base_path, "data", "topic_generated")
        
        counts = {cid: 0 for cid in self.channel_config.keys()}
        
        for topic in topics:
            cid = topic.get("channel")
            if cid not in self.channel_config:
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
            
        return counts

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    # Check for latest topics file
    TOPICS_DIR = os.path.join(BASE_DIR, "data", "topics")
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