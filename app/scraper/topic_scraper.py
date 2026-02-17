import os
import json
import yaml
import logging
from datetime import datetime
from .collectors import RedditCollector, TwitterCollector, YouTubeCollector, rank_topics
from app.services.ollama_client import OllamaClient

logger = logging.getLogger(__name__)

class TopicScraper:
    def __init__(self):
        self.collectors = [
            RedditCollector(),
            TwitterCollector(),
            YouTubeCollector()
        ]
        
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

    def run(self):
        print("Starting topic scraping...")
        all_topics = []
        for collector in self.collectors:
            try:
                collector_name = collector.__class__.__name__
                print(f"Running {collector_name}...")
                
                topics = collector.collect_topics()
                if topics:
                    print(f"Collected {len(topics)} topics from {collector_name}")
                    all_topics.extend(topics)
                else:
                    print(f"No topics collected from {collector_name}")
                    
            except Exception as e:
                print(f"Error in collector {collector}: {e}")
        
        print(f"Total raw topics collected: {len(all_topics)}")
        
        # Rank topics
        ranked_topics = rank_topics(all_topics)
        print(f"Ranked {len(ranked_topics)} topics")
        
        # Group and ensure target counts with fallback generation
        final_topics = self._ensure_channel_targets(ranked_topics)
        
        return final_topics

    def _ensure_channel_targets(self, ranked_topics):
        """
        Group topics by channel and generate fallback topics if targets aren't met.
        """
        channel_buckets = {cid: [] for cid in self.channel_config.keys()}
        
        # Group existing topics
        for topic in ranked_topics:
            cid = topic.get("channel")
            if cid in channel_buckets:
                channel_buckets[cid].append(topic)
        
        final_list = []
        log_stats = []

        for cid, config in self.channel_config.items():
            scraped_count = len(channel_buckets[cid])
            target = config["target_count"]
            
            # Take top scraped topics up to target
            selected = channel_buckets[cid][:target]
            final_list.extend(selected)
            
            fallback_count = 0
            if len(selected) < target:
                fallback_needed = target - len(selected)
                print(f"Channel {cid} needs {fallback_needed} fallback topics...")
                
                fallbacks = self._generate_fallback_topics(cid, config, fallback_needed)
                final_list.extend(fallbacks)
                fallback_count = len(fallbacks)
            
            log_stats.append({
                "channel": cid,
                "scraped": min(scraped_count, target),
                "fallback": fallback_count,
                "target": target
            })

        # Print summary logging
        print("\n--- Pipeline Stats ---")
        for stat in log_stats:
            print(f"{stat['channel']}: Scraped={stat['scraped']}, Fallback={stat['fallback']} (Target={stat['target']})")
        print("----------------------\n")
        
        return final_list

    def _generate_fallback_topics(self, cid, config, count):
        """
        Generate safe fallback topics using local Ollama.
        """
        fallbacks = []
        prompt = (
            f"Generate {count} unique, trending, and engaging content topics for a YouTube channel "
            f"about '{config['name']}' ({config['category']}).\n"
            f"Return ONLY a JSON list of objects with 'title' and 'summary' keys. "
            f"Keep summaries brief (2 sentences).\n"
            f"Format: [{{'title': '...', 'summary': '...'}}, ...]"
        )
        
        response = OllamaClient.generate_with_retry(prompt, model=config["model"])
        
        if response:
            try:
                # Basic cleaning of LLM response to find JSON
                start_idx = response.find("[")
                end_idx = response.rfind("]") + 1
                if start_idx != -1 and end_idx != -1:
                    data = json.loads(response[start_idx:end_idx])
                    for item in data[:count]:
                        fallbacks.append({
                            "title": item.get("title"),
                            "summary": item.get("summary"),
                            "channel": cid,
                            "source": "fallback_llm",
                            "score": 1.0,
                            "rank": 99,
                            "is_fallback": True,
                            "collected_at": datetime.now().isoformat()
                        })
            except Exception as e:
                print(f"Error parsing fallback topics for {cid}: {e}")
        
        # Urgent fallback if LLM fails
        while len(fallbacks) < count:
            fallbacks.append({
                "title": f"Trending {config['category']} update {len(fallbacks)+1}",
                "summary": f"A deep dive into the latest developments in {config['name']}.",
                "channel": cid,
                "source": "emergency_fallback",
                "score": 0.5,
                "rank": 100,
                "is_fallback": True,
                "collected_at": datetime.now().isoformat()
            })
            
        return fallbacks

if __name__ == "__main__":
    def save_topics(topics):
        import os
        from datetime import datetime
        import json
        
        # Determine paths
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        DATA_DIR = os.path.join(BASE_DIR, "data", "topics")
        os.makedirs(DATA_DIR, exist_ok=True)
        
        filename = f"{DATA_DIR}/{datetime.now().strftime('%Y%m%d_%H%M')}.json"
        
        with open(filename, "w") as f:
            json.dump(topics, f, indent=2)
        print(f"Topics saved to: {filename}")

    scraper = TopicScraper()
    results = scraper.run()
    save_topics(results)
    import json
    # print(json.dumps(results, indent=2))