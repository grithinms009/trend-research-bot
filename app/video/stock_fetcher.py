"""
Stock Footage Fetcher — downloads free vertical video clips from Pexels.

Uses Pexels API (free tier, 200 requests/hour) to find relevant stock footage
per scene. Falls back to channel-generic clips if no match found.
"""

import hashlib
import json
import logging
import os
import glob
import time
from typing import Dict, List, Optional
from datetime import datetime

import requests
import yaml
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv()

PEXELS_API_KEY = os.environ.get("PEXELS_API_KEY", "")
PEXELS_VIDEO_SEARCH = "https://api.pexels.com/videos/search"

# Minimum video dimensions for quality
MIN_WIDTH = 720
MIN_HEIGHT = 1280


def _load_channel_config() -> Dict:
    base = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    path = os.path.join(base, "app", "config", "channels.yaml")
    with open(path) as f:
        return yaml.safe_load(f).get("channels", {})


def _cache_key(query: str) -> str:
    return hashlib.md5(query.encode()).hexdigest()[:12]


def search_pexels(query: str, orientation: str = "portrait", per_page: int = 3) -> List[Dict]:
    """Search Pexels for video clips matching query."""
    if not PEXELS_API_KEY:
        print("  ⚠️  PEXELS_API_KEY not set — skipping stock footage")
        return []

    headers = {"Authorization": PEXELS_API_KEY}
    params = {
        "query": query,
        "orientation": orientation,
        "per_page": per_page,
        "size": "medium",
    }

    try:
        resp = requests.get(PEXELS_VIDEO_SEARCH, headers=headers, params=params, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            return data.get("videos", [])
        else:
            logger.warning("Pexels returned %s: %s", resp.status_code, resp.text[:200])
            return []
    except Exception as exc:
        logger.error("Pexels search failed: %s", exc)
        return []


def pick_best_file(video: Dict) -> Optional[str]:
    """Pick the best video file URL from Pexels result (prefer HD portrait)."""
    files = video.get("video_files", [])
    if not files:
        return None

    # Prefer: portrait, HD, smallest file that meets quality threshold
    portrait_files = [
        f for f in files
        if f.get("height", 0) >= MIN_HEIGHT and f.get("width", 0) >= MIN_WIDTH
    ]

    if not portrait_files:
        # Fall back to any file
        portrait_files = files

    # Sort by file size (smaller = faster download, still good quality)
    portrait_files.sort(key=lambda f: f.get("file_size", 999999999))

    return portrait_files[0].get("link")


def download_clip(url: str, output_path: str) -> bool:
    """Download a video clip to local path."""
    try:
        resp = requests.get(url, timeout=30, stream=True)
        if resp.status_code == 200:
            with open(output_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)
            size_mb = os.path.getsize(output_path) / (1024 * 1024)
            logger.info("Downloaded clip: %s (%.1fMB)", output_path, size_mb)
            return True
        else:
            logger.error("Download failed: HTTP %s for %s", resp.status_code, url[:80])
            return False
    except Exception as exc:
        logger.error("Download error: %s", exc)
        return False


class StockFetcher:
    """Fetches stock footage for each scene in a topic's scene plan."""

    def __init__(self):
        self.channel_config = _load_channel_config()
        self.metrics = {
            "scenes_processed": 0,
            "clips_downloaded": 0,
            "clips_cached": 0,
            "clips_failed": 0,
        }

    def fetch_for_topic(self, scene_plan: Dict, assets_dir: str) -> List[str]:
        """Fetch stock clips for each scene in a topic. Returns list of clip paths."""
        channel = scene_plan.get("channel_id", "C1")
        ch_config = self.channel_config.get(channel, {})
        search_terms = ch_config.get("search_terms", ["abstract background"])
        scenes = scene_plan.get("scenes", [])
        clip_paths = []

        for scene in scenes:
            self.metrics["scenes_processed"] += 1
            scene_num = scene.get("scene_number", 0)
            scene_text = scene.get("text", "")

            # Build search query: channel term + key words from scene
            words = scene_text.split()[:5]
            scene_query = " ".join(words) if words else search_terms[0]
            query = f"{search_terms[scene_num % len(search_terms)]} {scene_query}"

            # Check cache
            cache_name = f"scene_{str(scene_num).zfill(2)}_{_cache_key(query)}.mp4"
            clip_path = os.path.join(assets_dir, cache_name)

            if os.path.exists(clip_path) and os.path.getsize(clip_path) > 1000:
                self.metrics["clips_cached"] += 1
                clip_paths.append(clip_path)
                continue

            # Search Pexels
            videos = search_pexels(query[:100])  # Pexels has query length limits

            if not videos:
                # Fallback: try generic channel term
                videos = search_pexels(search_terms[0])

            if videos:
                url = pick_best_file(videos[0])
                if url and download_clip(url, clip_path):
                    self.metrics["clips_downloaded"] += 1
                    clip_paths.append(clip_path)
                    continue

            # Last resort: use a solid color background (FFmpeg will generate)
            self.metrics["clips_failed"] += 1
            clip_paths.append("")  # Empty = generate solid bg in video builder
            logger.warning("No clip for scene %d of '%s'", scene_num, scene_plan.get("title", "")[:40])

            # Rate limit: don't hammer Pexels
            time.sleep(0.5)

        return clip_paths

    def log_metrics(self):
        print("\n--- Stock Fetcher Metrics ---")
        for k, v in self.metrics.items():
            print(f"{k}: {v}")
        print("----------------------------\n")


def main():
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    scene_plan_dir = os.path.join(base_dir, "data", "scene_plans")
    assets_root = os.path.join(base_dir, "data", "shorts", "assets")

    # Find all scene plan files
    scene_files = sorted(glob.glob(os.path.join(scene_plan_dir, "*", "*.json")))
    scene_files += sorted(glob.glob(os.path.join(scene_plan_dir, "*.json")))

    if not scene_files:
        print("No scene plans found.")
        return

    fetcher = StockFetcher()
    total_clips = 0

    for plan_path in scene_files:
        with open(plan_path) as f:
            data = json.load(f)

        plans = data if isinstance(data, list) else [data]

        for plan in plans:
            title = plan.get("title", "unknown")
            channel = plan.get("channel_id", "XX")
            safe_title = "".join(c if c.isalnum() or c in "-_ " else "" for c in title)[:50].strip().replace(" ", "_")
            topic_id = f"{channel}_{safe_title}"

            topic_assets = os.path.join(assets_root, topic_id)
            os.makedirs(topic_assets, exist_ok=True)

            print(f"Fetching stock for '{title[:60]}' ({channel})...")
            clips = fetcher.fetch_for_topic(plan, topic_assets)
            total_clips += len([c for c in clips if c])

    fetcher.log_metrics()
    print(f"Total clips downloaded/cached: {total_clips}")


if __name__ == "__main__":
    main()
