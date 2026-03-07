"""
Visual Intent Stock Fetcher — maps scene emotion/intent to stock footage.

Replaces literal keyword search. Uses visual_intent field from scene planner
to find emotionally-matching stock clips from Pexels.

Never searches politician names or literal controversy keywords.
"""

import hashlib
import json
import logging
import os
import glob
import random
import time
from typing import Dict, List, Optional

import requests
import yaml
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv()

PEXELS_API_KEY = os.environ.get("PEXELS_API_KEY", "")
PEXELS_VIDEO_SEARCH = "https://api.pexels.com/videos/search"

MIN_WIDTH = 720
MIN_HEIGHT = 1280

# ============================================================
# VISUAL INTENT → STOCK SEARCH MAPPING
# Symbolic intent → list of Pexels search phrases (randomized)
# ============================================================
VISUAL_INTENT_MAP = {
    "abstract_tension": [
        "dark cinematic corridor",
        "dramatic shadows close up",
        "abstract dark motion",
        "moody atmospheric light",
        "dark tunnel perspective",
    ],
    "document": [
        "legal document close up",
        "paper signing macro",
        "official paperwork desk",
        "contract document hand",
        "newspaper headline close",
    ],
    "crowd_reaction": [
        "crowd reacting slow motion",
        "people shocked audience",
        "crowd watching event",
        "audience reaction close up",
        "group people dramatic",
    ],
    "building": [
        "government building exterior",
        "capitol building wide shot",
        "modern architecture city",
        "official building dramatic sky",
        "courthouse exterior cinematic",
    ],
    "tech_ui": [
        "futuristic digital interface",
        "AI data visualization",
        "holographic screen technology",
        "coding computer screen",
        "server room lights",
    ],
    "luxury": [
        "luxury mansion cinematic",
        "private jet interior",
        "luxury car driving",
        "premium hotel lobby",
        "champagne celebration slow motion",
    ],
    "nature": [
        "epic landscape aerial",
        "ocean waves cinematic",
        "mountain aerial dramatic",
        "forest canopy sunlight",
        "storm clouds timelapse",
    ],
    "urban": [
        "city night timelapse",
        "downtown skyline sunset",
        "busy street crowd motion",
        "neon city lights rain",
        "aerial city traffic night",
    ],
    "data_visualization": [
        "stock chart analysis",
        "data dashboard screen",
        "financial graph animation",
        "numbers data technology",
        "analytics screen close",
    ],
    "cinematic_dark": [
        "dark cinematic background",
        "moody silhouette dramatic",
        "shadow light contrast",
        "dramatic lens flare dark",
        "abstract smoke dark",
    ],
    "explosion_impact": [
        "explosion slow motion",
        "shockwave dramatic",
        "debris flying cinematic",
        "glass breaking slow motion",
        "fire dramatic close up",
    ],
    "timeline": [
        "clock time passing",
        "calendar pages flipping",
        "hourglass sand falling",
        "watch mechanism macro",
        "sunrise timelapse",
    ],
    "medical": [
        "laboratory research science",
        "hospital corridor cinematic",
        "microscope close up",
        "medical technology screen",
        "science experiment lab",
    ],
    "military": [
        "military formation dramatic",
        "radar screen technology",
        "strategic map planning",
        "helicopter aerial dramatic",
        "night vision technology",
    ],
    "money": [
        "cash money counting",
        "gold bars vault",
        "coins falling slow motion",
        "stock exchange trading floor",
        "cryptocurrency digital",
    ],
}

# Default fallback for unknown intents
DEFAULT_QUERIES = [
    "abstract cinematic background",
    "dramatic light dark",
    "atmospheric motion",
]


def _cache_key(query: str) -> str:
    return hashlib.md5(query.encode()).hexdigest()[:12]


def _load_channel_config() -> Dict:
    base = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    path = os.path.join(base, "app", "config", "channels.yaml")
    with open(path) as f:
        raw = yaml.safe_load(f) or {}
    return raw.get("channels", {}) if isinstance(raw, dict) else {}


def search_pexels(query: str, orientation: str = "portrait", per_page: int = 5) -> List[Dict]:
    """Search Pexels for video clips."""
    if not PEXELS_API_KEY:
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
            return resp.json().get("videos", [])
        else:
            logger.warning("Pexels %s: %s", resp.status_code, resp.text[:200])
            return []
    except Exception as exc:
        logger.error("Pexels search failed: %s", exc)
        return []


def pick_best_file(video: Dict) -> Optional[str]:
    """Pick the best video file URL (prefer HD portrait)."""
    files = video.get("video_files", [])
    if not files:
        return None

    portrait_files = [
        f for f in files
        if f.get("height", 0) >= MIN_HEIGHT and f.get("width", 0) >= MIN_WIDTH
    ]
    if not portrait_files:
        portrait_files = files

    portrait_files.sort(key=lambda f: f.get("file_size", 999999999))
    return portrait_files[0].get("link")


def download_clip(url: str, output_path: str) -> bool:
    """Download a video clip."""
    try:
        resp = requests.get(url, timeout=30, stream=True)
        if resp.status_code == 200:
            with open(output_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)
            return True
        return False
    except Exception as exc:
        logger.error("Download error: %s", exc)
        return False


def get_search_query(visual_intent: str, emotion: str = "neutral") -> str:
    """Map visual_intent + emotion to a Pexels search query."""
    queries = VISUAL_INTENT_MAP.get(visual_intent, DEFAULT_QUERIES)

    # Add emotion modifier for certain emotions
    query = random.choice(queries)

    if emotion in ("shock", "urgency"):
        query += " dramatic"
    elif emotion == "reveal":
        query += " cinematic"

    return query


class StockFetcher:
    """Fetches stock footage based on scene visual_intent and emotion."""

    def __init__(self):
        self.channel_config = _load_channel_config()
        self.metrics = {
            "scenes_processed": 0,
            "clips_downloaded": 0,
            "clips_cached": 0,
            "clips_failed": 0,
        }
        self._used_queries = set()  # Avoid duplicate clips

    def fetch_for_topic(self, scene_plan: Dict, assets_dir: str) -> List[str]:
        """Fetch stock clips for each scene using visual_intent."""
        scenes = scene_plan.get("scenes", [])
        clip_paths = []

        if not PEXELS_API_KEY:
            print("  ⚠️  PEXELS_API_KEY not set — using solid backgrounds")
            return [""] * len(scenes)

        for scene in scenes:
            self.metrics["scenes_processed"] += 1
            scene_num = scene.get("scene_id", scene.get("scene_number", 0))
            visual_intent = scene.get("visual_intent", "cinematic_dark")
            emotion = scene.get("emotion", "neutral")

            # v2: prefer pre-generated visual_prompts from enhanced scene planner
            visual_prompts = scene.get("visual_prompts", [])
            if visual_prompts:
                # Pick first unused prompt from the pre-generated list
                query = None
                for vp in visual_prompts:
                    if vp not in self._used_queries:
                        query = vp
                        break
                if not query:
                    query = random.choice(visual_prompts)
            else:
                # Fallback to visual intent mapping
                query = get_search_query(visual_intent, emotion)

            # Avoid repeating same query across scenes
            attempts = 0
            while query in self._used_queries and attempts < 3:
                if visual_prompts:
                    query = random.choice(visual_prompts)
                else:
                    query = get_search_query(visual_intent, emotion)
                attempts += 1
            self._used_queries.add(query)

            # Check cache
            cache_name = f"scene_{str(scene_num).zfill(2)}_{_cache_key(query)}.mp4"
            clip_path = os.path.join(assets_dir, cache_name)

            if os.path.exists(clip_path) and os.path.getsize(clip_path) > 1000:
                self.metrics["clips_cached"] += 1
                clip_paths.append(clip_path)
                continue

            # Search Pexels
            videos = search_pexels(query)

            if not videos:
                # Fallback to default
                videos = search_pexels(random.choice(DEFAULT_QUERIES))

            if videos:
                # Pick random from top results for variety
                video = random.choice(videos[:3]) if len(videos) >= 3 else videos[0]
                url = pick_best_file(video)
                if url and download_clip(url, clip_path):
                    self.metrics["clips_downloaded"] += 1
                    clip_paths.append(clip_path)
                    time.sleep(0.3)  # Rate limit
                    continue

            self.metrics["clips_failed"] += 1
            clip_paths.append("")
            time.sleep(0.3)

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
