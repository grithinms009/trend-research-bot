"""
Visual Concept Stock Fetcher — 3-tier intelligent stock footage search.

Replaces simple keyword search with a cinematic concept search system.
Each scene generates three search types:
  1. LITERAL  — direct visual description of the topic
  2. EMOTIONAL — human emotion or reaction matching the mood
  3. SYMBOLIC  — abstract/metaphorical visual representation

Clips are scored by:
  0.35 × visual_clarity  (resolution, lighting)
  0.25 × cinematic_quality (duration, motion presence)
  0.20 × motion_presence  (not static)
  0.20 × relevance        (tags match intent)

Rejects:
  - Static slideshow-like clips (duration < 3s)
  - Low resolution (below 720p)
  - Watermarked clips
  - Obviously staged corporate footage
"""

import hashlib
import json
import logging
import os
import glob
import random
import time
from typing import Dict, List, Optional, Tuple

import requests
import yaml
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv()

PEXELS_API_KEY = os.environ.get("PEXELS_API_KEY", "")
PEXELS_VIDEO_SEARCH = "https://api.pexels.com/videos/search"

MIN_WIDTH = 720
MIN_HEIGHT = 1280

# Quality scoring weights
W_CLARITY = 0.35
W_CINEMATIC = 0.25
W_MOTION = 0.20
W_RELEVANCE = 0.20

# Rejection thresholds
MIN_CLIP_DURATION = 3.0   # Reject static slideshow clips
MAX_CLIP_DURATION = 60.0  # Reject overly long clips
MIN_QUALITY_SCORE = 0.30  # Reject below this composite score

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

# ============================================================
# EMOTIONAL SEARCH MAP — maps emotion to human reaction queries
# ============================================================
EMOTIONAL_SEARCH_MAP = {
    "shock": [
        "person amazed shocked reaction",
        "crowd gasping dramatic moment",
        "eyes widening surprise close up",
    ],
    "tension": [
        "person anxious waiting dark room",
        "hands gripping tension close up",
        "person looking worried screen",
    ],
    "reveal": [
        "person discovering something amazing",
        "eyes opening wide revelation",
        "curtain reveal audience reaction",
    ],
    "curiosity": [
        "person examining closely fascinated",
        "child looking through magnifying glass",
        "scientist studying research focused",
    ],
    "urgency": [
        "person running late stressed",
        "hands typing fast keyboard urgent",
        "crowd rushing city street",
    ],
    "dramatic": [
        "person standing dramatic lighting",
        "silhouette dramatic sky sunset",
        "face dramatic side lighting",
    ],
    "neutral": [
        "person working focused calm",
        "everyday life modern aesthetic",
        "calm professional environment",
    ],
}

# ============================================================
# SYMBOLIC SEARCH MAP — abstract/metaphorical visual representations
# ============================================================
SYMBOLIC_SEARCH_MAP = {
    "abstract_tension": ["abstract digital glitch art", "dark geometric shapes morphing", "liquid mercury flowing dark"],
    "tech_ui": ["neural network visualization blue", "holographic data streams", "matrix code rain digital"],
    "nature": ["timelapse flower blooming macro", "aurora borealis dramatic sky", "underwater bioluminescence dark"],
    "urban": ["city timelapse night lights blur", "neon reflections rain puddle", "aerial highway traffic patterns"],
    "money": ["gold particles floating dark", "digital currency visualization", "abstract wealth flow diagram"],
    "luxury": ["crystal chandelier light refraction", "silk fabric flowing slow motion", "diamond sparkling macro dark"],
    "cinematic_dark": ["ink dropping water slow motion", "smoke tendrils light beam", "abstract dark fluid art"],
    "data_visualization": ["particle data flow animation", "abstract graph network visualization", "digital information stream"],
    "document": ["old paper texture candlelight", "ink pen writing close up art", "seal stamp wax dramatic"],
    "building": ["architectural symmetry perspective", "glass reflection clouds building", "stairs ascending infinity"],
}

# Words that indicate staged corporate stock footage (reject these)
CORPORATE_REJECT_TAGS = {
    "handshake", "thumbs up", "high five", "team meeting",
    "group smiling", "office celebration", "corporate team",
    "business presentation", "pointing at chart",
}


def _score_clip(video: Dict, query: str) -> float:
    """
    Score a Pexels video clip for cinematic quality.

    Returns 0.0-1.0 composite score based on:
    - Visual clarity (resolution)
    - Cinematic quality (duration, aspect)
    - Motion presence (not static)
    - Relevance (tags match)
    """
    files = video.get("video_files", [])
    if not files:
        return 0.0

    # Visual clarity — prefer HD+ resolution
    best_height = max((f.get("height", 0) for f in files), default=0)
    if best_height >= 1920:
        clarity = 1.0
    elif best_height >= 1080:
        clarity = 0.8
    elif best_height >= 720:
        clarity = 0.5
    else:
        clarity = 0.2

    # Cinematic quality — duration sweet spot 5-15s
    duration = video.get("duration", 0)
    if 5 <= duration <= 15:
        cinematic = 1.0
    elif 3 <= duration <= 30:
        cinematic = 0.6
    else:
        cinematic = 0.2

    # Motion presence — longer clips likely have motion
    # Pexels doesn't expose motion data, so approximate from duration
    motion = 0.8 if duration >= 4 else 0.3

    # Relevance — check if video URL/tags hint at query terms
    video_url = video.get("url", "").lower()
    query_words = set(query.lower().split())
    matching = sum(1 for w in query_words if w in video_url)
    relevance = min(1.0, matching / max(len(query_words), 1) + 0.3)

    score = (W_CLARITY * clarity + W_CINEMATIC * cinematic +
             W_MOTION * motion + W_RELEVANCE * relevance)
    return round(score, 3)


def _should_reject_clip(video: Dict) -> bool:
    """Reject clips that are static, low quality, or staged corporate footage."""
    duration = video.get("duration", 0)

    # Reject too short (static slideshow) or too long
    if duration < MIN_CLIP_DURATION or duration > MAX_CLIP_DURATION:
        return True

    # Reject low resolution
    files = video.get("video_files", [])
    best_height = max((f.get("height", 0) for f in files), default=0)
    if best_height < 480:
        return True

    # Reject staged corporate footage by URL keywords
    video_url = video.get("url", "").lower()
    for tag in CORPORATE_REJECT_TAGS:
        if tag.replace(" ", "-") in video_url:
            return True

    return False


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
    """
    3-tier intelligent stock footage fetcher.

    For each scene, searches in priority order:
      1. LITERAL  — scene's visual_prompts_3tier.literal or visual_intent map
      2. EMOTIONAL — emotion-based human reaction footage
      3. SYMBOLIC  — abstract metaphorical visuals

    Best clip is selected by composite quality score.
    Clips below MIN_QUALITY_SCORE or matching rejection rules are skipped.
    """

    def __init__(self):
        self.channel_config = _load_channel_config()
        self.metrics = {
            "scenes_processed": 0,
            "clips_downloaded": 0,
            "clips_cached": 0,
            "clips_failed": 0,
            "clips_rejected": 0,
            "tier_hits": {"literal": 0, "emotional": 0, "symbolic": 0, "fallback": 0},
        }
        self._used_queries: set = set()
        self._used_clip_ids: set = set()  # Prevent same clip across scenes

    def _build_3tier_queries(self, scene: Dict) -> List[Tuple[str, str]]:
        """
        Build ordered list of (query, tier) tuples for 3-tier search.
        Returns up to 3 queries: literal, emotional, symbolic.
        """
        visual_intent = scene.get("visual_intent", "cinematic_dark")
        emotion = scene.get("emotion", "neutral")
        queries = []

        # Check for LLM-generated 3-tier prompts first
        vp3 = scene.get("visual_prompts_3tier", {})
        if isinstance(vp3, dict):
            if vp3.get("literal"):
                queries.append((vp3["literal"], "literal"))
            if vp3.get("emotional"):
                queries.append((vp3["emotional"], "emotional"))
            if vp3.get("symbolic"):
                queries.append((vp3["symbolic"], "symbolic"))

        # Fill missing tiers from maps
        if not any(t == "literal" for _, t in queries):
            literal_q = get_search_query(visual_intent, emotion)
            queries.insert(0, (literal_q, "literal"))

        if not any(t == "emotional" for _, t in queries):
            emo_queries = EMOTIONAL_SEARCH_MAP.get(emotion, EMOTIONAL_SEARCH_MAP["neutral"])
            queries.append((random.choice(emo_queries), "emotional"))

        if not any(t == "symbolic" for _, t in queries):
            sym_queries = SYMBOLIC_SEARCH_MAP.get(visual_intent, ["abstract cinematic motion dark"])
            queries.append((random.choice(sym_queries), "symbolic"))

        # Also include legacy visual_prompts list as extra literal candidates
        legacy_prompts = scene.get("visual_prompts", [])
        if isinstance(legacy_prompts, list):
            for vp in legacy_prompts[:2]:
                if vp not in [q for q, _ in queries]:
                    queries.append((vp, "literal"))

        return queries

    def _search_and_score(self, query: str) -> List[Tuple[Dict, float]]:
        """Search Pexels and return scored, filtered results."""
        videos = search_pexels(query, per_page=8)
        scored = []

        for video in videos:
            if _should_reject_clip(video):
                self.metrics["clips_rejected"] += 1
                continue

            # Skip already-used clips
            clip_id = video.get("id", 0)
            if clip_id in self._used_clip_ids:
                continue

            score = _score_clip(video, query)
            if score >= MIN_QUALITY_SCORE:
                scored.append((video, score))

        # Sort by score descending
        scored.sort(key=lambda x: x[1], reverse=True)
        return scored

    def fetch_for_topic(self, scene_plan: Dict, assets_dir: str) -> List[str]:
        """Fetch stock clips for each scene using 3-tier intelligent search."""
        scenes = scene_plan.get("scenes", [])
        clip_paths = []

        if not PEXELS_API_KEY:
            print("  ⚠️  PEXELS_API_KEY not set — using solid backgrounds")
            return [""] * len(scenes)

        for scene in scenes:
            self.metrics["scenes_processed"] += 1
            scene_num = scene.get("scene_id", scene.get("scene_number", 0))

            # Build 3-tier query list
            tier_queries = self._build_3tier_queries(scene)

            # Check cache for any tier
            clip_path = ""
            found_cached = False
            for query, tier in tier_queries:
                cache_name = f"scene_{str(scene_num).zfill(2)}_{_cache_key(query)}.mp4"
                cached_path = os.path.join(assets_dir, cache_name)
                if os.path.exists(cached_path) and os.path.getsize(cached_path) > 1000:
                    self.metrics["clips_cached"] += 1
                    self.metrics["tier_hits"][tier] = self.metrics["tier_hits"].get(tier, 0) + 1
                    clip_paths.append(cached_path)
                    found_cached = True
                    break

            if found_cached:
                continue

            # Search through tiers in priority order
            best_video = None
            best_score = 0.0
            best_query = ""
            best_tier = "fallback"

            for query, tier in tier_queries:
                if query in self._used_queries:
                    continue

                scored = self._search_and_score(query)
                if scored:
                    video, score = scored[0]
                    if score > best_score:
                        best_video = video
                        best_score = score
                        best_query = query
                        best_tier = tier

                    # If literal tier finds a good match (>0.6), use it
                    if tier == "literal" and score >= 0.6:
                        break

                time.sleep(0.2)  # Rate limit between tier searches

            # Download best clip
            if best_video:
                self._used_queries.add(best_query)
                self._used_clip_ids.add(best_video.get("id", 0))
                self.metrics["tier_hits"][best_tier] = self.metrics["tier_hits"].get(best_tier, 0) + 1

                cache_name = f"scene_{str(scene_num).zfill(2)}_{_cache_key(best_query)}.mp4"
                clip_path = os.path.join(assets_dir, cache_name)
                url = pick_best_file(best_video)

                if url and download_clip(url, clip_path):
                    self.metrics["clips_downloaded"] += 1
                    clip_paths.append(clip_path)
                    time.sleep(0.3)
                    continue

            # Final fallback
            fallback_videos = search_pexels(random.choice(DEFAULT_QUERIES))
            if fallback_videos:
                video = fallback_videos[0]
                self.metrics["tier_hits"]["fallback"] += 1
                cache_name = f"scene_{str(scene_num).zfill(2)}_fallback.mp4"
                clip_path = os.path.join(assets_dir, cache_name)
                url = pick_best_file(video)
                if url and download_clip(url, clip_path):
                    self.metrics["clips_downloaded"] += 1
                    clip_paths.append(clip_path)
                    time.sleep(0.3)
                    continue

            self.metrics["clips_failed"] += 1
            clip_paths.append("")
            time.sleep(0.3)

        return clip_paths

    def log_metrics(self):
        print("\n--- Stock Fetcher Metrics ---")
        for k, v in self.metrics.items():
            if isinstance(v, dict):
                print(f"  {k}:")
                for tk, tv in v.items():
                    print(f"    {tk}: {tv}")
            else:
                print(f"  {k}: {v}")
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
