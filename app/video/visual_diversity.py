"""
Visual Diversity Engine — prevents repetitive video look across channel outputs.

Tracks visual styles, search queries, and color grades used per channel per run.
Enforces rotation and variety so 10 videos for one channel never look the same.

Integrates with:
- Scene Planner (visual_intent selection)
- Cinematic Director (color grade, camera motion rotation)
- Stock Fetcher (search query deduplication)
"""

import hashlib
import json
import logging
import os
import random
from collections import defaultdict
from typing import Dict, List, Optional, Set

logger = logging.getLogger(__name__)

# ============================================================
# VISUAL STYLE PALETTES — rotated across videos in a channel
# ============================================================
VISUAL_STYLE_PALETTES = {
    "C1": [  # AI / Tech
        {"color_grade": "cool_news", "dominant_mood": "futuristic", "stock_bias": "technology"},
        {"color_grade": "cinematic_dark", "dominant_mood": "dramatic", "stock_bias": "abstract"},
        {"color_grade": "neutral", "dominant_mood": "analytical", "stock_bias": "data_visualization"},
        {"color_grade": "dramatic", "dominant_mood": "intense", "stock_bias": "urban"},
    ],
    "C2": [  # Finance
        {"color_grade": "neutral", "dominant_mood": "professional", "stock_bias": "data_visualization"},
        {"color_grade": "cool_news", "dominant_mood": "urgent", "stock_bias": "urban"},
        {"color_grade": "cinematic_dark", "dominant_mood": "dramatic", "stock_bias": "money"},
        {"color_grade": "dramatic", "dominant_mood": "intense", "stock_bias": "building"},
    ],
    "C3": [  # History / Science
        {"color_grade": "cinematic_dark", "dominant_mood": "mysterious", "stock_bias": "nature"},
        {"color_grade": "warm_luxury", "dominant_mood": "wonder", "stock_bias": "timeline"},
        {"color_grade": "dramatic", "dominant_mood": "epic", "stock_bias": "cinematic_dark"},
        {"color_grade": "neutral", "dominant_mood": "educational", "stock_bias": "medical"},
    ],
    "C4": [  # Luxury / Travel
        {"color_grade": "warm_luxury", "dominant_mood": "aspirational", "stock_bias": "luxury"},
        {"color_grade": "bright_clean", "dominant_mood": "vibrant", "stock_bias": "nature"},
        {"color_grade": "cinematic_dark", "dominant_mood": "cinematic", "stock_bias": "urban"},
        {"color_grade": "neutral", "dominant_mood": "elegant", "stock_bias": "building"},
    ],
    "C5": [  # Productivity
        {"color_grade": "bright_clean", "dominant_mood": "clean", "stock_bias": "document"},
        {"color_grade": "neutral", "dominant_mood": "calm", "stock_bias": "nature"},
        {"color_grade": "warm_luxury", "dominant_mood": "warm", "stock_bias": "timeline"},
        {"color_grade": "cool_news", "dominant_mood": "focused", "stock_bias": "tech_ui"},
    ],
}

# Camera motion sequences — no two consecutive scenes use the same motion
CAMERA_SEQUENCES = [
    ["slow_push", "fast_punch", "drift", "static", "slow_push"],
    ["fast_punch", "drift", "slow_push", "lateral_pan", "pull_out"],
    ["drift", "slow_push", "fast_punch", "static", "drift"],
    ["static", "slow_push", "fast_punch", "drift", "lateral_pan"],
    ["lateral_pan", "fast_punch", "slow_push", "drift", "static"],
]

# Cut timing patterns — varied rhythm per video
CUT_PATTERNS = [
    ["hard_interrupt", "on_beat", "pre_beat", "on_beat", "hard_interrupt"],
    ["on_beat", "pre_beat", "hard_interrupt", "on_beat", "pre_beat"],
    ["pre_beat", "hard_interrupt", "on_beat", "pre_beat", "on_beat"],
]

# Per-sentence visual intent expansion — maps a base intent to diverse alternatives
INTENT_ALTERNATIVES = {
    "abstract_tension": [
        "abstract_tension", "cinematic_dark", "urban", "explosion_impact",
    ],
    "cinematic_dark": [
        "cinematic_dark", "abstract_tension", "nature", "urban",
    ],
    "tech_ui": [
        "tech_ui", "data_visualization", "abstract_tension", "urban",
    ],
    "document": [
        "document", "building", "timeline", "cinematic_dark",
    ],
    "nature": [
        "nature", "cinematic_dark", "timeline", "medical",
    ],
    "money": [
        "money", "data_visualization", "building", "urban",
    ],
    "luxury": [
        "luxury", "nature", "urban", "building",
    ],
    "urban": [
        "urban", "cinematic_dark", "abstract_tension", "building",
    ],
    "data_visualization": [
        "data_visualization", "tech_ui", "document", "abstract_tension",
    ],
    "building": [
        "building", "urban", "cinematic_dark", "document",
    ],
    "crowd_reaction": [
        "crowd_reaction", "urban", "cinematic_dark", "abstract_tension",
    ],
    "timeline": [
        "timeline", "nature", "cinematic_dark", "document",
    ],
    "medical": [
        "medical", "tech_ui", "nature", "document",
    ],
    "military": [
        "military", "cinematic_dark", "abstract_tension", "urban",
    ],
    "explosion_impact": [
        "explosion_impact", "abstract_tension", "cinematic_dark", "urban",
    ],
}


class VisualDiversityEngine:
    """
    Ensures visual diversity across videos within the same channel.

    Tracks:
    - Which style palettes have been used per channel
    - Which visual intents have been assigned per video
    - Which stock search queries have been used (prevents duplicate clips)
    - Camera motion and cut timing sequences
    """

    def __init__(self):
        self._channel_palette_index: Dict[str, int] = defaultdict(int)
        self._used_intents_per_video: Dict[str, Set[str]] = defaultdict(set)
        self._used_queries_global: Set[str] = set()
        self._video_count_per_channel: Dict[str, int] = defaultdict(int)
        self.metrics = {
            "intents_diversified": 0,
            "palette_rotations": 0,
            "query_dedup_hits": 0,
        }

    def get_style_palette(self, channel_id: str) -> Dict:
        """Get the next style palette for a channel (round-robin rotation)."""
        palettes = VISUAL_STYLE_PALETTES.get(channel_id, VISUAL_STYLE_PALETTES["C1"])
        idx = self._channel_palette_index[channel_id] % len(palettes)
        self._channel_palette_index[channel_id] += 1
        self.metrics["palette_rotations"] += 1
        return palettes[idx]

    def get_camera_sequence(self, channel_id: str) -> List[str]:
        """Get a camera motion sequence for this video (varied per video)."""
        count = self._video_count_per_channel[channel_id]
        self._video_count_per_channel[channel_id] += 1
        idx = count % len(CAMERA_SEQUENCES)
        return list(CAMERA_SEQUENCES[idx])

    def get_cut_pattern(self, channel_id: str) -> List[str]:
        """Get a cut timing pattern for this video."""
        count = self._video_count_per_channel.get(channel_id, 0)
        idx = count % len(CUT_PATTERNS)
        return list(CUT_PATTERNS[idx])

    def diversify_visual_intent(self, base_intent: str, video_id: str) -> str:
        """
        Pick a visual intent that hasn't been used in this video yet.
        Prevents all scenes in one video from using the same stock footage.
        """
        alternatives = INTENT_ALTERNATIVES.get(base_intent, [base_intent])
        used = self._used_intents_per_video[video_id]

        for alt in alternatives:
            if alt not in used:
                used.add(alt)
                if alt != base_intent:
                    self.metrics["intents_diversified"] += 1
                return alt

        # All alternatives used; cycle with randomization
        choice = random.choice(alternatives)
        used.add(choice)
        return choice

    def is_query_used(self, query: str) -> bool:
        """Check if a stock search query has been used globally."""
        key = hashlib.md5(query.lower().encode()).hexdigest()
        if key in self._used_queries_global:
            self.metrics["query_dedup_hits"] += 1
            return True
        self._used_queries_global.add(key)
        return False

    def diversify_scenes(self, scenes: List[Dict], channel_id: str, video_id: str) -> List[Dict]:
        """
        Apply visual diversity to a list of scenes.
        Modifies visual_intent per scene to ensure variety within one video.
        """
        palette = self.get_style_palette(channel_id)
        camera_seq = self.get_camera_sequence(channel_id)
        cut_seq = self.get_cut_pattern(channel_id)

        diversified = []
        for i, scene in enumerate(scenes):
            s = dict(scene)

            # Diversify visual intent
            base_intent = s.get("visual_intent", "cinematic_dark")
            s["visual_intent"] = self.diversify_visual_intent(base_intent, video_id)

            # Apply palette overrides (only if not already set by cinematic director)
            if "color_grade" not in s or not s["color_grade"]:
                s["color_grade"] = palette.get("color_grade", "neutral")

            # Apply camera sequence
            if i < len(camera_seq):
                s["camera_motion_hint"] = camera_seq[i]

            # Apply cut pattern
            if i < len(cut_seq):
                s["cut_timing_hint"] = cut_seq[i]

            # Tag with palette mood for downstream stages
            s["style_mood"] = palette.get("dominant_mood", "neutral")
            s["stock_bias"] = palette.get("stock_bias", "cinematic_dark")

            diversified.append(s)

        return diversified

    def log_metrics(self):
        print("\n--- Visual Diversity Engine ---")
        for k, v in self.metrics.items():
            print(f"  {k}: {v}")
        print("------------------------------\n")
