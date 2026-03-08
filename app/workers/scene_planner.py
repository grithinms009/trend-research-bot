"""
Scene Planner — LLM-based scene planner with emotion, energy, and visual intent.

Replaces the deterministic scene_splitter. Each scene gets rich metadata
for the rhythm engine, visual intent engine, and caption engine.

v2 enhancements:
- Per-sentence visual prompt generation (3 diverse prompts per scene)
- Visual diversity integration to prevent repetitive footage
- Richer visual_intent taxonomy
- Style mood tagging for downstream color grading

Uses Ollama to break narration into emotionally-tagged scenes.
Falls back to deterministic splitting if LLM fails.
"""

import os
import json
import glob
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional

from app.services.ollama_client import OllamaClient
from app.video.visual_diversity import VisualDiversityEngine

logger = logging.getLogger(__name__)

# Valid values for scene metadata
VALID_EMOTIONS = {"shock", "tension", "reveal", "neutral", "dramatic", "curiosity", "urgency"}
VALID_CUT_STYLES = {"hard", "smash", "slow"}
VALID_CAMERA_STYLES = {
    "slow_push_in", "fast_punch_in", "pull_out", "lateral_pan",
    "static_tension", "slow_drift", "rack_focus", "dolly_zoom",
    "overhead_descent", "handheld_shake",
}

# Visual contrast pattern — prevents visual boredom by cycling shot types
# Pattern: wide → macro → motion → abstract → wide
SHOT_CONTRAST_CYCLE = ["wide", "macro", "motion", "abstract", "wide", "detail"]

# Scene pacing bounds (seconds) — retention drops if scenes stay too long
MIN_SCENE_DURATION = 3.0
MAX_SCENE_DURATION = 6.0

# ============================================================
# VISUAL PROMPT TEMPLATES — per-sentence visual suggestions
# Maps visual_intent + emotion to 3 diverse stock search prompts
# ============================================================
VISUAL_PROMPT_MAP = {
    ("abstract_tension", "shock"): [
        "dramatic red warning light flashing dark room",
        "abstract glitch distortion digital",
        "dark corridor emergency lights cinematic",
    ],
    ("abstract_tension", "tension"): [
        "dark moody atmospheric fog",
        "clock ticking extreme close up",
        "shadowy figure silhouette dramatic lighting",
    ],
    ("tech_ui", "shock"): [
        "futuristic holographic alert system",
        "server room red warning lights",
        "cybersecurity breach screen visualization",
    ],
    ("tech_ui", "neutral"): [
        "modern dashboard data analytics screen",
        "AI neural network visualization blue",
        "typing on laptop code editor close up",
    ],
    ("data_visualization", "urgency"): [
        "stock market crash red graph dramatic",
        "financial data scrolling screen fast",
        "numbers falling digital rain cinematic",
    ],
    ("nature", "curiosity"): [
        "deep ocean bioluminescence dark water",
        "macro shot crystal formation geological",
        "aurora borealis timelapse night sky",
    ],
    ("nature", "reveal"): [
        "sunrise over mountain peak golden hour",
        "underwater cave light beam dramatic",
        "volcano eruption aerial dramatic cinematic",
    ],
    ("urban", "dramatic"): [
        "city skyline storm clouds dramatic",
        "neon signs rain night cinematic",
        "aerial traffic timelapse city night",
    ],
    ("luxury", "reveal"): [
        "luxury penthouse panoramic view sunset",
        "supercar driving coastal road aerial",
        "private yacht ocean aerial golden hour",
    ],
    ("money", "shock"): [
        "gold vault door opening cinematic",
        "cash money scattered dramatic lighting",
        "cryptocurrency chart explosion green red",
    ],
    ("building", "tension"): [
        "government building dramatic low angle",
        "courthouse steps cinematic fog",
        "corporate tower reflecting storm clouds",
    ],
    ("cinematic_dark", "dramatic"): [
        "dark cinematic smoke swirling slow motion",
        "dramatic spotlight single beam darkness",
        "abstract dark liquid flowing cinematic",
    ],
    ("timeline", "curiosity"): [
        "ancient manuscript close up candlelight",
        "hourglass sand falling macro dramatic",
        "old photographs fading memory effect",
    ],
    ("document", "neutral"): [
        "legal document signing close up dramatic",
        "newspaper headline zoom dramatic",
        "official papers desk professional lighting",
    ],
}

# Default visual prompts when no specific mapping exists
DEFAULT_VISUAL_PROMPTS = {
    "shock": ["dramatic impact flash dark", "abstract motion blur intense", "close up eye reaction dramatic"],
    "tension": ["dark atmospheric fog corridor", "ticking clock mechanism macro", "shadow light contrast cinematic"],
    "reveal": ["light breaking through darkness", "curtain opening dramatic", "door opening bright light"],
    "neutral": ["abstract soft motion background", "clean modern interior", "atmospheric clouds calm"],
    "dramatic": ["cinematic slow motion particles", "epic landscape aerial dramatic", "storm clouds timelapse"],
    "curiosity": ["mysterious light fog forest", "keyhole light dramatic", "question mark abstract neon"],
    "urgency": ["fast motion blur city", "countdown timer dramatic", "emergency lights flashing"],
}

SCENE_PLANNER_PROMPT = """Break this narration into 4-6 cinematic scenes for a YouTube Short.

NARRATION:
{narration}

For EACH scene output a JSON object with these exact fields:
- "scene_id": number (1, 2, 3...)
- "narration": the exact narration text for this scene (8-15 words, 3-6 seconds)
- "emotion": one of: shock, tension, reveal, neutral, dramatic, curiosity, urgency
- "energy": number 1-5 (1=calm, 5=intense)
- "visual_intent": symbolic description. Examples: "abstract_tension", "tech_ui", "nature", "urban", "luxury", "data_visualization", "cinematic_dark", "document", "building"
- "camera_style": one of: slow_push_in, fast_punch_in, pull_out, lateral_pan, static_tension, slow_drift, rack_focus, dolly_zoom
- "shot_type": one of: wide, macro, motion, abstract, detail
- "emphasis_words": list of 1-3 power words to highlight in captions
- "cut_style": one of: hard, smash, slow
- "visual_prompts": object with 3 search strategies:
  - "literal": direct visual description of the topic
  - "emotional": human emotion or reaction matching the mood
  - "symbolic": abstract/metaphorical visual representation

RULES:
- First scene MUST have energy >= 4 (it's the hook — movement, bright contrast, strong subject)
- Last scene MUST have emotion = "curiosity" or "tension" (open loop for retention)
- NEVER use politician names or person names in any field
- Each scene = 3-6 seconds of speech (8-15 words)
- Scenes MUST contrast visually: alternate shot_type (wide → macro → motion → abstract)
- No two consecutive scenes should have the same camera_style
- Output ONLY a JSON array. No explanation. No markdown.

Output format:
[
  {{"scene_id": 1, "narration": "...", "emotion": "shock", "energy": 5, "visual_intent": "abstract_tension", "camera_style": "fast_punch_in", "shot_type": "macro", "emphasis_words": ["word1"], "cut_style": "hard", "visual_prompts": {{"literal": "AI robot interface screen", "emotional": "person shocked by computer", "symbolic": "neural network explosion digital"}}}},
  ...
]"""


def _deterministic_fallback(narration: str) -> List[Dict]:
    """Fallback: split by paragraphs with cinematic defaults and visual contrast."""
    paragraphs = [p.strip() for p in narration.strip().split("\n\n") if p.strip()]
    if len(paragraphs) <= 1:
        paragraphs = [p.strip() for p in narration.strip().split("\n") if p.strip()]
    if len(paragraphs) <= 1 and paragraphs:
        words = paragraphs[0].split()
        # Target 8-15 words per scene (3-6 seconds)
        chunk_size = 12
        paragraphs = [" ".join(words[i:i + chunk_size]) for i in range(0, len(words), chunk_size)]

    # Camera style rotation — no consecutive repeats
    camera_cycle = [
        "fast_punch_in", "slow_push_in", "lateral_pan",
        "slow_drift", "pull_out", "rack_focus",
    ]
    # Visual intent rotation for variety
    intent_cycle = [
        "abstract_tension", "tech_ui", "cinematic_dark",
        "urban", "nature", "data_visualization",
    ]

    scenes = []
    for i, para in enumerate(paragraphs):
        wc = len(para.split())
        if wc == 0:
            continue

        # Emotion assignment by position
        if i == 0:
            emotion, energy = "shock", 5
        elif i == len(paragraphs) - 1:
            emotion, energy = "curiosity", 4
        elif i == 1:
            emotion, energy = "tension", 3
        else:
            emotion, energy = "dramatic", 3

        # Extract emphasis words (longest content words)
        all_words = [w.strip(".,!?;:'\"") for w in para.split()]
        emphasis = sorted(all_words, key=len, reverse=True)[:2]

        # Visual contrast pattern — cycle shot types
        shot_type = SHOT_CONTRAST_CYCLE[i % len(SHOT_CONTRAST_CYCLE)]
        camera_style = camera_cycle[i % len(camera_cycle)]
        visual_intent = intent_cycle[i % len(intent_cycle)]

        # Enforce pacing: clamp duration to 3-6 seconds
        raw_duration = wc / 2.5
        duration = max(MIN_SCENE_DURATION, min(MAX_SCENE_DURATION, raw_duration))

        scenes.append({
            "scene_id": i + 1,
            "narration": para,
            "emotion": emotion,
            "energy": energy,
            "visual_intent": visual_intent,
            "camera_style": camera_style,
            "shot_type": shot_type,
            "emphasis_words": emphasis,
            "cut_style": "hard" if energy >= 4 else "slow",
            "word_count": wc,
            "estimated_duration": round(duration, 2),
        })

    return scenes


def _parse_llm_scenes(raw_output: str, narration: str) -> Optional[List[Dict]]:
    """Parse LLM JSON output into validated scene list."""
    raw = raw_output.strip()

    # Find JSON array in output
    start = raw.find("[")
    end = raw.rfind("]")
    if start == -1 or end == -1:
        return None

    json_str = raw[start:end + 1]

    try:
        scenes = json.loads(json_str)
    except json.JSONDecodeError:
        return None

    if not isinstance(scenes, list) or len(scenes) < 2:
        return None

    # Validate and sanitize each scene
    validated = []
    prev_camera = ""
    for scene in scenes:
        if not isinstance(scene, dict):
            continue
        if not scene.get("narration"):
            continue

        emotion = scene.get("emotion", "neutral")
        if emotion not in VALID_EMOTIONS:
            emotion = "neutral"

        energy = scene.get("energy", 3)
        try:
            energy = max(1, min(5, int(energy)))
        except (ValueError, TypeError):
            energy = 3

        cut_style = scene.get("cut_style", "hard")
        if cut_style not in VALID_CUT_STYLES:
            cut_style = "hard"

        # Validate camera_style
        camera_style = scene.get("camera_style", "slow_push_in")
        if camera_style not in VALID_CAMERA_STYLES:
            camera_style = "slow_push_in"
        # Avoid consecutive same camera
        if camera_style == prev_camera:
            alts = [c for c in VALID_CAMERA_STYLES if c != camera_style]
            camera_style = alts[len(validated) % len(alts)] if alts else camera_style

        # Validate shot_type with contrast cycling
        shot_type = scene.get("shot_type", "")
        valid_shots = set(SHOT_CONTRAST_CYCLE)
        if shot_type not in valid_shots:
            shot_type = SHOT_CONTRAST_CYCLE[len(validated) % len(SHOT_CONTRAST_CYCLE)]

        emphasis = scene.get("emphasis_words", [])
        if not isinstance(emphasis, list):
            emphasis = []
        emphasis = [str(w) for w in emphasis[:3]]

        wc = len(scene["narration"].split())

        # Enforce pacing: clamp duration to 3-6 seconds
        raw_duration = wc / 2.5
        duration = max(MIN_SCENE_DURATION, min(MAX_SCENE_DURATION, raw_duration))

        # Parse 3-tier visual_prompts from LLM
        llm_vp = scene.get("visual_prompts", {})
        if isinstance(llm_vp, dict):
            visual_prompts_3tier = {
                "literal": llm_vp.get("literal", ""),
                "emotional": llm_vp.get("emotional", ""),
                "symbolic": llm_vp.get("symbolic", ""),
            }
        else:
            visual_prompts_3tier = {"literal": "", "emotional": "", "symbolic": ""}

        validated.append({
            "scene_id": scene.get("scene_id", len(validated) + 1),
            "narration": scene["narration"],
            "emotion": emotion,
            "energy": energy,
            "visual_intent": scene.get("visual_intent", "cinematic_dark"),
            "camera_style": camera_style,
            "shot_type": shot_type,
            "emphasis_words": emphasis,
            "cut_style": cut_style,
            "word_count": wc,
            "estimated_duration": round(duration, 2),
            "visual_prompts_3tier": visual_prompts_3tier,
        })
        prev_camera = camera_style

    return validated if len(validated) >= 2 else None


def _generate_visual_prompts(scene: Dict) -> List[str]:
    """Generate 3 diverse visual search prompts for a scene based on intent + emotion."""
    intent = scene.get("visual_intent", "cinematic_dark")
    emotion = scene.get("emotion", "neutral")

    # Try specific mapping first
    key = (intent, emotion)
    if key in VISUAL_PROMPT_MAP:
        return list(VISUAL_PROMPT_MAP[key])

    # Try emotion-only fallback
    if emotion in DEFAULT_VISUAL_PROMPTS:
        return list(DEFAULT_VISUAL_PROMPTS[emotion])

    return ["abstract cinematic background dark", "atmospheric motion blur", "dramatic light shadow"]


class ScenePlannerWorker:
    """LLM-based scene planner with emotion/energy metadata and visual diversity."""

    def __init__(self):
        self.diversity_engine = VisualDiversityEngine()
        self.metrics = {
            "scripts_processed": 0,
            "scenes_created": 0,
            "llm_successes": 0,
            "llm_failures": 0,
            "fallback_used": 0,
            "total_duration": 0.0,
            "planning_times": [],
            "visual_prompts_generated": 0,
        }

    def plan_scenes(self, narration: str, model: str = "mistral:latest",
                    max_retries: int = 2) -> List[Dict]:
        """Plan scenes for a narration using LLM with retry and deterministic fallback."""
        prompt = SCENE_PLANNER_PROMPT.format(narration=narration[:2000])

        for attempt in range(1, max_retries + 1):
            start = time.time()
            try:
                raw = OllamaClient.generate(prompt, model=model, timeout=180)
                elapsed = round(time.time() - start, 2)
                self.metrics["planning_times"].append(elapsed)

                if raw:
                    scenes = _parse_llm_scenes(raw, narration)
                    if scenes:
                        self.metrics["llm_successes"] += 1
                        return scenes

                logger.warning("LLM returned empty/unparseable response (attempt %d/%d)",
                               attempt, max_retries)
            except Exception as exc:
                logger.error("Scene planner LLM attempt %d/%d failed: %s", attempt, max_retries, exc)

            if attempt < max_retries:
                logger.info("Retrying LLM scene planning (attempt %d)...", attempt + 1)
                time.sleep(3)

        self.metrics["llm_failures"] += 1
        self.metrics["fallback_used"] += 1
        logger.info("Using deterministic fallback for scene planning")
        return _deterministic_fallback(narration)

    def process_scripts(self, scripts_dir: str, output_dir: str) -> int:
        """Process all cleaned script files and output scene plans with visual diversity."""
        os.makedirs(output_dir, exist_ok=True)
        total = 0

        script_files = glob.glob(os.path.join(scripts_dir, "*_scripts.json"))
        if not script_files:
            print("No scripts found for scene planning.")
            return 0

        for script_file in script_files:
            channel_id = os.path.basename(script_file).replace("_scripts.json", "")

            try:
                with open(script_file) as f:
                    scripts = json.load(f)
            except (json.JSONDecodeError, IOError) as exc:
                logger.error("Failed to read %s: %s", script_file, exc)
                continue

            if not isinstance(scripts, list):
                continue

            channel_scenes = []
            for si, script in enumerate(scripts):
                script_body = (script.get("script_body") or "").strip()
                if not script_body:
                    continue

                title_preview = (script.get("title") or "Unknown")[:50]
                print(f"  [{channel_id.upper()}] Planning {si+1}/{len(scripts)}: {title_preview}...",
                      flush=True)

                scenes = self.plan_scenes(script_body)
                if not scenes:
                    continue

                # Generate unique video ID for diversity tracking
                title = script.get("title", "Unknown")
                safe_title = "".join(c if c.isalnum() or c in "-_ " else "" for c in title)[:50]
                video_id = f"{channel_id}_{safe_title}"

                # Apply visual diversity — rotate styles, diversify intents
                scenes = self.diversity_engine.diversify_scenes(
                    scenes, channel_id.upper(), video_id
                )

                # Generate per-scene visual prompts (3 per scene)
                for scene in scenes:
                    scene["visual_prompts"] = _generate_visual_prompts(scene)
                    self.metrics["visual_prompts_generated"] += len(scene["visual_prompts"])

                total_duration = sum(s["estimated_duration"] for s in scenes)

                scene_plan = {
                    "channel_id": channel_id.upper(),
                    "title": title,
                    "total_scenes": len(scenes),
                    "total_estimated_duration": round(total_duration, 2),
                    "scenes": scenes,
                    "source_script": script,
                    "planned_at": datetime.now().isoformat(),
                }

                channel_scenes.append(scene_plan)
                self.metrics["scripts_processed"] += 1
                self.metrics["scenes_created"] += len(scenes)
                self.metrics["total_duration"] += total_duration

            if channel_scenes:
                outfile = os.path.join(output_dir, f"{channel_id}_scenes.json")
                with open(outfile, "w") as f:
                    json.dump(channel_scenes, f, indent=2)
                scene_count = sum(len(s["scenes"]) for s in channel_scenes)
                print(f"  Planned {len(channel_scenes)} scripts -> {scene_count} scenes for {channel_id.upper()}")
                total += len(channel_scenes)

        return total

    def log_metrics(self):
        print("\n--- Scene Planner Metrics ---")
        for key, value in self.metrics.items():
            if key == "planning_times" and value:
                print(f"  avg_planning_time: {sum(value) / len(value):.2f}s")
            elif key != "planning_times":
                print(f"{key}: {value}")
        print("-----------------------------\n")
        self.diversity_engine.log_metrics()


def main():
    from app.utils.pipeline_logger import StageLogger

    slog = StageLogger("scene_planner")

    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    DATE_STR = datetime.now().strftime("%Y%m%d")

    # Read from cleaned scripts (output of script_cleaner)
    SCRIPTS_DIR = os.path.join(BASE_DIR, "data", "topic_scripts_clean", DATE_STR)

    # Fallback to uncleaned if clean dir doesn't exist
    if not os.path.exists(SCRIPTS_DIR):
        SCRIPTS_DIR = os.path.join(BASE_DIR, "data", "topic_scripts", DATE_STR)

    SCENES_DIR = os.path.join(BASE_DIR, "data", "scene_plans", DATE_STR)

    if not os.path.exists(SCRIPTS_DIR):
        print("No scripts directory found. Skipping scene planning.")
        slog.warning("No scripts directory found", suggestion="Check script_cleaner output")
        slog.finish(success=False)
        return

    print(f"Planning scenes from: {SCRIPTS_DIR}")

    worker = ScenePlannerWorker()
    total = worker.process_scripts(SCRIPTS_DIR, SCENES_DIR)
    worker.log_metrics()

    # Emit structured metrics
    for k, v in worker.metrics.items():
        if k == "planning_times" and v:
            slog.metric("avg_planning_time_s", round(sum(v) / len(v), 2))
        elif k != "planning_times":
            slog.metric(k, v)

    # Diversity engine metrics
    for k, v in worker.diversity_engine.metrics.items():
        slog.metric(k, v)

    if worker.metrics["fallback_used"] > worker.metrics["llm_successes"]:
        slog.warning("LLM fallback rate > 50%",
                     suggestion="Check Ollama availability, increase timeout, or use faster model")
    if worker.metrics.get("scenes_created", 0) == 0:
        slog.error("No scenes created", detail="All scripts failed scene planning")

    if total == 0:
        print("No scene plans created.")
        slog.finish(success=False)
    else:
        print(f"\n✅ Scene plans created: {total}")
        slog.finish(success=True)
    print(f"Scene plans saved to: {SCENES_DIR}")


if __name__ == "__main__":
    main()
