"""
Scene Planner — LLM-based scene planner with emotion, energy, and visual intent.

Replaces the deterministic scene_splitter. Each scene gets rich metadata
for the rhythm engine, visual intent engine, and caption engine.

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

logger = logging.getLogger(__name__)

# Valid values for scene metadata
VALID_EMOTIONS = {"shock", "tension", "reveal", "neutral", "dramatic", "curiosity", "urgency"}
VALID_CUT_STYLES = {"hard", "smash", "slow"}

SCENE_PLANNER_PROMPT = """Break this narration into 4-6 short scenes for a YouTube Short.

NARRATION:
{narration}

For EACH scene output a JSON object with these exact fields:
- "scene_id": number (1, 2, 3...)
- "narration": the exact narration text for this scene (5-12 seconds of speech)
- "emotion": one of: shock, tension, reveal, neutral, dramatic, curiosity, urgency
- "energy": number 1-5 (1=calm, 5=intense)
- "visual_intent": what to show visually. Use SYMBOLIC descriptions, never person names.
  Examples: "abstract_tension", "document", "crowd_reaction", "building", "tech_ui", "luxury", "nature", "urban", "data_visualization", "cinematic_dark"
- "emphasis_words": list of 1-3 key words to highlight in captions
- "cut_style": one of: hard, smash, slow

RULES:
- First scene must have energy >= 4 (it's the hook)
- Last scene must have emotion = "curiosity" or "tension" (open loop)
- NEVER use politician names or person names in visual_intent
- Each narration chunk should be 15-40 words (5-12 seconds)
- Output ONLY a JSON array. No explanation. No markdown.

Output format:
[
  {{"scene_id": 1, "narration": "...", "emotion": "shock", "energy": 5, "visual_intent": "abstract_tension", "emphasis_words": ["word1"], "cut_style": "hard"}},
  ...
]"""


def _deterministic_fallback(narration: str) -> List[Dict]:
    """Fallback: split by paragraphs with basic emotion guessing."""
    paragraphs = [p.strip() for p in narration.strip().split("\n\n") if p.strip()]
    if len(paragraphs) <= 1:
        paragraphs = [p.strip() for p in narration.strip().split("\n") if p.strip()]
    if len(paragraphs) <= 1 and paragraphs:
        words = paragraphs[0].split()
        chunk_size = 25
        paragraphs = [" ".join(words[i:i + chunk_size]) for i in range(0, len(words), chunk_size)]

    scenes = []
    for i, para in enumerate(paragraphs):
        wc = len(para.split())
        if wc == 0:
            continue

        # Basic emotion assignment by position
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

        scenes.append({
            "scene_id": i + 1,
            "narration": para,
            "emotion": emotion,
            "energy": energy,
            "visual_intent": "abstract_tension" if i == 0 else "cinematic_dark",
            "emphasis_words": emphasis,
            "cut_style": "hard" if energy >= 4 else "slow",
            "word_count": wc,
            "estimated_duration": round(wc / 2.5, 2),
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

        emphasis = scene.get("emphasis_words", [])
        if not isinstance(emphasis, list):
            emphasis = []
        emphasis = [str(w) for w in emphasis[:3]]

        wc = len(scene["narration"].split())

        validated.append({
            "scene_id": scene.get("scene_id", len(validated) + 1),
            "narration": scene["narration"],
            "emotion": emotion,
            "energy": energy,
            "visual_intent": scene.get("visual_intent", "cinematic_dark"),
            "emphasis_words": emphasis,
            "cut_style": cut_style,
            "word_count": wc,
            "estimated_duration": round(wc / 2.5, 2),
        })

    return validated if len(validated) >= 2 else None


class ScenePlannerWorker:
    """LLM-based scene planner with emotion/energy metadata."""

    def __init__(self):
        self.metrics = {
            "scripts_processed": 0,
            "scenes_created": 0,
            "llm_successes": 0,
            "llm_failures": 0,
            "fallback_used": 0,
            "total_duration": 0.0,
            "planning_times": [],
        }

    def plan_scenes(self, narration: str, model: str = "mistral:latest") -> List[Dict]:
        """Plan scenes for a narration using LLM with deterministic fallback."""
        start = time.time()

        prompt = SCENE_PLANNER_PROMPT.format(narration=narration[:2000])

        try:
            raw = OllamaClient.generate(prompt, model=model, timeout=90)
            elapsed = round(time.time() - start, 2)
            self.metrics["planning_times"].append(elapsed)

            if raw:
                scenes = _parse_llm_scenes(raw, narration)
                if scenes:
                    self.metrics["llm_successes"] += 1
                    return scenes

        except Exception as exc:
            logger.error("Scene planner LLM failed: %s", exc)

        self.metrics["llm_failures"] += 1
        self.metrics["fallback_used"] += 1
        logger.info("Using deterministic fallback for scene planning")
        return _deterministic_fallback(narration)

    def process_scripts(self, scripts_dir: str, output_dir: str) -> int:
        """Process all cleaned script files and output scene plans."""
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
            for script in scripts:
                script_body = (script.get("script_body") or "").strip()
                if not script_body:
                    continue

                scenes = self.plan_scenes(script_body)
                if not scenes:
                    continue

                total_duration = sum(s["estimated_duration"] for s in scenes)

                scene_plan = {
                    "channel_id": channel_id.upper(),
                    "title": script.get("title", "Unknown"),
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


def main():
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
        return

    print(f"Planning scenes from: {SCRIPTS_DIR}")

    worker = ScenePlannerWorker()
    total = worker.process_scripts(SCRIPTS_DIR, SCENES_DIR)
    worker.log_metrics()

    if total == 0:
        print("No scene plans created.")
    else:
        print(f"\n✅ Scene plans created: {total}")
    print(f"Scene plans saved to: {SCENES_DIR}")


if __name__ == "__main__":
    main()
