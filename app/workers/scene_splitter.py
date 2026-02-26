"""
Deterministic Scene Splitter — splits scripts into scenes by paragraph.

No LLM calls. Pure Python. Instant execution (<0.01s per script).
Each paragraph in the script becomes one scene.
Duration estimated at: words / 2.5 = seconds.
"""

import os
import json
import glob
import logging
from datetime import datetime
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


def split_script(script_text: str) -> List[Dict]:
    """
    Split a script into scenes by double-newline paragraph breaks.

    Each paragraph becomes one scene with:
    - scene_number (int, 1-indexed)
    - text (str)
    - word_count (int)
    - estimated_duration (float, seconds at 2.5 words/sec)
    """
    # Split by double newline, or single newline for short scripts
    paragraphs = [p.strip() for p in script_text.strip().split("\n\n") if p.strip()]

    # Fallback: if no double-newline splits, try single newline
    if len(paragraphs) <= 1:
        paragraphs = [p.strip() for p in script_text.strip().split("\n") if p.strip()]

    # Fallback: if still just 1 block, split by sentences (every ~30 words)
    if len(paragraphs) <= 1 and paragraphs:
        words = paragraphs[0].split()
        chunk_size = 30
        paragraphs = []
        for i in range(0, len(words), chunk_size):
            chunk = " ".join(words[i : i + chunk_size])
            paragraphs.append(chunk)

    scenes = []
    for i, para in enumerate(paragraphs):
        word_count = len(para.split())
        if word_count == 0:
            continue
        scenes.append(
            {
                "scene_number": i + 1,
                "text": para,
                "word_count": word_count,
                "estimated_duration": round(word_count / 2.5, 2),
            }
        )

    return scenes


class SceneSplitterWorker:
    """Processes all generated scripts and creates deterministic scene plans."""

    def __init__(self):
        self.metrics = {
            "scripts_processed": 0,
            "scenes_created": 0,
            "scripts_skipped": 0,
            "total_duration": 0.0,
        }

    def process_scripts(self, scripts_dir: str, output_dir: str) -> int:
        """Process all script JSON files and output scene plans."""
        os.makedirs(output_dir, exist_ok=True)
        total = 0

        # Find all *_scripts.json files
        script_files = glob.glob(f"{scripts_dir}/*_scripts.json")
        if not script_files:
            print("No scripts found for scene splitting.")
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
                    self.metrics["scripts_skipped"] += 1
                    continue

                scenes = split_script(script_body)
                if not scenes:
                    self.metrics["scripts_skipped"] += 1
                    continue

                total_duration = sum(s["estimated_duration"] for s in scenes)

                scene_plan = {
                    "channel_id": channel_id.upper(),
                    "title": script.get("title", "Unknown"),
                    "total_scenes": len(scenes),
                    "total_estimated_duration": round(total_duration, 2),
                    "scenes": scenes,
                    "source_script": script,
                    "split_at": datetime.now().isoformat(),
                }

                channel_scenes.append(scene_plan)
                self.metrics["scripts_processed"] += 1
                self.metrics["scenes_created"] += len(scenes)
                self.metrics["total_duration"] += total_duration

            if channel_scenes:
                outfile = os.path.join(output_dir, f"{channel_id}_scenes.json")
                with open(outfile, "w") as f:
                    json.dump(channel_scenes, f, indent=2)
                print(f"  Split {len(channel_scenes)} scripts → {sum(len(s['scenes']) for s in channel_scenes)} scenes for {channel_id.upper()}")
                total += len(channel_scenes)

        return total

    def log_metrics(self):
        print("\n--- Scene Splitter Metrics ---")
        for key, value in self.metrics.items():
            print(f"{key}: {value}")
        print("-----------------------------\n")


def main():
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    DATE_STR = datetime.now().strftime("%Y%m%d")
    SCRIPTS_DIR = os.path.join(BASE_DIR, "data", "topic_scripts", DATE_STR)
    SCENES_DIR = os.path.join(BASE_DIR, "data", "scene_plans", DATE_STR)

    if not os.path.exists(SCRIPTS_DIR):
        print("No scripts directory found for today. Skipping scene splitting.")
        return

    print(f"Splitting scripts from: {SCRIPTS_DIR}")

    worker = SceneSplitterWorker()
    total = worker.process_scripts(SCRIPTS_DIR, SCENES_DIR)
    worker.log_metrics()

    if total == 0:
        print("No scene plans created.")
    else:
        print(f"\n✅ Scene plans created: {total}")
    print(f"Scene plans saved to: {SCENES_DIR}")


if __name__ == "__main__":
    main()
