import glob
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from app.services.ollama_client import OllamaClient

logger = logging.getLogger(__name__)

WORDS_PER_MINUTE = 150
MIN_SCENES = 4
MAX_SCENES = 6
MIN_SCRIPT_WORDS = 120


@dataclass
class ScriptInput:
    topic_id: str
    title: str
    script_text: str


class ScenePlanner:
    def __init__(self) -> None:
        self.metrics: Dict[str, Any] = {
            "scripts_seen": 0,
            "scripts_skipped_short": 0,
            "scene_plans_created": 0,
            "total_scenes": 0,
            "total_estimated_duration": 0.0,
            "processing_times": [],
        }

    def _build_prompt(self, script: ScriptInput) -> str:
        system_prompt = "You are a professional YouTube cinematic editor."
        user_prompt = (
            "Split the following narration script into 4–6 cinematic scenes.\n\n"
            "For each scene:\n"
            "- assign scene_id\n"
            "- assign type (hook/context/escalation/insight/resolution/cta)\n"
            "- keep exact narration text (do not rewrite)\n"
            "- generate detailed cinematic visual_prompt\n"
            "- estimate duration in seconds (150 words per minute)\n\n"
            "If the script length is less than 120 words, return exactly: SKIP_SHORT_SCRIPT.\n\n"
            "Return valid JSON only.\n\n"
            "SCRIPT:\n\"\"\"\n"
            f"{script.script_text}\n"
            "\"\"\"\n"
        )
        # Simple chat-style formatting for current Ollama usage
        return f"SYSTEM:\n{system_prompt}\n\nUSER:\n{user_prompt}"

    def _extract_json(self, raw: str) -> Optional[Dict[str, Any]]:
        raw = raw.strip()
        if not raw:
            return None

        # Direct parse first
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            pass

        # Try to extract JSON object from surrounding text
        start = raw.find("{")
        end = raw.rfind("}")
        if start == -1 or end == -1 or end <= start:
            return None

        candidate = raw[start : end + 1]
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            return None

    def _validate_scene_plan(self, plan: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not isinstance(plan, dict):
            return None

        scenes = plan.get("scenes")
        if not isinstance(scenes, list) or not scenes:
            return None

        if not (MIN_SCENES <= len(scenes) <= MAX_SCENES):
            logger.warning("Scene plan has %d scenes (expected %d-%d)", len(scenes), MIN_SCENES, MAX_SCENES)

        total_duration = 0.0
        for scene in scenes:
            if not isinstance(scene, dict):
                return None

            if not scene.get("visual_prompt") or not isinstance(scene.get("visual_prompt"), str):
                return None
            if not scene.get("narration") or not isinstance(scene.get("narration"), str):
                return None

            dur = scene.get("estimated_duration_sec")
            if not isinstance(dur, (int, float)) or dur <= 0:
                return None
            total_duration += float(dur)

        plan["total_scenes"] = len(scenes)
        plan["total_estimated_duration_sec"] = round(total_duration, 2)
        return plan

    def plan_for_script(self, script: ScriptInput) -> Optional[Dict[str, Any]]:
        self.metrics["scripts_seen"] += 1

        # Pre-check length to avoid wasting LLM calls on obviously short scripts
        word_count = len(script.script_text.split())
        if word_count < MIN_SCRIPT_WORDS:
            self.metrics["scripts_skipped_short"] += 1
            logger.info(
                "Skipping scene planning for '%s' — script has %d words (< %d)",
                script.title[:60],
                word_count,
                MIN_SCRIPT_WORDS,
            )
            return None

        prompt = self._build_prompt(script)
        start = time.time()
        raw = OllamaClient.generate_with_retry(prompt, model="mistral:latest", timeout=180, retries=1)
        duration = round(time.time() - start, 2)
        self.metrics["processing_times"].append(duration)

        if not raw:
            logger.error("Scene planner LLM returned empty output for '%s'", script.title[:60])
            return None

        if "SKIP_SHORT_SCRIPT" in raw.upper():
            self.metrics["scripts_skipped_short"] += 1
            logger.info("LLM returned SKIP_SHORT_SCRIPT for '%s'", script.title[:60])
            return None

        plan = self._extract_json(raw)
        if not plan:
            logger.error("Failed to parse JSON scene plan for '%s'", script.title[:60])
            return None

        # Ensure identity fields are present / correct
        plan.setdefault("topic_id", script.topic_id)
        plan.setdefault("title", script.title)

        validated = self._validate_scene_plan(plan)
        if not validated:
            logger.error("Scene plan validation failed for '%s'", script.title[:60])
            return None

        self.metrics["scene_plans_created"] += 1
        self.metrics["total_scenes"] += validated.get("total_scenes", 0)
        self.metrics["total_estimated_duration"] += validated.get("total_estimated_duration_sec", 0.0)
        logger.info(
            "Created scene plan for '%s' with %d scenes in %ss",
            script.title[:60],
            validated.get("total_scenes", 0),
            duration,
        )
        return validated

    def log_metrics(self) -> None:
        print("\n--- Scene Planner Metrics ---")
        for key, value in self.metrics.items():
            if key == "processing_times" and value:
                avg_time = sum(value) / len(value)
                print(f"  avg_processing_time: {avg_time:.2f}s")
                print(f"  total_processed: {len(value)}")
                print(f"  min_time: {min(value):.2f}s")
                print(f"  max_time: {max(value):.2f}s")
            elif key not in {"processing_times"}:
                print(f"{key}: {value}")
        if self.metrics.get("scene_plans_created"):
            avg_scenes = self.metrics["total_scenes"] / max(self.metrics["scene_plans_created"], 1)
            avg_duration = self.metrics["total_estimated_duration"] / max(self.metrics["scene_plans_created"], 1)
            print(f"  avg_scenes_per_plan: {avg_scenes:.2f}")
            print(f"  avg_plan_duration_sec: {avg_duration:.2f}")
        print("--------------------------------\n")


class ScenePlannerWorker:
    def __init__(self) -> None:
        self.planner = ScenePlanner()

    @staticmethod
    def _slugify(value: str) -> str:
        value = value.lower().strip()
        value = re.sub(r"[^a-z0-9]+", "-", value)
        value = re.sub(r"-+", "-", value).strip("-")
        return value or "topic"

    def _discover_input_scripts(self, base_dir: str) -> List[ScriptInput]:
        scripts: List[ScriptInput] = []

        generated_scripts_dir = os.path.join(base_dir, "data", "generated_scripts")
        topic_scripts_root = os.path.join(base_dir, "data", "topic_scripts")

        if os.path.isdir(generated_scripts_dir):
            json_files = sorted(glob.glob(os.path.join(generated_scripts_dir, "*.json")))
            if json_files:
                logger.info("ScenePlanner using generated_scripts as primary source (%d files)", len(json_files))
                for path in json_files:
                    with open(path) as f:
                        try:
                            payload = json.load(f)
                        except Exception as exc:
                            logger.error("Failed to read %s: %s", path, exc)
                            continue

                    if isinstance(payload, list):
                        iterable = payload
                    else:
                        iterable = [payload]

                    for idx, item in enumerate(iterable):
                        script = self._normalize_script(item, fallback_id=f"file-{os.path.basename(path)}-{idx}")
                        if script:
                            scripts.append(script)

                return scripts

        # Fallback: latest topic_scripts date directory
        if not os.path.isdir(topic_scripts_root):
            logger.warning("No script inputs found: neither generated_scripts nor topic_scripts exist")
            return scripts

        date_dirs = [
            d
            for d in os.listdir(topic_scripts_root)
            if os.path.isdir(os.path.join(topic_scripts_root, d))
        ]
        if not date_dirs:
            logger.warning("No dated folders found in topic_scripts")
            return scripts

        latest_dir = sorted(date_dirs)[-1]
        scripts_dir = os.path.join(topic_scripts_root, latest_dir)
        logger.info("ScenePlanner using topic_scripts from %s", scripts_dir)

        json_files = sorted(glob.glob(os.path.join(scripts_dir, "*_scripts.json")))
        for path in json_files:
            with open(path) as f:
                try:
                    payload = json.load(f)
                except Exception as exc:
                    logger.error("Failed to read %s: %s", path, exc)
                    continue

            if not isinstance(payload, list):
                continue

            for idx, item in enumerate(payload):
                script = self._normalize_script(item, fallback_id=f"{os.path.basename(path)}-{idx}")
                if script:
                    scripts.append(script)

        return scripts

    def _normalize_script(self, raw: Dict[str, Any], fallback_id: str) -> Optional[ScriptInput]:
        if not isinstance(raw, dict):
            return None

        title = (raw.get("title") or raw.get("topic_title") or "Untitled").strip()
        script_text = (
            raw.get("script_text")
            or raw.get("script_body")
            or raw.get("script")
            or ""
        )
        script_text = str(script_text).strip()
        if not script_text:
            return None

        topic_id = (
            raw.get("topic_id")
            or raw.get("id")
            or raw.get("topicId")
            or (raw.get("source_topic") or {}).get("id")
            or self._slugify(title)
        )
        topic_id = str(topic_id)

        return ScriptInput(topic_id=topic_id, title=title, script_text=script_text)

    def run(self) -> int:
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        scene_plans_dir = os.path.join(base_dir, "data", "scene_plans")
        os.makedirs(scene_plans_dir, exist_ok=True)

        scripts = self._discover_input_scripts(base_dir)
        if not scripts:
            print("No scripts found for scene planning.")
            return 0

        print(f"Found {len(scripts)} scripts for scene planning...")

        created = 0
        for script in scripts:
            outfile = os.path.join(scene_plans_dir, f"{script.topic_id}.json")
            if os.path.exists(outfile):
                # Idempotent: do not regenerate existing plans
                continue

            plan = self.planner.plan_for_script(script)
            if not plan:
                continue

            with open(outfile, "w") as f:
                json.dump(plan, f, indent=2)
            created += 1

        self.planner.log_metrics()
        print(f"Scene plans created: {created}")
        return created


if __name__ == "__main__":
    worker = ScenePlannerWorker()
    worker.run()
