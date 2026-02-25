import glob
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

ELEVENLABS_URL = "https://api.elevenlabs.io/v1/text-to-speech/{voice_id}"
DEFAULT_MODEL_ID = "eleven_multilingual_v2"
VOICE_SETTINGS = {
    "stability": 0.5,
    "similarity_boost": 0.75,
}


class VoiceGeneratorWorker:
    def __init__(self) -> None:
        load_dotenv()
        self.api_key = os.environ.get("ELEVENLABS_API_KEY")
        if not self.api_key:
            raise EnvironmentError("Missing ELEVENLABS_API_KEY environment variable.")

        self.voice_id = os.environ.get("ELEVENLABS_VOICE_ID")
        if not self.voice_id:
            raise EnvironmentError("Missing ELEVENLABS_VOICE_ID environment variable.")

        self.api_url = ELEVENLABS_URL.format(voice_id=self.voice_id)
        self.metrics: Dict[str, Any] = {
            "topics_processed": 0,
            "scenes_total": 0,
            "scenes_generated": 0,
            "scenes_skipped_existing": 0,
            "scenes_failed": 0,
            "scene_generation_times": [],
            "total_estimated_duration_sec": 0.0,
        }

    def _load_scene_plan(self, path: str) -> Optional[Dict[str, Any]]:
        try:
            with open(path) as f:
                return json.load(f)
        except Exception as exc:
            logger.error("Failed to read scene plan %s: %s", path, exc)
            return None

    def _synthesize_audio(self, narration: str, retries: int = 1) -> Optional[bytes]:
        headers = {
            "xi-api-key": self.api_key,
            "Content-Type": "application/json",
            "Accept": "audio/mpeg",
        }
        payload = {
            "text": narration,
            "model_id": DEFAULT_MODEL_ID,
            "voice_settings": VOICE_SETTINGS,
        }

        for attempt in range(retries + 1):
            try:
                response = requests.post(
                    self.api_url,
                    headers=headers,
                    json=payload,
                    timeout=60,
                )
            except requests.RequestException as exc:
                logger.error("ElevenLabs request failed: %s", exc)
                response = None

            if response and response.status_code == 200 and response.content:
                return response.content

            if response is not None:
                logger.error(
                    "ElevenLabs returned %s: %s",
                    response.status_code,
                    response.text[:200],
                )

            if attempt < retries:
                time.sleep(2)

        return None

    def _topic_output_dir(self, base_output: str, topic_id: str) -> str:
        topic_dir = os.path.join(base_output, topic_id)
        os.makedirs(topic_dir, exist_ok=True)
        return topic_dir

    def run(self) -> int:
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        scene_plan_dir = os.path.join(base_dir, "data", "scene_plans")
        audio_root = os.path.join(base_dir, "data", "audio")
        os.makedirs(audio_root, exist_ok=True)

        scene_files = sorted(glob.glob(os.path.join(scene_plan_dir, "*.json")))
        if not scene_files:
            print("No scene plans found for voice generation.")
            return 0

        print(f"Found {len(scene_files)} scene plans...")

        for plan_path in scene_files:
            plan = self._load_scene_plan(plan_path)
            if not plan:
                continue

            topic_id = str(plan.get("topic_id") or os.path.splitext(os.path.basename(plan_path))[0])
            scenes: List[Dict[str, Any]] = plan.get("scenes") or []
            if not scenes:
                logger.warning("Scene plan %s has no scenes; skipping", plan_path)
                continue

            self.metrics["topics_processed"] += 1
            topic_duration = 0.0
            for scene in scenes:
                dur = scene.get("estimated_duration_sec")
                if isinstance(dur, (int, float)):
                    topic_duration += float(dur)

            self.metrics["total_estimated_duration_sec"] += topic_duration

            topic_dir = self._topic_output_dir(audio_root, topic_id)
            logger.info("Generating audio for topic %s (%d scenes)", topic_id, len(scenes))

            for idx, scene in enumerate(scenes, start=1):
                narration = (scene.get("narration") or "").strip()
                if not narration:
                    logger.warning("Scene %s in topic %s has empty narration; skipping", scene.get("scene_id"), topic_id)
                    continue

                scene_id = scene.get("scene_id") or idx
                filename = f"scene_{str(scene_id).zfill(2)}.mp3"
                output_path = os.path.join(topic_dir, filename)

                self.metrics["scenes_total"] += 1

                if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
                    self.metrics["scenes_skipped_existing"] += 1
                    continue

                start_time = time.time()
                audio_bytes = self._synthesize_audio(narration, retries=1)
                elapsed = round(time.time() - start_time, 2)

                if not audio_bytes:
                    self.metrics["scenes_failed"] += 1
                    logger.error("Failed to generate audio for topic %s scene %s", topic_id, scene_id)
                    continue

                with open(output_path, "wb") as audio_file:
                    audio_file.write(audio_bytes)

                if os.path.getsize(output_path) == 0:
                    self.metrics["scenes_failed"] += 1
                    logger.error("Empty audio file for topic %s scene %s", topic_id, scene_id)
                    continue

                self.metrics["scenes_generated"] += 1
                self.metrics["scene_generation_times"].append(elapsed)
                logger.info(
                    "Generated audio for topic %s scene %s in %ss",
                    topic_id,
                    scene_id,
                    elapsed,
                )

            if topic_duration:
                logger.info("Estimated total audio duration for topic %s: %.1fs", topic_id, topic_duration)

        self._log_metrics()
        print(f"Audio files generated: {self.metrics['scenes_generated']}")
        return self.metrics["scenes_generated"]

    def _log_metrics(self) -> None:
        print("\n--- Voice Generator Metrics ---")
        for key, value in self.metrics.items():
            if key == "scene_generation_times" and value:
                avg_time = sum(value) / len(value)
                print(f"  avg_generation_time: {avg_time:.2f}s")
                print(f"  total_scenes_attempted: {len(value)}")
                print(f"  min_time: {min(value):.2f}s")
                print(f"  max_time: {max(value):.2f}s")
            elif key not in {"scene_generation_times"}:
                print(f"{key}: {value}")
        print("--------------------------------\n")


if __name__ == "__main__":
    worker = VoiceGeneratorWorker()
    worker.run()
