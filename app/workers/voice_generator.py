"""
Voice Generator — converts scene narration text to audio using ElevenLabs TTS.

Reads scene plans from data/scene_plans/{date}/ (matching scene_splitter output).
Generates MP3 audio files per scene using ElevenLabs API.
"""

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

    def _load_scene_plans(self, path: str) -> List[Dict[str, Any]]:
        """Load scene plans from a JSON file (may be a list of plans)."""
        try:
            with open(path) as f:
                data = json.load(f)
            # scene_splitter outputs a list of scene plans per channel
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                return [data]
            return []
        except Exception as exc:
            logger.error("Failed to read scene plan %s: %s", path, exc)
            return []

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
            response = None
            try:
                response = requests.post(
                    self.api_url,
                    headers=headers,
                    json=payload,
                    timeout=60,
                )
            except requests.ConnectionError as exc:
                print(f"    ❌ CONNECTION ERROR: Cannot reach ElevenLabs API: {exc}")
                logger.error("ElevenLabs connection failed: %s", exc)
            except requests.Timeout:
                print(f"    ❌ TIMEOUT: ElevenLabs did not respond within 60s")
                logger.error("ElevenLabs request timed out")
            except requests.RequestException as exc:
                print(f"    ❌ REQUEST ERROR: {type(exc).__name__}: {exc}")
                logger.error("ElevenLabs request failed: %s", exc)

            if response is not None:
                if response.status_code == 200 and response.content and len(response.content) > 100:
                    return response.content

                # Log the ACTUAL error with full detail
                status = response.status_code
                body = response.text[:300]
                print(f"    ❌ ElevenLabs HTTP {status}: {body}")
                logger.error("ElevenLabs returned %s: %s", status, body)

                if status == 401:
                    print("    🔑 Invalid API key — check ELEVENLABS_API_KEY in .env")
                elif status == 404:
                    print(f"    🔍 Voice ID '{self.voice_id}' not found — check ELEVENLABS_VOICE_ID")
                elif status == 422:
                    print("    📝 Invalid request payload")
                elif status == 429:
                    print("    ⏳ Rate limited or quota exceeded")

            if attempt < retries:
                print(f"    Retrying in 2s... (attempt {attempt + 1}/{retries})")
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

        # Find scene plan files — check both flat and date-subdirectory structures
        scene_files = sorted(glob.glob(os.path.join(scene_plan_dir, "*.json")))
        # Also check subdirectories (scene_splitter writes to scene_plans/{date}/)
        scene_files += sorted(glob.glob(os.path.join(scene_plan_dir, "*", "*.json")))

        if not scene_files:
            print("No scene plans found for voice generation.")
            return 0

        print(f"Found {len(scene_files)} scene plan files...")

        for plan_path in scene_files:
            plans = self._load_scene_plans(plan_path)

            for plan in plans:
                if not isinstance(plan, dict):
                    continue

                # Build topic ID from plan data
                title = plan.get("title", "unknown")
                channel = plan.get("channel_id", "XX")
                # Create a clean filesystem-safe topic ID
                safe_title = "".join(c if c.isalnum() or c in "-_ " else "" for c in title)[:50].strip().replace(" ", "_")
                topic_id = f"{channel}_{safe_title}"

                # Support both scene_splitter output format ("text") and
                # legacy scene_planner format ("narration")
                scenes: List[Dict[str, Any]] = plan.get("scenes") or []
                if not scenes:
                    logger.warning("Scene plan '%s' has no scenes; skipping", title[:60])
                    continue

                self.metrics["topics_processed"] += 1
                topic_duration = 0.0
                for scene in scenes:
                    # Support both key names
                    dur = (
                        scene.get("estimated_duration_sec")
                        or scene.get("estimated_duration")
                        or 0
                    )
                    if isinstance(dur, (int, float)):
                        topic_duration += float(dur)

                self.metrics["total_estimated_duration_sec"] += topic_duration

                topic_dir = self._topic_output_dir(audio_root, topic_id)
                logger.info("Generating audio for topic '%s' (%d scenes)", title[:60], len(scenes))
                print(f"  Generating audio for '{title[:60]}' ({len(scenes)} scenes)...")

                for idx, scene in enumerate(scenes, start=1):
                    # Support both "narration" (legacy) and "text" (scene_splitter) keys
                    narration = (
                        scene.get("narration")
                        or scene.get("text")
                        or ""
                    ).strip()
                    if not narration:
                        logger.warning("Scene %d in '%s' has empty text; skipping", idx, title[:60])
                        continue

                    scene_num = scene.get("scene_number") or scene.get("scene_id") or idx
                    filename = f"scene_{str(scene_num).zfill(2)}.mp3"
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
                        logger.error("Failed to generate audio for '%s' scene %s", title[:60], scene_num)
                        continue

                    with open(output_path, "wb") as audio_file:
                        audio_file.write(audio_bytes)

                    if os.path.getsize(output_path) == 0:
                        self.metrics["scenes_failed"] += 1
                        logger.error("Empty audio file for '%s' scene %s", title[:60], scene_num)
                        continue

                    self.metrics["scenes_generated"] += 1
                    self.metrics["scene_generation_times"].append(elapsed)
                    logger.info(
                        "Generated audio for '%s' scene %s in %ss",
                        title[:60], scene_num, elapsed,
                    )

                if topic_duration:
                    logger.info("Estimated total audio duration for '%s': %.1fs", title[:60], topic_duration)

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
