"""
Voice Generator — converts scene narration text to audio using Edge-TTS.

Free, unlimited, no API key required. Uses Microsoft Edge's TTS service.
Reads scene plans from data/scene_plans/{date}/ (matching scene_splitter output).
Generates MP3 audio files per scene.
"""

import asyncio
import glob
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# High-quality English voices for YouTube narration
# Guy = deep male news voice, Jenny = clear female narrator
VOICE_MAP = {
    "C1": "en-US-GuyNeural",       # Tech/AI — authoritative male
    "C2": "en-US-JennyNeural",     # Finance — clear female
    "C3": "en-GB-RyanNeural",      # Science — British male
    "C4": "en-US-AriaNeural",      # Lifestyle — warm female
    "C5": "en-US-DavisNeural",     # Productivity — calm male
    "default": "en-US-GuyNeural",
}


class VoiceGeneratorWorker:
    def __init__(self) -> None:
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
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                return [data]
            return []
        except Exception as exc:
            logger.error("Failed to read scene plan %s: %s", path, exc)
            return []

    async def _synthesize_audio(self, text: str, voice: str, output_path: str) -> bool:
        """Generate audio using edge-tts."""
        try:
            import edge_tts

            communicate = edge_tts.Communicate(text, voice)
            await communicate.save(output_path)

            # Verify file was created and has content
            if os.path.exists(output_path) and os.path.getsize(output_path) > 100:
                return True
            else:
                print(f"    ⚠️  File created but too small: {os.path.getsize(output_path) if os.path.exists(output_path) else 0} bytes")
                return False

        except Exception as exc:
            print(f"    ❌ TTS Error: {type(exc).__name__}: {exc}")
            logger.error("Edge-TTS failed: %s", exc)
            return False

    def _topic_output_dir(self, base_output: str, topic_id: str) -> str:
        topic_dir = os.path.join(base_output, topic_id)
        os.makedirs(topic_dir, exist_ok=True)
        return topic_dir

    def run(self) -> int:
        """Run voice generation synchronously (wraps async internally)."""
        return asyncio.run(self._run_async())

    async def _run_async(self) -> int:
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        scene_plan_dir = os.path.join(base_dir, "data", "scene_plans")
        audio_root = os.path.join(base_dir, "data", "audio")
        os.makedirs(audio_root, exist_ok=True)

        # Find scene plan files in both flat and date-subdirectory structures
        scene_files = sorted(glob.glob(os.path.join(scene_plan_dir, "*.json")))
        scene_files += sorted(glob.glob(os.path.join(scene_plan_dir, "*", "*.json")))

        if not scene_files:
            print("No scene plans found for voice generation.")
            return 0

        print(f"Found {len(scene_files)} scene plan files...")
        print(f"Using Edge-TTS (free, unlimited) 🎙️\n")

        for plan_path in scene_files:
            plans = self._load_scene_plans(plan_path)

            for plan in plans:
                if not isinstance(plan, dict):
                    continue

                title = plan.get("title", "unknown")
                channel = plan.get("channel_id", "XX")

                # Pick voice based on channel
                voice = VOICE_MAP.get(channel, VOICE_MAP["default"])

                # Create filesystem-safe topic ID
                safe_title = "".join(c if c.isalnum() or c in "-_ " else "" for c in title)[:50].strip().replace(" ", "_")
                topic_id = f"{channel}_{safe_title}"

                scenes: List[Dict[str, Any]] = plan.get("scenes") or []
                if not scenes:
                    continue

                self.metrics["topics_processed"] += 1
                topic_duration = sum(
                    float(s.get("estimated_duration_sec") or s.get("estimated_duration") or 0)
                    for s in scenes
                )
                self.metrics["total_estimated_duration_sec"] += topic_duration

                topic_dir = self._topic_output_dir(audio_root, topic_id)
                print(f"  🎙️ '{title[:60]}' ({len(scenes)} scenes, voice={voice})")

                for idx, scene in enumerate(scenes, start=1):
                    narration = (scene.get("narration") or scene.get("text") or "").strip()
                    if not narration:
                        continue

                    scene_num = scene.get("scene_number") or scene.get("scene_id") or idx
                    filename = f"scene_{str(scene_num).zfill(2)}.mp3"
                    output_path = os.path.join(topic_dir, filename)

                    self.metrics["scenes_total"] += 1

                    if os.path.exists(output_path) and os.path.getsize(output_path) > 100:
                        self.metrics["scenes_skipped_existing"] += 1
                        continue

                    start_time = time.time()
                    success = await self._synthesize_audio(narration, voice, output_path)
                    elapsed = round(time.time() - start_time, 2)

                    if success:
                        self.metrics["scenes_generated"] += 1
                        self.metrics["scene_generation_times"].append(elapsed)
                        size_kb = os.path.getsize(output_path) / 1024
                        print(f"    ✅ scene_{str(scene_num).zfill(2)}.mp3 ({size_kb:.0f}KB, {elapsed}s)")
                    else:
                        self.metrics["scenes_failed"] += 1
                        print(f"    ❌ scene_{str(scene_num).zfill(2)}.mp3 FAILED")

        self._log_metrics()
        generated = self.metrics["scenes_generated"]
        total = self.metrics["scenes_total"]
        print(f"\nAudio files generated: {generated}/{total}")
        return generated

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
