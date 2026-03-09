"""
Voice Generator — Edge-TTS with micro-pauses and scene-aware chunking.

Reads narration from scene planner output (cleaned narration only).
Generates per-scene MP3 audio. Inserts micro-pauses between scenes.
Uses channel-specific voices for brand identity.
"""

import asyncio
import glob
import json
import logging
import os
import subprocess
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Channel-specific voices for brand identity
VOICE_MAP = {
    "C1": "en-US-GuyNeural",       # Tech — authoritative male
    "C5": "en-US-DavisNeural",     # Productivity — calm male
    "default": "en-US-GuyNeural",
}

# Micro-pause duration between scenes (seconds)
SCENE_PAUSE_MS = 300


def _get_audio_duration(path: str) -> float:
    """Get actual audio duration in seconds via ffprobe."""
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "quiet", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", path],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0 and result.stdout.strip():
            return float(result.stdout.strip())
    except Exception as exc:
        logger.warning("ffprobe failed for %s: %s", path, exc)
    return 0.0


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
        try:
            with open(path) as f:
                data = json.load(f)
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                return [data]
            return []
        except Exception as exc:
            logger.error("Failed to read %s: %s", path, exc)
            return []

    async def _synthesize_scene(self, text: str, voice: str, output_path: str,
                                max_retries: int = 3) -> bool:
        """Generate audio for a single scene narration chunk with retry logic."""
        import edge_tts

        for attempt in range(1, max_retries + 1):
            try:
                communicate = edge_tts.Communicate(text, voice)
                await communicate.save(output_path)

                if os.path.exists(output_path) and os.path.getsize(output_path) > 100:
                    return True

                logger.warning("TTS produced empty/tiny file (attempt %d/%d)", attempt, max_retries)
            except Exception as exc:
                logger.warning("TTS attempt %d/%d failed: %s: %s", attempt, max_retries,
                               type(exc).__name__, exc)
                if attempt < max_retries:
                    wait = 2 ** attempt  # 2s, 4s backoff
                    print(f"    TTS retry {attempt}/{max_retries} in {wait}s...")
                    await asyncio.sleep(wait)
                else:
                    print(f"    TTS FAILED after {max_retries} attempts: {type(exc).__name__}: {exc}")

        return False

    def _add_silence_padding(self, audio_path: str, pause_ms: int = 300) -> bool:
        """Add silence after audio file for natural pacing between scenes."""
        padded = audio_path + ".padded.mp3"
        try:
            cmd = [
                "ffmpeg", "-y",
                "-i", audio_path,
                "-af", f"apad=pad_dur={pause_ms / 1000}",
                "-c:a", "libmp3lame", "-b:a", "192k",
                padded,
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
            if result.returncode == 0 and os.path.exists(padded):
                os.replace(padded, audio_path)
                return True
            # Cleanup on failure
            if os.path.exists(padded):
                os.remove(padded)
        except Exception:
            if os.path.exists(padded):
                os.remove(padded)
        return False

    def _topic_output_dir(self, base_output: str, topic_id: str) -> str:
        d = os.path.join(base_output, topic_id)
        os.makedirs(d, exist_ok=True)
        return d

    def run(self) -> int:
        return asyncio.run(self._run_async())

    async def _run_async(self) -> int:
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        scene_plan_dir = os.path.join(base_dir, "data", "scene_plans")
        audio_root = os.path.join(base_dir, "data", "audio")
        os.makedirs(audio_root, exist_ok=True)

        scene_files = sorted(glob.glob(os.path.join(scene_plan_dir, "*.json")))
        scene_files += sorted(glob.glob(os.path.join(scene_plan_dir, "*", "*.json")))

        if not scene_files:
            print("No scene plans found for voice generation.")
            return 0

        print(f"Found {len(scene_files)} scene plan files...")
        print(f"Using Edge-TTS (free, unlimited)\n")

        for plan_path in scene_files:
            plans = self._load_scene_plans(plan_path)

            for plan in plans:
                if not isinstance(plan, dict):
                    continue

                title = plan.get("title", "unknown")
                channel = plan.get("channel_id", "XX")
                voice = VOICE_MAP.get(channel, VOICE_MAP["default"])

                safe_title = "".join(c if c.isalnum() or c in "-_ " else "" for c in title)[:50].strip().replace(" ", "_")
                topic_id = f"{channel}_{safe_title}"

                scenes = plan.get("scenes") or []
                if not scenes:
                    continue

                self.metrics["topics_processed"] += 1
                topic_duration = sum(
                    float(s.get("estimated_duration_sec") or s.get("estimated_duration") or 0)
                    for s in scenes
                )
                self.metrics["total_estimated_duration_sec"] += topic_duration

                topic_dir = self._topic_output_dir(audio_root, topic_id)
                durations: Dict[str, float] = {}  # actual audio durations
                print(f"  '{title[:60]}' ({len(scenes)} scenes, {voice})")

                for idx, scene in enumerate(scenes, start=1):
                    # Read from narration (scene_planner) or text (legacy splitter)
                    narration = (scene.get("narration") or scene.get("text") or "").strip()
                    if not narration:
                        continue

                    scene_num = scene.get("scene_id") or scene.get("scene_number") or idx
                    filename = f"scene_{str(scene_num).zfill(2)}.mp3"
                    output_path = os.path.join(topic_dir, filename)

                    self.metrics["scenes_total"] += 1

                    if os.path.exists(output_path) and os.path.getsize(output_path) > 100:
                        self.metrics["scenes_skipped_existing"] += 1
                        continue

                    start_time = time.time()
                    success = await self._synthesize_scene(narration, voice, output_path)
                    elapsed = round(time.time() - start_time, 2)

                    if success:
                        # Add micro-pause after each scene (except last)
                        if idx < len(scenes):
                            self._add_silence_padding(output_path, SCENE_PAUSE_MS)

                        # Record actual audio duration
                        real_dur = _get_audio_duration(output_path)
                        if real_dur > 0:
                            durations[f"scene_{str(scene_num).zfill(2)}"] = round(real_dur, 3)

                        self.metrics["scenes_generated"] += 1
                        self.metrics["scene_generation_times"].append(elapsed)
                        size_kb = os.path.getsize(output_path) / 1024
                        print(f"    scene_{str(scene_num).zfill(2)}.mp3 ({size_kb:.0f}KB, {elapsed}s)")
                    else:
                        self.metrics["scenes_failed"] += 1

                # Save actual durations manifest
                if durations:
                    dur_path = os.path.join(topic_dir, "durations.json")
                    with open(dur_path, "w") as f:
                        json.dump(durations, f, indent=2)
                    logger.info("Saved durations.json for %s (%d scenes)", topic_id, len(durations))

        self._log_metrics()
        g = self.metrics["scenes_generated"]
        t = self.metrics["scenes_total"]
        print(f"\nAudio: {g}/{t}")
        return g

    def _log_metrics(self) -> None:
        print("\n--- Voice Generator Metrics ---")
        for k, v in self.metrics.items():
            if k == "scene_generation_times" and v:
                print(f"  avg_time: {sum(v) / len(v):.2f}s")
            elif k != "scene_generation_times":
                print(f"{k}: {v}")
        print("--------------------------------\n")


if __name__ == "__main__":
    from app.utils.pipeline_logger import StageLogger

    slog = StageLogger("voice_generator")
    worker = VoiceGeneratorWorker()
    worker.run()

    # Emit structured metrics
    for k, v in worker.metrics.items():
        if k == "scene_generation_times" and v:
            slog.metric("avg_scene_gen_time_s", round(sum(v) / len(v), 2))
        elif k != "scene_generation_times":
            slog.metric(k, v)

    if worker.metrics["scenes_failed"] > 0:
        slog.warning(f"{worker.metrics['scenes_failed']} voice scenes failed",
                     suggestion="Check edge-tts installation and internet connectivity")
    if worker.metrics["scenes_generated"] == 0 and worker.metrics["scenes_total"] > 0:
        slog.error("No voice audio generated", detail="All TTS calls failed")

    slog.finish(success=worker.metrics["scenes_failed"] == 0)
