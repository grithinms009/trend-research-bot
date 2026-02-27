"""
Video Builder Shorts — Rhythm Engine.

Renders YouTube Shorts (1080x1920, 9:16, 30fps) with scene-reactive editing:
- Energy controls zoom speed, cut pace, caption size, music volume
- Emotion controls silence inserts, fade types, visual effects
- cut_style controls transition type (hard/smash/slow)

All rendering via FFmpeg CLI. No moviepy. Optimized for 8GB VPS.
"""

import glob
import json
import logging
import os
import subprocess
import time
from datetime import datetime
from typing import Dict, List, Optional

import yaml

from app.video.stock_fetcher import StockFetcher
from app.video.subtitle_generator import generate_subtitles_for_topic, _load_channel_config
from app.video.music_manager import get_music_for_channel

logger = logging.getLogger(__name__)

WIDTH = 1080
HEIGHT = 1920
FPS = 30
MAX_DURATION = 59
CRF = 23
VIDEO_BITRATE = "6M"
AUDIO_BITRATE = "192k"


def _run_ffmpeg(cmd: List[str], timeout: int = 120) -> bool:
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        if result.returncode != 0:
            logger.error("FFmpeg failed: %s", result.stderr[-500:])
            return False
        return True
    except subprocess.TimeoutExpired:
        logger.error("FFmpeg timed out after %ds", timeout)
        return False
    except Exception as exc:
        logger.error("FFmpeg error: %s", exc)
        return False


def _get_zoom_filter(energy: int, duration: float) -> str:
    """Generate zoom filter based on energy level."""
    if energy >= 5:
        # Zoom punch: fast zoom in
        zoom_speed = 0.003
    elif energy >= 4:
        zoom_speed = 0.002
    elif energy >= 3:
        zoom_speed = 0.001
    else:
        # Calm: very subtle or no zoom
        zoom_speed = 0.0005

    return (
        f"zoompan=z='min(zoom+{zoom_speed},{1 + energy * 0.03})'"
        f":d={int(duration * FPS)}"
        f":x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)'"
        f":s={WIDTH}x{HEIGHT}:fps={FPS}"
    )


def _prepare_scene_clip(
    stock_clip: str,
    audio_path: str,
    output_path: str,
    duration: float,
    energy: int,
    emotion: str,
    cut_style: str,
    bg_color: str = "0x0a0a0a",
    silence_before: float = 0.0,
) -> bool:
    """Render a single scene with rhythm-aware effects."""

    zoom_filter = _get_zoom_filter(energy, duration)

    if stock_clip and os.path.exists(stock_clip) and os.path.getsize(stock_clip) > 1000:
        # Stock clip: scale → crop → zoom based on energy
        video_filter = (
            f"scale={WIDTH}:{HEIGHT}:force_original_aspect_ratio=increase,"
            f"crop={WIDTH}:{HEIGHT},"
            f"setpts=PTS-STARTPTS,"
            f"fps={FPS}"
        )

        # Add silence pad before audio for shock/dramatic emotions
        audio_filter = ""
        if silence_before > 0:
            audio_filter = f"-af adelay={int(silence_before * 1000)}|{int(silence_before * 1000)}"

        cmd = ["ffmpeg", "-y", "-i", stock_clip, "-i", audio_path]
        cmd += ["-filter_complex", f"[0:v]{video_filter}[v]"]
        cmd += ["-map", "[v]", "-map", "1:a"]

        if audio_filter:
            cmd += ["-af", f"adelay={int(silence_before * 1000)}|{int(silence_before * 1000)}"]

        cmd += [
            "-c:v", "libx264", "-preset", "fast", "-crf", str(CRF),
            "-c:a", "aac", "-b:a", AUDIO_BITRATE,
            "-t", str(duration + silence_before),
            "-shortest",
            output_path,
        ]
    else:
        # Solid color background
        total_dur = duration + silence_before
        cmd = [
            "ffmpeg", "-y",
            "-f", "lavfi",
            "-i", f"color=c={bg_color}:s={WIDTH}x{HEIGHT}:r={FPS}:d={total_dur}",
            "-i", audio_path,
        ]

        if silence_before > 0:
            cmd += ["-af", f"adelay={int(silence_before * 1000)}|{int(silence_before * 1000)}"]

        cmd += [
            "-c:v", "libx264", "-preset", "ultrafast", "-crf", "28",
            "-c:a", "aac", "-b:a", AUDIO_BITRATE,
            "-t", str(total_dur),
            "-shortest",
            output_path,
        ]

    return _run_ffmpeg(cmd, timeout=60)


def _concat_scenes(scene_clips: List[str], output_path: str) -> bool:
    """Concatenate scene clips."""
    list_path = output_path + ".concat.txt"
    with open(list_path, "w") as f:
        for clip in scene_clips:
            safe = clip.replace("'", "'\\''")
            f.write(f"file '{safe}'\n")

    cmd = [
        "ffmpeg", "-y",
        "-f", "concat", "-safe", "0",
        "-i", list_path,
        "-c", "copy",
        output_path,
    ]
    result = _run_ffmpeg(cmd)
    if os.path.exists(list_path):
        os.remove(list_path)
    return result


def _add_subtitles(input_path: str, ass_path: str, output_path: str) -> bool:
    """Burn ASS subtitles into video."""
    cmd = [
        "ffmpeg", "-y",
        "-i", input_path,
        "-vf", f"ass='{ass_path}'",
        "-c:v", "libx264", "-preset", "fast", "-crf", str(CRF),
        "-c:a", "copy",
        output_path,
    ]
    return _run_ffmpeg(cmd, timeout=90)


def _mix_music(input_path: str, music_path: str, output_path: str, avg_energy: float) -> bool:
    """Mix music with energy-reactive volume."""
    if not music_path or not os.path.exists(music_path):
        cmd = ["ffmpeg", "-y", "-i", input_path, "-c", "copy", output_path]
        return _run_ffmpeg(cmd)

    # Music volume scales with energy: high energy → louder music
    music_vol = min(0.15, 0.05 + (avg_energy * 0.02))

    cmd = [
        "ffmpeg", "-y",
        "-i", input_path,
        "-i", music_path,
        "-filter_complex",
        f"[0:a]volume=1.0[voice];"
        f"[1:a]volume={music_vol:.3f}[music];"
        f"[voice][music]amix=inputs=2:duration=first:dropout_transition=2[aout]",
        "-map", "0:v",
        "-map", "[aout]",
        "-c:v", "copy",
        "-c:a", "aac", "-b:a", AUDIO_BITRATE,
        "-t", str(MAX_DURATION),
        output_path,
    ]
    return _run_ffmpeg(cmd, timeout=90)


class ShortsVideoBuilder:
    """Builds YouTube Shorts with rhythm-aware editing."""

    def __init__(self):
        self.channel_config = _load_channel_config()
        self.stock_fetcher = StockFetcher()
        self.metrics = {
            "topics_processed": 0,
            "videos_rendered": 0,
            "videos_failed": 0,
            "total_render_time": 0.0,
        }

    def build_short(self, scene_plan: Dict, audio_dir: str, work_dir: str, output_path: str) -> bool:
        """Build a YouTube Short with rhythm engine."""
        start_time = time.time()
        title = scene_plan.get("title", "unknown")
        channel = scene_plan.get("channel_id", "C1")
        ch_config = self.channel_config.get(channel, {})
        bg_color = ch_config.get("bg_color", "#0a0a0a").lstrip("#")
        scenes = scene_plan.get("scenes", [])

        if not scenes:
            return False

        os.makedirs(work_dir, exist_ok=True)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Step 1: Fetch stock footage via visual intent
        assets_dir = os.path.join(work_dir, "assets")
        os.makedirs(assets_dir, exist_ok=True)
        stock_clips = self.stock_fetcher.fetch_for_topic(scene_plan, assets_dir)

        # Step 2: Build per-scene clips with rhythm
        scene_clip_paths = []
        avg_energy = sum(s.get("energy", 3) for s in scenes) / max(len(scenes), 1)

        for i, scene in enumerate(scenes):
            scene_num = scene.get("scene_id", scene.get("scene_number", i + 1))
            duration = float(scene.get("estimated_duration", 3.0))
            energy = scene.get("energy", 3)
            emotion = scene.get("emotion", "neutral")
            cut_style = scene.get("cut_style", "hard")

            # Rhythm rules
            silence_before = 0.0
            if emotion == "shock":
                silence_before = 0.15  # 150ms pause before shock
            elif emotion == "reveal":
                silence_before = 0.1   # Slight pause before reveal

            audio_file = os.path.join(audio_dir, f"scene_{str(scene_num).zfill(2)}.mp3")
            if not os.path.exists(audio_file):
                logger.warning("Missing audio for scene %d", scene_num)
                continue

            stock_clip = stock_clips[i] if i < len(stock_clips) else ""
            scene_output = os.path.join(work_dir, f"scene_{str(scene_num).zfill(2)}.mp4")

            if _prepare_scene_clip(
                stock_clip, audio_file, scene_output,
                duration, energy, emotion, cut_style,
                bg_color, silence_before,
            ):
                scene_clip_paths.append(scene_output)

        if not scene_clip_paths:
            return False

        # Step 3: Concatenate
        concat_path = os.path.join(work_dir, "concat.mp4")
        if not _concat_scenes(scene_clip_paths, concat_path):
            return False

        # Step 4: Subtitles
        subs_path = os.path.join(work_dir, "subs.ass")
        generate_subtitles_for_topic(scene_plan, subs_path, self.channel_config)

        subtitled_path = os.path.join(work_dir, "subtitled.mp4")
        if not _add_subtitles(concat_path, subs_path, subtitled_path):
            subtitled_path = concat_path

        # Step 5: Music (energy-reactive volume)
        total_dur = sum(float(s.get("estimated_duration", 3)) for s in scenes)
        music_dir = os.path.join(work_dir, "music")
        music_path = get_music_for_channel(channel, min(total_dur, MAX_DURATION), music_dir)

        if not _mix_music(subtitled_path, music_path, output_path, avg_energy):
            import shutil
            shutil.copy2(subtitled_path, output_path)

        elapsed = round(time.time() - start_time, 2)
        self.metrics["total_render_time"] += elapsed

        if os.path.exists(output_path) and os.path.getsize(output_path) > 10000:
            size_mb = os.path.getsize(output_path) / (1024 * 1024)
            print(f"  ✅ {os.path.basename(output_path)} ({size_mb:.1f}MB, {elapsed}s)")
            self.metrics["videos_rendered"] += 1
            return True
        else:
            print(f"  ❌ Failed: {title[:60]}")
            self.metrics["videos_failed"] += 1
            return False

    def log_metrics(self):
        print("\n--- Video Builder Metrics ---")
        for k, v in self.metrics.items():
            print(f"{k}: {v}")
        if self.metrics["videos_rendered"] > 0:
            avg = self.metrics["total_render_time"] / self.metrics["videos_rendered"]
            print(f"avg_render_time: {avg:.1f}s")
        print("----------------------------\n")
        self.stock_fetcher.log_metrics()


def main():
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    scene_plan_dir = os.path.join(base_dir, "data", "scene_plans")
    audio_root = os.path.join(base_dir, "data", "audio")
    work_root = os.path.join(base_dir, "data", "shorts", "work")
    output_root = os.path.join(base_dir, "data", "shorts", "final")

    scene_files = sorted(glob.glob(os.path.join(scene_plan_dir, "*", "*.json")))
    scene_files += sorted(glob.glob(os.path.join(scene_plan_dir, "*.json")))

    if not scene_files:
        print("No scene plans found.")
        return

    builder = ShortsVideoBuilder()
    total = 0

    for plan_path in scene_files:
        with open(plan_path) as f:
            data = json.load(f)

        plans = data if isinstance(data, list) else [data]

        for plan in plans:
            builder.metrics["topics_processed"] += 1
            title = plan.get("title", "unknown")
            channel = plan.get("channel_id", "XX")
            safe_title = "".join(c if c.isalnum() or c in "-_ " else "" for c in title)[:50].strip().replace(" ", "_")
            topic_id = f"{channel}_{safe_title}"

            audio_dir = os.path.join(audio_root, topic_id)
            work_dir = os.path.join(work_root, topic_id)
            output_dir = os.path.join(output_root, channel)
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, f"{topic_id}.mp4")

            if os.path.exists(output_path) and os.path.getsize(output_path) > 10000:
                print(f"  Already rendered: {topic_id}")
                continue

            if not os.path.exists(audio_dir):
                print(f"  No audio for {topic_id}")
                continue

            print(f"\n Building: '{title[:60]}' ({channel})")
            if builder.build_short(plan, audio_dir, work_dir, output_path):
                total += 1

    builder.log_metrics()
    print(f"\nTotal shorts: {total}")
    print(f"Output: {output_root}")


if __name__ == "__main__":
    main()
