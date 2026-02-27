"""
Video Builder Shorts — FFmpeg-based vertical video renderer.

Renders YouTube Shorts (1080x1920, 9:16, 30fps, max 60s) by:
1. Creating per-scene video segments (stock clip or solid color bg)
2. Overlaying voice audio per scene
3. Concatenating all scenes
4. Burning subtitles
5. Mixing background music (ducked under voice)
6. Exporting final MP4

All rendering uses FFmpeg CLI directly — no moviepy, no Python video libs.
Optimized for 8GB RAM VPS.
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

# Output settings
WIDTH = 1080
HEIGHT = 1920
FPS = 30
MAX_DURATION = 59  # Just under 60s for Shorts
CRF = 23  # Quality (lower = better, 18-28 range)
VIDEO_BITRATE = "6M"
AUDIO_BITRATE = "192k"


def _run_ffmpeg(cmd: List[str], timeout: int = 120) -> bool:
    """Run an FFmpeg command and return success status."""
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
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


def _create_color_bg(output_path: str, duration: float, bg_color: str = "0x0a0a0a") -> bool:
    """Create a solid-color background video as fallback when no stock clip."""
    cmd = [
        "ffmpeg", "-y",
        "-f", "lavfi",
        "-i", f"color=c={bg_color}:s={WIDTH}x{HEIGHT}:r={FPS}:d={duration}",
        "-c:v", "libx264", "-preset", "ultrafast", "-crf", "28",
        "-t", str(duration),
        output_path,
    ]
    return _run_ffmpeg(cmd)


def _prepare_scene_clip(
    stock_clip: str,
    audio_path: str,
    output_path: str,
    duration: float,
    bg_color: str = "0x0a0a0a",
) -> bool:
    """Prepare a single scene: stock clip (or color bg) + voice audio."""
    
    if stock_clip and os.path.exists(stock_clip) and os.path.getsize(stock_clip) > 1000:
        # Use stock clip: scale, crop to vertical, subtle zoom, mute original audio
        cmd = [
            "ffmpeg", "-y",
            "-i", stock_clip,
            "-i", audio_path,
            "-filter_complex",
            f"[0:v]scale={WIDTH}:{HEIGHT}:force_original_aspect_ratio=increase,"
            f"crop={WIDTH}:{HEIGHT},"
            f"setpts=PTS-STARTPTS,"
            f"fps={FPS}[v]",
            "-map", "[v]",
            "-map", "1:a",
            "-c:v", "libx264", "-preset", "fast", "-crf", str(CRF),
            "-c:a", "aac", "-b:a", AUDIO_BITRATE,
            "-t", str(duration),
            "-shortest",
            output_path,
        ]
    else:
        # Solid color background + audio
        cmd = [
            "ffmpeg", "-y",
            "-f", "lavfi",
            "-i", f"color=c={bg_color}:s={WIDTH}x{HEIGHT}:r={FPS}:d={duration}",
            "-i", audio_path,
            "-c:v", "libx264", "-preset", "ultrafast", "-crf", "28",
            "-c:a", "aac", "-b:a", AUDIO_BITRATE,
            "-t", str(duration),
            "-shortest",
            output_path,
        ]

    return _run_ffmpeg(cmd, timeout=60)


def _concat_scenes(scene_clips: List[str], output_path: str) -> bool:
    """Concatenate scene clips into one video using FFmpeg concat demuxer."""
    # Create concat list file
    list_path = output_path + ".concat.txt"
    with open(list_path, "w") as f:
        for clip in scene_clips:
            # FFmpeg concat requires escaped single quotes in paths
            safe_path = clip.replace("'", "'\\''")
            f.write(f"file '{safe_path}'\n")

    cmd = [
        "ffmpeg", "-y",
        "-f", "concat", "-safe", "0",
        "-i", list_path,
        "-c", "copy",
        output_path,
    ]

    result = _run_ffmpeg(cmd)

    # Clean up list file
    if os.path.exists(list_path):
        os.remove(list_path)

    return result


def _add_subtitles(input_path: str, ass_path: str, output_path: str) -> bool:
    """Burn ASS subtitles into video."""
    # Use the subtitles filter with the ASS file
    safe_ass = ass_path.replace(":", "\\:").replace("'", "\\'")
    cmd = [
        "ffmpeg", "-y",
        "-i", input_path,
        "-vf", f"ass='{ass_path}'",
        "-c:v", "libx264", "-preset", "fast", "-crf", str(CRF),
        "-c:a", "copy",
        output_path,
    ]
    return _run_ffmpeg(cmd, timeout=90)


def _mix_music(input_path: str, music_path: str, output_path: str) -> bool:
    """Mix background music under voice audio with ducking."""
    if not music_path or not os.path.exists(music_path):
        # No music — just copy
        cmd = ["ffmpeg", "-y", "-i", input_path, "-c", "copy", output_path]
        return _run_ffmpeg(cmd)

    cmd = [
        "ffmpeg", "-y",
        "-i", input_path,
        "-i", music_path,
        "-filter_complex",
        # Voice at full volume, music at -18dB (0.125), mix together
        "[0:a]volume=1.0[voice];"
        "[1:a]volume=0.08[music];"
        "[voice][music]amix=inputs=2:duration=first:dropout_transition=2[aout]",
        "-map", "0:v",
        "-map", "[aout]",
        "-c:v", "copy",
        "-c:a", "aac", "-b:a", AUDIO_BITRATE,
        "-t", str(MAX_DURATION),
        output_path,
    ]
    return _run_ffmpeg(cmd, timeout=90)


class ShortsVideoBuilder:
    """Builds YouTube Shorts from scene plans + audio + stock footage."""

    def __init__(self):
        self.channel_config = _load_channel_config()
        self.stock_fetcher = StockFetcher()
        self.metrics = {
            "topics_processed": 0,
            "videos_rendered": 0,
            "videos_failed": 0,
            "total_render_time": 0.0,
        }

    def build_short(
        self,
        scene_plan: Dict,
        audio_dir: str,
        work_dir: str,
        output_path: str,
    ) -> bool:
        """Build a single YouTube Short from scene plan + audio files."""
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

        # Step 1: Fetch stock footage
        assets_dir = os.path.join(work_dir, "assets")
        os.makedirs(assets_dir, exist_ok=True)
        stock_clips = self.stock_fetcher.fetch_for_topic(scene_plan, assets_dir)

        # Step 2: Build per-scene clips (video + audio)
        scene_clips = []
        for i, scene in enumerate(scenes):
            scene_num = scene.get("scene_number", i + 1)
            duration = float(scene.get("estimated_duration", scene.get("estimated_duration_sec", 3.0)))

            # Find audio file for this scene
            audio_file = os.path.join(audio_dir, f"scene_{str(scene_num).zfill(2)}.mp3")
            if not os.path.exists(audio_file):
                logger.warning("Missing audio for scene %d, skipping", scene_num)
                continue

            # Get stock clip (or empty string for color bg)
            stock_clip = stock_clips[i] if i < len(stock_clips) else ""

            scene_output = os.path.join(work_dir, f"scene_{str(scene_num).zfill(2)}.mp4")
            if _prepare_scene_clip(stock_clip, audio_file, scene_output, duration, bg_color):
                scene_clips.append(scene_output)
            else:
                logger.error("Failed to render scene %d for '%s'", scene_num, title[:40])

        if not scene_clips:
            logger.error("No scenes rendered for '%s'", title[:40])
            return False

        # Step 3: Concatenate all scenes
        concat_path = os.path.join(work_dir, "concat.mp4")
        if not _concat_scenes(scene_clips, concat_path):
            logger.error("Concat failed for '%s'", title[:40])
            return False

        # Step 4: Generate and burn subtitles
        subs_path = os.path.join(work_dir, "subs.ass")
        generate_subtitles_for_topic(scene_plan, subs_path, self.channel_config)
        
        subtitled_path = os.path.join(work_dir, "subtitled.mp4")
        if os.path.exists(subs_path):
            if not _add_subtitles(concat_path, subs_path, subtitled_path):
                # Subtitle burn failed — continue without subs
                logger.warning("Subtitle burn failed, continuing without subs")
                subtitled_path = concat_path
        else:
            subtitled_path = concat_path

        # Step 5: Mix background music
        total_duration = sum(
            float(s.get("estimated_duration", s.get("estimated_duration_sec", 3.0)))
            for s in scenes
        )
        music_dir = os.path.join(work_dir, "music")
        music_path = get_music_for_channel(channel, min(total_duration, MAX_DURATION), music_dir)

        if not _mix_music(subtitled_path, music_path, output_path):
            # Music mix failed — use subtitled version as final
            logger.warning("Music mix failed, using version without music")
            import shutil
            shutil.copy2(subtitled_path, output_path)

        elapsed = round(time.time() - start_time, 2)
        self.metrics["total_render_time"] += elapsed

        if os.path.exists(output_path) and os.path.getsize(output_path) > 10000:
            size_mb = os.path.getsize(output_path) / (1024 * 1024)
            print(f"  ✅ Rendered: {os.path.basename(output_path)} ({size_mb:.1f}MB, {elapsed}s)")
            self.metrics["videos_rendered"] += 1
            return True
        else:
            print(f"  ❌ Render failed: {title[:60]}")
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
        print("No scene plans found for video building.")
        return

    builder = ShortsVideoBuilder()
    total_built = 0

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

            # Paths
            audio_dir = os.path.join(audio_root, topic_id)
            work_dir = os.path.join(work_root, topic_id)
            output_dir = os.path.join(output_root, channel)
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, f"{topic_id}.mp4")

            # Skip if already rendered
            if os.path.exists(output_path) and os.path.getsize(output_path) > 10000:
                print(f"  ⏭️  Already rendered: {topic_id}")
                continue

            if not os.path.exists(audio_dir):
                print(f"  ⚠️  No audio for {topic_id}, skipping")
                continue

            print(f"\n🎬 Building short: '{title[:60]}' ({channel})")
            if builder.build_short(plan, audio_dir, work_dir, output_path):
                total_built += 1

    builder.log_metrics()

    if total_built == 0:
        print("⚠️  No videos rendered.")
    else:
        print(f"\n🎬 Total shorts rendered: {total_built}")
        print(f"Output: {output_root}")


if __name__ == "__main__":
    main()
