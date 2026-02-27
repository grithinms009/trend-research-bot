"""
Cinematic Video Builder — programmable film editor for YouTube Shorts.

Integrates:
- Cinematic Director (camera motion, shot type, color grade per scene)
- Sound Designer (SFX: impact hits, bass rumble, whoosh, tension hum)
- Rhythm Engine (energy-based cut timing, zoom speed, silence inserts)
- Subtitle Engine (animated captions with typography)
- Stock Fetcher (visual intent + frame planning)
- Quality Checker (pre-export validation)

All rendering via FFmpeg CLI. No moviepy. Optimized for 8GB VPS.
Output: 1080x1920, 9:16, 30fps, < 60s, upload-ready.
"""

import glob
import json
import logging
import os
import shutil
import subprocess
import time
from datetime import datetime
from typing import Dict, List, Optional

import yaml

from app.video.stock_fetcher import StockFetcher
from app.video.subtitle_generator import generate_subtitles_for_topic
from app.video.music_manager import get_music_for_channel
from app.video.sound_designer import get_sfx_for_scene
from app.video.quality_checker import run_quality_check

logger = logging.getLogger(__name__)

WIDTH = 1080
HEIGHT = 1920
FPS = 30
MAX_DURATION = 59
CRF = 23
AUDIO_BITRATE = "192k"


def _load_channel_config() -> Dict:
    base = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    path = os.path.join(base, "app", "config", "channels.yaml")
    with open(path) as f:
        return yaml.safe_load(f).get("channels", {})


def _run_ffmpeg(cmd: List[str], timeout: int = 120) -> bool:
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        if result.returncode != 0:
            logger.error("FFmpeg: %s", result.stderr[-500:])
            return False
        return True
    except subprocess.TimeoutExpired:
        logger.error("FFmpeg timeout %ds", timeout)
        return False
    except Exception as exc:
        logger.error("FFmpeg error: %s", exc)
        return False


# ============================================================
# RHYTHM ENGINE — energy/emotion-based FFmpeg parameters
# ============================================================
def _get_zoom_params(energy: int, camera_motion: str, duration: float) -> str:
    """Generate zoompan filter based on energy + camera motion."""
    zoom_speeds = {
        "fast_punch": 0.004,
        "slow_push": 0.001,
        "drift": 0.0008,
        "static": 0.0003,
        "pull_out": -0.001,
        "lateral_pan": 0.0005,
    }
    speed = zoom_speeds.get(camera_motion, 0.001)
    max_zoom = 1 + (energy * 0.025)

    if speed < 0:
        # Pull out: start zoomed, zoom out
        return (
            f"zoompan=z='if(eq(on,1),{max_zoom},max(zoom+{speed},1))'"
            f":d={int(duration * FPS)}"
            f":x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)'"
            f":s={WIDTH}x{HEIGHT}:fps={FPS}"
        )
    else:
        return (
            f"zoompan=z='min(zoom+{speed},{max_zoom})'"
            f":d={int(duration * FPS)}"
            f":x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)'"
            f":s={WIDTH}x{HEIGHT}:fps={FPS}"
        )


def _get_color_filter(color_grade: str) -> str:
    """Generate FFmpeg color grading filter."""
    grades = {
        "dramatic": "eq=contrast=1.2:brightness=-0.05:saturation=1.3",
        "cool_news": "eq=contrast=1.1:saturation=0.9,colorbalance=bs=0.05:ms=0.02",
        "warm_luxury": "eq=brightness=0.05:saturation=1.2,colorbalance=rs=0.04:gs=0.02",
        "cinematic_dark": "eq=contrast=1.3:brightness=-0.1:saturation=1.1",
        "bright_clean": "eq=brightness=0.08:contrast=1.05:saturation=1.0",
        "neutral": "eq=contrast=1.0:brightness=0.0:saturation=1.0",
    }
    return grades.get(color_grade, grades["neutral"])


# ============================================================
# SCENE RENDERING
# ============================================================
def _prepare_scene(
    stock_clip: str, audio_path: str, output_path: str,
    duration: float, scene: Dict, bg_color: str,
) -> bool:
    """Render one scene with cinematic direction."""
    energy = scene.get("energy", 3)
    camera_motion = scene.get("camera_motion", "static")
    color_grade = scene.get("color_grade", "neutral")
    cut_timing = scene.get("cut_timing", "on_beat")

    # Silence insert for shock/reveal
    silence_ms = 0
    if scene.get("sound_design") == "silence_pause":
        silence_ms = 200
    elif scene.get("emotion") == "shock" and cut_timing == "hard_interrupt":
        silence_ms = 150

    color_filter = _get_color_filter(color_grade)

    if stock_clip and os.path.exists(stock_clip) and os.path.getsize(stock_clip) > 1000:
        video_filter = (
            f"scale={WIDTH}:{HEIGHT}:force_original_aspect_ratio=increase,"
            f"crop={WIDTH}:{HEIGHT},"
            f"setpts=PTS-STARTPTS,"
            f"fps={FPS},"
            f"{color_filter}"
        )

        cmd = [
            "ffmpeg", "-y",
            "-i", stock_clip, "-i", audio_path,
            "-filter_complex", f"[0:v]{video_filter}[v]",
            "-map", "[v]", "-map", "1:a",
        ]

        if silence_ms > 0:
            cmd += ["-af", f"adelay={silence_ms}|{silence_ms}"]

        cmd += [
            "-c:v", "libx264", "-preset", "fast", "-crf", str(CRF),
            "-c:a", "aac", "-b:a", AUDIO_BITRATE,
            "-t", str(duration + silence_ms / 1000),
            "-shortest", output_path,
        ]
    else:
        total_dur = duration + silence_ms / 1000
        cmd = [
            "ffmpeg", "-y",
            "-f", "lavfi",
            "-i", f"color=c={bg_color}:s={WIDTH}x{HEIGHT}:r={FPS}:d={total_dur}",
            "-i", audio_path,
        ]
        if silence_ms > 0:
            cmd += ["-af", f"adelay={silence_ms}|{silence_ms}"]
        cmd += [
            "-c:v", "libx264", "-preset", "ultrafast", "-crf", "28",
            "-c:a", "aac", "-b:a", AUDIO_BITRATE,
            "-t", str(total_dur), "-shortest", output_path,
        ]

    return _run_ffmpeg(cmd, timeout=60)


def _concat_scenes(clips: List[str], output: str) -> bool:
    list_file = output + ".list.txt"
    with open(list_file, "w") as f:
        for c in clips:
            f.write(f"file '{c.replace(chr(39), chr(39) + chr(92) + chr(39) + chr(39))}'\n")

    cmd = ["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", list_file, "-c", "copy", output]
    ok = _run_ffmpeg(cmd)
    if os.path.exists(list_file):
        os.remove(list_file)
    return ok


def _add_subtitles(inp: str, ass: str, out: str) -> bool:
    cmd = [
        "ffmpeg", "-y", "-i", inp,
        "-vf", f"ass='{ass}'",
        "-c:v", "libx264", "-preset", "fast", "-crf", str(CRF),
        "-c:a", "copy", out,
    ]
    return _run_ffmpeg(cmd, timeout=90)


def _mix_audio_layers(inp: str, music: str, sfx_paths: List[str],
                      out: str, avg_energy: float) -> bool:
    """Mix voice + music + SFX into final audio."""
    music_vol = min(0.12, 0.04 + (avg_energy * 0.015))

    if not music or not os.path.exists(music):
        if not sfx_paths:
            return _run_ffmpeg(["ffmpeg", "-y", "-i", inp, "-c", "copy", out])

    inputs = ["-i", inp]
    filter_parts = ["[0:a]volume=1.0[voice]"]
    mix_inputs = "[voice]"
    mix_count = 1

    if music and os.path.exists(music):
        inputs += ["-i", music]
        filter_parts.append(f"[{mix_count}:a]volume={music_vol:.3f}[music]")
        mix_inputs += "[music]"
        mix_count += 1

    # SFX layers (max 2 to avoid complexity)
    for i, sfx in enumerate(sfx_paths[:2]):
        if sfx and os.path.exists(sfx):
            inputs += ["-i", sfx]
            filter_parts.append(f"[{mix_count}:a]volume=0.3[sfx{i}]")
            mix_inputs += f"[sfx{i}]"
            mix_count += 1

    if mix_count <= 1:
        return _run_ffmpeg(["ffmpeg", "-y", "-i", inp, "-c", "copy", out])

    filter_parts.append(
        f"{mix_inputs}amix=inputs={mix_count}:duration=first:dropout_transition=2[aout]"
    )
    filter_str = ";".join(filter_parts)

    cmd = ["ffmpeg", "-y"] + inputs + [
        "-filter_complex", filter_str,
        "-map", "0:v", "-map", "[aout]",
        "-c:v", "copy", "-c:a", "aac", "-b:a", AUDIO_BITRATE,
        "-t", str(MAX_DURATION), out,
    ]
    return _run_ffmpeg(cmd, timeout=90)


class ShortsVideoBuilder:
    """Cinematic film editor for YouTube Shorts."""

    def __init__(self):
        self.channel_config = _load_channel_config()
        self.stock_fetcher = StockFetcher()
        self.metrics = {
            "topics_processed": 0,
            "videos_rendered": 0,
            "videos_failed": 0,
            "qc_passed": 0,
            "qc_failed": 0,
            "total_render_time": 0.0,
        }

    def build_short(self, plan: Dict, audio_dir: str, work_dir: str, output: str) -> bool:
        start_time = time.time()
        title = plan.get("title", "?")
        channel = plan.get("channel_id", "C1")
        ch = self.channel_config.get(channel, {})
        bg_color = ch.get("bg_color", "#0a0a0a").lstrip("#")
        scenes = plan.get("scenes", [])

        if not scenes:
            return False

        os.makedirs(work_dir, exist_ok=True)
        os.makedirs(os.path.dirname(output), exist_ok=True)

        # 1. Stock footage via visual intent
        assets = os.path.join(work_dir, "assets")
        os.makedirs(assets, exist_ok=True)
        stock_clips = self.stock_fetcher.fetch_for_topic(plan, assets)

        # 2. Per-scene rendering with cinematic direction
        scene_clips = []
        sfx_all = []
        avg_energy = sum(s.get("energy", 3) for s in scenes) / max(len(scenes), 1)

        for i, scene in enumerate(scenes):
            sid = scene.get("scene_id", scene.get("scene_number", i + 1))
            duration = float(scene.get("estimated_duration", 3.0))

            audio = os.path.join(audio_dir, f"scene_{str(sid).zfill(2)}.mp3")
            if not os.path.exists(audio):
                continue

            stock = stock_clips[i] if i < len(stock_clips) else ""
            scene_out = os.path.join(work_dir, f"scene_{str(sid).zfill(2)}.mp4")

            if _prepare_scene(stock, audio, scene_out, duration, scene, bg_color):
                scene_clips.append(scene_out)

            # Generate SFX for this scene
            sfx_dir = os.path.join(work_dir, "sfx")
            sfx = get_sfx_for_scene(scene, sfx_dir)
            if sfx:
                sfx_all.append(sfx)

        if not scene_clips:
            return False

        # 3. Concatenate
        concat_path = os.path.join(work_dir, "concat.mp4")
        if not _concat_scenes(scene_clips, concat_path):
            return False

        # 4. Burn cinematic subtitles
        subs_path = os.path.join(work_dir, "subs.ass")
        generate_subtitles_for_topic(plan, subs_path, self.channel_config)

        subtitled = os.path.join(work_dir, "subtitled.mp4")
        if not _add_subtitles(concat_path, subs_path, subtitled):
            subtitled = concat_path

        # 5. Mix audio layers (voice + music + SFX)
        total_dur = sum(float(s.get("estimated_duration", 3)) for s in scenes)
        music_dir = os.path.join(work_dir, "music")
        music = get_music_for_channel(channel, min(total_dur, MAX_DURATION), music_dir)

        if not _mix_audio_layers(subtitled, music, sfx_all, output, avg_energy):
            shutil.copy2(subtitled, output)

        elapsed = round(time.time() - start_time, 2)
        self.metrics["total_render_time"] += elapsed

        if os.path.exists(output) and os.path.getsize(output) > 10000:
            size_mb = os.path.getsize(output) / (1024 * 1024)

            # 6. Quality check
            qc = run_quality_check(output, plan)
            if qc["passed"]:
                self.metrics["qc_passed"] += 1
                print(f"  OK {os.path.basename(output)} ({size_mb:.1f}MB {elapsed}s) QC:PASS")
            else:
                self.metrics["qc_failed"] += 1
                issues = ", ".join(qc["issues"][:2])
                print(f"  OK {os.path.basename(output)} ({size_mb:.1f}MB {elapsed}s) QC:WARN [{issues}]")

            self.metrics["videos_rendered"] += 1
            return True
        else:
            self.metrics["videos_failed"] += 1
            return False

    def log_metrics(self):
        print("\n--- Cinematic Video Builder ---")
        for k, v in self.metrics.items():
            print(f"{k}: {v}")
        if self.metrics["videos_rendered"] > 0:
            avg = self.metrics["total_render_time"] / self.metrics["videos_rendered"]
            print(f"avg_render_time: {avg:.1f}s")
        print("-------------------------------\n")
        self.stock_fetcher.log_metrics()


def main():
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    # Prefer directed plans, fallback to scene plans
    directed_dir = os.path.join(base_dir, "data", "directed_plans")
    scene_plan_dir = os.path.join(base_dir, "data", "scene_plans")
    source = directed_dir if os.path.exists(directed_dir) and os.listdir(directed_dir) else scene_plan_dir

    audio_root = os.path.join(base_dir, "data", "audio")
    work_root = os.path.join(base_dir, "data", "shorts", "work")
    output_root = os.path.join(base_dir, "data", "shorts", "final")

    plans = sorted(glob.glob(os.path.join(source, "*", "*.json")))
    plans += sorted(glob.glob(os.path.join(source, "*.json")))

    if not plans:
        print("No plans found.")
        return

    builder = ShortsVideoBuilder()
    total = 0

    for plan_path in plans:
        with open(plan_path) as f:
            data = json.load(f)

        items = data if isinstance(data, list) else [data]

        for plan in items:
            builder.metrics["topics_processed"] += 1
            title = plan.get("title", "?")
            channel = plan.get("channel_id", "XX")
            safe = "".join(c if c.isalnum() or c in "-_ " else "" for c in title)[:50].strip().replace(" ", "_")
            tid = f"{channel}_{safe}"

            audio_dir = os.path.join(audio_root, tid)
            work_dir = os.path.join(work_root, tid)
            out_dir = os.path.join(output_root, channel)
            os.makedirs(out_dir, exist_ok=True)
            out_path = os.path.join(out_dir, f"{tid}.mp4")

            if os.path.exists(out_path) and os.path.getsize(out_path) > 10000:
                continue

            if not os.path.exists(audio_dir):
                continue

            print(f"\n  Building: '{title[:55]}' ({channel})")
            if builder.build_short(plan, audio_dir, work_dir, out_path):
                total += 1

    builder.log_metrics()
    print(f"Shorts rendered: {total}")


if __name__ == "__main__":
    main()
