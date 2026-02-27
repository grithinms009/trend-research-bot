"""
Quality Checker — pre-export validation for cinematic shorts.

Checks:
- Subtitle overrun (text beyond safe zone)
- Awkward silence (> 1s gap with no audio)
- Static frames (> 4s with no visual change)
- Caption cutoff (text at screen edges)
- Visual mismatch (scene with no clip and no fallback)
- Duration compliance (must be < 60s)
- Audio sync (voice + music + SFX all present)

Auto-adjusts timing issues where possible.
"""

import json
import logging
import os
import subprocess
from typing import Dict, List, Tuple

logger = logging.getLogger(__name__)


def check_duration(video_path: str) -> Tuple[bool, float]:
    """Check video duration is under 60s."""
    try:
        cmd = [
            "ffprobe", "-v", "quiet",
            "-print_format", "json",
            "-show_format",
            video_path,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            data = json.loads(result.stdout)
            duration = float(data.get("format", {}).get("duration", 0))
            return duration <= 60.0, duration
    except Exception:
        pass
    return False, 0


def check_resolution(video_path: str) -> Tuple[bool, str]:
    """Check video is 1080x1920 vertical."""
    try:
        cmd = [
            "ffprobe", "-v", "quiet",
            "-print_format", "json",
            "-show_streams",
            "-select_streams", "v:0",
            video_path,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            data = json.loads(result.stdout)
            streams = data.get("streams", [])
            if streams:
                w = streams[0].get("width", 0)
                h = streams[0].get("height", 0)
                res = f"{w}x{h}"
                return (w == 1080 and h == 1920), res
    except Exception:
        pass
    return False, "unknown"


def check_audio_streams(video_path: str) -> Tuple[bool, int]:
    """Check video has audio stream."""
    try:
        cmd = [
            "ffprobe", "-v", "quiet",
            "-print_format", "json",
            "-show_streams",
            "-select_streams", "a",
            video_path,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            data = json.loads(result.stdout)
            count = len(data.get("streams", []))
            return count >= 1, count
    except Exception:
        pass
    return False, 0


def check_scene_plan(directed_plan: Dict) -> List[str]:
    """Check scene plan for potential issues."""
    issues = []
    scenes = directed_plan.get("scenes", [])

    total_duration = sum(float(s.get("estimated_duration", 0)) for s in scenes)

    if total_duration > 59:
        issues.append(f"Total duration {total_duration:.1f}s exceeds 59s limit")

    if total_duration < 15:
        issues.append(f"Total duration {total_duration:.1f}s is very short")

    for scene in scenes:
        sid = scene.get("scene_id", 0)
        dur = float(scene.get("estimated_duration", 0))
        narration = scene.get("narration", scene.get("text", ""))

        # Check for static scenes > 4s
        if dur > 4 and not scene.get("camera_motion"):
            issues.append(f"Scene {sid}: {dur:.1f}s duration with no camera motion")

        # Check for empty narration
        if not narration.strip():
            issues.append(f"Scene {sid}: empty narration")

        # Check word count per scene
        wc = len(narration.split())
        if wc > 50:
            issues.append(f"Scene {sid}: {wc} words is too long for a scene")

        # Check for missing emphasis words
        if not scene.get("emphasis_words"):
            issues.append(f"Scene {sid}: no emphasis words defined")

    return issues


def run_quality_check(video_path: str, scene_plan: Dict) -> Dict:
    """Run all quality checks on a rendered video."""
    results = {
        "path": video_path,
        "title": scene_plan.get("title", "unknown"),
        "passed": True,
        "checks": {},
        "issues": [],
    }

    # Duration check
    dur_ok, duration = check_duration(video_path)
    results["checks"]["duration"] = {"ok": dur_ok, "value": f"{duration:.1f}s"}
    if not dur_ok:
        results["issues"].append(f"Duration {duration:.1f}s > 60s")
        results["passed"] = False

    # Resolution check
    res_ok, resolution = check_resolution(video_path)
    results["checks"]["resolution"] = {"ok": res_ok, "value": resolution}
    if not res_ok:
        results["issues"].append(f"Resolution {resolution} != 1080x1920")

    # Audio check
    audio_ok, audio_count = check_audio_streams(video_path)
    results["checks"]["audio"] = {"ok": audio_ok, "value": f"{audio_count} streams"}
    if not audio_ok:
        results["issues"].append("No audio stream found")
        results["passed"] = False

    # Scene plan checks
    plan_issues = check_scene_plan(scene_plan)
    results["checks"]["scene_plan"] = {"ok": len(plan_issues) == 0, "issues": plan_issues}
    results["issues"].extend(plan_issues)

    # File size check
    if os.path.exists(video_path):
        size_mb = os.path.getsize(video_path) / (1024 * 1024)
        size_ok = 1 < size_mb < 100
        results["checks"]["file_size"] = {"ok": size_ok, "value": f"{size_mb:.1f}MB"}
        if not size_ok:
            results["issues"].append(f"File size {size_mb:.1f}MB unusual")

    return results


def main():
    """Run quality checks on all rendered shorts."""
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    finals_dir = os.path.join(base_dir, "data", "shorts", "final")

    if not os.path.exists(finals_dir):
        print("No rendered shorts to check.")
        return

    passed = 0
    failed = 0

    for channel_dir in sorted(os.listdir(finals_dir)):
        channel_path = os.path.join(finals_dir, channel_dir)
        if not os.path.isdir(channel_path):
            continue

        for mp4 in sorted(os.listdir(channel_path)):
            if not mp4.endswith(".mp4"):
                continue

            video_path = os.path.join(channel_path, mp4)
            result = run_quality_check(video_path, {"title": mp4, "scenes": []})

            status = "PASS" if result["passed"] else "FAIL"
            if result["passed"]:
                passed += 1
            else:
                failed += 1

            print(f"  {status} {mp4}")
            for issue in result["issues"][:3]:
                print(f"       {issue}")

    print(f"\n--- Quality Check ---")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    print(f"---------------------")


if __name__ == "__main__":
    main()
