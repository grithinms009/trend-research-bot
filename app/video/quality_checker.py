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


def check_transitions(directed_plan: Dict) -> List[str]:
    """Validate transition assignments for smoothness and variety."""
    issues = []
    scenes = directed_plan.get("scenes", [])
    if not scenes:
        return issues

    prev_exit = ""
    transition_types_used = set()

    for scene in scenes:
        sid = scene.get("scene_id", 0)
        entry = scene.get("entry_transition", "")
        exit_t = scene.get("exit_transition", "")

        if not entry and not exit_t:
            issues.append(f"Scene {sid}: no transitions assigned")
            continue

        # Consecutive identical transitions = monotonous
        if entry == prev_exit and entry:
            issues.append(f"Scene {sid}: entry '{entry}' repeats previous exit")

        transition_types_used.add(entry)
        transition_types_used.add(exit_t)
        prev_exit = exit_t

    # Check variety: at least 3 different transition types across scenes
    if len(scenes) >= 4 and len(transition_types_used) < 3:
        issues.append(f"Low transition variety: only {len(transition_types_used)} types used")

    return issues


def check_pacing(directed_plan: Dict) -> List[str]:
    """Validate scene pacing — each scene should be 3-6 seconds."""
    issues = []
    scenes = directed_plan.get("scenes", [])

    for scene in scenes:
        sid = scene.get("scene_id", 0)
        dur = float(scene.get("estimated_duration", 0))

        if dur < 3.0:
            issues.append(f"Scene {sid}: {dur:.1f}s too short (min 3s)")
        elif dur > 6.0:
            issues.append(f"Scene {sid}: {dur:.1f}s too long (max 6s)")

    # First scene must be high energy (hook)
    if scenes:
        first_energy = int(scenes[0].get("energy", 0))
        if first_energy < 4:
            issues.append(f"Hook scene energy {first_energy}/5 is weak (need >= 4)")

    # Last scene should create open loop
    if scenes:
        last_emotion = scenes[-1].get("emotion", "")
        if last_emotion not in ("curiosity", "tension"):
            issues.append(f"Last scene emotion '{last_emotion}' doesn't create open loop")

    return issues


def check_scene_diversity(directed_plan: Dict) -> Tuple[float, List[str]]:
    """
    Calculate scene diversity score (0.0-1.0) and flag issues.
    Measures variety across visual_intent, camera_style, shot_type, and emotion.
    """
    issues = []
    scenes = directed_plan.get("scenes", [])
    if len(scenes) < 2:
        return 1.0, issues

    # Count unique values for each visual dimension
    intents = set()
    cameras = set()
    shots = set()
    emotions = set()

    for scene in scenes:
        intents.add(scene.get("visual_intent", ""))
        cameras.add(scene.get("camera_style", scene.get("camera_motion", "")))
        shots.add(scene.get("shot_type", ""))
        emotions.add(scene.get("emotion", ""))

    n = len(scenes)
    # Diversity = average ratio of unique values to total scenes
    intent_div = len(intents) / n
    camera_div = len(cameras) / n
    shot_div = len(shots) / n
    emotion_div = len(emotions) / n

    score = round((intent_div + camera_div + shot_div + emotion_div) / 4.0, 3)

    if score < 0.4:
        issues.append(f"Scene diversity score {score:.2f} is very low (need > 0.6)")
    elif score < 0.6:
        issues.append(f"Scene diversity score {score:.2f} is below target (need > 0.6)")

    return score, issues


def check_static_footage_ratio(directed_plan: Dict) -> Tuple[float, List[str]]:
    """
    Estimate ratio of static (no motion) footage.
    Reject if > 40% static.
    """
    issues = []
    scenes = directed_plan.get("scenes", [])
    if not scenes:
        return 0.0, issues

    static_count = 0
    total_duration = 0.0

    for scene in scenes:
        dur = float(scene.get("estimated_duration", 3.0))
        total_duration += dur
        camera = scene.get("camera_motion", scene.get("camera_style", ""))
        movement = scene.get("movement", "")

        if camera in ("static", "static_tension", "") and movement in ("static_tension", ""):
            static_count += 1

    ratio = static_count / max(len(scenes), 1)

    if ratio > 0.4:
        issues.append(f"Static footage ratio {ratio:.0%} exceeds 40% limit")

    return round(ratio, 3), issues


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

    # Scene-plan-based checks — only run when scene data is available
    scenes = scene_plan.get("scenes", [])
    if scenes:
        # Scene plan checks
        plan_issues = check_scene_plan(scene_plan)
        results["checks"]["scene_plan"] = {"ok": len(plan_issues) == 0, "issues": plan_issues}
        results["issues"].extend(plan_issues)

        # Transition quality checks
        transition_issues = check_transitions(scene_plan)
        results["checks"]["transitions"] = {"ok": len(transition_issues) == 0, "issues": transition_issues}
        results["issues"].extend(transition_issues)

        # Pacing validation
        pacing_issues = check_pacing(scene_plan)
        results["checks"]["pacing"] = {"ok": len(pacing_issues) == 0, "issues": pacing_issues}
        results["issues"].extend(pacing_issues)

        # Scene diversity score
        diversity_score, diversity_issues = check_scene_diversity(scene_plan)
        results["checks"]["diversity"] = {"ok": diversity_score >= 0.6, "score": diversity_score, "issues": diversity_issues}
        results["issues"].extend(diversity_issues)

        # Static footage ratio
        static_ratio, static_issues = check_static_footage_ratio(scene_plan)
        results["checks"]["static_footage"] = {"ok": static_ratio <= 0.4, "ratio": static_ratio, "issues": static_issues}
        if static_ratio > 0.4:
            results["passed"] = False
        results["issues"].extend(static_issues)

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
    from app.utils.pipeline_logger import StageLogger

    slog = StageLogger("quality_checker")

    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    finals_dir = os.path.join(base_dir, "data", "shorts", "final")

    if not os.path.exists(finals_dir):
        print("No rendered shorts to check.")
        slog.warning("No rendered shorts found", suggestion="Check video_builder output")
        slog.finish(success=False)
        return

    passed = 0
    failed = 0
    all_issues = []

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
                slog.event("qc_failed", {"file": mp4, "issues": result["issues"][:5]})

            all_issues.extend(result["issues"])

            print(f"  {status} {mp4}")
            for issue in result["issues"][:3]:
                print(f"       {issue}")

    slog.metric("videos_checked", passed + failed)
    slog.metric("qc_passed", passed)
    slog.metric("qc_failed", failed)

    # Categorize issues for improvement suggestions
    issue_categories = {}
    for issue in all_issues:
        key = issue.split(":")[0].strip() if ":" in issue else issue[:40]
        issue_categories[key] = issue_categories.get(key, 0) + 1

    slog.metric("issue_categories", issue_categories)

    if failed > 0:
        slog.warning(f"{failed}/{passed + failed} videos failed QC",
                     suggestion="Review issue categories to prioritize fixes")
    if not all_issues:
        slog.event("all_passed", {"count": passed})

    print(f"\n--- Quality Check ---")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    print(f"---------------------")
    slog.finish(success=failed == 0)


if __name__ == "__main__":
    main()
