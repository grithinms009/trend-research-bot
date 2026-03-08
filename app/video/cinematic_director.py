"""
Cinematic Director — the creative brain between scene planner and video builder.

Takes raw scene plans and adds detailed film editing instructions:
- Camera motion (slow_push, fast_punch, static, drift)
- Cut timing (on_beat, pre_beat, hard_interrupt)
- Text styling (bold_impact, minimal, documentary)
- Text position + animation
- Sound design cues (impact_hit, bass_rumble, silence_pause)
- Color grade per scene

Uses LLM to decide HOW the video feels, not just what is shown.
Falls back to rule-based decisions if LLM fails.
"""

import os
import json
import glob
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional

from app.services.ollama_client import OllamaClient
from app.video.transition_engine import TransitionEngine

logger = logging.getLogger(__name__)

# ============================================================
# VALID OPTIONS
# ============================================================
CAMERA_MOTIONS = {"slow_push", "fast_punch", "static", "drift", "pull_out", "lateral_pan"}
CUT_TIMINGS = {"on_beat", "pre_beat", "hard_interrupt"}
TEXT_STYLES = {"bold_impact", "minimal", "documentary"}
TEXT_POSITIONS = {"center", "upper_middle", "lower_third"}
TEXT_ANIMATIONS = {"fade_pop", "slide_up", "word_by_word", "scale_in"}
SOUND_DESIGNS = {"impact_hit", "bass_rumble", "silence_pause", "whoosh", "tension_hum", "none"}
COLOR_GRADES = {"dramatic", "cool_news", "warm_luxury", "cinematic_dark", "bright_clean", "neutral"}
SHOT_TYPES = {"wide", "medium", "close_up", "detail", "abstract"}
MOVEMENTS = {"push_in", "pull_out", "lateral_pan", "static_tension", "slow_drift"}

# ============================================================
# RULE-BASED CINEMATIC DECISIONS (deterministic fallback)
# ============================================================
EMOTION_TO_CAMERA = {
    "shock": "fast_punch",
    "tension": "slow_push",
    "reveal": "slow_push",
    "dramatic": "drift",
    "curiosity": "slow_push",
    "urgency": "fast_punch",
    "neutral": "static",
}

EMOTION_TO_CUT = {
    "shock": "hard_interrupt",
    "tension": "pre_beat",
    "reveal": "on_beat",
    "dramatic": "on_beat",
    "curiosity": "pre_beat",
    "urgency": "hard_interrupt",
    "neutral": "on_beat",
}

EMOTION_TO_SOUND = {
    "shock": "impact_hit",
    "tension": "tension_hum",
    "reveal": "bass_rumble",
    "dramatic": "bass_rumble",
    "curiosity": "whoosh",
    "urgency": "impact_hit",
    "neutral": "none",
}

ENERGY_TO_TEXT = {
    5: ("bold_impact", "center", "fade_pop"),
    4: ("bold_impact", "upper_middle", "scale_in"),
    3: ("minimal", "center", "word_by_word"),
    2: ("documentary", "lower_third", "slide_up"),
    1: ("minimal", "lower_third", "fade_pop"),
}

# ============================================================
# VISUAL METAPHOR ENGINE
# ============================================================
VISUAL_METAPHORS = {
    "change": ["time-lapse city night", "pages flipping fast", "metamorphosis nature"],
    "danger": ["dark corridor cinematic", "storm clouds dramatic", "red warning light"],
    "power": ["lightning strike dramatic", "chess piece close up", "gavel striking"],
    "money": ["gold bars vault", "stock ticker fast", "cash counting macro"],
    "speed": ["sports car blur", "train rushing past", "typing fast close up"],
    "mystery": ["fog dark forest", "door opening slowly", "silhouette dramatic"],
    "growth": ["plant timelapse", "rocket launch", "sunrise mountain"],
    "destruction": ["glass breaking slow motion", "building demolition", "fire dramatic"],
    "technology": ["circuit board macro", "holographic display", "server lights blinking"],
    "conflict": ["chess game overhead", "boxing ring dramatic", "courtroom gavel"],
}

# Channel-specific color grades
CHANNEL_COLOR_GRADES = {
    "C1": "cool_news",
    "C5": "bright_clean",
}

CINEMATIC_PROMPT = """You are a Senior Film Editor directing a YouTube Short.

Given this scene from a narration, decide the exact editing style.

SCENE:
- Narration: "{narration}"
- Emotion: {emotion}
- Energy: {energy}/5
- Visual Intent: {visual_intent}
- Channel: {channel} ({channel_tone})

Output a JSON object with EXACTLY these fields:
- "camera_motion": one of: slow_push, fast_punch, static, drift, pull_out, lateral_pan
- "cut_timing": one of: on_beat, pre_beat, hard_interrupt
- "text_style": one of: bold_impact, minimal, documentary
- "text_position": one of: center, upper_middle, lower_third
- "text_animation": one of: fade_pop, slide_up, word_by_word, scale_in
- "sound_design": one of: impact_hit, bass_rumble, silence_pause, whoosh, tension_hum, none
- "color_grade": one of: dramatic, cool_news, warm_luxury, cinematic_dark, bright_clean, neutral
- "shot_type": one of: wide, medium, close_up, detail, abstract
- "movement": one of: push_in, pull_out, lateral_pan, static_tension, slow_drift
- "visual_metaphor_override": string or null (if literal footage would be weak, suggest a symbolic alternative)

Rules:
- First scene of every video MUST have energy-matching intensity
- Shock = hard_interrupt + impact_hit + fast_punch
- Reveal = on_beat + bass_rumble + slow_push
- No two consecutive scenes should have identical camera_motion
- Output ONLY the JSON object. No explanation."""


def _rule_based_direction(scene: Dict, channel: str, prev_camera: str = "") -> Dict:
    """Deterministic fallback for cinematic decisions."""
    emotion = scene.get("emotion", "neutral")
    energy = int(scene.get("energy", 3))

    camera = EMOTION_TO_CAMERA.get(emotion, "static")
    # Avoid repeating same camera motion
    if camera == prev_camera:
        alt = {"slow_push": "drift", "fast_punch": "slow_push", "static": "drift",
               "drift": "slow_push", "pull_out": "static", "lateral_pan": "drift"}
        camera = alt.get(camera, "static")

    cut = EMOTION_TO_CUT.get(emotion, "on_beat")
    sound = EMOTION_TO_SOUND.get(emotion, "none")

    text_style, text_pos, text_anim = ENERGY_TO_TEXT.get(energy, ENERGY_TO_TEXT[3])
    color = CHANNEL_COLOR_GRADES.get(channel, "neutral")

    # Shot type based on energy
    if energy >= 4:
        shot = "close_up"
        movement = "push_in"
    elif energy >= 3:
        shot = "medium"
        movement = "slow_drift"
    else:
        shot = "wide"
        movement = "static_tension"

    return {
        "camera_motion": camera,
        "cut_timing": cut,
        "text_style": text_style,
        "text_position": text_pos,
        "text_animation": text_anim,
        "sound_design": sound,
        "color_grade": color,
        "shot_type": shot,
        "movement": movement,
        "visual_metaphor_override": None,
    }


def _parse_llm_direction(raw: str) -> Optional[Dict]:
    """Parse LLM output into validated direction dict."""
    raw = raw.strip()
    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1:
        return None

    try:
        d = json.loads(raw[start:end + 1])
    except json.JSONDecodeError:
        return None

    if not isinstance(d, dict):
        return None

    # Validate and sanitize
    result = {}
    result["camera_motion"] = d.get("camera_motion", "static")
    if result["camera_motion"] not in CAMERA_MOTIONS:
        result["camera_motion"] = "static"

    result["cut_timing"] = d.get("cut_timing", "on_beat")
    if result["cut_timing"] not in CUT_TIMINGS:
        result["cut_timing"] = "on_beat"

    result["text_style"] = d.get("text_style", "minimal")
    if result["text_style"] not in TEXT_STYLES:
        result["text_style"] = "minimal"

    result["text_position"] = d.get("text_position", "center")
    if result["text_position"] not in TEXT_POSITIONS:
        result["text_position"] = "center"

    result["text_animation"] = d.get("text_animation", "word_by_word")
    if result["text_animation"] not in TEXT_ANIMATIONS:
        result["text_animation"] = "word_by_word"

    result["sound_design"] = d.get("sound_design", "none")
    if result["sound_design"] not in SOUND_DESIGNS:
        result["sound_design"] = "none"

    result["color_grade"] = d.get("color_grade", "neutral")
    if result["color_grade"] not in COLOR_GRADES:
        result["color_grade"] = "neutral"

    result["shot_type"] = d.get("shot_type", "medium")
    if result["shot_type"] not in SHOT_TYPES:
        result["shot_type"] = "medium"

    result["movement"] = d.get("movement", "slow_drift")
    if result["movement"] not in MOVEMENTS:
        result["movement"] = "slow_drift"

    result["visual_metaphor_override"] = d.get("visual_metaphor_override")

    return result


class CinematicDirector:
    """Adds film-level editing instructions and cinematic transitions to each scene."""

    def __init__(self):
        self.transition_engine = TransitionEngine()
        self.metrics = {
            "scenes_directed": 0,
            "llm_used": 0,
            "fallback_used": 0,
            "direction_times": [],
        }

    def direct_scene(self, scene: Dict, channel: str, channel_tone: str,
                     prev_camera: str = "", model: str = "mistral:latest") -> Dict:
        """Generate cinematic direction for a single scene."""
        start = time.time()

        # Try LLM first
        prompt = CINEMATIC_PROMPT.format(
            narration=scene.get("narration", "")[:200],
            emotion=scene.get("emotion", "neutral"),
            energy=scene.get("energy", 3),
            visual_intent=scene.get("visual_intent", "cinematic_dark"),
            channel=channel,
            channel_tone=channel_tone,
        )

        try:
            raw = OllamaClient.generate(prompt, model=model, timeout=60)
            if raw:
                direction = _parse_llm_direction(raw)
                if direction:
                    # Validate no repeat camera
                    if direction["camera_motion"] == prev_camera:
                        alt = {"slow_push": "drift", "fast_punch": "slow_push",
                               "static": "drift", "drift": "slow_push"}
                        direction["camera_motion"] = alt.get(direction["camera_motion"], "static")

                    elapsed = round(time.time() - start, 2)
                    self.metrics["llm_used"] += 1
                    self.metrics["direction_times"].append(elapsed)
                    return direction
        except Exception as exc:
            logger.error("Cinematic LLM failed: %s", exc)

        # Fallback
        self.metrics["fallback_used"] += 1
        return _rule_based_direction(scene, channel, prev_camera)

    def direct_topic(self, scene_plan: Dict, channel_config: Dict,
                     model: str = "mistral:latest") -> Dict:
        """Add cinematic direction and transitions to all scenes in a topic."""
        channel = scene_plan.get("channel_id", "C1")
        ch = channel_config.get(channel, {})
        tone = ch.get("tone", "neutral")
        scenes = scene_plan.get("scenes", [])

        prev_camera = ""
        directed_scenes = []

        for scene in scenes:
            direction = self.direct_scene(scene, channel, tone, prev_camera, model)
            self.metrics["scenes_directed"] += 1

            # Merge direction into scene
            directed = dict(scene)
            directed.update(direction)
            directed_scenes.append(directed)

            prev_camera = direction.get("camera_motion", "")

        # Apply transition engine — adds entry/exit transitions, overlap, momentum
        directed_scenes = self.transition_engine.plan_transitions(directed_scenes)

        # Validate transitions for quality issues
        transition_issues = self.transition_engine.validate_transitions(directed_scenes)

        result = dict(scene_plan)
        result["scenes"] = directed_scenes
        result["cinematic_directed"] = True
        result["transitions_applied"] = True
        result["transition_issues"] = transition_issues
        result["directed_at"] = datetime.now().isoformat()
        return result

    def log_metrics(self):
        print("\n--- Cinematic Director Metrics ---")
        for k, v in self.metrics.items():
            if k == "direction_times" and v:
                print(f"  avg_direction_time: {sum(v) / len(v):.2f}s")
            elif k != "direction_times":
                print(f"  {k}: {v}")
        self.transition_engine.log_metrics()


def main():
    import yaml
    from app.utils.pipeline_logger import StageLogger

    slog = StageLogger("cinematic_director")

    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    scene_plan_dir = os.path.join(base_dir, "data", "scene_plans")
    directed_dir = os.path.join(base_dir, "data", "directed_plans")
    os.makedirs(directed_dir, exist_ok=True)

    config_path = os.path.join(base_dir, "app", "config", "channels.yaml")
    with open(config_path) as f:
        raw = yaml.safe_load(f) or {}
    channel_config = raw.get("channels", {}) if isinstance(raw, dict) else {}

    scene_files = sorted(glob.glob(os.path.join(scene_plan_dir, "*", "*.json")))
    scene_files += sorted(glob.glob(os.path.join(scene_plan_dir, "*.json")))

    if not scene_files:
        print("No scene plans found for cinematic direction.")
        slog.warning("No scene plans found", suggestion="Check scene_planner output")
        slog.finish(success=False)
        return

    director = CinematicDirector()
    total = 0
    total_transition_issues = 0

    for plan_path in scene_files:
        with open(plan_path) as f:
            data = json.load(f)

        plans = data if isinstance(data, list) else [data]
        directed_plans = []

        for plan in plans:
            directed = director.direct_topic(plan, channel_config)
            directed_plans.append(directed)
            total += 1
            title = plan.get("title", "?")[:50]
            n = len(directed["scenes"])
            t_issues = len(directed.get("transition_issues", []))
            total_transition_issues += t_issues
            slog.event("topic_directed", {"title": title, "scenes": n, "transition_issues": t_issues})
            print(f"  Directed: {title} ({n} scenes)")

        fname = os.path.basename(plan_path)
        outfile = os.path.join(directed_dir, fname)
        with open(outfile, "w") as f:
            json.dump(directed_plans, f, indent=2)

    # Log metrics
    slog.metric("topics_directed", total)
    slog.metric("llm_used", director.metrics["llm_used"])
    slog.metric("fallback_used", director.metrics["fallback_used"])
    slog.metric("transition_issues", total_transition_issues)
    if director.metrics["direction_times"]:
        avg_time = sum(director.metrics["direction_times"]) / len(director.metrics["direction_times"])
        slog.metric("avg_direction_time_s", round(avg_time, 2))

    if director.metrics["fallback_used"] > director.metrics["llm_used"]:
        slog.warning("High fallback rate for cinematic direction",
                     suggestion="Check Ollama model availability or increase timeout")
    if total_transition_issues > 2:
        slog.warning(f"{total_transition_issues} transition issues detected",
                     suggestion="Review transition variety and energy-tier matching")

    director.log_metrics()
    print(f"\nTotal topics directed: {total}")
    print(f"Output: {directed_dir}")
    slog.finish(success=True)


if __name__ == "__main__":
    main()
