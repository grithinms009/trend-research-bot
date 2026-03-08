"""
Transition Engine — cinematic transition planning between scenes.

Removes the "hard stop" feel between scenes by assigning energy-aware
entry/exit transitions with visual momentum continuity.

Rules:
1. No same transition type used twice consecutively
2. Scene energy drives transition speed and style
3. High energy → fast transitions (whip_pan, zoom_match, light_flash)
4. Low energy → slow transitions (slow_crossfade, blur_morph)
5. Scene overlap: next scene starts 0.15s before previous ends
6. First scene always gets a dramatic entry (fade_from_black or zoom_in)
7. Last scene always gets an open-loop exit (slow_fade or blur_morph)

Integration:
- Called by cinematic_director after scene planning
- Output is merged into each scene's directed plan
- video_builder reads transition fields for ffmpeg filter assembly
"""

import logging
import random
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# ============================================================
# TRANSITION LIBRARY
# ============================================================

# Transitions grouped by energy tier
HIGH_ENERGY_TRANSITIONS = [
    "whip_pan",
    "zoom_match",
    "light_flash",
    "fast_cut",
    "glitch_cut",
]

MEDIUM_ENERGY_TRANSITIONS = [
    "crossfade",
    "directional_cut",
    "motion_match",
    "slide_wipe",
    "push_cut",
]

LOW_ENERGY_TRANSITIONS = [
    "slow_crossfade",
    "blur_morph",
    "dissolve",
    "fade_through_black",
    "soft_wipe",
]

# Special transitions for first/last scenes
ENTRY_TRANSITIONS = ["fade_from_black", "zoom_in_reveal", "blur_to_sharp"]
EXIT_TRANSITIONS = ["slow_fade", "blur_morph", "fade_to_black"]

# Transition duration mapping (seconds)
TRANSITION_DURATIONS = {
    # High energy
    "whip_pan": 0.20,
    "zoom_match": 0.25,
    "light_flash": 0.15,
    "fast_cut": 0.10,
    "glitch_cut": 0.18,
    # Medium energy
    "crossfade": 0.40,
    "directional_cut": 0.30,
    "motion_match": 0.35,
    "slide_wipe": 0.35,
    "push_cut": 0.30,
    # Low energy
    "slow_crossfade": 0.60,
    "blur_morph": 0.50,
    "dissolve": 0.55,
    "fade_through_black": 0.50,
    "soft_wipe": 0.45,
    # Special
    "fade_from_black": 0.40,
    "zoom_in_reveal": 0.35,
    "blur_to_sharp": 0.30,
    "slow_fade": 0.50,
    "fade_to_black": 0.45,
    "hard_cut": 0.0,
}

# Visual direction map for directional continuity
# Maps camera movement → compatible exit/entry directions
DIRECTION_CONTINUITY = {
    "push_in": ["zoom_match", "crossfade", "fast_cut"],
    "pull_out": ["dissolve", "slow_crossfade", "directional_cut"],
    "lateral_pan": ["whip_pan", "slide_wipe", "push_cut"],
    "slow_drift": ["crossfade", "blur_morph", "slow_crossfade"],
    "static_tension": ["light_flash", "fast_cut", "crossfade"],
}

# Scene overlap: next scene starts before previous ends
SCENE_OVERLAP_SECONDS = 0.15

# Emotion → transition mood bias
EMOTION_TRANSITION_BIAS = {
    "shock": HIGH_ENERGY_TRANSITIONS,
    "urgency": HIGH_ENERGY_TRANSITIONS,
    "tension": MEDIUM_ENERGY_TRANSITIONS + ["light_flash"],
    "dramatic": MEDIUM_ENERGY_TRANSITIONS + ["zoom_match"],
    "reveal": ["crossfade", "blur_morph", "zoom_match", "dissolve"],
    "curiosity": MEDIUM_ENERGY_TRANSITIONS + ["blur_morph"],
    "neutral": LOW_ENERGY_TRANSITIONS + ["crossfade"],
}


def _get_energy_tier(energy: int) -> str:
    """Classify energy level into tier."""
    if energy >= 4:
        return "high"
    elif energy >= 2:
        return "medium"
    return "low"


def _get_transition_pool(energy: int, emotion: str, movement: str = "") -> List[str]:
    """Build a pool of candidate transitions based on energy, emotion, and movement."""
    tier = _get_energy_tier(energy)

    # Start with energy-tier pool
    if tier == "high":
        pool = list(HIGH_ENERGY_TRANSITIONS)
    elif tier == "medium":
        pool = list(MEDIUM_ENERGY_TRANSITIONS)
    else:
        pool = list(LOW_ENERGY_TRANSITIONS)

    # Add emotion-biased transitions
    emotion_bias = EMOTION_TRANSITION_BIAS.get(emotion, [])
    for t in emotion_bias:
        if t not in pool:
            pool.append(t)

    # Add movement-compatible transitions
    if movement:
        compatible = DIRECTION_CONTINUITY.get(movement, [])
        for t in compatible:
            if t not in pool:
                pool.append(t)

    return pool


def _pick_transition(pool: List[str], previous: str) -> str:
    """Pick a transition from pool, avoiding the previous one."""
    candidates = [t for t in pool if t != previous]
    if not candidates:
        candidates = pool
    return random.choice(candidates)


class TransitionEngine:
    """
    Plans cinematic transitions between scenes.

    Each scene receives:
    - entry_transition: how this scene enters
    - exit_transition: how this scene exits
    - transition_duration: seconds for the transition effect
    - scene_overlap: seconds of overlap with adjacent scene
    - visual_momentum: direction continuity tag
    """

    def __init__(self):
        self.metrics = {
            "scenes_processed": 0,
            "transitions_planned": 0,
            "continuity_matches": 0,
        }

    def plan_transitions(self, scenes: List[Dict]) -> List[Dict]:
        """
        Plan entry/exit transitions for a sequence of scenes.

        Args:
            scenes: List of scene dicts with energy, emotion, movement fields.

        Returns:
            Same scenes with transition fields added.
        """
        if not scenes:
            return scenes

        result = []
        prev_exit = ""
        num_scenes = len(scenes)

        for i, scene in enumerate(scenes):
            s = dict(scene)
            energy = int(s.get("energy", 3))
            emotion = s.get("emotion", "neutral")
            movement = s.get("movement", s.get("camera_motion", ""))
            is_first = (i == 0)
            is_last = (i == num_scenes - 1)

            pool = _get_transition_pool(energy, emotion, movement)

            # --- Entry transition ---
            if is_first:
                entry = random.choice(ENTRY_TRANSITIONS)
            else:
                # Entry should complement the previous scene's exit
                entry = _pick_transition(pool, prev_exit)

            # --- Exit transition ---
            if is_last:
                exit_t = random.choice(EXIT_TRANSITIONS)
            else:
                # Look ahead at next scene energy for smooth handoff
                next_energy = int(scenes[i + 1].get("energy", 3)) if i + 1 < num_scenes else 3
                next_emotion = scenes[i + 1].get("emotion", "neutral") if i + 1 < num_scenes else "neutral"

                # Blend current and next scene characteristics
                avg_energy = (energy + next_energy) // 2
                blend_pool = _get_transition_pool(avg_energy, next_emotion, movement)
                exit_t = _pick_transition(blend_pool, entry)

            # --- Transition durations ---
            entry_dur = TRANSITION_DURATIONS.get(entry, 0.30)
            exit_dur = TRANSITION_DURATIONS.get(exit_t, 0.30)

            # --- Visual momentum ---
            # Check if movement direction creates natural continuity
            momentum = "neutral"
            if movement in DIRECTION_CONTINUITY:
                compatible = DIRECTION_CONTINUITY[movement]
                if exit_t in compatible:
                    momentum = "matched"
                    self.metrics["continuity_matches"] += 1

            # --- Scene overlap ---
            overlap = SCENE_OVERLAP_SECONDS if not is_first else 0.0

            s["entry_transition"] = entry
            s["exit_transition"] = exit_t
            s["entry_transition_duration"] = round(entry_dur, 3)
            s["exit_transition_duration"] = round(exit_dur, 3)
            s["scene_overlap"] = round(overlap, 3)
            s["visual_momentum"] = momentum

            result.append(s)
            prev_exit = exit_t
            self.metrics["scenes_processed"] += 1
            self.metrics["transitions_planned"] += 2  # entry + exit

        return result

    def validate_transitions(self, scenes: List[Dict]) -> List[str]:
        """Validate transition assignments for quality issues."""
        issues = []
        prev_exit = ""

        for i, scene in enumerate(scenes):
            entry = scene.get("entry_transition", "")
            exit_t = scene.get("exit_transition", "")

            # Check for consecutive identical transitions
            if entry == prev_exit and entry:
                issues.append(
                    f"Scene {scene.get('scene_id', i+1)}: entry '{entry}' "
                    f"matches previous exit (monotonous)"
                )

            # Check transition exists in library
            if entry and entry not in TRANSITION_DURATIONS:
                issues.append(f"Scene {scene.get('scene_id', i+1)}: unknown entry '{entry}'")
            if exit_t and exit_t not in TRANSITION_DURATIONS:
                issues.append(f"Scene {scene.get('scene_id', i+1)}: unknown exit '{exit_t}'")

            # Check energy alignment
            energy = int(scene.get("energy", 3))
            tier = _get_energy_tier(energy)
            if tier == "high" and entry in LOW_ENERGY_TRANSITIONS:
                issues.append(
                    f"Scene {scene.get('scene_id', i+1)}: high energy scene "
                    f"with slow transition '{entry}'"
                )

            prev_exit = exit_t

        return issues

    def log_metrics(self):
        print("\n--- Transition Engine Metrics ---")
        for k, v in self.metrics.items():
            print(f"  {k}: {v}")
        print("--------------------------------\n")
