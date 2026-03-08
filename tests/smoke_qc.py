"""Smoke test for new/upgraded modules."""
import sys
sys.path.insert(0, ".")

from app.video.quality_checker import (
    check_transitions, check_pacing,
    check_scene_diversity, check_static_footage_ratio,
)
from app.video.visual_diversity import VisualDiversityEngine

plan = {"scenes": [
    {"scene_id": 1, "energy": 5, "emotion": "shock", "estimated_duration": 4.0,
     "entry_transition": "fade_from_black", "exit_transition": "whip_pan",
     "visual_intent": "abstract_tension", "camera_style": "fast_punch_in", "shot_type": "macro"},
    {"scene_id": 2, "energy": 3, "emotion": "tension", "estimated_duration": 5.0,
     "entry_transition": "crossfade", "exit_transition": "push_cut",
     "visual_intent": "tech_ui", "camera_style": "slow_push_in", "shot_type": "wide"},
    {"scene_id": 3, "energy": 4, "emotion": "reveal", "estimated_duration": 4.5,
     "entry_transition": "slide_wipe", "exit_transition": "blur_morph",
     "visual_intent": "nature", "camera_style": "lateral_pan", "shot_type": "motion"},
    {"scene_id": 4, "energy": 3, "emotion": "curiosity", "estimated_duration": 4.0,
     "entry_transition": "directional_cut", "exit_transition": "fade_to_black",
     "visual_intent": "urban", "camera_style": "slow_drift", "shot_type": "abstract"},
]}

print("Transitions:", check_transitions(plan))
print("Pacing:", check_pacing(plan))
d_score, d_issues = check_scene_diversity(plan)
print(f"Diversity: score={d_score} issues={d_issues}")
s_ratio, s_issues = check_static_footage_ratio(plan)
print(f"Static: ratio={s_ratio} issues={s_issues}")

# Visual diversity similarity
vde = VisualDiversityEngine()
print(f"Similar (first):  {vde.is_prompt_similar('dark cinematic corridor')}")
print(f"Similar (same):   {vde.is_prompt_similar('dark cinematic corridor')}")
print(f"Similar (diff):   {vde.is_prompt_similar('ocean waves sunset beach')}")
print(f"Rejected similar: {vde.metrics['prompts_rejected_similar']}")

print("\nAll smoke tests passed!")
