"""
Caption Engine — animated, emphasis-aware ASS subtitle generator.

Creates word-by-word appearing captions with:
- emphasis_words highlighted in accent color + larger size
- Larger font for high energy scenes (energy >= 4)
- Channel-specific styling
- Bounce animation on impact words via ASS override tags
"""

import os
import json
import glob
import yaml
from typing import Dict, List
from datetime import datetime


def _load_channel_config() -> Dict:
    base = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    path = os.path.join(base, "app", "config", "channels.yaml")
    with open(path) as f:
        return yaml.safe_load(f).get("channels", {})


def _hex_to_ass(hex_color: str) -> str:
    """Convert hex #RRGGBB to ASS &HBBGGRR&."""
    h = hex_color.lstrip("#")
    r, g, b = h[0:2], h[2:4], h[4:6]
    return f"&H00{b}{g}{r}&"


def _time(seconds: float) -> str:
    """Format seconds to ASS timestamp H:MM:SS.CC"""
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    cs = int((seconds % 1) * 100)
    return f"{h}:{m:02d}:{s:02d}.{cs:02d}"


def generate_ass_subtitle(scenes: List[Dict], channel_id: str, channel_config: Dict) -> str:
    """Generate animated ASS subtitles from scene plan."""
    ch = channel_config.get(channel_id, {})
    font_color = _hex_to_ass(ch.get("font_color", "#FFFFFF"))
    accent_color = _hex_to_ass(ch.get("accent_color", "#FFFF00"))

    # Caption style based on channel creative profile
    creative = ch.get("creative", {})
    caption_style = creative.get("caption_style", "bold_impact")

    # Base font sizes
    base_size = 80 if caption_style == "bold_impact" else 70
    emphasis_size = base_size + 20
    high_energy_boost = 15

    header = f"""[Script Info]
Title: YouTube Short
ScriptType: v4.00+
PlayResX: 1080
PlayResY: 1920
WrapStyle: 0

[V4+ Styles]
Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
Style: Main,Arial Black,{base_size},{font_color},{accent_color},&H00000000&,&H80000000&,-1,0,0,0,100,100,0,0,1,4,2,2,40,40,250,1
Style: Emphasis,Arial Black,{emphasis_size},{accent_color},{font_color},&H00000000&,&H80000000&,-1,0,0,0,100,100,0,0,1,5,2,2,40,40,250,1
Style: HighEnergy,Arial Black,{base_size + high_energy_boost},{font_color},{accent_color},&H00000000&,&H80000000&,-1,0,0,0,100,100,0,0,1,5,2,2,40,40,250,1

[Events]
Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
"""

    events = []
    current_time = 0.0

    for scene in scenes:
        narration = scene.get("narration", scene.get("text", ""))
        duration = float(scene.get("estimated_duration", 3.0))
        energy = scene.get("energy", 3)
        emotion = scene.get("emotion", "neutral")
        emphasis_words = [w.lower() for w in scene.get("emphasis_words", [])]
        cut_style = scene.get("cut_style", "hard")

        words = narration.split()
        if not words:
            current_time += duration
            continue

        # Word-by-word timing (2-4 words at a time for readability)
        chunk_size = 2 if energy >= 4 else 3
        chunks = []
        for i in range(0, len(words), chunk_size):
            chunk_words = words[i:i + chunk_size]
            chunks.append(chunk_words)

        time_per_chunk = duration / max(len(chunks), 1)

        for i, chunk_words in enumerate(chunks):
            start = current_time + (i * time_per_chunk)
            end = start + time_per_chunk

            # Determine if this chunk has emphasis words
            has_emphasis = any(w.lower().strip(".,!?;:'\"") in emphasis_words for w in chunk_words)

            # Build the display text with inline ASS overrides
            display_parts = []
            for word in chunk_words:
                clean_word = word.strip(".,!?;:'\"").lower()
                if clean_word in emphasis_words:
                    # Emphasis: accent color + bounce effect (scale up briefly)
                    display_parts.append(
                        f"{{\\c{accent_color}\\fscx120\\fscy120\\t(0,100,\\fscx100\\fscy100)}}{word.upper()}"
                    )
                else:
                    display_parts.append(word.upper())

            display_text = " ".join(display_parts)

            # Choose style based on energy level
            if has_emphasis:
                style = "Emphasis"
            elif energy >= 4:
                style = "HighEnergy"
            else:
                style = "Main"

            # Add fade-in effect for non-smash cuts
            if cut_style != "smash" and i == 0:
                display_text = f"{{\\fad(100,0)}}{display_text}"

            start_ts = _time(start)
            end_ts = _time(end)
            events.append(f"Dialogue: 0,{start_ts},{end_ts},{style},,0,0,0,,{display_text}")

        current_time += duration

    return header + "\n".join(events) + "\n"


def generate_subtitles_for_topic(scene_plan: Dict, output_path: str, channel_config: Dict):
    """Generate .ass subtitle file for a topic."""
    channel = scene_plan.get("channel_id", "C1")
    scenes = scene_plan.get("scenes", [])
    ass_content = generate_ass_subtitle(scenes, channel, channel_config)

    with open(output_path, "w") as f:
        f.write(ass_content)


def main():
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    scene_plan_dir = os.path.join(base_dir, "data", "scene_plans")
    subs_dir = os.path.join(base_dir, "data", "shorts", "subs")
    os.makedirs(subs_dir, exist_ok=True)

    channel_config = _load_channel_config()

    scene_files = sorted(glob.glob(os.path.join(scene_plan_dir, "*", "*.json")))
    scene_files += sorted(glob.glob(os.path.join(scene_plan_dir, "*.json")))

    if not scene_files:
        print("No scene plans found.")
        return

    total = 0
    for plan_path in scene_files:
        with open(plan_path) as f:
            data = json.load(f)

        plans = data if isinstance(data, list) else [data]

        for plan in plans:
            title = plan.get("title", "unknown")
            channel = plan.get("channel_id", "XX")
            safe_title = "".join(c if c.isalnum() or c in "-_ " else "" for c in title)[:50].strip().replace(" ", "_")
            topic_id = f"{channel}_{safe_title}"
            out_path = os.path.join(subs_dir, f"{topic_id}.ass")

            generate_subtitles_for_topic(plan, out_path, channel_config)
            total += 1

    print(f"Total subtitle files: {total}")


if __name__ == "__main__":
    main()
