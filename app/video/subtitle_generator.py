"""
Subtitle Generator — creates .ass subtitle files for YouTube Shorts.

Generates large, bold, centered captions in channel-specific colors.
Words are timed against audio duration for punchy 2-3 word display.
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


def _hex_to_ass_color(hex_color: str) -> str:
    """Convert hex color #RRGGBB to ASS format &HBBGGRR&."""
    hex_color = hex_color.lstrip("#")
    r, g, b = hex_color[0:2], hex_color[2:4], hex_color[4:6]
    return f"&H00{b}{g}{r}&"


def _format_ass_time(seconds: float) -> str:
    """Format seconds to ASS timestamp: H:MM:SS.CC"""
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    cs = int((seconds % 1) * 100)
    return f"{h}:{m:02d}:{s:02d}.{cs:02d}"


def generate_ass_subtitle(scenes: List[Dict], channel_id: str, channel_config: Dict) -> str:
    """Generate an ASS subtitle file content for a topic's scenes."""
    ch = channel_config.get(channel_id, {})
    font_color = _hex_to_ass_color(ch.get("font_color", "#FFFFFF"))
    accent_color = _hex_to_ass_color(ch.get("accent_color", "#FFFF00"))
    bg_color = "&H80000000&"  # Semi-transparent black outline

    # ASS header with YouTube Shorts styling
    header = f"""[Script Info]
Title: YouTube Short
ScriptType: v4.00+
PlayResX: 1080
PlayResY: 1920
WrapStyle: 0

[V4+ Styles]
Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
Style: Main,Arial Black,90,{font_color},{accent_color},&H00000000&,{bg_color},-1,0,0,0,100,100,0,0,1,4,2,2,40,40,200,1
Style: Accent,Arial Black,100,{accent_color},{font_color},&H00000000&,{bg_color},-1,0,0,0,100,100,0,0,1,5,2,2,40,40,200,1

[Events]
Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
"""

    events = []
    current_time = 0.0

    for scene in scenes:
        text = scene.get("text", "")
        duration = float(scene.get("estimated_duration", scene.get("estimated_duration_sec", 3.0)))
        words = text.split()

        if not words:
            current_time += duration
            continue

        # Split words into groups of 2-3 for punchy captions
        chunk_size = 3
        chunks = []
        for i in range(0, len(words), chunk_size):
            chunk = " ".join(words[i:i + chunk_size])
            chunks.append(chunk)

        # Distribute time across chunks
        time_per_chunk = duration / max(len(chunks), 1)

        for i, chunk in enumerate(chunks):
            start = current_time + (i * time_per_chunk)
            end = start + time_per_chunk

            # Use accent style for first chunk of each scene (hook words)
            style = "Accent" if i == 0 else "Main"

            # Clean text for ASS format
            clean = chunk.replace("\\", "").replace("{", "").replace("}", "")
            clean = clean.upper()  # Shorts captions are typically uppercase

            start_ts = _format_ass_time(start)
            end_ts = _format_ass_time(end)
            events.append(f"Dialogue: 0,{start_ts},{end_ts},{style},,0,0,0,,{clean}")

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
            print(f"  Generated subtitles: {topic_id}.ass")

    print(f"\nTotal subtitle files: {total}")


if __name__ == "__main__":
    main()
