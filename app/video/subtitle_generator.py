"""
Cinematic Subtitle Engine — film-grade ASS subtitle generator.

Features:
- Word-group timing (2-4 words max per display)
- Emphasis word highlighting with scale animation
- Cinematic typography (custom fonts, stroke, shadow)
- Position control (center, upper_middle, lower_third)
- Animation per directive (fade_pop, slide_up, word_by_word, scale_in)
- Energy-reactive font sizing
- Safe margins (avoid bottom cutoff on mobile)
- Channel-specific styling from creative profiles
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
    h = hex_color.lstrip("#")
    r, g, b = h[0:2], h[2:4], h[4:6]
    return f"&H00{b}{g}{r}&"


def _time(seconds: float) -> str:
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    cs = int((seconds % 1) * 100)
    return f"{h}:{m:02d}:{s:02d}.{cs:02d}"


# ============================================================
# POSITION → ASS ALIGNMENT + MARGIN
# ============================================================
POSITION_MAP = {
    "center": {"alignment": 5, "marginV": 400},       # Vertically centered
    "upper_middle": {"alignment": 8, "marginV": 350},  # Upper area
    "lower_third": {"alignment": 2, "marginV": 250},   # Above safe zone
}

# ============================================================
# TEXT ANIMATION → ASS OVERRIDE TAGS
# ============================================================
def _anim_fade_pop(word: str, is_emphasis: bool, accent: str) -> str:
    """Fade in + slight scale pop."""
    if is_emphasis:
        return f"{{\\fad(80,0)\\fscx130\\fscy130\\t(0,120,\\fscx100\\fscy100)\\c{accent}}}{word}"
    return f"{{\\fad(80,0)\\fscx110\\fscy110\\t(0,100,\\fscx100\\fscy100)}}{word}"


def _anim_slide_up(word: str, is_emphasis: bool, accent: str) -> str:
    """Slide up from below."""
    if is_emphasis:
        return f"{{\\move(0,30,0,0,0,100)\\c{accent}\\fscx120\\fscy120\\t(80,180,\\fscx100\\fscy100)}}{word}"
    return f"{{\\move(0,20,0,0,0,80)}}{word}"


def _anim_word_by_word(word: str, is_emphasis: bool, accent: str) -> str:
    """Simple appear with emphasis highlight."""
    if is_emphasis:
        return f"{{\\c{accent}\\fscx125\\fscy125\\t(0,150,\\fscx100\\fscy100)}}{word}"
    return word


def _anim_scale_in(word: str, is_emphasis: bool, accent: str) -> str:
    """Scale from 0 to full size."""
    if is_emphasis:
        return f"{{\\fscx0\\fscy0\\t(0,120,\\fscx120\\fscy120)\\t(120,200,\\fscx100\\fscy100)\\c{accent}}}{word}"
    return f"{{\\fscx0\\fscy0\\t(0,100,\\fscx100\\fscy100)}}{word}"


ANIM_FUNCS = {
    "fade_pop": _anim_fade_pop,
    "slide_up": _anim_slide_up,
    "word_by_word": _anim_word_by_word,
    "scale_in": _anim_scale_in,
}


def generate_ass_subtitle(scenes: List[Dict], channel_id: str, channel_config: Dict) -> str:
    """Generate cinematic ASS subtitles with per-scene directives."""
    ch = channel_config.get(channel_id, {})
    font_color = _hex_to_ass(ch.get("font_color", "#FFFFFF"))
    accent_color = _hex_to_ass(ch.get("accent_color", "#FFFF00"))

    creative = ch.get("creative", {})
    caption_style = creative.get("caption_style", "bold_impact")

    # Typography: use cinematic fonts
    font_name = "Montserrat"
    if caption_style == "elegant_minimal":
        font_name = "Poppins"
    elif caption_style == "clean_data":
        font_name = "Inter"
    elif caption_style == "dramatic_reveal":
        font_name = "Montserrat"

    # Base sizes by style
    base_sizes = {
        "bold_impact": 85,
        "minimal": 65,
        "documentary": 60,
        "clean_data": 70,
        "elegant_minimal": 68,
        "clean_modern": 72,
        "dramatic_reveal": 80,
    }
    base_size = base_sizes.get(caption_style, 75)

    # Stroke + shadow for readability
    outline = 4
    shadow = 2

    header = f"""[Script Info]
Title: Cinematic Short
ScriptType: v4.00+
PlayResX: 1080
PlayResY: 1920
WrapStyle: 0

[V4+ Styles]
Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
Style: Center,{font_name},{base_size},{font_color},{accent_color},&H00000000&,&H80000000&,-1,0,0,0,100,100,2,0,1,{outline},{shadow},5,60,60,400,1
Style: CenterBig,{font_name},{base_size + 20},{font_color},{accent_color},&H00000000&,&H80000000&,-1,0,0,0,100,100,2,0,1,{outline + 1},{shadow},5,60,60,400,1
Style: Upper,{font_name},{base_size},{font_color},{accent_color},&H00000000&,&H80000000&,-1,0,0,0,100,100,2,0,1,{outline},{shadow},8,60,60,350,1
Style: Lower,{font_name},{base_size - 5},{font_color},{accent_color},&H00000000&,&H80000000&,-1,0,0,0,100,100,2,0,1,{outline},{shadow},2,60,60,250,1
Style: Emphasis,{font_name},{base_size + 15},{accent_color},{font_color},&H00000000&,&H80000000&,-1,0,0,0,100,100,2,0,1,{outline + 1},{shadow},5,60,60,400,1

[Events]
Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
"""

    events = []
    current_time = 0.0

    for scene in scenes:
        narration = scene.get("narration", scene.get("text", ""))
        duration = float(scene.get("estimated_duration", 3.0))
        energy = scene.get("energy", 3)
        emphasis_words = set(w.lower() for w in scene.get("emphasis_words", []))

        # Cinematic director fields (or defaults)
        text_style = scene.get("text_style", "bold_impact" if energy >= 4 else "minimal")
        text_position = scene.get("text_position", "center")
        text_animation = scene.get("text_animation", "word_by_word")
        cut_timing = scene.get("cut_timing", "on_beat")

        # Get animation function
        anim_func = ANIM_FUNCS.get(text_animation, _anim_word_by_word)

        # Choose style based on position + energy
        pos_config = POSITION_MAP.get(text_position, POSITION_MAP["center"])
        if text_position == "upper_middle":
            style_name = "Upper"
        elif text_position == "lower_third":
            style_name = "Lower"
        elif energy >= 5:
            style_name = "CenterBig"
        else:
            style_name = "Center"

        words = narration.split()
        if not words:
            current_time += duration
            continue

        # Word-group sizing: 2-4 words based on energy
        if energy >= 5:
            chunk_size = 2  # Fast, punchy
        elif energy >= 3:
            chunk_size = 3
        else:
            chunk_size = 4  # Calm, longer groups

        chunks = []
        for i in range(0, len(words), chunk_size):
            chunks.append(words[i:i + chunk_size])

        time_per_chunk = duration / max(len(chunks), 1)

        # Add silence offset for pre_beat cut timing
        time_offset = 0.0
        if cut_timing == "pre_beat":
            time_offset = 0.1  # Slight early appearance
        elif cut_timing == "hard_interrupt":
            time_offset = -0.05  # Slightly late for impact

        for i, chunk_words in enumerate(chunks):
            start = current_time + (i * time_per_chunk) + time_offset
            end = start + time_per_chunk - time_offset
            start = max(0, start)

            # Build animated text
            display_parts = []
            has_emphasis = False
            for word in chunk_words:
                clean = word.strip(".,!?;:'\"").lower()
                is_emph = clean in emphasis_words
                if is_emph:
                    has_emphasis = True
                animated_word = anim_func(word.upper(), is_emph, accent_color)
                display_parts.append(animated_word)

            display_text = " ".join(display_parts)

            # Use emphasis style if chunk has emphasis words
            final_style = "Emphasis" if has_emphasis else style_name

            events.append(
                f"Dialogue: 0,{_time(start)},{_time(end)},{final_style},,0,0,0,,{display_text}"
            )

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

    # Read from directed plans if available, otherwise scene plans
    directed_dir = os.path.join(base_dir, "data", "directed_plans")
    scene_plan_dir = os.path.join(base_dir, "data", "scene_plans")

    source = directed_dir if os.path.exists(directed_dir) else scene_plan_dir
    subs_dir = os.path.join(base_dir, "data", "shorts", "subs")
    os.makedirs(subs_dir, exist_ok=True)

    channel_config = _load_channel_config()

    scene_files = sorted(glob.glob(os.path.join(source, "*", "*.json")))
    scene_files += sorted(glob.glob(os.path.join(source, "*.json")))

    if not scene_files:
        print("No scene/directed plans found.")
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

    print(f"Cinematic subtitles: {total}")


if __name__ == "__main__":
    main()
