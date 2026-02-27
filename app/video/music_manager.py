"""
Music Manager — provides royalty-free background music per channel profile.

Uses FFmpeg to generate simple ambient tones as background music.
In production, replace with actual royalty-free music files.
"""

import os
import subprocess
import logging
import yaml
from typing import Dict, Optional

logger = logging.getLogger(__name__)


def _load_channel_config() -> Dict:
    base = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    path = os.path.join(base, "app", "config", "channels.yaml")
    with open(path) as f:
        return yaml.safe_load(f).get("channels", {})


# Ambient tone configs per music profile (frequency, volume)
# These generate simple sine-wave ambient backgrounds via FFmpeg
MUSIC_PROFILES = {
    "tech_ambient": {"freq": 220, "vol": 0.03, "desc": "Low ambient hum"},
    "corporate": {"freq": 330, "vol": 0.02, "desc": "Subtle corporate tone"},
    "atmospheric": {"freq": 174, "vol": 0.04, "desc": "Deep atmospheric"},
    "upbeat_classy": {"freq": 392, "vol": 0.02, "desc": "Light upbeat tone"},
    "motivational": {"freq": 261, "vol": 0.03, "desc": "Motivational C note"},
}


def generate_ambient_track(
    output_path: str,
    duration: float,
    music_profile: str = "tech_ambient",
) -> bool:
    """Generate a simple ambient background track using FFmpeg.
    
    This creates a subtle sine-wave background. For real production,
    replace with actual royalty-free music files per channel.
    """
    profile = MUSIC_PROFILES.get(music_profile, MUSIC_PROFILES["tech_ambient"])
    freq = profile["freq"]
    vol = profile["vol"]

    # Generate a layered ambient sound: base freq + subtle harmonics
    cmd = [
        "ffmpeg", "-y",
        "-f", "lavfi",
        "-i", f"sine=frequency={freq}:duration={duration}:sample_rate=44100",
        "-f", "lavfi",
        "-i", f"sine=frequency={freq * 1.5}:duration={duration}:sample_rate=44100",
        "-filter_complex",
        f"[0:a]volume={vol}[a1];"
        f"[1:a]volume={vol * 0.5}[a2];"
        f"[a1][a2]amix=inputs=2:duration=first[mixed];"
        f"[mixed]afade=t=in:st=0:d=2,afade=t=out:st={max(0, duration - 2)}:d=2[out]",
        "-map", "[out]",
        "-c:a", "aac", "-b:a", "128k",
        output_path,
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0 and os.path.exists(output_path):
            return True
        else:
            logger.error("FFmpeg music gen failed: %s", result.stderr[:300])
            return False
    except Exception as exc:
        logger.error("Music generation error: %s", exc)
        return False


def get_music_for_channel(channel_id: str, duration: float, output_dir: str) -> Optional[str]:
    """Get or generate background music for a channel."""
    config = _load_channel_config()
    ch = config.get(channel_id, {})
    music_profile = ch.get("music_profile", "tech_ambient")

    os.makedirs(output_dir, exist_ok=True)
    music_path = os.path.join(output_dir, f"{channel_id}_bg_music.aac")

    # Check if we already have a music file for this channel
    if os.path.exists(music_path) and os.path.getsize(music_path) > 100:
        return music_path

    print(f"  Generating {music_profile} background music ({duration:.1f}s)...")
    if generate_ambient_track(music_path, duration, music_profile):
        return music_path

    return None


if __name__ == "__main__":
    # Test: generate a 30-second ambient track
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    out_dir = os.path.join(base_dir, "data", "shorts", "music")
    os.makedirs(out_dir, exist_ok=True)

    for cid in ["C1", "C2", "C3", "C4", "C5"]:
        path = get_music_for_channel(cid, 30.0, out_dir)
        if path:
            size_kb = os.path.getsize(path) / 1024
            print(f"  ✅ {cid}: {path} ({size_kb:.0f}KB)")
        else:
            print(f"  ❌ {cid}: failed")
