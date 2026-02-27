"""
Sound Designer — generates SFX audio layers for cinematic shorts.

Creates subtle sound effects via FFmpeg:
- impact_hit: low frequency bass hit
- bass_rumble: deep sub-bass rumble
- whoosh: fast frequency sweep
- tension_hum: sustained low drone
- silence_pause: silence gap for dramatic pause

These get mixed into the final video by the builder.
"""

import os
import subprocess
import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)


# FFmpeg lavfi filters for each SFX type
SFX_CONFIGS = {
    "impact_hit": {
        "filter": "sine=frequency=60:duration=0.3:sample_rate=44100,afade=t=out:st=0.05:d=0.25,volume=0.5",
        "duration": 0.3,
        "desc": "Low bass hit impact",
    },
    "bass_rumble": {
        "filter": "sine=frequency=40:duration=1.0:sample_rate=44100,afade=t=in:st=0:d=0.2,afade=t=out:st=0.5:d=0.5,volume=0.3",
        "duration": 1.0,
        "desc": "Deep sub-bass rumble",
    },
    "whoosh": {
        "filter": "sine=frequency=200:duration=0.4:sample_rate=44100,asetrate=44100*2,afade=t=in:st=0:d=0.1,afade=t=out:st=0.2:d=0.2,volume=0.4",
        "duration": 0.4,
        "desc": "Fast transition whoosh",
    },
    "tension_hum": {
        "filter": "sine=frequency=55:duration=3.0:sample_rate=44100,afade=t=in:st=0:d=0.5,afade=t=out:st=2.0:d=1.0,volume=0.15",
        "duration": 3.0,
        "desc": "Sustained tension drone",
    },
    "silence_pause": {
        "filter": "anullsrc=r=44100:cl=stereo,atrim=0:0.2",
        "duration": 0.2,
        "desc": "Dramatic silence",
    },
}


def generate_sfx(sfx_type: str, output_path: str, duration: Optional[float] = None) -> bool:
    """Generate a sound effect audio file using FFmpeg."""
    config = SFX_CONFIGS.get(sfx_type)
    if not config:
        return False

    audio_filter = config["filter"]
    if duration and sfx_type == "tension_hum":
        # Adjust tension hum to scene duration
        audio_filter = (
            f"sine=frequency=55:duration={duration}:sample_rate=44100,"
            f"afade=t=in:st=0:d=0.5,afade=t=out:st={max(0, duration - 1)}:d=1.0,volume=0.15"
        )

    cmd = [
        "ffmpeg", "-y",
        "-f", "lavfi",
        "-i", audio_filter,
        "-c:a", "aac", "-b:a", "128k",
        output_path,
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        if result.returncode == 0 and os.path.exists(output_path):
            return True
        logger.error("SFX gen failed for %s: %s", sfx_type, result.stderr[:200])
        return False
    except Exception as exc:
        logger.error("SFX error: %s", exc)
        return False


def get_sfx_for_scene(scene: Dict, sfx_dir: str) -> Optional[str]:
    """Generate or retrieve the SFX file for a scene's sound_design cue."""
    sound_design = scene.get("sound_design", "none")
    if sound_design == "none" or sound_design not in SFX_CONFIGS:
        return None

    os.makedirs(sfx_dir, exist_ok=True)
    scene_id = scene.get("scene_id", 0)
    sfx_path = os.path.join(sfx_dir, f"sfx_{scene_id}_{sound_design}.aac")

    if os.path.exists(sfx_path) and os.path.getsize(sfx_path) > 50:
        return sfx_path

    duration = float(scene.get("estimated_duration", 3.0))
    if generate_sfx(sound_design, sfx_path, duration):
        return sfx_path

    return None


if __name__ == "__main__":
    # Test: generate all SFX types
    test_dir = "/tmp/sfx_test"
    os.makedirs(test_dir, exist_ok=True)

    for sfx_type in SFX_CONFIGS:
        path = os.path.join(test_dir, f"{sfx_type}.aac")
        ok = generate_sfx(sfx_type, path)
        status = "OK" if ok else "FAIL"
        print(f"  {status} {sfx_type}: {SFX_CONFIGS[sfx_type]['desc']}")
