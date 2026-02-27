#!/usr/bin/env python3
"""Run only the video builder pipeline stages (after audio is already generated)."""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

print("\n🎬 Running Video Builder on existing audio...\n")

# Stage 1: Build shorts
print("=" * 50)
print("STAGE: video_builder_shorts")
print("=" * 50)
from app.video.video_builder_shorts import main as build_videos
build_videos()

# Stage 2: Cleanup
print("\n" + "=" * 50)
print("STAGE: cleanup")
print("=" * 50)
from app.video.cleanup import main as cleanup
cleanup()

print("\n✅ Video pipeline complete!")
print("Check: data/shorts/final/")
