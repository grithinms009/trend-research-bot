"""
Auto-Cleanup — removes temp assets after video rendering.

Keeps only final MP4 files. Deletes:
- Stock footage downloads
- Work directory (scene clips, concat files)
- Subtitle files
- Music files

Prevents disk from filling up at scale (each short uses ~100MB temp).
"""

import glob
import logging
import os
import shutil

logger = logging.getLogger(__name__)


def cleanup_work_dirs(base_dir: str) -> int:
    """Remove all temp work directories after rendering."""
    work_root = os.path.join(base_dir, "data", "shorts", "work")
    assets_root = os.path.join(base_dir, "data", "shorts", "assets")
    subs_root = os.path.join(base_dir, "data", "shorts", "subs")
    music_root = os.path.join(base_dir, "data", "shorts", "music")

    freed_bytes = 0
    removed_dirs = 0

    for root_dir in [work_root, assets_root, subs_root, music_root]:
        if os.path.exists(root_dir):
            try:
                dir_size = sum(
                    os.path.getsize(os.path.join(dp, f))
                    for dp, _, fns in os.walk(root_dir)
                    for f in fns
                )
                freed_bytes += dir_size
                shutil.rmtree(root_dir)
                removed_dirs += 1
                logger.info("Cleaned: %s (%.1fMB)", root_dir, dir_size / (1024 * 1024))
            except Exception as exc:
                logger.error("Cleanup failed for %s: %s", root_dir, exc)

    return freed_bytes


def main():
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    final_dir = os.path.join(base_dir, "data", "shorts", "final")

    # Count final videos
    final_count = 0
    final_size = 0
    if os.path.exists(final_dir):
        for root, _, files in os.walk(final_dir):
            for f in files:
                if f.endswith(".mp4"):
                    final_count += 1
                    final_size += os.path.getsize(os.path.join(root, f))

    print(f"Final videos: {final_count} ({final_size / (1024 * 1024):.1f}MB)")

    # Clean temp dirs
    freed = cleanup_work_dirs(base_dir)
    freed_mb = freed / (1024 * 1024)

    if freed > 0:
        print(f"🧹 Cleaned {freed_mb:.1f}MB of temp files")
    else:
        print("Nothing to clean.")


if __name__ == "__main__":
    main()
