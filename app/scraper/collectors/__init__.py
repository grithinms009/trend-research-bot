"""
Scraper collectors package.
"""

from pathlib import Path
import sys

if __package__ in {None, ""}:
    # Ensure project root is on sys.path when executed as a script.
    project_root = Path(__file__).resolve().parents[3]
    if project_root not in map(Path, sys.path):
        sys.path.append(str(project_root))

from app.scraper.collectors.reddit import RedditCollector
from app.scraper.collectors.twitter import TwitterCollector
from app.scraper.collectors.youtube import YouTubeCollector
from app.scraper.collectors.ranker import rank_topics

__all__ = [
    "RedditCollector",
    "TwitterCollector",
    "YouTubeCollector",
    "rank_topics",
]
