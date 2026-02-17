from __future__ import annotations

import asyncio
import logging
from typing import List

import asyncpraw

from app.content_intelligence.connectors.base import BaseConnector
from app.content_intelligence.models.topic import RawTopic

logger = logging.getLogger(__name__)


class RedditTopicsConnector(BaseConnector):
    name = "reddit_topics"

    async def _fetch_impl(self) -> List[RawTopic]:
        subreddits = self.config.get("subreddits", [])
        limit = self.config.get("limit", 20)
        if not subreddits:
            return []

        reddit = asyncpraw.Reddit(
            client_id=self.config.get("client_id", "dummy"),
            client_secret=self.config.get("client_secret", "dummy"),
            user_agent=self.config.get("user_agent", "content-intel-bot")
        )

        tasks = [self._fetch_subreddit(reddit, name, limit) for name in subreddits]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        await reddit.close()

        topics: List[RawTopic] = []
        for result in results:
            if isinstance(result, list):
                topics.extend(result)
            else:
                logger.warning("Reddit connector subreddit fetch failed: %s", result)
        return topics

    async def _fetch_subreddit(self, reddit: asyncpraw.Reddit, name: str, limit: int) -> List[RawTopic]:
        subreddit = await reddit.subreddit(name)
        topics: List[RawTopic] = []
        async for submission in subreddit.hot(limit=limit):
            urls = [submission.url] if submission.url else []
            topics.append(
                RawTopic(
                    title=submission.title,
                    summary=submission.selftext[:280],
                    source_urls=urls,
                    category_hint="productivity",
                    metadata={"subreddit": name, "score": submission.score},
                )
            )
        await asyncio.sleep(0.05)
        return topics
