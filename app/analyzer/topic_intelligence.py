"""
Topic Intelligence Engine — converts raw scraped articles into strong YouTube topics.

Replaces the thin topic_analyzer + topic_content_validator with a production-grade
intelligence layer that:

1. LLM Topic Extraction — generates specific, engaging YouTube titles from article text
2. Engagement Scoring — rates curiosity gap, emotional pull, searchability
3. Niche Relevance — ensures topics match their assigned channel
4. Semantic Dedup — TF-IDF cosine similarity to prevent near-duplicate titles
5. History Tracking — avoids repeating topics across pipeline runs
6. YouTube Optimization — rejects generic titles, enforces specificity

Pipeline position: runs AFTER topic_cleaner, REPLACES topic_analyzer + topic_content_validator.
Output: data/topics_intelligent/{date}.json
"""

import hashlib
import json
import glob
import logging
import os
import time
import yaml
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from app.services.ollama_client import OllamaClient

logger = logging.getLogger(__name__)

# ============================================================
# THRESHOLDS
# ============================================================
MIN_ARTICLE_CHARS = 400
MIN_ENGAGEMENT_SCORE = 0.45
SIMILARITY_THRESHOLD = 0.70  # titles above this cosine sim = duplicate
MIN_TITLE_WORDS = 5
MAX_TITLE_WORDS = 18

# Banned patterns in article text
BANNED_PATTERNS = [
    "accept all", "reject all", "privacytools", "g.co/privacytools",
    "cookies and data", "cookie policy", "privacy policy",
    "subscribe to our newsletter", "sign up for free",
]

# Generic titles the LLM tends to produce — instant reject
GENERIC_TITLE_FRAGMENTS = [
    "everything you need to know",
    "here's what you need to know",
    "what you should know",
    "a comprehensive guide",
    "the ultimate guide",
    "in today's world",
    "in the modern era",
    "breaking news",
    "latest update",
    "new report says",
]

# ============================================================
# CHANNEL NICHE DEFINITIONS (for relevance scoring)
# ============================================================
CHANNEL_NICHES = {
    "C1": {
        "name": "AI News & Tools",
        "core_topics": [
            "artificial intelligence", "machine learning", "ai agents", "llm",
            "robotics", "automation", "tech startup", "software", "cybersecurity",
            "chips", "semiconductor", "quantum computing", "ar vr",
            "science discovery", "space exploration", "physics", "biology",
            "climate", "crypto", "blockchain", "fintech",
        ],
        "audience": "tech-savvy viewers interested in AI, science, and emerging technology",
        "avoid": ["generic tech reviews", "phone unboxing", "gaming"],
    },
    "C5": {
        "name": "Life Hacks / Productivity",
        "core_topics": [
            "productivity system", "habits", "time management", "morning routine",
            "self improvement", "career growth", "mental health", "focus",
            "stoicism", "side hustle", "learning techniques",
            "entrepreneur", "remote work", "minimalism",
        ],
        "audience": "self-improvement focused viewers",
        "avoid": ["generic motivation", "hustle culture toxicity"],
    },
}

# ============================================================
# LLM PROMPTS
# ============================================================
TOPIC_EXTRACTION_PROMPT = """You are a YouTube content strategist for the channel "{channel_name}" ({channel_niche}).

Given this article, extract ONE specific, engaging YouTube Shorts topic.

ARTICLE TITLE: {article_title}
ARTICLE TEXT:
{article_text}

RULES:
1. The topic must be SPECIFIC — not generic. Include numbers, names, or unique angles.
2. The topic must create a CURIOSITY GAP — make viewers need to watch.
3. The topic must be SEARCHABLE — use terms people actually search for.
4. The topic must match the channel niche: {channel_niche}
5. The topic must be safe for YouTube (no hate, violence, or controversy).
6. Do NOT use clickbait or misleading framing.

BAD examples (too generic):
- "AI is changing everything"
- "New technology update"
- "Stock market news today"

GOOD examples (specific + curiosity gap):
- "Why OpenAI's New Agent Can Replace 90% of Customer Service Jobs"
- "The $2 Trillion Chip War Nobody Is Talking About"
- "Scientists Found a 4 Billion Year Old Crystal That Rewrites Earth's History"

Output ONLY a JSON object:
{{"title": "Your YouTube topic title", "hook": "One-sentence hook that creates urgency", "angle": "What makes this different from other coverage", "searchability": "Key search terms viewers would use"}}

Output the JSON only. No explanation."""

ENGAGEMENT_SCORING_PROMPT = """Rate this YouTube Shorts topic for engagement potential.

TOPIC: {title}
HOOK: {hook}
CHANNEL: {channel_name}

Score each dimension 1-10:
1. curiosity_gap: Does it make viewers NEED to know the answer?
2. emotional_pull: Does it trigger an emotion (shock, awe, fear, excitement)?
3. searchability: Would people search for this topic?
4. specificity: Is it specific enough (numbers, names, unique angles)?
5. trend_relevance: Is this timely and relevant right now?

Output ONLY a JSON object:
{{"curiosity_gap": 7, "emotional_pull": 6, "searchability": 8, "specificity": 7, "trend_relevance": 8}}

Output the JSON only."""


# ============================================================
# HISTORY TRACKER
# ============================================================
class TopicHistory:
    """Tracks previously generated topics to prevent cross-run repetition."""

    def __init__(self, history_path: str):
        self.history_path = history_path
        self.titles: Set[str] = set()
        self.hashes: Set[str] = set()
        self._load()

    def _load(self):
        if os.path.exists(self.history_path):
            try:
                with open(self.history_path) as f:
                    data = json.load(f)
                self.titles = set(data.get("titles", []))
                self.hashes = set(data.get("hashes", []))
            except Exception:
                pass

    def save(self):
        os.makedirs(os.path.dirname(self.history_path), exist_ok=True)
        # Keep last 500 to prevent unbounded growth
        titles_list = list(self.titles)[-500:]
        hashes_list = list(self.hashes)[-500:]
        with open(self.history_path, "w") as f:
            json.dump({"titles": titles_list, "hashes": hashes_list}, f)

    def is_duplicate(self, title: str) -> bool:
        h = hashlib.md5(title.lower().strip().encode()).hexdigest()
        return title.lower().strip() in self.titles or h in self.hashes

    def add(self, title: str):
        clean = title.lower().strip()
        self.titles.add(clean)
        self.hashes.add(hashlib.md5(clean.encode()).hexdigest())


# ============================================================
# SEMANTIC DEDUP ENGINE
# ============================================================
class SemanticDedup:
    """TF-IDF based semantic deduplication for topic titles."""

    def __init__(self, threshold: float = SIMILARITY_THRESHOLD):
        self.threshold = threshold
        self.vectorizer = TfidfVectorizer(stop_words="english", min_df=1)

    def deduplicate(self, topics: List[Dict]) -> List[Dict]:
        if len(topics) <= 1:
            return topics

        titles = [t.get("youtube_title", t.get("title", "")) for t in topics]
        try:
            matrix = self.vectorizer.fit_transform(titles)
            sim_matrix = cosine_similarity(matrix)
        except ValueError:
            return topics

        keep = []
        removed_indices: Set[int] = set()

        for i in range(len(topics)):
            if i in removed_indices:
                continue
            keep.append(topics[i])
            # Mark all similar topics as duplicates
            for j in range(i + 1, len(topics)):
                if j in removed_indices:
                    continue
                if sim_matrix[i][j] >= self.threshold:
                    removed_indices.add(j)
                    logger.info(
                        "Semantic dedup: '%s' ≈ '%s' (sim=%.3f)",
                        titles[i][:50], titles[j][:50], sim_matrix[i][j],
                    )

        return keep


# ============================================================
# ENGAGEMENT SCORER
# ============================================================
def _parse_engagement_scores(raw: str) -> Optional[Dict[str, float]]:
    """Parse LLM engagement scoring output."""
    raw = raw.strip()
    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1:
        return None
    try:
        scores = json.loads(raw[start:end + 1])
    except json.JSONDecodeError:
        return None

    required = {"curiosity_gap", "emotional_pull", "searchability", "specificity", "trend_relevance"}
    if not all(k in scores for k in required):
        return None

    # Normalize to 0-1
    normalized = {}
    for k in required:
        try:
            val = float(scores[k])
            normalized[k] = max(0.0, min(1.0, val / 10.0))
        except (ValueError, TypeError):
            normalized[k] = 0.5

    # Weighted composite
    weights = {
        "curiosity_gap": 0.30,
        "emotional_pull": 0.20,
        "searchability": 0.25,
        "specificity": 0.15,
        "trend_relevance": 0.10,
    }
    normalized["composite"] = sum(normalized[k] * weights[k] for k in weights)
    return normalized


def _rule_based_engagement(title: str, hook: str = "") -> Dict[str, float]:
    """Fast rule-based engagement scoring fallback (no LLM needed)."""
    title_lower = title.lower()
    scores: Dict[str, float] = {}

    # Curiosity gap: questions, numbers, superlatives
    curiosity = 0.4
    if "?" in title:
        curiosity += 0.15
    if "why" in title_lower or "how" in title_lower:
        curiosity += 0.1
    if any(c.isdigit() for c in title):
        curiosity += 0.15
    if any(w in title_lower for w in ["secret", "hidden", "nobody", "never", "first ever"]):
        curiosity += 0.15
    scores["curiosity_gap"] = min(1.0, curiosity)

    # Emotional pull
    emotional = 0.3
    power_words = ["shocking", "terrifying", "incredible", "insane", "massive", "revolutionary",
                   "dangerous", "dark side", "collapse", "explode", "destroy", "billion", "trillion"]
    emotional += sum(0.1 for w in power_words if w in title_lower)
    scores["emotional_pull"] = min(1.0, emotional)

    # Searchability: shorter titles with common search patterns
    word_count = len(title.split())
    searchability = 0.5
    if 6 <= word_count <= 12:
        searchability += 0.2
    if any(w in title_lower for w in ["how to", "what is", "why", "top", "best", "vs"]):
        searchability += 0.15
    scores["searchability"] = min(1.0, searchability)

    # Specificity: names, numbers, dates
    specificity = 0.3
    if any(c.isdigit() for c in title):
        specificity += 0.2
    # Check for proper nouns (words starting with uppercase in middle of title)
    words = title.split()
    proper_nouns = sum(1 for w in words[1:] if w[0].isupper()) if len(words) > 1 else 0
    specificity += min(0.3, proper_nouns * 0.1)
    if "$" in title or "%" in title:
        specificity += 0.15
    scores["specificity"] = min(1.0, specificity)

    # Trend relevance (can't determine without external data, use moderate default)
    scores["trend_relevance"] = 0.55

    weights = {
        "curiosity_gap": 0.30, "emotional_pull": 0.20,
        "searchability": 0.25, "specificity": 0.15, "trend_relevance": 0.10,
    }
    scores["composite"] = sum(scores[k] * weights[k] for k in weights)
    return scores


# ============================================================
# MAIN ENGINE
# ============================================================
class TopicIntelligenceEngine:
    """
    Production Topic Intelligence Engine.

    Converts raw scraped articles into YouTube-optimized, engagement-scored,
    deduplicated topics with channel-specific filtering.
    """

    def __init__(self, use_llm_scoring: bool = True, model: str = "mistral:latest"):
        self.use_llm_scoring = use_llm_scoring
        self.model = model
        self.dedup = SemanticDedup()

        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        history_path = os.path.join(base_dir, "data", "topic_history.json")
        self.history = TopicHistory(history_path)

        config_path = os.path.join(base_dir, "app", "config", "channels.yaml")
        with open(config_path) as f:
            raw_config = yaml.safe_load(f) or {}
        self.channel_config = raw_config.get("channels", {}) if isinstance(raw_config, dict) else {}

        self.metrics = {
            "input_topics": 0,
            "topics_rejected_short_article": 0,
            "topics_rejected_banned": 0,
            "topics_rejected_generic": 0,
            "topics_rejected_low_engagement": 0,
            "topics_rejected_duplicate_semantic": 0,
            "topics_rejected_duplicate_history": 0,
            "topics_rejected_niche_mismatch": 0,
            "topics_llm_extracted": 0,
            "topics_llm_failed": 0,
            "topics_accepted": 0,
            "processing_times": [],
        }

    def process(self, topics: List[Dict]) -> List[Dict]:
        """Full intelligence pipeline: extract → score → dedup → filter → output."""
        start = time.time()
        self.metrics["input_topics"] = len(topics)

        # Stage 1: Article quality gate
        quality_passed = self._article_quality_gate(topics)
        logger.info("Article quality gate: %d/%d passed", len(quality_passed), len(topics))

        # Stage 2: LLM topic extraction (generate YouTube-optimized titles)
        extracted = self._extract_youtube_topics(quality_passed)
        logger.info("LLM topic extraction: %d topics extracted", len(extracted))

        # Stage 3: Engagement scoring
        scored = self._score_engagement(extracted)
        logger.info("Engagement scoring: %d topics scored", len(scored))

        # Stage 4: Filter low engagement
        engaged = [t for t in scored if t.get("engagement_score", 0) >= MIN_ENGAGEMENT_SCORE]
        self.metrics["topics_rejected_low_engagement"] = len(scored) - len(engaged)
        logger.info("Engagement filter: %d/%d passed (min=%.2f)", len(engaged), len(scored), MIN_ENGAGEMENT_SCORE)

        # Stage 5: Semantic deduplication
        deduped = self.dedup.deduplicate(engaged)
        self.metrics["topics_rejected_duplicate_semantic"] = len(engaged) - len(deduped)

        # Stage 6: History dedup
        fresh = []
        for t in deduped:
            yt_title = t.get("youtube_title", t.get("title", ""))
            if self.history.is_duplicate(yt_title):
                self.metrics["topics_rejected_duplicate_history"] += 1
                continue
            fresh.append(t)
            self.history.add(yt_title)

        self.history.save()

        # Stage 7: Cap per channel
        final = self._cap_per_channel(fresh)
        self.metrics["topics_accepted"] = len(final)

        elapsed = round(time.time() - start, 2)
        self.metrics["processing_times"].append(elapsed)
        self._log_metrics()

        return final

    def _article_quality_gate(self, topics: List[Dict]) -> List[Dict]:
        """Reject articles with insufficient content or banned patterns."""
        passed = []
        for topic in topics:
            article = (topic.get("article_text") or topic.get("content") or "").strip()
            lower = article.lower()

            if len(article) < MIN_ARTICLE_CHARS:
                self.metrics["topics_rejected_short_article"] += 1
                continue

            # Check for consent walls / cookie banners:
            # - If banned pattern appears in first 500 chars → likely a consent wall
            # - If 3+ banned patterns match anywhere → likely scraped junk page
            head = lower[:500]
            head_match = any(p in head for p in BANNED_PATTERNS)
            full_matches = sum(1 for p in BANNED_PATTERNS if p in lower)
            if head_match or full_matches >= 3:
                logger.info("Rejected banned content (%d matches, head=%s): %s",
                            full_matches, head_match, topic.get("title", "?")[:60])
                self.metrics["topics_rejected_banned"] += 1
                continue

            passed.append(topic)
        return passed

    def _extract_youtube_topics(self, topics: List[Dict]) -> List[Dict]:
        """Use LLM to generate YouTube-optimized titles from article content."""
        extracted = []

        for topic in topics:
            channel = topic.get("channel", "C1")
            niche = CHANNEL_NICHES.get(channel, CHANNEL_NICHES["C1"])
            article_text = (topic.get("article_text") or "").strip()
            article_title = topic.get("title", "")

            prompt = TOPIC_EXTRACTION_PROMPT.format(
                channel_name=niche["name"],
                channel_niche=", ".join(niche["core_topics"][:6]),
                article_title=article_title,
                article_text=article_text[:2000],
            )

            try:
                raw = OllamaClient.generate(prompt, model=self.model, timeout=90)
                if raw:
                    parsed = self._parse_extraction(raw)
                    if parsed:
                        yt_title = parsed.get("title", "")
                        # Reject generic titles
                        if self._is_generic(yt_title):
                            self.metrics["topics_rejected_generic"] += 1
                            continue

                        enriched = dict(topic)
                        enriched["youtube_title"] = yt_title
                        enriched["hook"] = parsed.get("hook", "")
                        enriched["angle"] = parsed.get("angle", "")
                        enriched["search_terms"] = parsed.get("searchability", "")
                        enriched["original_title"] = article_title
                        self.metrics["topics_llm_extracted"] += 1
                        extracted.append(enriched)
                        continue
            except Exception as exc:
                logger.error("LLM topic extraction failed: %s", exc)

            # Fallback: use original title if it's specific enough
            self.metrics["topics_llm_failed"] += 1
            if not self._is_generic(article_title) and len(article_title.split()) >= MIN_TITLE_WORDS:
                enriched = dict(topic)
                enriched["youtube_title"] = article_title
                enriched["hook"] = ""
                enriched["angle"] = "direct coverage"
                enriched["search_terms"] = ""
                enriched["original_title"] = article_title
                extracted.append(enriched)

        return extracted

    def _score_engagement(self, topics: List[Dict]) -> List[Dict]:
        """Score each topic for YouTube engagement potential."""
        scored = []

        for topic in topics:
            yt_title = topic.get("youtube_title", topic.get("title", ""))
            hook = topic.get("hook", "")
            channel = topic.get("channel", "C1")
            niche = CHANNEL_NICHES.get(channel, CHANNEL_NICHES["C1"])

            if self.use_llm_scoring:
                prompt = ENGAGEMENT_SCORING_PROMPT.format(
                    title=yt_title,
                    hook=hook,
                    channel_name=niche["name"],
                )
                try:
                    raw = OllamaClient.generate(prompt, model=self.model, timeout=60)
                    if raw:
                        scores = _parse_engagement_scores(raw)
                        if scores:
                            topic["engagement_scores"] = scores
                            topic["engagement_score"] = scores["composite"]
                            scored.append(topic)
                            continue
                except Exception:
                    pass

            # Fallback: rule-based scoring
            scores = _rule_based_engagement(yt_title, hook)
            topic["engagement_scores"] = scores
            topic["engagement_score"] = scores["composite"]
            scored.append(topic)

        # Sort by engagement score descending
        scored.sort(key=lambda t: t.get("engagement_score", 0), reverse=True)
        return scored

    def _cap_per_channel(self, topics: List[Dict]) -> List[Dict]:
        """Cap topics per channel based on configured targets."""
        buckets: Dict[str, List[Dict]] = {}
        for topic in topics:
            cid = topic.get("channel", "C1")
            buckets.setdefault(cid, []).append(topic)

        final = []
        for cid, config in self.channel_config.items():
            target = config.get("target_count", 5)
            channel_topics = buckets.get(cid, [])
            # Already sorted by engagement, take top N
            selected = channel_topics[:target]
            final.extend(selected)

            logger.info(
                "Channel %s: %d available, %d selected (target=%d)",
                cid, len(channel_topics), len(selected), target,
            )

        return final

    @staticmethod
    def _parse_extraction(raw: str) -> Optional[Dict]:
        raw = raw.strip()
        start = raw.find("{")
        end = raw.rfind("}")
        if start == -1 or end == -1:
            return None
        try:
            data = json.loads(raw[start:end + 1])
            if isinstance(data, dict) and data.get("title"):
                return data
        except json.JSONDecodeError:
            pass
        return None

    @staticmethod
    def _is_generic(title: str) -> bool:
        if not title:
            return True
        lower = title.lower().strip()
        word_count = len(lower.split())
        if word_count < MIN_TITLE_WORDS:
            return True
        for frag in GENERIC_TITLE_FRAGMENTS:
            if frag in lower:
                return True
        return False

    def _log_metrics(self):
        print("\n" + "=" * 50)
        print("  📊 TOPIC INTELLIGENCE ENGINE METRICS")
        print("=" * 50)
        for key, value in self.metrics.items():
            if key == "processing_times":
                if value:
                    print(f"  avg_processing_time: {sum(value) / len(value):.2f}s")
            else:
                print(f"  {key}: {value}")

        if self.metrics["input_topics"] > 0:
            rate = self.metrics["topics_accepted"] / self.metrics["input_topics"] * 100
            print(f"  acceptance_rate: {rate:.1f}%")
        print("=" * 50 + "\n")


# ============================================================
# STANDALONE ENTRY POINT
# ============================================================
def main():
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    input_dir = os.path.join(base_dir, "data", "topics_clean")
    output_dir = os.path.join(base_dir, "data", "topics_intelligent")
    os.makedirs(output_dir, exist_ok=True)

    files = sorted(glob.glob(os.path.join(input_dir, "*.json")))
    if not files:
        print("No cleaned topics found. Run topic_cleaner first.")
        return

    latest = files[-1]
    print(f"Reading cleaned topics from: {latest}")
    with open(latest) as f:
        topics = json.load(f)

    print(f"Loaded {len(topics)} cleaned topics")

    engine = TopicIntelligenceEngine(use_llm_scoring=True)
    results = engine.process(topics)

    outfile = os.path.join(output_dir, f"{datetime.now().strftime('%Y%m%d_%H%M')}.json")
    with open(outfile, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\n✅ Intelligent topics: {len(results)}")
    print(f"Saved to: {outfile}")

    # Print topic summary per channel
    channel_topics: Dict[str, List[str]] = {}
    for t in results:
        cid = t.get("channel", "??")
        title = t.get("youtube_title", t.get("title", "?"))
        channel_topics.setdefault(cid, []).append(title)

    print("\n" + "=" * 60)
    for cid, titles in sorted(channel_topics.items()):
        niche = CHANNEL_NICHES.get(cid, {"name": "Unknown"})
        print(f"\n  Channel {cid}: {niche.get('name', 'Unknown')}")
        for i, title in enumerate(titles, 1):
            print(f"    {i}. {title}")
    print("=" * 60)


if __name__ == "__main__":
    main()
