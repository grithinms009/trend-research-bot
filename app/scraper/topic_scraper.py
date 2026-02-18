import os
import json
import yaml
import logging
from datetime import datetime, date
from typing import Any, List, Tuple
from urllib.parse import quote_plus

import feedparser
import requests as http_requests
import yake
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
from newspaper import Article

try:  # Optional dependency for content extraction fallbacks
    import trafilatura  # type: ignore[import]
except ImportError:  # pragma: no cover - optional
    trafilatura = None  # type: ignore[assignment]

from .collectors import RedditCollector, TwitterCollector, YouTubeCollector, rank_topics

logger = logging.getLogger(__name__)

KW_EXTRACTOR = yake.KeywordExtractor(lan="en", n=2, top=12, dedupLim=0.5)


class TopicScraper:
    MIN_ARTICLE_CHARS = 300
    IDEAL_ARTICLE_CHARS = 600
    SEARCH_RSS_TEMPLATE = "https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"

    def __init__(self):
        self.collectors = [
            RedditCollector(),
            TwitterCollector(),
            YouTubeCollector()
        ]
        
        # Load channel configuration
        base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        config_path = os.path.join(base_path, "app", "config", "channels.yaml")
        with open(config_path, "r") as f:
            config_data = yaml.safe_load(f) or {}

        if not isinstance(config_data, dict):
            raise ValueError("channels.yaml must contain a mapping at the root level")

        channels = config_data.get("channels")
        if not isinstance(channels, dict) or not channels:
            raise ValueError("Channel configuration missing 'channels' section in channels.yaml")

        self.channel_config = channels
        self.metrics = {
            "topics_scraped": 0,
            "topics_with_articles": 0,
            "topics_with_short_content": 0,
            "topics_failed_extraction": 0,
            "topics_discarded_no_article": 0,
        }

    def run(self):
        print("Starting topic scraping...")
        all_topics = []
        for collector in self.collectors:
            try:
                collector_name = collector.__class__.__name__
                print(f"Running {collector_name}...")
                
                topics = collector.collect_topics()
                self.metrics["topics_scraped"] += len(topics or [])
                if topics:
                    print(f"Collected {len(topics)} topics from {collector_name}")
                    all_topics.extend(topics)
                else:
                    print(f"No topics collected from {collector_name}")
                    
            except Exception as e:
                print(f"Error in collector {collector}: {e}")
        
        print(f"Total raw topics collected: {len(all_topics)}")
        
        # Rank topics
        ranked_topics = rank_topics(all_topics)
        print(f"Ranked {len(ranked_topics)} topics")
        
        validated_topics = []
        for topic in ranked_topics:
            prepared = self._prepare_topic(topic)
            if prepared:
                # Hard gate: only keep topics with sufficient article text
                article_len = len((prepared.get("article_text") or "").strip())
                if article_len >= self.MIN_ARTICLE_CHARS:
                    validated_topics.append(prepared)
                else:
                    self.metrics["topics_discarded_no_article"] += 1
                    logger.info(
                        "Discarded topic '%s' — article_text only %d chars (min %d)",
                        prepared.get("title", "unknown"), article_len, self.MIN_ARTICLE_CHARS,
                    )

        print(f"Topics with valid articles (>={self.MIN_ARTICLE_CHARS} chars): {len(validated_topics)}")

        # Group and trim per channel targets
        final_topics = self._group_by_channel(validated_topics)

        self._log_metrics()

        return final_topics

    def _group_by_channel(self, ranked_topics):
        """Group topics by channel and cap at configured targets."""
        channel_buckets = {cid: [] for cid in self.channel_config.keys()}

        # Group existing topics
        for topic in ranked_topics:
            cid = topic.get("channel")
            if cid in channel_buckets:
                channel_buckets[cid].append(topic)
        
        final_list = []
        log_stats = []

        for cid, config in self.channel_config.items():
            scraped_count = len(channel_buckets[cid])
            target = config["target_count"]

            selected = channel_buckets[cid][:target]
            final_list.extend(selected)

            log_stats.append({
                "channel": cid,
                "scraped": scraped_count,
                "used": len(selected),
                "target": target,
            })

        print("\n--- Channel Allocation ---")
        for stat in log_stats:
            print(
                f"{stat['channel']}: Validated={stat['scraped']}, "
                f"Queued={stat['used']} (Target={stat['target']})"
            )
        print("--------------------------\n")

        return final_list

    def _prepare_topic(self, topic):
        """Ensure each topic has a verified article backing it."""
        if not topic:
            return None

        prepared = dict(topic)
        prepared.setdefault("summary", "")
        prepared.setdefault("keywords", [])
        prepared.setdefault("source", "")

        summary = (prepared.get("summary") or "").strip()
        article_text = (prepared.get("article_text") or prepared.get("content") or "").strip()
        published_at = prepared.get("published_at", "")

        if len(article_text) >= self.MIN_ARTICLE_CHARS:
            logger.debug(
                "Topic '%s' retained existing article from collector",
                prepared.get("title", "unknown"),
            )
        else:
            candidate_urls = self._gather_candidate_urls(prepared)
            extracted_any = False

            for url, candidate_published in candidate_urls:
                if not url:
                    continue

                extracted_text, extracted_summary, extracted_published = self._extract_article(url)
                if extracted_text:
                    article_text = extracted_text.strip()
                    summary = extracted_summary or summary or prepared["title"]
                    published_at = (
                        extracted_published
                        or candidate_published
                        or published_at
                    )
                    extracted_any = True
                    prepared["url"] = url
                    if len(article_text) >= self.MIN_ARTICLE_CHARS:
                        break

            if not extracted_any:
                self.metrics["topics_failed_extraction"] += 1

        if not summary:
            summary = prepared.get("title", "")

        content_length = len(article_text or "")
        if content_length >= self.IDEAL_ARTICLE_CHARS:
            prepared["content_tier"] = "full"
            self.metrics["topics_with_articles"] += 1
        elif content_length >= self.MIN_ARTICLE_CHARS:
            prepared["content_tier"] = "short"
            self.metrics["topics_with_short_content"] += 1
        else:
            prepared["content_tier"] = "minimal"
            self.metrics["topics_discarded_no_article"] += 1

        prepared["article_text"] = article_text
        prepared["content"] = article_text
        prepared["summary"] = self._shorten_summary(summary or prepared["title"])
        prepared["keywords"] = prepared.get("keywords") or self._extract_keywords(article_text or prepared.get("title", ""))
        prepared["published_at"] = published_at
        prepared["has_article"] = content_length >= self.MIN_ARTICLE_CHARS
        prepared["validated_at"] = datetime.now().isoformat()

        return prepared

    def _gather_candidate_urls(self, prepared: dict) -> List[Tuple[str, str]]:
        candidates: List[Tuple[str, str]] = []
        seen = set()

        existing_url = prepared.get("url", "")
        if existing_url:
            candidates.append((existing_url, prepared.get("published_at", "")))
            seen.add(existing_url)

        for url, published in self._discover_articles_from_search(prepared.get("title", "")):
            if url and url not in seen:
                candidates.append((url, published))
                seen.add(url)

        return candidates

    def _discover_articles_from_search(self, title: str, max_results: int = 5) -> List[Tuple[str, str]]:
        if not title:
            return []

        query = quote_plus(title)
        search_url = self.SEARCH_RSS_TEMPLATE.format(query=query)
        try:
            feed = feedparser.parse(search_url)
        except Exception as exc:
            logger.debug(f"Search feed failed for {title}: {exc}")
            return []

        results: List[Tuple[str, str]] = []
        for entry in feed.entries:
            link = getattr(entry, "link", "")
            if not link:
                continue
            published_raw = getattr(entry, "published", "")
            published_at = self._safe_parse_datetime(published_raw)
            results.append((link, published_at))
            if len(results) >= max_results:
                break

        return results

    def _extract_article(self, url: str):
        """Multi-tier extraction: newspaper3k → trafilatura → BS4 paragraphs."""
        article_text = ""
        summary = ""
        published_at = ""

        # Tier 1: newspaper3k
        try:
            article = Article(url)
            article.download()
            article.parse()
            article_text = (article.text or "").strip()
            if article.publish_date:
                published_at = self._safe_parse_datetime(article.publish_date)
            try:
                article.nlp()
                summary = (article.summary or "").strip()
            except Exception:
                pass
        except Exception as exc:
            logger.debug("newspaper3k failed for %s: %s", url, exc)

        # Tier 2: trafilatura
        if len(article_text) < self.MIN_ARTICLE_CHARS and trafilatura:
            try:
                downloaded = trafilatura.fetch_url(url)
                extracted = trafilatura.extract(downloaded) if downloaded else ""
                if extracted and len(extracted) > len(article_text):
                    article_text = extracted.strip()
            except Exception as exc:
                logger.debug("trafilatura failed for %s: %s", url, exc)

        # Tier 3: requests + BeautifulSoup paragraph aggregation
        if len(article_text) < self.MIN_ARTICLE_CHARS:
            try:
                resp = http_requests.get(
                    url,
                    timeout=15,
                    headers={"User-Agent": "Mozilla/5.0 (compatible; TrendBot/1.0)"},
                )
                resp.raise_for_status()
                soup = BeautifulSoup(resp.text, "html.parser")
                for tag in soup(["script", "style", "nav", "footer", "header", "aside"]):
                    tag.decompose()
                paragraphs = soup.find_all("p")
                p_texts = [p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 40]
                bs_text = "\n\n".join(p_texts)
                if len(bs_text) > len(article_text):
                    article_text = bs_text.strip()
            except Exception as exc:
                logger.debug("BS4 paragraph extraction failed for %s: %s", url, exc)

        return article_text, summary, published_at

    def _extract_keywords(self, text: str):
        if not text or len(text.strip()) < 30:
            return []
        try:
            keywords = [kw for kw, _score in KW_EXTRACTOR.extract_keywords(text)]
            return keywords[:12]
        except Exception:
            return []

    @staticmethod
    def _shorten_summary(text: str, max_sentences: int = 4):
        if not text:
            return ""
        sentences = text.replace("\n", " ").split(". ")
        trimmed = ". ".join(sentences[:max_sentences]).strip()
        if not trimmed.endswith("."):
            trimmed += "."
        return trimmed

    def _log_metrics(self):
        print("\n--- Scraper Metrics ---")
        for key, value in self.metrics.items():
            print(f"{key}: {value}")
        print("-----------------------\n")

    @staticmethod
    def _safe_parse_datetime(value: Any) -> str:
        if not value:
            return ""

        parsed_value = value
        if isinstance(value, str):
            try:
                parsed_value = dateparser.parse(value)
            except Exception:
                parsed_value = value

        if isinstance(parsed_value, (datetime, date)):
            try:
                return parsed_value.isoformat()
            except Exception:
                return str(parsed_value)

        return str(parsed_value)

if __name__ == "__main__":
    def save_topics(topics):
        import os
        from datetime import datetime
        import json
        
        # Determine paths
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        DATA_DIR = os.path.join(BASE_DIR, "data", "topics")
        os.makedirs(DATA_DIR, exist_ok=True)
        
        filename = f"{DATA_DIR}/{datetime.now().strftime('%Y%m%d_%H%M')}.json"
        
        with open(filename, "w") as f:
            json.dump(topics, f, indent=2)
        print(f"Topics saved to: {filename}")

    scraper = TopicScraper()
    results = scraper.run()
    save_topics(results)
    import json
    # print(json.dumps(results, indent=2))