import os
import json
import yaml
import logging
from datetime import datetime, date
from typing import Any
from urllib.parse import quote_plus

import feedparser
import yake
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
                validated_topics.append(prepared)

        self.metrics["topics_with_articles"] = len(validated_topics)

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

        url = prepared.get("url", "")
        if not url:
            discovered_url, discovered_published = self._discover_article_from_search(prepared["title"])
            url = discovered_url
            prepared["url"] = url
            if discovered_published and not prepared.get("published_at"):
                prepared["published_at"] = discovered_published

        article_text = (prepared.get("article_text") or "").strip()
        summary = (prepared.get("summary") or "").strip()

        if url and len(article_text) < self.MIN_ARTICLE_CHARS:
            extracted_text, extracted_summary, extracted_published = self._extract_article(url)
            if extracted_text:
                article_text = extracted_text
            if extracted_summary and not summary:
                summary = extracted_summary
            if extracted_published and not prepared.get("published_at"):
                prepared["published_at"] = extracted_published

        if not url or len(article_text) < self.MIN_ARTICLE_CHARS:
            return None

        prepared["article_text"] = article_text
        prepared["summary"] = self._shorten_summary(summary or prepared["title"])
        prepared["keywords"] = prepared.get("keywords") or self._extract_keywords(article_text)
        prepared["validated_at"] = datetime.now().isoformat()

        return prepared

    def _discover_article_from_search(self, title: str):
        if not title:
            return "", ""

        query = quote_plus(title)
        search_url = self.SEARCH_RSS_TEMPLATE.format(query=query)
        try:
            feed = feedparser.parse(search_url)
        except Exception as exc:
            logger.debug(f"Search feed failed for {title}: {exc}")
            return "", ""

        for entry in feed.entries:
            link = getattr(entry, "link", "")
            if not link:
                continue
            published_raw = getattr(entry, "published", "")
            published_at = self._safe_parse_datetime(published_raw)
            return link, published_at

        return "", ""

    def _extract_article(self, url: str):
        article_text = ""
        summary = ""
        published_at = ""

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
                summary = summary
        except Exception as exc:
            logger.debug(f"Primary article extraction failed for {url}: {exc}")

        if len(article_text) < self.MIN_ARTICLE_CHARS and trafilatura:
            try:
                downloaded = trafilatura.fetch_url(url)
                extracted = trafilatura.extract(downloaded) if downloaded else ""
                if extracted and len(extracted) > len(article_text):
                    article_text = extracted.strip()
            except Exception as exc:
                logger.debug(f"Trafilatura extraction failed for {url}: {exc}")

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