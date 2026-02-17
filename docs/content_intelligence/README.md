# Content Intelligence Scraper Layer

## Overview
The Content Intelligence Scraper Layer implements a content-first discovery pipeline that can serve 100+ YouTube channels by sharing a single global topic intelligence backbone. Channels subscribe to topics downstream; no channel-specific logic exists inside the scraper services.

## Architecture Diagram (Textual)
```
[Connectors]
  |-- YouTube Trending
  |-- Google Trends
  |-- News RSS
  |-- Reddit Topics
  |-- X Social (optional)
        |
        v
Trend Discovery Service (async fan-out)
        |
        v
Source Fetch Service (normalize + keywords + category routing)
        |
        v
Topic Cluster Service (merge duplicates)
        |
        v
Quality Filter Service (deterministic checks)
        |
        v
Content Score Service (deterministic scoring)
        |
        v
Queue Publisher Service -> Redis Stream (content_topics)
```

## Folder Structure
```
app/content_intelligence/
├── __init__.py
├── config.py
├── connectors/
│   ├── base.py
│   ├── google_trends.py
│   ├── news_rss.py
│   ├── reddit_topics.py
│   ├── x_social.py
│   └── youtube_trending.py
├── models/
│   └── topic.py
├── queue/
│   ├── __init__.py
│   ├── base.py
│   └── redis_publisher.py
├── services/
│   ├── content_intel_pipeline.py
│   ├── content_score_service.py
│   ├── quality_filter_service.py
│   ├── queue_publisher_service.py
│   ├── source_fetch_service.py
│   ├── topic_cluster_service.py
│   └── trend_discovery_service.py
└── utils/
    ├── category.py
    └── text.py
```

## Data Model
Each discovered topic is normalized into a `Topic` object:
```
{
  "topic_id": "ai-news-chip-xyz",
  "topic_title": "Apple reveals M5 Neural Engine",
  "keywords": ["apple", "m5", "neural", "engine"],
  "source_urls": [...],
  "category": "ai_news",
  "trend_score": 0.82,
  "freshness_score": 0.61,
  "monetization_score": 0.73,
  "global_score": 0.74,
  "created_at": "ISO timestamp"
}
```

## Services
- **TrendDiscoveryService** – orchestrates async connector fan-out.
- **SourceFetchService** – normalizes raw connector payloads, generates IDs, keywords, and routes categories using config.
- **TopicClusterService** – deduplicates similar topics via keyword signatures.
- **QualityFilterService** – enforces minimum keyword/source thresholds.
- **ContentScoreService** – deterministic scoring: `(trend_velocity * 0.35) + (search_volume * 0.25) + (news_frequency * 0.15) + (youtube_activity * 0.15) + (monetization_value * 0.10)`.
- **QueuePublisherService** – idempotently emits topics to downstream queues.

## Connectors
All connectors implement `BaseConnector` and return `RawTopic` records. New sources only need to subclass `BaseConnector` and register in `connectors/__init__.py`.

## Queue Publishing
`queue/redis_publisher.py` pushes JSON payloads into a Redis stream (configurable). Downstream services consume `content_topics` stream entries.

## Running the Pipeline
```bash
python -m app.content_intelligence.services.content_intel_pipeline
```
Optionally pass `CONTENT_INTEL_CONFIG` env var or `--config` CLI flag (extend as needed) to point at a custom YAML config.

## Observability & Resilience
- Connectors run concurrently via asyncio gather.
- Each connector handles its own errors and the pipeline logs warnings without halting.
- Quality filter + scoring provide deterministic, audit-friendly decisions.
- Queue publisher tracks IDs to avoid duplicate dispatches.
- Config-driven architecture enables microservice extraction per service if required.

## Extending
1. Add new connector class.
2. Register in `connectors/__init__.py`.
3. Update YAML config.
4. (Optional) Adjust scoring weights or quality rules.

No LLMs are used in the scraping path; all scoring and filtering are deterministic.
