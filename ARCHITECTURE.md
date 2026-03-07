# YouTube Content Pipeline — Architecture v2

## Pipeline Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                    AI FACTORY PIPELINE v2                            │
│                    15-stage cinematic shorts engine                  │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌──────────┐   ┌──────────┐   ┌─────────────────────┐            │
│  │ SCRAPER  │──▶│ CLEANER  │──▶│ TOPIC INTELLIGENCE  │ ◀── NEW    │
│  │ (4 src)  │   │ (dedup)  │   │ (LLM extract/score) │            │
│  └──────────┘   └──────────┘   └────────┬────────────┘            │
│                                          │                          │
│                              ╔═══════════╧══════════╗               │
│                              ║  HALT IF 0 TOPICS    ║               │
│                              ╚═══════════╤══════════╝               │
│                                          │                          │
│  ┌──────────┐   ┌────────────┐   ┌──────┴──────┐                  │
│  │DISPATCHER│◀──│ PRIORITIZER│◀──│  CLUSTERER  │                  │
│  │(per-chan) │   │ (freshness)│   │ (TF-IDF+KM) │                  │
│  └────┬─────┘   └────────────┘   └─────────────┘                  │
│       │                                                             │
│  ┌────┴──────┐   ┌──────────┐   ┌──────────────┐                  │
│  │  SCRIPT   │──▶│  SCRIPT  │──▶│SCENE PLANNER │ ◀── ENHANCED    │
│  │ GENERATOR │   │ CLEANER  │   │(visual div.) │                  │
│  └───────────┘   └──────────┘   └──────┬───────┘                  │
│                                         │                           │
│  ┌───────────┐   ┌──────────────┐  ┌───┴──────────┐               │
│  │ COPYRIGHT │◀──│  CINEMATIC   │◀─┘               │               │
│  │  GUARD    │   │  DIRECTOR    │   ◀── NEW        │               │
│  └─────┬─────┘   └──────────────┘                  │               │
│        │                                            │               │
│  ┌─────┴─────┐   ┌──────────┐   ┌──────────┐      │               │
│  │   VOICE   │──▶│  VIDEO   │──▶│ QUALITY  │      │               │
│  │ GENERATOR │   │ BUILDER  │   │ CHECKER  │      │               │
│  └───────────┘   └──────────┘   └──────────┘      │               │
│                                                     │               │
└─────────────────────────────────────────────────────────────────────┘
```

## v2 Changes Summary

| Component | Before | After |
|-----------|--------|-------|
| Topic Discovery | Thin analyzer + title-only validator | **Topic Intelligence Engine**: LLM extraction, engagement scoring, semantic dedup, history tracking |
| Script Prompts | Generic title-only prompts | **Hook + angle + youtube_title** passed from intelligence engine |
| Visual Planning | Basic visual_intent per scene | **3 diverse visual prompts per scene** + visual diversity rotation |
| Stock Footage | Same-looking clips per channel | **Visual Diversity Engine** rotates style palettes, camera sequences, color grades |
| Copyright Safety | None | **Copyright Guard** sanitizes queries, blocks copyrighted terms, checks plagiarism |
| LLM Calls | Every call hits Ollama | **Prompt cache** with TTL — identical prompts return cached results |
| Pipeline Flow | analyzer → content_validator | **topic_intelligence** (single stage, 7 sub-stages) |

---

## Stage-by-Stage Breakdown

### Stage 1: Topic Scraper (`app/scraper/topic_scraper.py`)
- **Sources**: Reddit, Twitter/X, YouTube Trending, Google News RSS
- **Article extraction**: `ArticleExtractor` with readability-lxml + BeautifulSoup fallback
- **Hard gate**: minimum 400 chars article text
- **Output**: `data/topics/{timestamp}.json`

### Stage 2: Topic Cleaner (`app/scraper/topic_cleaner.py`)
- Title, URL, article_text validation
- Exact title deduplication
- Article length distribution logging
- **Output**: `data/topics_clean/{timestamp}.json`

### Stage 3: Topic Intelligence Engine (`app/analyzer/topic_intelligence.py`) — **NEW**
Seven sub-stages in one module:
1. **Article Quality Gate** — reject short/banned articles (400+ chars, no cookie banners)
2. **LLM Topic Extraction** — generate YouTube-optimized titles from article content
3. **Generic Title Rejection** — block "everything you need to know" style titles
4. **Engagement Scoring** — LLM + rule-based fallback (curiosity gap, emotional pull, searchability, specificity, trend relevance)
5. **Engagement Filter** — reject topics below 0.45 composite score
6. **Semantic Deduplication** — TF-IDF cosine similarity, threshold 0.70
7. **History Dedup** — cross-run tracking via `data/topic_history.json`

**Hard halt if 0 topics survive.**

- **Output**: `data/topics_intelligent/{timestamp}.json`

### Stage 4: Topic Cluster (`app/analyzer/topic_cluster.py`)
- TF-IDF + KMeans clustering (k = min(5, len/3))
- Intra-cluster cosine similarity
- Now reads from `data/topics_intelligent/`
- **Output**: `data/topic_clusters/{timestamp}.json`

### Stage 5: Topic Prioritizer (`app/analyzer/topic_prioritizer.py`)
- Priority = trend_score × weight + freshness_score × weight
- Sorted descending
- **Output**: `data/topic_queue/{timestamp}.json`

### Stage 6: Topic Dispatcher (`app/dispatcher/topic_dispatcher.py`)
- Flattens clusters → individual topics
- Now passes `youtube_title`, `hook`, `angle`, `engagement_score` to generation requests
- Creates per-channel request files
- **Output**: `data/topic_generated/{channel_id}/req_{timestamp}.json`

### Stage 7: Script Generator (`app/workers/topic_script_generator.py`)
- Uses `youtube_title` (from intelligence engine) instead of raw article title
- Prompt now includes `hook` guidance and `angle` direction
- 5-section structure: Hook → Escalation → Twist → Impact → Open Loop
- 130-170 words, retry if too short, trim if too long
- Hallucination phrase detection
- **Output**: `data/topic_scripts/{date}/{channel}_scripts.json`

### Stage 8: Script Cleaner (`app/workers/script_cleaner.py`)
- Strips markdown, labels, emojis, formatting artifacts
- Preserves raw script for debugging
- **Output**: `data/topic_scripts_clean/{date}/{channel}_scripts.json`

### Stage 9: Scene Planner (`app/workers/scene_planner.py`) — **ENHANCED**
- LLM-based scene breakdown (4-6 scenes per script)
- Emotion, energy, visual_intent, emphasis_words per scene
- **NEW: 3 diverse visual prompts per scene** (intent+emotion mapped)
- **NEW: Visual Diversity Engine integration** — rotates style palettes, diversifies intents across scenes
- Deterministic fallback if LLM fails
- **Output**: `data/scene_plans/{date}/{channel}_scenes.json`

### Stage 10: Cinematic Director (`app/video/cinematic_director.py`)
- Adds camera_motion, cut_timing, text_style, sound_design, color_grade per scene
- LLM with rule-based fallback
- No-repeat camera motion validation
- **Output**: `data/directed_plans/{date}/{channel}_directed.json`

### Stage 11: Copyright Guard (`app/video/copyright_guard.py`) — **NEW**
- Sanitizes visual_intent fields (blocks celebrity/brand names)
- Sanitizes visual_prompts search queries
- Script plagiarism check (4-gram overlap, threshold 40%)
- Adds `copyright_report` to each plan
- **Output**: Updates `data/directed_plans/` in-place

### Stage 12: Voice Generator (`app/workers/voice_generator.py`)
- Edge-TTS (free, unlimited)
- Per-scene audio files with channel-specific voices
- Micro-pause padding between scenes
- **Output**: `data/audio/{topic_id}/scene_XX.mp3`

### Stage 13: Video Builder (`app/video/video_builder_shorts.py`)
- Stock Fetcher now uses **visual_prompts** from scene planner (3 options per scene)
- FFmpeg rendering with zoompan, color grading, silence inserts
- Subtitle engine with cinematic ASS typography
- Audio mixing (voice + music + SFX)
- 1080x1920, 9:16, 30fps, < 60s
- **Output**: `data/shorts/final/{channel}/{topic}.mp4`

### Stage 14: Quality Checker (`app/video/quality_checker.py`)
- Duration, resolution, audio streams, file size validation
- Scene plan consistency checks
- **Output**: Pass/fail report per video

### Stage 15: Cleanup (`app/video/cleanup.py`)
- Removes work files and intermediate artifacts

---

## New Modules

| File | Purpose |
|------|---------|
| `app/analyzer/topic_intelligence.py` | Topic Intelligence Engine — LLM extraction, scoring, dedup |
| `app/video/visual_diversity.py` | Visual style rotation and diversity tracking |
| `app/video/copyright_guard.py` | Copyright safety validation |

## Enhanced Modules

| File | Change |
|------|--------|
| `app/services/ollama_client.py` | In-memory prompt cache with TTL (200 entries, 1hr) |
| `app/workers/scene_planner.py` | Per-sentence visual prompts, visual diversity integration |
| `app/workers/topic_script_generator.py` | Uses youtube_title + hook + angle from intelligence engine |
| `app/dispatcher/topic_dispatcher.py` | Passes intelligence engine fields to generation requests |
| `app/video/stock_fetcher.py` | Uses visual_prompts from scene planner for targeted footage |
| `app/analyzer/topic_cluster.py` | Reads from `topics_intelligent/` instead of `topics_analyzed/` |
| `app/main_pipeline.py` | New stage ordering, intelligence halt check, copyright guard |

---

## Data Flow

```
data/topics/                  ← raw scraped topics
data/topics_clean/            ← validated + deduped
data/topics_intelligent/      ← YouTube-optimized, engagement-scored  [NEW]
data/topic_clusters/          ← KMeans clusters
data/topic_queue/             ← prioritized queue
data/topic_generated/         ← per-channel dispatch requests
data/topic_scripts/           ← generated narration scripts
data/topic_scripts_clean/     ← cleaned scripts
data/scene_plans/             ← scene breakdowns + visual prompts
data/directed_plans/          ← cinematic direction + copyright check
data/audio/                   ← per-scene MP3 files
data/shorts/final/            ← upload-ready MP4 videos
data/topic_history.json       ← cross-run dedup tracking  [NEW]
```

---

## Speed Optimizations

1. **Prompt Cache** — `OllamaClient._cache` avoids re-calling Ollama for identical prompts within a run. Stats available via `OllamaClient.cache_stats()`.

2. **Rule-Based Fallbacks** — Every LLM-dependent stage (intelligence, scene planner, cinematic director) has a deterministic fallback, so pipeline never blocks on LLM failures.

3. **Visual Prompt Pre-generation** — Scene planner generates 3 stock search queries per scene at planning time, eliminating runtime query generation in the stock fetcher.

4. **History-Based Dedup** — `TopicHistory` prevents re-processing the same topics across pipeline runs, saving all downstream LLM calls.

---

## Configuration Files

| File | Purpose |
|------|---------|
| `app/config/channels.yaml` | Channel definitions (niche, tone, model, visual profile, creative) |
| `app/config/model_routing.yaml` | Per-channel LLM model assignment |
| `app/config/quality_thresholds.yaml` | Script quality gating thresholds |
| `app/config/quota_targets.yaml` | Daily video quotas per channel |
| `app/config/content_intel.yaml` | Scraper connectors, category keywords, queue backend |

---

## Dependencies

Key packages (see `requirements.txt`):
- `scikit-learn` — TF-IDF vectorization, KMeans clustering, cosine similarity
- `feedparser` — RSS feed parsing
- `readability-lxml` — Article text extraction
- `edge-tts` — Free TTS voice generation
- `pyyaml` — Config file parsing
- `requests` — Pexels API, web scraping
- `python-dotenv` — Environment variable management
- `numpy` — Numerical operations

External services:
- **Ollama** (local) — LLM inference via CLI
- **Pexels API** — Stock video footage (requires `PEXELS_API_KEY`)
- **FFmpeg** — Video rendering and audio processing
