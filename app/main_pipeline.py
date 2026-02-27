"""
AI Factory Pipeline — production-grade orchestrator.

10-stage pipeline optimized for throughput:
  scraper → cleaner → analyzer → validator(HALT if 0) → cluster →
  prioritizer → dispatcher → script_generator → scene_splitter → voice_generator

Hard halt after validator if 0 topics survive.
Comprehensive health report with pipeline health flags.
"""

import os
import sys
import json
import glob
import time
import logging
import subprocess
from datetime import datetime

# Setup logging
LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M')}.log")),
        logging.StreamHandler(),
    ],
)

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(PROJECT_ROOT)

# ==================== PRODUCTION PIPELINE ====================
# 14 stages — full creative shorts pipeline
PIPELINE = [
    "app.scraper.topic_scraper",
    "app.scraper.topic_cleaner",
    "app.analyzer.topic_analyzer",
    "app.analyzer.topic_content_validator",
    # --- HARD HALT CHECK HERE IF 0 VALIDATED ---
    "app.analyzer.topic_cluster",
    "app.analyzer.topic_prioritizer",
    "app.dispatcher.topic_dispatcher",
    "app.workers.topic_script_generator",
    "app.workers.script_cleaner",           # NEW: strip labels/markdown
    "app.workers.scene_planner",            # REPLACED: LLM scene planner with emotion/energy
    "app.workers.voice_generator",
    # --- VIDEO PIPELINE ---
    "app.video.video_builder_shorts",
    "app.video.cleanup",
]

# Validator is the halt-check stage
HALT_CHECK_STAGE = "app.analyzer.topic_content_validator"
HALT_CHECK_DIR = "data/topics_validated"

# Stage-to-data mapping for metrics collection
STAGE_DATA_DIRS = {
    "app.scraper.topic_scraper": ("data/topics", "scraped"),
    "app.scraper.topic_cleaner": ("data/topics_clean", "cleaned"),
    "app.analyzer.topic_analyzer": ("data/topics_analyzed", "analyzed"),
    "app.analyzer.topic_content_validator": ("data/topics_validated", "validated"),
    "app.analyzer.topic_cluster": ("data/topic_clusters", "clustered"),
    "app.analyzer.topic_prioritizer": ("data/topic_queue", "queued"),
    "app.dispatcher.topic_dispatcher": ("data/topic_generated", "dispatched"),
    "app.workers.topic_script_generator": ("data/topic_scripts", "generated"),
    "app.workers.script_cleaner": ("data/topic_scripts_clean", "cleaned_scripts"),
    "app.workers.scene_planner": ("data/scene_plans", "scene_planned"),
    "app.workers.voice_generator": ("data/audio", "audio"),
    "app.video.video_builder_shorts": ("data/shorts/final", "video"),
    "app.video.cleanup": ("", "cleanup"),
}

stage_metrics = {}


def count_items_in_latest_json(data_dir_rel):
    """Count items in the latest JSON file in a data directory."""
    data_dir = os.path.join(BASE_DIR, data_dir_rel)
    if not os.path.exists(data_dir):
        return 0

    # Check for JSON files directly
    files = sorted(glob.glob(f"{data_dir}/*.json"))
    if files:
        try:
            with open(files[-1]) as f:
                data = json.load(f)
            if isinstance(data, list):
                # For clusters, count total topics inside
                total = 0
                for item in data:
                    if isinstance(item, dict) and "topics" in item:
                        total += len(item.get("topics", []))
                    else:
                        total += 1
                return total
            return 1
        except Exception:
            return 0

    # Check subdirectories (for dispatched/generated/audio)
    total = 0
    for entry in os.listdir(data_dir):
        entry_path = os.path.join(data_dir, entry)
        if os.path.isdir(entry_path):
            json_files = glob.glob(f"{entry_path}/*.json")
            if json_files:
                for jf in json_files:
                    try:
                        with open(jf) as f:
                            data = json.load(f)
                        if isinstance(data, list):
                            total += len(data)
                        else:
                            total += 1
                    except Exception:
                        total += 1
            else:
                # Count non-JSON files (e.g., mp3 audio assets)
                files_in_dir = [
                    f
                    for f in os.listdir(entry_path)
                    if os.path.isfile(os.path.join(entry_path, f))
                ]
                total += len(files_in_dir)
        elif entry.endswith(".mp3"):
            total += 1
    return total


def run_step(module_name):
    start = time.time()
    logging.info(f"STARTING {module_name}")

    env = os.environ.copy()
    env["PYTHONPATH"] = os.path.dirname(PROJECT_ROOT)

    result = subprocess.run(
        ["python3", "-m", module_name],
        cwd=os.path.dirname(PROJECT_ROOT),
        env=env,
        capture_output=True,
        text=True,
    )

    end = time.time()
    duration = round(float(end - start), 2)

    stage_output = result.stdout

    # Track metrics per stage
    data_dir_rel, label = STAGE_DATA_DIRS.get(module_name, ("", module_name))
    output_count = count_items_in_latest_json(data_dir_rel) if data_dir_rel else 0

    stage_metrics[module_name] = {
        "label": label,
        "duration": duration,
        "output_count": output_count,
        "success": result.returncode == 0,
        "stderr": result.stderr.strip() if result.stderr else "",
    }

    if result.returncode != 0:
        logging.error(f"FAILED {module_name} after {duration}s")
        logging.error(result.stderr)
        print(f"\n❌ {module_name} FAILED after {duration}s")
        print(result.stderr)
        print(result.stdout)
        raise Exception(f"{module_name} failed")

    logging.info(f"COMPLETED {module_name} in {duration}s")
    logging.info(stage_output)
    print(f"✔ {module_name} completed in {duration}s")

    # Print stage output
    if stage_output:
        for line in stage_output.strip().split("\n"):
            line = line.strip()
            if line and not line.startswith("---"):
                print(f"  │ {line}")

    return output_count


def check_halt_condition():
    """Check if pipeline should halt after validator stage."""
    validated_count = count_items_in_latest_json(HALT_CHECK_DIR)
    if validated_count == 0:
        print("\n" + "=" * 60)
        print("🛑 PIPELINE HALTED — 0 validated topics")
        print("   No content survived validation. Downstream stages skipped.")
        print("   Check scraper sources and article extraction quality.")
        print("=" * 60)
        logging.error("PIPELINE HALTED: 0 validated topics after content validation")
        return True
    print(f"\n✅ {validated_count} validated topics — continuing pipeline\n")
    return False


def get_script_stats():
    """Compute script-level stats for health report."""
    scripts_dir = os.path.join(BASE_DIR, "data", "topic_scripts")
    if not os.path.exists(scripts_dir):
        return 0, 0.0, []

    word_counts = []
    gen_times = []

    for root, _, files in os.walk(scripts_dir):
        for fname in files:
            if fname.endswith("_scripts.json"):
                fpath = os.path.join(root, fname)
                try:
                    with open(fpath) as f:
                        scripts = json.load(f)
                    for s in scripts:
                        wc = s.get("word_count", 0)
                        gt = s.get("generation_time_seconds", 0)
                        if wc > 0:
                            word_counts.append(wc)
                        if gt > 0:
                            gen_times.append(gt)
                except Exception:
                    pass

    avg_length = float(sum(word_counts) / len(word_counts)) if word_counts else 0.0
    avg_gen_time = float(sum(gen_times) / len(gen_times)) if gen_times else 0.0

    return len(word_counts), avg_length, avg_gen_time


def print_health_report():
    """Print comprehensive pipeline health report."""
    print("\n" + "=" * 60)
    print("         🏥 PIPELINE HEALTH REPORT")
    print("=" * 60)

    # Collect counts
    scraped = count_items_in_latest_json("data/topics")
    cleaned = count_items_in_latest_json("data/topics_clean")
    analyzed = count_items_in_latest_json("data/topics_analyzed")
    validated = count_items_in_latest_json("data/topics_validated")

    # Cluster info
    cluster_dir = os.path.join(BASE_DIR, "data", "topic_clusters")
    cluster_count = 0
    clustered_topics = 0
    cluster_files = sorted(glob.glob(f"{cluster_dir}/*.json"))
    if cluster_files:
        try:
            with open(cluster_files[-1]) as f:
                clusters = json.load(f)
            cluster_count = len(clusters)
            clustered_topics = sum(len(c.get("topics", [])) for c in clusters)
        except Exception:
            pass

    queued = count_items_in_latest_json("data/topic_queue")
    dispatched = count_items_in_latest_json("data/topic_generated")
    generated = count_items_in_latest_json("data/topic_scripts")
    scenes = count_items_in_latest_json("data/scene_plans")
    audio_clips = count_items_in_latest_json("data/audio")

    # Script stats
    script_count, avg_script_length, avg_gen_time = get_script_stats()

    print(f"\n  📊 Stage Results:")
    print(f"  {'─' * 40}")
    print(f"  Scraped (raw topics):    {scraped}")
    print(f"  Cleaned (valid):         {cleaned}")
    print(f"  Analyzed:                {analyzed}")
    print(f"  Validated:               {validated}")
    print(f"  Clusters:                {cluster_count} (containing {clustered_topics} topics)")
    print(f"  Queued (prioritized):    {queued}")
    print(f"  Dispatched:              {dispatched}")
    print(f"  Scripts Generated:       {generated}")
    print(f"  Scenes Created:          {scenes}")
    print(f"  Audio Clips:             {audio_clips}")

    # Success rates
    validation_rate = (validated / analyzed * 100) if analyzed > 0 else 0
    # Use validated as denominator for script rate (dispatched files get consumed)
    script_rate = (generated / max(validated, 1) * 100) if validated > 0 else 0
    scene_rate = (scenes / max(generated, 1) * 100) if generated > 0 else 0
    audio_rate = (audio_clips / max(scenes, 1) * 100) if scenes > 0 else 0

    print(f"\n  📈 Key Metrics:")
    print(f"  {'─' * 40}")
    print(f"  validation_success_rate: {validation_rate:.1f}%")
    print(f"  script_success_rate:     {script_rate:.1f}%")
    print(f"  avg_script_length:       {avg_script_length:.0f} words")
    print(f"  avg_script_gen_time:     {avg_gen_time:.1f}s")
    print(f"  scenes_created:          {scenes}")
    print(f"  audio_files_created:     {audio_clips}")

    # Per-stage timing
    print(f"\n  ⏱  Stage Timings:")
    print(f"  {'─' * 40}")
    total_time = 0
    for module_name, metrics in stage_metrics.items():
        status = "✅" if metrics["success"] else "❌"
        short_name = module_name.split(".")[-1]
        print(f"  {status} {short_name:30s} {metrics['duration']:6.1f}s")
        total_time += metrics["duration"]
    print(f"  {'─' * 40}")
    print(f"  Total pipeline time:     {total_time:.1f}s")

    # Errors
    errors = [(m, s) for m, s in stage_metrics.items() if s.get("stderr")]
    if errors:
        print(f"\n  ⚠️  Warnings/Errors:")
        print(f"  {'─' * 40}")
        for module_name, metrics in errors:
            short_name = module_name.split(".")[-1]
            print(f"  {short_name}: {metrics['stderr'][:200]}")

    print(f"\n{'=' * 60}\n")

    # ========== PIPELINE HEALTH FLAG ==========
    if script_rate < 50 and dispatched > 0:
        print("🚨 PIPELINE UNHEALTHY — script_success_rate < 50%")
    elif generated >= 5 and scenes >= 5:
        print("🏆 PIPELINE HEALTHY — targets met!")
    elif generated > 0 and scenes > 0:
        print("⚠️  PIPELINE PARTIAL — producing output, below targets")
    elif validated > 0:
        print("❌ PIPELINE DEGRADED — validated topics exist but no scripts generated")
    else:
        print("🚨 PIPELINE BROKEN — no topics surviving pipeline")


def main():
    print("\n🚀 Starting AI Factory Pipeline\n")
    logging.info("========== PIPELINE START ==========")

    pipeline_start = time.time()
    halted = False

    for step in PIPELINE:
        try:
            run_step(step)

            # Hard halt check after validator
            if step == HALT_CHECK_STAGE:
                if check_halt_condition():
                    halted = True
                    break

        except Exception as e:
            logging.error(f"Pipeline stopped at {step}: {e}")
            print(f"\n🛑 Pipeline stopped at {step}")
            break

    total_time = round(float(time.time() - pipeline_start), 2)

    logging.info(f"PIPELINE FINISHED in {total_time}s")
    logging.info("========== PIPELINE END ==========")

    if halted:
        print(f"\n🛑 Pipeline halted after {total_time}s (no validated topics)")
    else:
        print(f"\n✅ Pipeline completed in {total_time}s")

    # Print comprehensive health report
    print_health_report()


if __name__ == "__main__":
    main()