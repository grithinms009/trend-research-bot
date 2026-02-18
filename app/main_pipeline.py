import subprocess
import time
import logging
import os
import json
import glob
from datetime import datetime

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(PROJECT_ROOT)  # trend-research-bot root
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    filename=f"{LOG_DIR}/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Also log to console
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logging.getLogger().addHandler(console_handler)

# Use module names for python -m execution
PIPELINE = [
    "app.scraper.topic_scraper",
    "app.scraper.topic_cleaner",
    "app.analyzer.topic_analyzer",
    "app.analyzer.topic_cluster",
    "app.analyzer.topic_prioritizer",
    "app.dispatcher.topic_dispatcher",
    "app.workers.topic_generator_worker",
    "app.workers.topic_script_generator"
]

# Stage-to-data mapping for metrics collection
STAGE_DATA_DIRS = {
    "app.scraper.topic_scraper": ("data/topics", "scraped"),
    "app.scraper.topic_cleaner": ("data/topics_clean", "cleaned"),
    "app.analyzer.topic_analyzer": ("data/topics_analyzed", "analyzed"),
    "app.analyzer.topic_cluster": ("data/topic_clusters", "clustered"),
    "app.analyzer.topic_prioritizer": ("data/topic_queue", "queued"),
    "app.dispatcher.topic_dispatcher": ("data/topic_generated", "dispatched"),
    "app.workers.topic_generator_worker": ("data/topic_scripts", "generated"),
    "app.workers.topic_script_generator": ("data/topic_scripts", "generated"),
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

    # Check subdirectories (for dispatched/generated)
    subdirs = [d for d in os.listdir(data_dir) if os.path.isdir(os.path.join(data_dir, d))]
    total = 0
    for subdir in subdirs:
        subdir_path = os.path.join(data_dir, subdir)
        json_files = glob.glob(f"{subdir_path}/*.json")
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
    return total


def run_step(module_name):
    start = time.time()
    logging.info(f"STARTING {module_name}")

    env = os.environ.copy()
    # PYTHONPATH should include the project root (parent of app)
    env["PYTHONPATH"] = os.path.dirname(PROJECT_ROOT)

    result = subprocess.run(
        ["python3", "-m", module_name],
        cwd=os.path.dirname(PROJECT_ROOT),  # Run from parent of app to find app module
        env=env,
        capture_output=True,
        text=True
    )

    end = time.time()
    duration = round(float(end - start), 2)  # pyre-ignore[6]

    # Collect stage output for logging
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
    
    # Print stage output (filtered for key lines)
    if stage_output:
        for line in stage_output.strip().split("\n"):
            line = line.strip()
            if line and not line.startswith("---"):
                print(f"  │ {line}")


def print_health_report():
    """Print comprehensive pipeline health report."""
    print("\n" + "=" * 60)
    print("         🏥 PIPELINE HEALTH REPORT")
    print("=" * 60)

    # Collect counts from data directories
    scraped = count_items_in_latest_json("data/topics")
    cleaned = count_items_in_latest_json("data/topics_clean")
    analyzed = count_items_in_latest_json("data/topics_analyzed")
    
    # For clusters, count both clusters and total topics
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

    print(f"\n  📊 Stage Results:")
    print(f"  {'─' * 40}")
    print(f"  Scraped (raw topics):    {scraped}")
    print(f"  Cleaned (valid):         {cleaned}")
    print(f"  Analyzed:                {analyzed}")
    print(f"  Clusters:                {cluster_count} (containing {clustered_topics} topics)")
    print(f"  Queued (prioritized):    {queued}")
    print(f"  Dispatched:              {dispatched}")
    print(f"  Scripts Generated:       {generated}")

    # Success rate
    if scraped > 0:
        clean_rate = (cleaned / scraped) * 100
        gen_rate = (generated / scraped) * 100 if generated > 0 else 0
        print(f"\n  📈 Success Rates:")
        print(f"  {'─' * 40}")
        print(f"  Extraction → Clean:     {clean_rate:.1f}%")
        print(f"  End-to-End (→ Script):   {gen_rate:.1f}%")

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
            # Show first 200 chars of stderr
            print(f"  {short_name}: {metrics['stderr'][:200]}")

    print(f"\n{'=' * 60}\n")

    # Final verdict
    if generated >= 3 and cleaned >= 10:
        print("🏆 PIPELINE HEALTHY — targets met!")
    elif generated > 0 and cleaned >= 5:
        print("⚠️  PIPELINE PARTIAL — some targets not met, but producing output")
    elif cleaned > 0:
        print("❌ PIPELINE UNHEALTHY — cleaning works but generation failed")
    else:
        print("🚨 PIPELINE BROKEN — no valid topics surviving the pipeline")


def main():
    print("\n🚀 Starting AI Factory Pipeline\n")
    logging.info("========== PIPELINE START ==========")

    pipeline_start = time.time()

    for step in PIPELINE:
        try:
            run_step(step)
        except Exception as e:
            logging.error(f"Pipeline stopped at {step}: {e}")
            print(f"\n🛑 Pipeline stopped at {step}")
            break

    total_time = round(float(time.time() - pipeline_start), 2)  # pyre-ignore[6]

    logging.info(f"PIPELINE FINISHED in {total_time}s")
    logging.info("========== PIPELINE END ==========")

    print(f"\n✅ Pipeline completed in {total_time}s")
    
    # Print comprehensive health report
    print_health_report()


if __name__ == "__main__":
    main()