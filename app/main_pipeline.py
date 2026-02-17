import subprocess
import time
import logging
import os
from datetime import datetime

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    filename=f"{LOG_DIR}/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

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

def run_step(module_name):
    start = time.time()
    logging.info(f"STARTING {module_name}")

    env = os.environ.copy()
    # PYTHONPATH should include the project root (parent of app)
    env["PYTHONPATH"] = os.path.dirname(PROJECT_ROOT)

    result = subprocess.run(
        ["python3", "-m", module_name],
        cwd=os.path.dirname(PROJECT_ROOT), # Run from parent of app to find app module
        env=env,
        capture_output=True,
        text=True
    )

    end = time.time()
    duration = round(float(end - start), 2)  # pyre-ignore[6]

    if result.returncode != 0:
        logging.error(f"FAILED {module_name} after {duration}s")
        logging.error(result.stderr)
        print(result.stderr)
        raise Exception(f"{module_name} failed")

    logging.info(f"COMPLETED {module_name} in {duration}s")
    logging.info(result.stdout)
    print(f"✔ {module_name} completed in {duration}s")


def main():
    print("\n🚀 Starting AI Factory Pipeline\n")
    logging.info("========== PIPELINE START ==========")

    pipeline_start = time.time()

    for step in PIPELINE:
        run_step(step)

    total_time = round(float(time.time() - pipeline_start), 2)  # pyre-ignore[6]

    logging.info(f"PIPELINE FINISHED in {total_time}s")
    logging.info("========== PIPELINE END ==========")

    print(f"\n✅ Pipeline completed successfully in {total_time}s\n")


if __name__ == "__main__":
    main()