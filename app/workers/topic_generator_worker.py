"""
Topic Generator Worker — processes generation requests concurrently.

Reads dispatched requests from data/topic_generated/{channel_id}/
and uses TopicScriptGenerator with ThreadPoolExecutor for parallel Ollama calls.
"""

import os
import json
import glob
import time
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from app.workers.topic_script_generator import TopicScriptGenerator

logger = logging.getLogger(__name__)

MAX_PARALLEL_WORKERS = 3


class TopicGeneratorWorker:
    def __init__(self):
        self.generator = TopicScriptGenerator()
        self.metrics = {
            "input_count": 0,
            "output_count": 0,
            "failed_count": 0,
            "skipped_count": 0,
            "execution_time": 0.0,
            "per_script_times": [],
        }

    def process_request(self, fpath):
        """Process a single generation request file. Returns (script, filepath) or (None, filepath)."""
        start = time.time()
        try:
            with open(fpath) as f:
                req = json.load(f)
        except Exception as exc:
            logger.error("Failed to read request file %s: %s", fpath, exc)
            return None, fpath

        title = req.get("topic", {}).get("title", "unknown")[:60]
        
        try:
            script = self.generator.generate_script(req)
            duration = round(time.time() - start, 2)
            self.metrics["per_script_times"].append(duration)
            
            if script:
                logger.info("Generated script for '%s' in %ss", title, duration)
                return script, fpath
            else:
                logger.info("Skipped script for '%s' (no output) in %ss", title, duration)
                return None, fpath
        except Exception as exc:
            logger.error("Error generating script for '%s': %s", title, exc)
            return None, fpath

    def run(self):
        """Process all pending generation requests across all channels."""
        start = time.time()
        
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        GENERATED_DIR = os.path.join(BASE_DIR, "data", "topic_generated")
        DATE_STR = datetime.now().strftime("%Y%m%d")
        SCRIPTS_DIR = os.path.join(BASE_DIR, "data", "topic_scripts", DATE_STR)
        os.makedirs(SCRIPTS_DIR, exist_ok=True)

        total_scripts = 0

        for cid in ["C1", "C2", "C3", "C4", "C5"]:
            channel_req_dir = os.path.join(GENERATED_DIR, cid)
            if not os.path.exists(channel_req_dir):
                continue

            files = glob.glob(f"{channel_req_dir}/req_*.json")
            if not files:
                continue

            self.metrics["input_count"] += len(files)
            print(f"\nProcessing {len(files)} requests for channel {cid} (max {MAX_PARALLEL_WORKERS} parallel)...")
            
            channel_scripts = []
            
            # Process concurrently with ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=MAX_PARALLEL_WORKERS) as executor:
                future_to_file = {
                    executor.submit(self.process_request, fpath): fpath 
                    for fpath in files
                }
                
                for future in as_completed(future_to_file):
                    fpath = future_to_file[future]
                    try:
                        script, processed_path = future.result()
                        if script:
                            channel_scripts.append(script)
                            self.metrics["output_count"] += 1
                            # Remove processed request file
                            try:
                                os.remove(processed_path)
                            except OSError:
                                pass
                        else:
                            self.metrics["skipped_count"] += 1
                    except Exception as exc:
                        logger.error("Worker future failed for %s: %s", fpath, exc)
                        self.metrics["failed_count"] += 1

            if channel_scripts:
                outfile = os.path.join(SCRIPTS_DIR, f"{cid}_scripts.json")
                with open(outfile, "w") as f:
                    json.dump(channel_scripts, f, indent=2)
                print(f"  Saved {len(channel_scripts)} scripts to {outfile}")
                total_scripts += len(channel_scripts)
            else:
                print(f"  No scripts generated for channel {cid}")

        self.metrics["execution_time"] = round(time.time() - start, 2)
        self._log_metrics()

        if total_scripts == 0:
            print("\nWARNING: 0 scripts generated across all channels!")
        else:
            print(f"\n✅ Total scripts generated: {total_scripts}")

        return total_scripts

    def _log_metrics(self):
        print("\n--- Generator Worker Metrics ---")
        for key, value in self.metrics.items():
            if key == "per_script_times" and value:
                print(f"  avg_per_script_time: {sum(value)/len(value):.2f}s")
                print(f"  min_per_script_time: {min(value):.2f}s")
                print(f"  max_per_script_time: {max(value):.2f}s")
            else:
                print(f"{key}: {value}")
        print("-------------------------------\n")


if __name__ == "__main__":
    worker = TopicGeneratorWorker()
    total = worker.run()
    worker.generator.log_metrics()
    print("Generation worker cycle complete.")