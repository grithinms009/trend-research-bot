#!/bin/bash
# Daily pipeline runner — designed for cron automation
# Produces 2 videos per channel (10 total) per run
#
# Cron example (run at 6 AM UTC daily):
#   0 6 * * * /root/trend-research-bot/daily_run.sh >> /root/trend-research-bot/logs/cron.log 2>&1

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Setup
DATE=$(date +%Y%m%d_%H%M%S)
LOG_DIR="$SCRIPT_DIR/logs"
LOG_FILE="$LOG_DIR/pipeline_${DATE}.log"
mkdir -p "$LOG_DIR" "data"

# Activate virtual environment
if [ -f ".venv/bin/activate" ]; then
    source .venv/bin/activate
elif [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
fi

echo "========================================" | tee -a "$LOG_FILE"
echo "  Daily Pipeline Run: $(date)" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"

# Ensure Ollama is running
if ! curl -s http://localhost:11434/api/tags > /dev/null 2>&1; then
    echo "[WARN] Ollama not responding, attempting to start..." | tee -a "$LOG_FILE"
    nohup ollama serve > "$LOG_DIR/ollama.log" 2>&1 &
    sleep 5
    if ! curl -s http://localhost:11434/api/tags > /dev/null 2>&1; then
        echo "[ERROR] Ollama failed to start. Aborting." | tee -a "$LOG_FILE"
        exit 1
    fi
    echo "[OK] Ollama started successfully" | tee -a "$LOG_FILE"
fi

# Clean previous run data (keep logs and history)
echo "[INFO] Cleaning previous data..." | tee -a "$LOG_FILE"
find data/ -maxdepth 1 -mindepth 1 -type d \
    ! -name 'shorts' \
    ! -name 'topic_history*' \
    -exec rm -rf {} +

# Preserve final videos and history, clean intermediate data
for dir in topics topics_clean topics_intelligent topic_clusters topic_queue \
           topic_generated topic_scripts topic_scripts_clean scene_plans \
           directed_plans audio; do
    rm -rf "data/$dir"
done

# Run the pipeline
echo "[INFO] Starting pipeline..." | tee -a "$LOG_FILE"
python3 -m app.main_pipeline 2>&1 | tee -a "$LOG_FILE"
EXIT_CODE=${PIPESTATUS[0]}

# Summary
echo "" | tee -a "$LOG_FILE"
if [ $EXIT_CODE -eq 0 ]; then
    VIDEO_COUNT=$(find data/shorts/final -name "*.mp4" 2>/dev/null | wc -l)
    echo "[SUCCESS] Pipeline completed. Videos produced: $VIDEO_COUNT" | tee -a "$LOG_FILE"
else
    echo "[FAILED] Pipeline exited with code $EXIT_CODE" | tee -a "$LOG_FILE"
fi

# Cleanup old logs (keep 30 days)
find "$LOG_DIR" -name "pipeline_*.log" -mtime +30 -delete 2>/dev/null || true

echo "[DONE] $(date)" | tee -a "$LOG_FILE"
