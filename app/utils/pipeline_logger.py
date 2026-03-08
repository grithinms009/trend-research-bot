"""
Pipeline Logger — structured per-stage JSON logging with improvement tracking.

Usage in any stage:
    from app.utils.pipeline_logger import StageLogger
    log = StageLogger("cinematic_director")
    log.event("llm_call", {"model": "mistral", "duration": 12.3, "success": True})
    log.metric("scenes_directed", 5)
    log.warning("High fallback rate", suggestion="Check Ollama model availability")
    log.finish(success=True)

Logs are written to: data/logs/stages/<date>/<stage_name>.jsonl
Each line is a JSON object with timestamp, level, and payload.
"""

import os
import json
import time
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
STAGE_LOG_DIR = os.path.join(BASE_DIR, "data", "logs", "stages")


class StageLogger:
    """Structured logger for a single pipeline stage run."""

    def __init__(self, stage_name: str):
        self.stage_name = stage_name
        self.start_time = time.time()
        self.date_str = datetime.now().strftime("%Y%m%d")
        self._metrics: Dict[str, Any] = {}
        self._events: List[Dict] = []
        self._warnings: List[Dict] = []
        self._errors: List[Dict] = []

        # Setup log directory and file
        self._log_dir = os.path.join(STAGE_LOG_DIR, self.date_str)
        os.makedirs(self._log_dir, exist_ok=True)
        self._log_path = os.path.join(self._log_dir, f"{stage_name}.jsonl")

        self._write_entry("START", {"stage": stage_name})

    def _write_entry(self, level: str, data: Dict):
        """Append a JSON line to the stage log file."""
        entry = {
            "ts": datetime.now().isoformat(),
            "stage": self.stage_name,
            "level": level,
            **data,
        }
        try:
            with open(self._log_path, "a") as f:
                f.write(json.dumps(entry) + "\n")
        except Exception as exc:
            logger.debug("StageLogger write failed: %s", exc)

    def event(self, name: str, data: Optional[Dict] = None):
        """Log a named event (e.g., llm_call, clip_downloaded, transition_planned)."""
        payload = {"event": name, **(data or {})}
        self._events.append(payload)
        self._write_entry("EVENT", payload)

    def metric(self, key: str, value: Any):
        """Track a numeric or categorical metric."""
        self._metrics[key] = value
        self._write_entry("METRIC", {"key": key, "value": value})

    def warning(self, message: str, suggestion: str = ""):
        """Log a warning with an optional improvement suggestion."""
        payload = {"message": message, "suggestion": suggestion}
        self._warnings.append(payload)
        self._write_entry("WARN", payload)

    def error(self, message: str, detail: str = ""):
        """Log an error."""
        payload = {"message": message, "detail": detail}
        self._errors.append(payload)
        self._write_entry("ERROR", payload)

    def finish(self, success: bool = True):
        """Finalize the stage log with summary."""
        elapsed = round(time.time() - self.start_time, 2)
        summary = {
            "success": success,
            "duration_s": elapsed,
            "metrics": self._metrics,
            "event_count": len(self._events),
            "warning_count": len(self._warnings),
            "error_count": len(self._errors),
            "warnings": self._warnings,
            "errors": self._errors,
        }
        self._write_entry("FINISH", summary)

        # Also write a compact summary JSON for the review tool
        summary_path = os.path.join(self._log_dir, f"{self.stage_name}_summary.json")
        try:
            with open(summary_path, "w") as f:
                json.dump(summary, f, indent=2)
        except Exception:
            pass

        status = "OK" if success else "FAILED"
        print(f"  [{self.stage_name}] {status} in {elapsed}s | warnings={len(self._warnings)} errors={len(self._errors)}")

    def get_metrics(self) -> Dict[str, Any]:
        return dict(self._metrics)
