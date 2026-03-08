"""
Pipeline Review Tool — analyzes stage logs and generates actionable improvement report.

Usage:
    python3 scripts/review_pipeline.py                  # Review today's run
    python3 scripts/review_pipeline.py --date 20260308  # Review specific date
    python3 scripts/review_pipeline.py --last 3         # Review last 3 runs
"""

import os
import sys
import json
import glob
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Any

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STAGE_LOG_DIR = os.path.join(BASE_DIR, "data", "logs", "stages")

# ============================================================
# IMPROVEMENT RULES — pattern-based suggestions
# ============================================================
IMPROVEMENT_RULES = [
    {
        "check": lambda s: s.get("metrics", {}).get("fallback_used", 0) > s.get("metrics", {}).get("llm_successes", 0),
        "stage": "scene_planner",
        "severity": "high",
        "message": "LLM fallback rate > 50%",
        "suggestion": "Check Ollama availability, increase timeout, or switch to a faster model. "
                      "Fallback scenes have generic camera styles and miss 3-tier visual prompts.",
    },
    {
        "check": lambda s: s.get("duration_s", 0) > 600,
        "stage": "*",
        "severity": "medium",
        "message": "Stage took > 10 minutes",
        "suggestion": "Consider model optimization, caching, or parallel processing.",
    },
    {
        "check": lambda s: s.get("metrics", {}).get("clips_rejected", 0) > s.get("metrics", {}).get("clips_downloaded", 0),
        "stage": "stock_fetcher",
        "severity": "high",
        "message": "More clips rejected than downloaded",
        "suggestion": "Loosen rejection filters or improve search queries. Check if Pexels API key is valid.",
    },
    {
        "check": lambda s: s.get("metrics", {}).get("prompts_rejected_similar", 0) > 5,
        "stage": "scene_planner",
        "severity": "medium",
        "message": "Many visual prompts rejected for similarity",
        "suggestion": "Increase visual prompt diversity in LLM prompt or add more symbolic alternatives.",
    },
    {
        "check": lambda s: s.get("metrics", {}).get("static_footage_ratio", 0) > 0.3,
        "stage": "quality_checker",
        "severity": "high",
        "message": "Static footage ratio > 30%",
        "suggestion": "Add more camera motion directives. Check if stock clips have motion metadata.",
    },
    {
        "check": lambda s: s.get("metrics", {}).get("diversity_score", 1.0) < 0.6,
        "stage": "quality_checker",
        "severity": "medium",
        "message": "Scene diversity score below 0.6",
        "suggestion": "Ensure shot_type cycling and camera_style variety in scene planner.",
    },
    {
        "check": lambda s: s.get("metrics", {}).get("transition_issues", 0) > 2,
        "stage": "cinematic_director",
        "severity": "medium",
        "message": "Multiple transition issues detected",
        "suggestion": "Check transition engine variety. Consecutive identical transitions hurt retention.",
    },
    {
        "check": lambda s: s.get("error_count", 0) > 0,
        "stage": "*",
        "severity": "high",
        "message": "Stage had errors",
        "suggestion": "Review error details in the stage log file.",
    },
    {
        "check": lambda s: s.get("metrics", {}).get("topics_dispatched", 0) == 0,
        "stage": "topic_dispatcher",
        "severity": "critical",
        "message": "0 topics dispatched",
        "suggestion": "Check scraper sources, article quality gate, and channel classifier accuracy.",
    },
    {
        "check": lambda s: s.get("metrics", {}).get("voice_failures", 0) > 0,
        "stage": "voice_generator",
        "severity": "high",
        "message": "Voice generation failures",
        "suggestion": "Check edge-tts installation and internet connectivity for TTS API.",
    },
    {
        "check": lambda s: s.get("metrics", {}).get("qc_failed", 0) > 0,
        "stage": "quality_checker",
        "severity": "high",
        "message": "Videos failed quality check",
        "suggestion": "Review QC issues: duration, resolution, audio, pacing, or static footage.",
    },
]


def load_summaries(date_str: str) -> Dict[str, Dict]:
    """Load all stage summary files for a given date."""
    day_dir = os.path.join(STAGE_LOG_DIR, date_str)
    if not os.path.exists(day_dir):
        return {}

    summaries = {}
    for f in sorted(glob.glob(os.path.join(day_dir, "*_summary.json"))):
        stage = os.path.basename(f).replace("_summary.json", "")
        try:
            with open(f) as fh:
                summaries[stage] = json.load(fh)
        except Exception:
            pass
    return summaries


def load_events(date_str: str, stage: str) -> List[Dict]:
    """Load all JSONL events for a stage."""
    log_path = os.path.join(STAGE_LOG_DIR, date_str, f"{stage}.jsonl")
    if not os.path.exists(log_path):
        return []
    events = []
    with open(log_path) as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    events.append(json.loads(line))
                except Exception:
                    pass
    return events


def run_improvement_checks(summaries: Dict[str, Dict]) -> List[Dict]:
    """Run all improvement rules against stage summaries."""
    findings = []
    for rule in IMPROVEMENT_RULES:
        for stage_name, summary in summaries.items():
            if rule["stage"] != "*" and rule["stage"] != stage_name:
                continue
            try:
                if rule["check"](summary):
                    findings.append({
                        "stage": stage_name,
                        "severity": rule["severity"],
                        "message": rule["message"],
                        "suggestion": rule["suggestion"],
                    })
            except Exception:
                pass
    return findings


def print_report(date_str: str, summaries: Dict[str, Dict], findings: List[Dict]):
    """Print the improvement report."""
    print(f"\n{'=' * 65}")
    print(f"  PIPELINE REVIEW — {date_str}")
    print(f"{'=' * 65}")

    # Stage overview
    print(f"\n  Stage Summary:")
    print(f"  {'─' * 55}")
    total_time = 0.0
    for stage, s in summaries.items():
        status = "✅" if s.get("success") else "❌"
        dur = s.get("duration_s", 0)
        total_time += dur
        warns = s.get("warning_count", 0)
        errs = s.get("error_count", 0)
        flag = ""
        if errs > 0:
            flag = f" ⚠ {errs} errors"
        elif warns > 0:
            flag = f" ⚡ {warns} warnings"
        print(f"  {status} {stage:30s} {dur:7.1f}s{flag}")

    print(f"  {'─' * 55}")
    print(f"  Total: {total_time:.1f}s ({total_time/60:.1f} min)")

    # Key metrics across stages
    print(f"\n  Key Metrics:")
    print(f"  {'─' * 55}")
    for stage, s in summaries.items():
        metrics = s.get("metrics", {})
        if metrics:
            print(f"  [{stage}]")
            for k, v in metrics.items():
                print(f"    {k}: {v}")

    # Warnings from stages
    all_warnings = []
    for stage, s in summaries.items():
        for w in s.get("warnings", []):
            all_warnings.append((stage, w))

    if all_warnings:
        print(f"\n  Stage Warnings:")
        print(f"  {'─' * 55}")
        for stage, w in all_warnings:
            msg = w.get("message", "")
            sug = w.get("suggestion", "")
            print(f"  [{stage}] {msg}")
            if sug:
                print(f"    → {sug}")

    # Errors from stages
    all_errors = []
    for stage, s in summaries.items():
        for e in s.get("errors", []):
            all_errors.append((stage, e))

    if all_errors:
        print(f"\n  ❌ Stage Errors:")
        print(f"  {'─' * 55}")
        for stage, e in all_errors:
            msg = e.get("message", "")
            detail = e.get("detail", "")
            print(f"  [{stage}] {msg}")
            if detail:
                print(f"    Detail: {detail[:200]}")

    # Improvement findings
    if findings:
        print(f"\n  💡 Improvement Suggestions:")
        print(f"  {'─' * 55}")

        # Sort by severity
        sev_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
        findings.sort(key=lambda x: sev_order.get(x["severity"], 99))

        for f in findings:
            sev = f["severity"].upper()
            icon = {"CRITICAL": "🔴", "HIGH": "🟠", "MEDIUM": "🟡", "LOW": "🟢"}.get(sev, "⚪")
            print(f"  {icon} [{sev}] {f['stage']}: {f['message']}")
            print(f"    → {f['suggestion']}")
    else:
        print(f"\n  ✅ No improvement suggestions — pipeline looks good!")

    print(f"\n{'=' * 65}")

    # Log file locations
    log_dir = os.path.join(STAGE_LOG_DIR, date_str)
    print(f"\n  📁 Detailed logs: {log_dir}/")
    print(f"     View events:  cat {log_dir}/<stage>.jsonl")
    print(f"     View summary: cat {log_dir}/<stage>_summary.json\n")


def main():
    parser = argparse.ArgumentParser(description="Review pipeline run logs")
    parser.add_argument("--date", type=str, default=None, help="Date to review (YYYYMMDD)")
    parser.add_argument("--last", type=int, default=1, help="Review last N runs")
    args = parser.parse_args()

    if args.date:
        dates = [args.date]
    else:
        # Find available dates
        if not os.path.exists(STAGE_LOG_DIR):
            print("No pipeline logs found yet. Run the pipeline first.")
            return
        dates = sorted(os.listdir(STAGE_LOG_DIR))
        dates = [d for d in dates if os.path.isdir(os.path.join(STAGE_LOG_DIR, d))]
        if not dates:
            print("No pipeline logs found yet.")
            return
        dates = dates[-args.last:]

    for date_str in dates:
        summaries = load_summaries(date_str)
        if not summaries:
            print(f"No stage summaries found for {date_str}")
            continue

        findings = run_improvement_checks(summaries)
        print_report(date_str, summaries, findings)


if __name__ == "__main__":
    main()
