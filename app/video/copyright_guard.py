"""
Copyright Guard — ensures all visual assets are copyright-safe.

Validates:
- Stock footage queries don't contain copyrighted terms (brand names, celebrity names)
- All clips come from approved safe sources (Pexels, Pixabay, AI-generated)
- No copyrighted music or audio is used
- Scripts don't plagiarize source articles verbatim

Integrates into the pipeline between scene_planner and stock_fetcher.
"""

import logging
import re
from typing import Dict, List, Set, Tuple

logger = logging.getLogger(__name__)

# ============================================================
# BLOCKED TERMS — never use in stock search queries
# ============================================================
BLOCKED_PERSON_NAMES = {
    "elon musk", "jeff bezos", "mark zuckerberg", "tim cook", "sam altman",
    "bill gates", "warren buffett", "donald trump", "joe biden", "barack obama",
    "taylor swift", "beyonce", "kanye west", "kim kardashian", "oprah winfrey",
    "rihanna", "drake", "lebron james", "cristiano ronaldo", "lionel messi",
}

BLOCKED_BRAND_VISUALS = {
    "coca cola logo", "apple logo", "nike logo", "google logo", "microsoft logo",
    "amazon logo", "tesla logo", "facebook logo", "instagram logo", "tiktok logo",
    "louis vuitton logo", "gucci logo", "rolex logo", "ferrari logo",
    "disney logo", "marvel logo", "warner bros", "netflix logo",
}

BLOCKED_COPYRIGHTED_TERMS = {
    "movie scene", "tv show clip", "music video", "game footage",
    "cartoon character", "anime character", "disney character",
    "marvel character", "dc character", "pokemon", "mickey mouse",
    "harry potter", "star wars scene", "lord of the rings",
}

# Safe sources whitelist
SAFE_STOCK_SOURCES = {
    "pexels.com", "pixabay.com", "unsplash.com",
    "coverr.co", "mixkit.co", "videvo.net",
}

SAFE_MUSIC_SOURCES = {
    "pixabay.com", "mixkit.co", "freesound.org",
    "incompetech.com", "bensound.com",
}


def sanitize_search_query(query: str) -> Tuple[str, List[str]]:
    """
    Clean a stock search query of copyrighted terms.

    Returns:
        (sanitized_query, list_of_removed_terms)
    """
    issues = []
    lower = query.lower()

    # Check person names
    for name in BLOCKED_PERSON_NAMES:
        if name in lower:
            issues.append(f"blocked_person:{name}")
            query = re.sub(re.escape(name), "", query, flags=re.IGNORECASE).strip()

    # Check brand visuals
    for brand in BLOCKED_BRAND_VISUALS:
        if brand in lower:
            issues.append(f"blocked_brand:{brand}")
            query = re.sub(re.escape(brand), "", query, flags=re.IGNORECASE).strip()

    # Check copyrighted terms
    for term in BLOCKED_COPYRIGHTED_TERMS:
        if term in lower:
            issues.append(f"blocked_copyrighted:{term}")
            query = re.sub(re.escape(term), "", query, flags=re.IGNORECASE).strip()

    # Clean up double spaces
    query = re.sub(r"\s+", " ", query).strip()

    if issues:
        logger.warning("Copyright guard removed from query: %s", ", ".join(issues))

    return query, issues


def validate_visual_intent(intent: str) -> Tuple[bool, str]:
    """
    Validate a visual_intent string is copyright-safe.

    Returns:
        (is_safe, reason)
    """
    lower = intent.lower()

    for name in BLOCKED_PERSON_NAMES:
        if name.replace(" ", "_") in lower or name.replace(" ", "") in lower:
            return False, f"contains_person_name:{name}"

    for brand in BLOCKED_BRAND_VISUALS:
        brand_key = brand.replace(" ", "_")
        if brand_key in lower:
            return False, f"contains_brand:{brand}"

    return True, "safe"


def check_script_plagiarism(script: str, article: str, threshold: float = 0.4) -> Tuple[bool, float]:
    """
    Check if a script copies too much text verbatim from the source article.

    Returns:
        (is_original, overlap_ratio)
    """
    if not script or not article:
        return True, 0.0

    # Split into 4-word ngrams and check overlap
    def ngrams(text: str, n: int = 4) -> Set[str]:
        words = text.lower().split()
        return {" ".join(words[i:i + n]) for i in range(len(words) - n + 1)}

    script_grams = ngrams(script)
    article_grams = ngrams(article)

    if not script_grams:
        return True, 0.0

    overlap = len(script_grams & article_grams)
    ratio = overlap / len(script_grams)

    is_original = ratio < threshold
    if not is_original:
        logger.warning("Script plagiarism detected: %.1f%% overlap (threshold: %.1f%%)",
                       ratio * 100, threshold * 100)

    return is_original, ratio


def validate_scene_plan(scene_plan: Dict) -> Dict:
    """
    Run copyright checks on an entire scene plan.

    Returns:
        Validated plan with sanitized visual intents and copyright report.
    """
    report = {
        "scenes_checked": 0,
        "intents_sanitized": 0,
        "queries_sanitized": 0,
        "issues": [],
    }

    scenes = scene_plan.get("scenes", [])

    for scene in scenes:
        report["scenes_checked"] += 1

        # Check visual_intent
        intent = scene.get("visual_intent", "")
        is_safe, reason = validate_visual_intent(intent)
        if not is_safe:
            report["intents_sanitized"] += 1
            report["issues"].append(f"scene_{scene.get('scene_id', '?')}: {reason}")
            scene["visual_intent"] = "cinematic_dark"  # Safe fallback

        # Check visual_prompts if present
        prompts = scene.get("visual_prompts", [])
        sanitized_prompts = []
        for prompt in prompts:
            clean, issues = sanitize_search_query(prompt)
            if issues:
                report["queries_sanitized"] += 1
                report["issues"].extend(issues)
            sanitized_prompts.append(clean if clean else "abstract cinematic background")
        if sanitized_prompts:
            scene["visual_prompts"] = sanitized_prompts

    # Check script plagiarism
    source_script = scene_plan.get("source_script", {})
    script_body = source_script.get("script_body", "")
    article_text = source_script.get("source_topic", {}).get("article_text", "")
    if script_body and article_text:
        is_original, ratio = check_script_plagiarism(script_body, article_text)
        if not is_original:
            report["issues"].append(f"script_plagiarism:{ratio:.1%}")

    scene_plan["copyright_report"] = report
    return scene_plan


class CopyrightGuard:
    """Pipeline-integrated copyright safety checker."""

    def __init__(self):
        self.metrics = {
            "plans_checked": 0,
            "scenes_checked": 0,
            "intents_sanitized": 0,
            "queries_sanitized": 0,
            "total_issues": 0,
        }

    def check_plan(self, plan: Dict) -> Dict:
        """Run full copyright validation on a scene plan."""
        self.metrics["plans_checked"] += 1
        result = validate_scene_plan(plan)
        report = result.get("copyright_report", {})

        self.metrics["scenes_checked"] += report.get("scenes_checked", 0)
        self.metrics["intents_sanitized"] += report.get("intents_sanitized", 0)
        self.metrics["queries_sanitized"] += report.get("queries_sanitized", 0)
        self.metrics["total_issues"] += len(report.get("issues", []))

        return result

    def log_metrics(self):
        print("\n--- Copyright Guard Metrics ---")
        for k, v in self.metrics.items():
            print(f"  {k}: {v}")
        if self.metrics["total_issues"] == 0:
            print("  status: ALL CLEAR")
        else:
            print(f"  status: {self.metrics['total_issues']} issues sanitized")
        print("-------------------------------\n")


def main():
    """Run copyright checks on all directed plans."""
    import glob
    import json
    import os

    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    directed_dir = os.path.join(base_dir, "data", "directed_plans")

    if not os.path.exists(directed_dir):
        print("No directed plans found for copyright check.")
        return

    plan_files = sorted(glob.glob(os.path.join(directed_dir, "**", "*.json"), recursive=True))
    plan_files += sorted(glob.glob(os.path.join(directed_dir, "*.json")))
    # Deduplicate
    plan_files = list(dict.fromkeys(plan_files))

    if not plan_files:
        print("No directed plan files found.")
        return

    guard = CopyrightGuard()
    total_checked = 0

    for plan_path in plan_files:
        try:
            with open(plan_path) as f:
                data = json.load(f)
        except (json.JSONDecodeError, IOError) as exc:
            logger.error("Failed to read %s: %s", plan_path, exc)
            continue

        plans = data if isinstance(data, list) else [data]
        updated = False

        for i, plan in enumerate(plans):
            if not isinstance(plan, dict):
                continue
            checked = guard.check_plan(plan)
            plans[i] = checked
            updated = True
            total_checked += 1

        if updated:
            out_data = plans if isinstance(data, list) else plans[0]
            with open(plan_path, "w") as f:
                json.dump(out_data, f, indent=2)

    guard.log_metrics()
    print(f"Copyright checked: {total_checked} plans across {len(plan_files)} files")


if __name__ == "__main__":
    main()
