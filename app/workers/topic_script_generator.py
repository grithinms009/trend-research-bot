"""
Topic Script Generator — production YouTube script writer.

Generates structured 5-paragraph scripts (130-170 words) optimized for
YouTube retention. Single generation per topic, with one retry if too short.
"""

import os
import json
import glob
import logging
import time
import yaml
from datetime import datetime
from app.services.ollama_client import OllamaClient

logger = logging.getLogger(__name__)

MIN_ARTICLE_CHARS = 300
TARGET_MIN_WORDS = 130
TARGET_MAX_WORDS = 170

# Hard hallucination phrases — instant reject
HALLUCINATION_PHRASES = [
    "it is believed",
    "sources suggest",
    "it is rumored",
    "unconfirmed reports",
    "one can only imagine",
    "only time will tell",
]


class TopicScriptGenerator:
    def __init__(self):
        # Load channel configuration
        base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        config_path = os.path.join(base_path, "app", "config", "channels.yaml")
        with open(config_path, "r") as f:
            config_data = yaml.safe_load(f) or {}

        if not isinstance(config_data, dict):
            raise ValueError("channels.yaml must contain a mapping at the root level")

        channels = config_data.get("channels")
        if not isinstance(channels, dict) or not channels:
            raise ValueError("Channel configuration missing 'channels' section in channels.yaml")

        self.channel_config = channels
        self.metrics = {
            "topics_generated": 0,
            "topics_skipped_no_content": 0,
            "topics_skipped_insufficient": 0,
            "topics_rejected_hallucination": 0,
            "topics_rejected_short": 0,
            "topics_retried": 0,
            "generation_times": [],
        }

    def _build_prompt(self, title: str, article_text: str,
                       hook: str = "", angle: str = "") -> str:
        """Build tension-driven YouTube Shorts script prompt. No labels, pure narration."""
        hook_guidance = ""
        if hook:
            hook_guidance = f"\nSUGGESTED HOOK (use this energy, but rewrite in your own words): {hook}\n"
        angle_guidance = ""
        if angle:
            angle_guidance = f"UNIQUE ANGLE: {angle}\n"

        return (
            f"Write a 45-second YouTube Shorts narration about: {title}\n\n"
            f"{hook_guidance}"
            f"{angle_guidance}"
            "STRUCTURE (follow exactly, but do NOT label sections):\n"
            "1. HOOK — First 2 sentences. Create instant curiosity or shock. Make them stop scrolling.\n"
            "2. ESCALATION — Build tension. Layer facts that raise stakes.\n"
            "3. TWIST — Reveal the surprising angle. Subvert expectation.\n"
            "4. IMPACT — Explain why this changes everything. Make it personal.\n"
            "5. OPEN LOOP — End with an unresolved question or cliffhanger.\n\n"
            "ABSOLUTE RULES:\n"
            "- Output ONLY the narration. Nothing else.\n"
            "- NO section labels. No 'Hook:', 'Title:', 'Paragraph:', 'Scene:' etc.\n"
            "- NO markdown. No bullets. No numbered lists. No emojis. No asterisks.\n"
            "- NO generic intros like 'In recent developments', 'Recently', 'In today\'s news'.\n"
            "- Total: 130-170 words. No less. No more.\n"
            "- Separate each section with ONE blank line.\n"
            "- Short punchy sentences. Dramatic pauses. Active voice only.\n"
            "- Write like a human storyteller keeping someone from scrolling.\n"
            "- Use ONLY facts from the article below. No speculation.\n\n"
            "ARTICLE:\n"
            f"{article_text[:1500]}\n\n"
            "Begin the narration now. First word should grab attention."
        )

    def _build_retry_prompt(self, title: str, article_text: str, previous_output: str, word_count: int) -> str:
        """Retry prompt — expand while keeping tension structure."""
        return (
            f"Your narration was only {word_count} words. It MUST be 130-170 words.\n\n"
            f"Rewrite the YouTube Shorts narration about: {title}\n\n"
            "Keep the same tension structure: hook, escalation, twist, impact, open loop.\n"
            "Add more factual detail from the article. Expand each section.\n"
            "NO labels. NO markdown. NO bullets. Pure narration only.\n"
            "Separate sections with blank lines. Target: 150 words.\n\n"
            "ARTICLE:\n"
            f"{article_text[:1500]}\n\n"
            "Previous attempt (too short):\n"
            f"{previous_output}\n\n"
            "Rewrite the narration now. First word grabs attention."
        )

    def generate_script(self, request):
        """Generate a structured YouTube script for a dispatched topic."""
        start_time = time.time()

        cid = request.get("channel_id")
        topic = request.get("topic", {})
        # Prefer youtube_title from intelligence engine, fall back to raw title
        title = request.get("youtube_title") or topic.get("youtube_title") or topic.get("title", "Unknown Topic")
        hook = request.get("hook") or topic.get("hook", "")
        angle = request.get("angle") or topic.get("angle", "")
        model = request.get("model", "mistral:latest")
        tone = request.get("tone", "neutral")
        article_text = (topic.get("article_text") or "").strip()
        summary = (topic.get("summary") or "").strip()
        url = topic.get("url", "")

        # ========== CONTENT GATE ==========
        if not article_text or len(article_text) < MIN_ARTICLE_CHARS:
            self.metrics["topics_skipped_no_content"] += 1
            logger.warning(
                "GATE BLOCKED: Topic '%s' — article_text is %d chars (min %d)",
                title[:60], len(article_text), MIN_ARTICLE_CHARS,
            )
            return None

        print(f"Generating {tone} script for {cid} using {model}...")

        # ========== FIRST ATTEMPT ==========
        prompt = self._build_prompt(title, article_text, hook=hook, angle=angle)
        script_text = OllamaClient.generate_with_retry(
            prompt,
            model=model,
            timeout=120,
            retries=1,
        )

        gen_time = round(time.time() - start_time, 2)
        self.metrics["generation_times"].append(gen_time)

        if not script_text:
            logger.error("Failed to generate script for '%s' after retries", title[:60])
            return None

        cleaned_output = script_text.strip()

        # Check for SKIP signal
        if "SKIP_INSUFFICIENT_DATA" in cleaned_output.upper():
            self.metrics["topics_skipped_insufficient"] += 1
            logger.warning("Model returned SKIP_INSUFFICIENT_DATA for '%s'", title[:60])
            return None

        # ========== WORD COUNT CHECK + RETRY ==========
        word_count = len(cleaned_output.split())

        if word_count < TARGET_MIN_WORDS:
            # Retry once with expansion prompt
            self.metrics["topics_retried"] += 1
            logger.info("Script for '%s' too short (%d words), retrying...", title[:60], word_count)

            retry_prompt = self._build_retry_prompt(title, article_text, cleaned_output, word_count)
            retry_start = time.time()
            retry_text = OllamaClient.generate(retry_prompt, model=model, timeout=120)
            retry_time = round(time.time() - retry_start, 2)
            self.metrics["generation_times"].append(retry_time)

            if retry_text:
                retry_cleaned = retry_text.strip()
                retry_wc = len(retry_cleaned.split())
                if retry_wc >= TARGET_MIN_WORDS:
                    cleaned_output = retry_cleaned
                    word_count = retry_wc
                    gen_time += retry_time
                    logger.info("Retry succeeded: %d words", retry_wc)
                else:
                    self.metrics["topics_rejected_short"] += 1
                    logger.error(
                        "REJECTED: Script for '%s' still too short after retry — %d words",
                        title[:60], retry_wc,
                    )
                    return None
            else:
                self.metrics["topics_rejected_short"] += 1
                logger.error("Retry failed for '%s'", title[:60])
                return None

        if word_count > TARGET_MAX_WORDS:
            # Trim to TARGET_MAX_WORDS at sentence boundary
            words = cleaned_output.split()
            trimmed = " ".join(words[:TARGET_MAX_WORDS])
            # Cut at last sentence-ending punctuation to keep clean
            for punct in [".", "!", "?"]:
                last_idx = trimmed.rfind(punct)
                if last_idx > len(trimmed) // 2:  # Only if not cutting too much
                    trimmed = trimmed[: last_idx + 1]
                    break
            cleaned_output = trimmed
            word_count = len(cleaned_output.split())
            logger.info(
                "Script for '%s' trimmed to %d words (was over %d)",
                title[:60], word_count, TARGET_MAX_WORDS,
            )

        # ========== HALLUCINATION CHECK (lightweight) ==========
        lower_output = cleaned_output.lower()
        for phrase in HALLUCINATION_PHRASES:
            if phrase in lower_output:
                self.metrics["topics_rejected_hallucination"] += 1
                logger.error(
                    "REJECTED: Script for '%s' contains hallucination phrase '%s'",
                    title[:60], phrase,
                )
                return None

        # ========== SCRIPT ACCEPTED ==========
        self.metrics["topics_generated"] += 1
        logger.info("Generated script for '%s' (%d words) in %ss", title[:60], word_count, gen_time)

        script = {
            "channel_id": cid,
            "title": title,
            "script_body": cleaned_output,
            "word_count": word_count,
            "model_used": model,
            "generation_time_seconds": gen_time,
            "generated_at": datetime.now().isoformat(),
            "source_url": url,
            "hook": hook,
            "angle": angle,
            "engagement_score": request.get("engagement_score", 0),
            "source_topic": topic,
        }
        return script

    def log_metrics(self):
        print("\n--- Script Generator Metrics ---")
        for key, value in self.metrics.items():
            if key == "generation_times":
                if value:
                    avg_time = sum(value) / len(value)
                    print(f"  avg_generation_time: {avg_time:.2f}s")
                    print(f"  total_generations: {len(value)}")
                    print(f"  min_time: {min(value):.2f}s")
                    print(f"  max_time: {max(value):.2f}s")
            else:
                print(f"{key}: {value}")
        print("--------------------------------\n")


if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    GENERATED_DIR = os.path.join(BASE_DIR, "data", "topic_generated")
    DATE_STR = datetime.now().strftime("%Y%m%d")
    SCRIPTS_DIR = os.path.join(BASE_DIR, "data", "topic_scripts", DATE_STR)
    os.makedirs(SCRIPTS_DIR, exist_ok=True)

    generator = TopicScriptGenerator()
    total_scripts = 0

    # Process each channel folder
    for cid in ["C1", "C2", "C3", "C4", "C5"]:
        channel_req_dir = os.path.join(GENERATED_DIR, cid)
        if not os.path.exists(channel_req_dir):
            continue

        files = glob.glob(f"{channel_req_dir}/req_*.json")
        if not files:
            continue

        print(f"Processing {len(files)} requests for channel {cid}...")
        channel_scripts = []

        for fpath in files:
            with open(fpath) as f:
                req = json.load(f)

            script = generator.generate_script(req)
            if script:
                channel_scripts.append(script)
                # Remove request file after processing
                os.remove(fpath)

        if channel_scripts:
            outfile = os.path.join(SCRIPTS_DIR, f"{cid}_scripts.json")
            with open(outfile, "w") as f:
                json.dump(channel_scripts, f, indent=2)
            print(f"Saved {len(channel_scripts)} scripts to {outfile}")
            total_scripts += len(channel_scripts)

    generator.log_metrics()

    if total_scripts == 0:
        print("WARNING: 0 scripts generated! Check article content and Ollama availability.")
    else:
        print(f"\n✅ Total scripts generated: {total_scripts}")

    print("Generation cycle complete.")
