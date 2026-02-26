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

    def _build_prompt(self, title: str, article_text: str) -> str:
        """Build the structured YouTube script generation prompt."""
        return (
            f"Write a YouTube news script about: {title}\n\n"
            "STRICT STRUCTURE:\n"
            "Paragraph 1 – Hook (25–35 words). Open with urgency. Grab attention immediately.\n"
            "Paragraph 2 – Background (25–35 words). Give context. Short sentences.\n"
            "Paragraph 3 – Key Development (25–35 words). State the main news. Be specific.\n"
            "Paragraph 4 – Why It Matters (25–35 words). Explain the impact. Make it personal.\n"
            "Paragraph 5 – Closing Insight (20–30 words). End strong. Encourage retention.\n\n"
            "RULES:\n"
            "- Total length MUST be between 130 and 170 words.\n"
            "- If below 130 words, expand with more detail.\n"
            "- Do not produce fewer than 5 paragraphs.\n"
            "- Separate each paragraph with a blank line.\n"
            "- No markdown. Plain text only.\n"
            "- Use ONLY facts from the article below.\n"
            "- No speculation. No opinions. No passive voice.\n"
            "- Short punchy sentences. This is YouTube, not a blog.\n\n"
            "ARTICLE:\n"
            f"{article_text[:1500]}\n\n"
            "Output ONLY the script paragraphs. Nothing else."
        )

    def _build_retry_prompt(self, title: str, article_text: str, previous_output: str, word_count: int) -> str:
        """Build a retry prompt when first attempt was too short."""
        return (
            f"Your previous script was only {word_count} words. It MUST be 130-170 words.\n\n"
            f"Rewrite this YouTube script about: {title}\n\n"
            "EXPAND each paragraph with more factual detail from the article.\n"
            "Keep 5 paragraphs. Separate with blank lines. Plain text only.\n"
            "Target: 150 words total.\n\n"
            "ARTICLE:\n"
            f"{article_text[:1500]}\n\n"
            "Previous attempt (too short):\n"
            f"{previous_output}\n\n"
            "Output ONLY the expanded script. Nothing else."
        )

    def generate_script(self, request):
        """Generate a structured YouTube script for a dispatched topic."""
        start_time = time.time()

        cid = request.get("channel_id")
        topic = request.get("topic", {})
        title = topic.get("title", "Unknown Topic")
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
        prompt = self._build_prompt(title, article_text)
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
