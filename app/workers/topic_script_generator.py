import os
import json
import glob
import logging
import re
import time
import yaml
from datetime import datetime
from app.services.ollama_client import OllamaClient

logger = logging.getLogger(__name__)

MIN_ARTICLE_CHARS = 300
MIN_SCRIPT_WORDS = 100
MAX_SCRIPT_WORDS = 200
TARGET_MIN_WORDS = 120
TARGET_MAX_WORDS = 160

# Speculation phrases that indicate hallucination
SPECULATION_PHRASES = [
    "it is believed",
    "sources suggest",
    "it is rumored",
    "allegedly",
    "unconfirmed reports",
    "some experts believe",
    "it remains to be seen",
    "only time will tell",
    "one can only imagine",
]


class TopicScriptGenerator:
    def __init__(self):
        # Load channel configuration for prompt templates and models
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
            "topics_rejected_word_count": 0,
            "generation_times": [],
        }
            
        self.prompts = {
            "C1": (
                "You are a tech news reporter. Write a factual, high-energy breaking news style "
                "short-form video script about: {title}. Focus on the verified technical impact. "
                "Script must be exactly 120-160 words."
            ),
            "C2": (
                "You are a financial analyst. Write a concise, data-driven market update script "
                "about: {title}. Focus on verified numbers and market data. "
                "Script must be exactly 120-160 words."
            ),
            "C3": (
                "You are a science communicator. Write an engaging factual script about: {title}. "
                "Start with a 'Did you know?' hook and explain verifiable facts clearly. "
                "Script must be exactly 120-160 words."
            ),
            "C4": (
                "You are a luxury lifestyle narrator. Write a cinematic, sophisticated script "
                "about: {title}. Use descriptive language based only on facts from the article. "
                "Script must be exactly 120-160 words."
            ),
            "C5": (
                "You are a productivity coach. Write an actionable, punchy script with tips "
                "about: {title}. Focus on verified, actionable advice from the article. "
                "Script must be exactly 120-160 words."
            ),
        }

    def generate_script(self, request):
        """Generate a script for a dispatched topic request."""
        start_time = time.time()
        
        cid = request.get("channel_id")
        topic = request.get("topic", {})
        title = topic.get("title", "Unknown Topic")
        model = request.get("model", "mistral:latest")
        tone = request.get("tone", "neutral")
        article_text = (topic.get("article_text") or "").strip()
        summary = (topic.get("summary") or "").strip()
        url = topic.get("url", "")

        # ========== STRICT GENERATION GATE ==========
        if not article_text or len(article_text) < MIN_ARTICLE_CHARS:
            self.metrics["topics_skipped_no_content"] += 1
            logger.warning(
                "GATE BLOCKED: Topic '%s' — article_text is %d chars (min %d)",
                title[:60], len(article_text), MIN_ARTICLE_CHARS,
            )
            return None

        print(f"Generating {tone} script for {cid} using {model}...")

        base_prompt = self.prompts.get(cid, "Write a short-form video script about: {title}")
        
        instruction_block = (
            "STRICT RULES:\n"
            "1. Use ONLY information from the article text below. Do NOT invent or assume any facts.\n"
            "2. No speculation, no opinions, no phrases like 'may', 'could', 'might' unless quoting the article.\n"
            "3. Only include verifiable facts directly stated in the article.\n"
            "4. The script must SUMMARIZE the article faithfully.\n"
            "5. Script length: exactly 120-160 words. No more, no less.\n"
            "6. If the article text does not contain sufficient factual detail, "
            "return exactly: SKIP_INSUFFICIENT_DATA\n\n"
            f"TITLE: {title}\n"
            f"ARTICLE SUMMARY: {summary or title}\n"
            f"SOURCE URL: {url or 'unknown'}\n"
            "ARTICLE TEXT:\n"
            f"{article_text[:3000]}\n"  # Cap article text to avoid prompt overflow
        )

        full_prompt = (
            f"{base_prompt.format(title=title)}\n\n"
            f"{instruction_block}\n"
            "Output ONLY the script text, nothing else."
        )

        script_text = OllamaClient.generate_with_retry(
            full_prompt, 
            model=model,
            timeout=120,
            retries=1
        )

        gen_time = round(time.time() - start_time, 2)
        self.metrics["generation_times"].append(gen_time)

        if not script_text:
            logger.error("Failed to generate script for '%s' after retries", title[:60])
            return None
        
        cleaned_output = script_text.strip()
        
        # Check for SKIP_INSUFFICIENT_DATA
        if "SKIP_INSUFFICIENT_DATA" in cleaned_output.upper():
            self.metrics["topics_skipped_insufficient"] += 1
            logger.warning("Model returned SKIP_INSUFFICIENT_DATA for '%s'", title[:60])
            return None

        # ========== POST-GENERATION VALIDATION ==========
        
        # 1. Word count check
        word_count = len(cleaned_output.split())
        if word_count < MIN_SCRIPT_WORDS or word_count > MAX_SCRIPT_WORDS:
            logger.warning(
                "Script for '%s' has %d words (target %d-%d) — allowing with warning",
                title[:60], word_count, TARGET_MIN_WORDS, TARGET_MAX_WORDS,
            )
            # Still allow but log; only hard-reject extreme cases
            if word_count < 50 or word_count > 300:
                self.metrics["topics_rejected_word_count"] += 1
                logger.error("REJECTED: Script for '%s' has extreme word count: %d", title[:60], word_count)
                return None

        # 2. Hallucination check — reject speculation not from article
        lower_output = cleaned_output.lower()
        lower_article = article_text.lower()
        
        for phrase in SPECULATION_PHRASES:
            if phrase in lower_output and phrase not in lower_article:
                self.metrics["topics_rejected_hallucination"] += 1
                logger.error(
                    "REJECTED: Script for '%s' contains hallucination phrase '%s' not in article",
                    title[:60], phrase,
                )
                return None

        # 3. Check for hedge words not present in original article
        hedge_words = ["may ", "could ", "might ", "perhaps ", "possibly "]
        hedge_count = 0
        for hw in hedge_words:
            output_count = lower_output.count(hw)
            article_count = lower_article.count(hw)
            if output_count > article_count:
                hedge_count += (output_count - article_count)
        
        if hedge_count >= 3:
            self.metrics["topics_rejected_hallucination"] += 1
            logger.error(
                "REJECTED: Script for '%s' has %d excess hedge words — likely hallucination",
                title[:60], hedge_count,
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
