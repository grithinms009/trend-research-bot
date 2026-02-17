import os
import json
import glob
import logging
import yaml
from datetime import datetime
from app.services.ollama_client import OllamaClient

logger = logging.getLogger(__name__)

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
            
        self.prompts = {
            "C1": "You are a tech news reporter. Write a high-energy, breaking news style short-form video script about: {title}. Focus on the technical impact and why it matters now. Keep it under 60 seconds.",
            "C2": "You are a financial analyst. Write a concise, professional market update script about: {title}. Focus on the numbers, market sentiment, and potential investor impact. Keep it under 60 seconds.",
            "C3": "You are a science communicator. Write an engaging, storytelling-style script about: {title}. Start with a 'Did you know?' hook and explain the mystery or fact clearly. Keep it under 60 seconds.",
            "C4": "You are a luxury lifestyle narrator. Write a cinematic, sophisticated script for a 'Top 5' or exclusive feature about: {title}. Use descriptive, high-end language. Keep it under 60 seconds.",
            "C5": "You are a productivity coach. Write an actionable, punchy script with life hacks or tips about: {title}. Focus on the immediate benefit to the viewer. Keep it under 60 seconds."
        }

    def generate_script(self, request):
        cid = request.get("channel_id")
        topic = request.get("topic", {})
        title = topic.get("title", "Unknown Topic")
        summary = topic.get("summary", "")
        model = request.get("model", "mistral:instruct")
        tone = request.get("tone", "neutral")
        
        print(f"Generating {tone} script for {cid} using {model}...")
        
        base_prompt = self.prompts.get(cid, "Write a short-form video script about: {title}")
        full_prompt = f"{base_prompt.format(title=title)}\n\nContext: {summary}\n\nOutput only the script text."
        
        script_text = OllamaClient.generate_with_retry(
            full_prompt, 
            model=model,
            timeout=120,
            retries=1
        )
        
        if not script_text:
            logger.error(f"Failed to generate script for {title}")
            return None
            
        script = {
            "channel_id": cid,
            "title": title,
            "script_body": script_text,
            "model_used": model,
            "generated_at": datetime.now().isoformat(),
            "source_topic": topic
        }
        return script

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    GENERATED_DIR = os.path.join(BASE_DIR, "data", "topic_generated")
    DATE_STR = datetime.now().strftime("%Y%m%d")
    SCRIPTS_DIR = os.path.join(BASE_DIR, "data", "topic_scripts", DATE_STR)
    os.makedirs(SCRIPTS_DIR, exist_ok=True)
    
    generator = TopicScriptGenerator()
    
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

    print("Generation cycle complete.")
