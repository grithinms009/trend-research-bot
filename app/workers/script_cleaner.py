"""
Script Cleaner — strips labels, markdown, and formatting from LLM output.

Sits between script_generator and scene_planner to ensure only clean
narration text reaches the voice generator. No metadata leaks.
"""

import os
import json
import glob
import re
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# Labels that LLMs commonly inject despite instructions
LABEL_PATTERNS = [
    r"^(Title|Hook|Scene|Paragraph|Section|Part|Intro|Outro|Opening|Closing)\s*[\d]*\s*[:—–\-]\s*",
    r"^(Background|Escalation|Twist|Impact|Cliffhanger|Conclusion|Summary)\s*[:—–\-]\s*",
    r"^\*\*.*?\*\*\s*[:—–\-]?\s*",        # **Bold Label:**
    r"^#{1,6}\s+",                          # Markdown headers
    r"^\d+[\.\)]\s+",                       # Numbered lists
    r"^[-•●▪]\s+",                          # Bullet points
]

# Characters/patterns to strip
STRIP_CHARS = {
    "**": "",
    "__": "",
    "~~": "",
    "``": "",
    "`": "",
    "###": "",
    "##": "",
    "#": "",
}


def clean_script(raw_script: str) -> str:
    """Clean a raw LLM script output to pure narration text."""
    if not raw_script:
        return ""

    lines = raw_script.strip().split("\n")
    cleaned_lines = []

    for line in lines:
        line = line.strip()
        if not line:
            cleaned_lines.append("")
            continue

        # Strip markdown formatting
        for pattern, replacement in STRIP_CHARS.items():
            line = line.replace(pattern, replacement)

        # Strip labels at start of line
        for pattern in LABEL_PATTERNS:
            line = re.sub(pattern, "", line, flags=re.IGNORECASE).strip()

        # Strip emojis (Unicode emoji ranges)
        line = re.sub(
            r"[\U0001f600-\U0001f64f\U0001f300-\U0001f5ff\U0001f680-\U0001f6ff"
            r"\U0001f900-\U0001f9ff\U0001fa00-\U0001fa6f\U0001fa70-\U0001faff"
            r"\u2600-\u26ff\u2700-\u27bf]",
            "", line,
        )

        # Strip "Note:", "Output:", "Script:" prefix
        line = re.sub(r"^(Note|Output|Script|Narration|Voice|Audio)\s*:\s*", "", line, flags=re.IGNORECASE)

        # Strip quotation marks wrapping entire line
        if (line.startswith('"') and line.endswith('"')) or (line.startswith("'") and line.endswith("'")):
            line = line[1:-1]

        line = line.strip()
        if line:
            cleaned_lines.append(line)

    # Collapse multiple blank lines into single blank line
    result = "\n".join(cleaned_lines)
    result = re.sub(r"\n{3,}", "\n\n", result)

    return result.strip()


def main():
    """Process all script files and output cleaned versions."""
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    date_str = datetime.now().strftime("%Y%m%d")
    scripts_dir = os.path.join(base_dir, "data", "topic_scripts", date_str)
    clean_dir = os.path.join(base_dir, "data", "topic_scripts_clean", date_str)
    os.makedirs(clean_dir, exist_ok=True)

    script_files = glob.glob(os.path.join(scripts_dir, "*_scripts.json"))
    if not script_files:
        print("No script files found for cleaning.")
        return

    total_cleaned = 0
    total_labels_removed = 0

    for script_file in script_files:
        channel_id = os.path.basename(script_file).replace("_scripts.json", "")

        with open(script_file) as f:
            scripts = json.load(f)

        if not isinstance(scripts, list):
            continue

        cleaned_scripts = []
        for script in scripts:
            raw = (script.get("script_body") or "").strip()
            cleaned = clean_script(raw)

            raw_lines = len(raw.split("\n"))
            clean_lines = len(cleaned.split("\n"))
            labels_found = raw_lines - clean_lines

            script_copy = dict(script)
            script_copy["script_body"] = cleaned
            script_copy["raw_script_body"] = raw  # Preserve original
            script_copy["cleaning_applied"] = True
            cleaned_scripts.append(script_copy)

            total_cleaned += 1
            total_labels_removed += max(0, labels_found)

        outfile = os.path.join(clean_dir, f"{channel_id}_scripts.json")
        with open(outfile, "w") as f:
            json.dump(cleaned_scripts, f, indent=2)

        print(f"  Cleaned {len(cleaned_scripts)} scripts for {channel_id.upper()}")

    print(f"\n--- Script Cleaner Metrics ---")
    print(f"scripts_cleaned: {total_cleaned}")
    print(f"labels_removed: {total_labels_removed}")
    print(f"-----------------------------\n")
    print(f"Cleaned scripts saved to: {clean_dir}")


if __name__ == "__main__":
    main()
