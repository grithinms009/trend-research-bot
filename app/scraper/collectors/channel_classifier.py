"""
Keyword-based channel classifier.

Active channels:
  C1 — AI / Tech / Science News
  C5 — Productivity / Life Hacks
"""

from typing import Dict, List, Optional, Set

CHANNEL_KEYWORDS = {
    "C1": {  # AI News & Tools (Tech + Science)
        "ai", "artificial intelligence", "machine learning", "deep learning",
        "neural network", "gpt", "openai", "chatgpt", "llm", "generative ai",
        "robotics", "robot", "automation", "tech", "technology", "software",
        "programming", "coding", "developer", "startup", "silicon valley",
        "cybersecurity", "hack", "data science", "cloud", "saas", "api",
        "smartphone", "gadget", "chip", "semiconductor", "apple", "google",
        "microsoft", "meta", "tesla", "nvidia", "quantum", "vr", "ar",
        "augmented reality", "virtual reality", "computer", "algorithm",
        "model", "transformer", "diffusion",
        # Science / space (absorbed from former C3)
        "science", "scientific", "research", "discovery", "discovered",
        "space", "nasa", "planet", "mars", "moon", "astronomy",
        "physics", "biology", "chemistry", "genetics", "dna", "genome",
        "climate", "environment", "experiment",
        # Crypto / fintech (tech-adjacent from former C2)
        "crypto", "cryptocurrency", "bitcoin", "ethereum", "blockchain",
        "defi", "nft", "fintech",
    },
    "C5": {  # Life Hacks / Productivity
        "productivity", "productive", "efficiency", "efficient", "organize",
        "habit", "habits", "routine", "morning routine", "discipline",
        "motivation", "motivational", "mindset", "success", "goal", "goals",
        "life hack", "life hacks", "hack", "tips", "self improvement",
        "self help", "personal development", "growth", "mental health",
        "meditation", "mindfulness", "focus", "time management", "career",
        "work life balance", "burnout", "journal", "journaling", "stoic",
        "stoicism", "reading", "books", "learning", "skills", "side hustle",
        "entrepreneur", "freelance", "remote work", "minimalism",
    },
}


def classify_channel(title: str, keywords: Optional[List[str]] = None) -> str:
    """
    Classify a topic into a channel (C1 or C5) based on keyword matching.

    Args:
        title: The topic title.
        keywords: Optional list of extracted keywords.

    Returns:
        Channel tag string: 'C1' or 'C5'.
    """
    # Build a combined text blob to match against
    text_parts = [title.lower()]
    if keywords:
        text_parts.extend(k.lower() for k in keywords)
    combined = " ".join(text_parts)

    scores = {}
    for channel, kw_set in CHANNEL_KEYWORDS.items():
        score = sum(1 for kw in kw_set if kw in combined)
        scores[channel] = score

    best = max(scores, key=lambda k: scores[k])

    # Default to C1 if no keyword matched at all
    if scores[best] == 0:
        return "C1"

    return best
