"""
Keyword-based channel classifier.

Channels:
  C1 — AI / Tech News
  C2 — Finance / Markets / Crypto
  C3 — History / Science / Facts
  C4 — Luxury / Travel
  C5 — Productivity / Life Hacks
"""

from typing import Dict, List, Optional, Set

CHANNEL_KEYWORDS = {
    "C1": {  # AI News & Tools (Tech)
        "ai", "artificial intelligence", "machine learning", "deep learning",
        "neural network", "gpt", "openai", "chatgpt", "llm", "generative ai",
        "robotics", "robot", "automation", "tech", "technology", "software",
        "programming", "coding", "developer", "startup", "silicon valley",
        "cybersecurity", "hack", "data science", "cloud", "saas", "api",
        "smartphone", "gadget", "chip", "semiconductor", "apple", "google",
        "microsoft", "meta", "tesla", "nvidia", "quantum", "vr", "ar",
        "augmented reality", "virtual reality", "computer", "algorithm",
        "model", "transformer", "diffusion",
    },
    "C2": {  # Daily Market Bites (Finance)
        "finance", "financial", "money", "investment", "investing", "investor",
        "stock", "stocks", "market", "markets", "wall street", "nasdaq",
        "s&p", "dow jones", "trading", "trader", "crypto", "cryptocurrency",
        "bitcoin", "ethereum", "btc", "eth", "defi", "nft", "token",
        "bank", "banking", "federal reserve", "fed", "interest rate",
        "inflation", "recession", "gdp", "economy", "economic", "forex",
        "commodity", "gold", "oil", "real estate", "mortgage", "ipo",
        "earnings", "revenue", "profit", "dividend", "hedge fund",
    },
    "C3": {  # Did You Know (History/Science)
        "history", "historical", "ancient", "civilization", "archaeology",
        "science", "scientific", "research", "study", "discovery", "discovered",
        "space", "nasa", "planet", "mars", "moon", "astronomy", "universe",
        "physics", "quantum physics", "biology", "chemistry", "evolution",
        "fossil", "dinosaur", "ocean", "climate", "environment", "nature",
        "geology", "earthquake", "volcano", "genetics", "dna", "genome",
        "experiment", "laboratory", "theory", "einstein", "newton",
        "mathematics", "math", "fact", "facts", "trivia", "explained",
    },
    "C4": {  # Luxury / Travel Top 5
        "luxury", "luxurious", "expensive", "premium", "elite", "exclusive",
        "travel", "traveling", "destination", "vacation", "holiday", "resort",
        "hotel", "villa", "yacht", "cruise", "island", "beach", "paradise",
        "fashion", "designer", "brand", "gucci", "louis vuitton", "rolex",
        "lamborghini", "ferrari", "bugatti", "supercar", "mansion", "penthouse",
        "wealth", "wealthy", "billionaire", "millionaire", "lifestyle",
        "gourmet", "michelin", "wine", "champagne", "first class", "private jet",
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
    },
}


def classify_channel(title: str, keywords: Optional[List[str]] = None) -> str:
    """
    Classify a topic into a channel (C1–C5) based on keyword matching.

    Args:
        title: The topic title.
        keywords: Optional list of extracted keywords.

    Returns:
        Channel tag string, e.g. 'C1'.
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
