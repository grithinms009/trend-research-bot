import re
import unicodedata
from typing import Iterable, List


def slugify(value: str, allow_unicode: bool = False) -> str:
    value = str(value)
    if allow_unicode:
        value = unicodedata.normalize("NFKC", value)
    else:
        value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    value = re.sub(r"[\s\-]+", "-", re.sub(r"[^\w\s-]", "", value).strip().lower())
    return value or "topic"


def extract_keywords(text: str, limit: int = 8) -> List[str]:
    words = re.findall(r"[a-zA-Z0-9]+", text.lower())
    seen = []
    for w in words:
        if w not in seen:
            seen.append(w)
        if len(seen) >= limit:
            break
    return seen


def normalize_urls(urls: Iterable[str]) -> List[str]:
    clean = []
    for url in urls:
        if not url:
            continue
        url = url.strip()
        if url and url not in clean:
            clean.append(url)
    return clean
