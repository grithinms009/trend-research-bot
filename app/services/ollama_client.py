import hashlib
import subprocess
import logging
import time
from typing import Dict, Optional, Tuple

logger = logging.getLogger(__name__)


class _PromptCache:
    """In-memory LRU cache for LLM responses. Avoids redundant calls within a run."""

    def __init__(self, max_size: int = 200, ttl_seconds: int = 3600):
        self._cache: Dict[str, Tuple[str, float]] = {}  # hash -> (response, timestamp)
        self._max_size = max_size
        self._ttl = ttl_seconds
        self.hits = 0
        self.misses = 0

    @staticmethod
    def _key(prompt: str, model: str) -> str:
        return hashlib.md5(f"{model}::{prompt}".encode()).hexdigest()

    def get(self, prompt: str, model: str) -> Optional[str]:
        key = self._key(prompt, model)
        entry = self._cache.get(key)
        if entry:
            response, ts = entry
            if time.time() - ts < self._ttl:
                self.hits += 1
                return response
            else:
                del self._cache[key]
        self.misses += 1
        return None

    def put(self, prompt: str, model: str, response: str):
        key = self._key(prompt, model)
        # Evict oldest if at capacity
        if len(self._cache) >= self._max_size:
            oldest_key = min(self._cache, key=lambda k: self._cache[k][1])
            del self._cache[oldest_key]
        self._cache[key] = (response, time.time())


class OllamaClient:
    """
    Local LLM client using Ollama CLI with prompt caching.
    """

    ALLOWED_MODELS = {
        "mistral:latest",
        "deepseek-r1:7b",
    }

    _cache = _PromptCache()

    @classmethod
    def generate(cls, prompt: str, model: str = "mistral:latest", timeout: int = 300,
                 use_cache: bool = True) -> Optional[str]:
        """
        Generate text using Ollama 'run' command.
        
        Args:
            prompt: The input prompt for the LLM.
            model: The model name (e.g., 'mistral:instruct', 'llama3').
            timeout: Command timeout in seconds.
            use_cache: If True, check/store in prompt cache.
            
        Returns:
            Cleaned response text or None if failed.
        """
        start_time = time.time()

        if model not in cls.ALLOWED_MODELS:
            logger.error(f"Model '{model}' is not allowed.")
            return None

        # Check cache first
        if use_cache:
            cached = cls._cache.get(prompt, model)
            if cached is not None:
                logger.info("Cache HIT for model '%s' (saved LLM call)", model)
                return cached

        cmd = ["ollama", "run", model]
        
        try:
            logger.info(f"Ollama generating with model '{model}'...")
            result = subprocess.run(
                cmd,
                input=prompt,
                capture_output=True,
                text=True,
                timeout=timeout,
                encoding='utf-8'
            )
            
            if result.returncode != 0:
                logger.error(f"Ollama error (returncode {result.returncode}): {result.stderr}")
                return None
            
            response = result.stdout.strip()
            duration = time.time() - start_time
            logger.info(f"Ollama generation completed in {duration:.2f}s (model: {model})")

            # Store in cache
            if use_cache and response:
                cls._cache.put(prompt, model, response)
            
            return response
            
        except subprocess.TimeoutExpired:
            logger.error(f"Ollama generation timed out after {timeout}s (model: {model})")
            return None
        except Exception as e:
            logger.error(f"Ollama generation failed: {str(e)}")
            return None

    @classmethod
    def cache_stats(cls) -> Dict[str, int]:
        """Return prompt cache statistics."""
        return {"hits": cls._cache.hits, "misses": cls._cache.misses}

    @classmethod
    def generate_with_retry(cls, prompt: str, model: str = "mistral:latest", timeout: int = 300, retries: int = 2) -> Optional[str]:
        """Generate with retry and exponential backoff."""
        backoff_delays = [2, 5, 10]
        for attempt in range(retries + 1):
            response = cls.generate(prompt, model, timeout)
            if response:
                return response
            if attempt < retries:
                delay = backoff_delays[min(attempt, len(backoff_delays) - 1)]
                logger.warning(f"Retrying Ollama generation (attempt {attempt + 1}/{retries}), backoff {delay}s...")
                time.sleep(delay)
        return None
