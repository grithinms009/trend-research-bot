import subprocess
import logging
import time
from typing import Optional

logger = logging.getLogger(__name__)

class OllamaClient:
    """
    Local LLM client using Ollama CLI.
    """

    ALLOWED_MODELS = {
        "mistral:latest",
        "deepseek-r1:7b",
    }

    @classmethod
    def generate(cls, prompt: str, model: str = "mistral:latest", timeout: int = 120) -> Optional[str]:
        """
        Generate text using Ollama 'run' command.
        
        Args:
            prompt: The input prompt for the LLM.
            model: The model name (e.g., 'mistral:instruct', 'llama3').
            timeout: Command timeout in seconds.
            
        Returns:
            Cleaned response text or None if failed.
        """
        start_time = time.time()

        if model not in cls.ALLOWED_MODELS:
            logger.error(f"Model '{model}' is not allowed.")
            return None

        # We use 'ollama run' via subprocess.
        # Note: --nowarn is used to suppress some CLI warnings if supported by the version.
        # We pass the prompt via stdin to avoid shell escaping issues with long prompts.
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
            
            return response
            
        except subprocess.TimeoutExpired:
            logger.error(f"Ollama generation timed out after {timeout}s (model: {model})")
            return None
        except Exception as e:
            logger.error(f"Ollama generation failed: {str(e)}")
            return None

    @classmethod
    def generate_with_retry(cls, prompt: str, model: str = "mistral:latest", timeout: int = 120, retries: int = 1) -> Optional[str]:
        """Generate with a simple retry mechanism."""
        for attempt in range(retries + 1):
            response = cls.generate(prompt, model, timeout)
            if response:
                return response
            if attempt < retries:
                logger.warning(f"Retrying Ollama generation (attempt {attempt + 1}/{retries})...")
                time.sleep(2)  # Small delay before retry
        return None
