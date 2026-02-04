"""Ollama API client for LLM-powered summaries.

Simple client to interact with a local Ollama instance for generating
text summaries. Provides graceful fallback when Ollama is unavailable.
"""

from __future__ import annotations

import requests

OLLAMA_URL = "http://localhost:11434"
DEFAULT_MODEL = "llama3.1:8b"


def is_ollama_available() -> bool:
    """Check if Ollama is running and accessible."""
    try:
        response = requests.get(f"{OLLAMA_URL}/api/tags", timeout=2)
        return response.status_code == 200
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
        return False


def list_models() -> list[str]:
    """List available models in Ollama."""
    try:
        response = requests.get(f"{OLLAMA_URL}/api/tags", timeout=5)
        response.raise_for_status()
        data = response.json()
        return [model["name"] for model in data.get("models", [])]
    except (requests.exceptions.RequestException, KeyError):
        return []


def generate_summary(
    prompt: str,
    model: str = DEFAULT_MODEL,
    timeout: int = 120,
) -> str:
    """Generate a text summary using Ollama.

    Parameters
    ----------
    prompt : str
        The prompt to send to the model.
    model : str
        The Ollama model to use (default: llama3.2:3b).
    timeout : int
        Request timeout in seconds (default: 120).

    Returns
    -------
    str
        The generated response, or an error message if generation fails.
    """
    try:
        response = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={
                "model": model,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.3,  # Lower temperature for more consistent output
                    "num_predict": 256,  # Limit response length
                },
            },
            timeout=timeout,
        )
        response.raise_for_status()
        return response.json().get("response", "").strip()
    except requests.exceptions.Timeout:
        return "[LLM timeout]"
    except requests.exceptions.ConnectionError:
        return "[LLM unavailable]"
    except requests.exceptions.RequestException as e:
        return f"[LLM error: {e}]"
    except (KeyError, ValueError):
        return "[LLM response error]"
