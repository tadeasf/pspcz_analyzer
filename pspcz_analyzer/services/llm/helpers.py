"""LLM helper utilities — sanitization, truncation, and factory."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pspcz_analyzer.services.llm.client import LLMClient

from pspcz_analyzer.config import (
    LLM_MAX_TEXT_CHARS,
    LLM_PROVIDER,
    LLM_STRUCTURED_OUTPUT,
    LLM_VERBATIM_CHARS,
    OLLAMA_API_KEY,
    OLLAMA_BASE_URL,
    OLLAMA_MODEL,
    OPENAI_API_KEY,
    OPENAI_BASE_URL,
    OPENAI_MODEL,
    TISK_SHORTENER,
)

_INJECTION_PHRASES_RE = re.compile(
    r"(?:ignore (?:all )?(?:previous|above|prior) instructions"
    r"|you are now"
    r"|new instructions:"
    r"|system prompt:"
    r"|---END USER TEXT---)",
    re.IGNORECASE,
)


def _sanitize_llm_input(text: str) -> str:
    """Strip common prompt injection phrases from user-supplied text."""
    return _INJECTION_PHRASES_RE.sub("[REDACTED]", text)


# Strip <think>...</think> blocks from responses (defensive — models with chain-of-thought)
_THINK_RE = re.compile(r"<think>.*?</think>", re.DOTALL)

# Pattern for heading lines in Czech legislative texts
_HEADING_RE = re.compile(
    r"^(?:"
    r"[A-ZÁČĎÉĚÍŇÓŘŠŤÚŮÝŽ\s]{10,}"  # ALL CAPS lines (10+ chars)
    r"|(?:Část|ČÁST|Hlava|HLAVA|Článek|ČLÁNEK|Díl|DÍL)\s"  # Section markers
    r"|(?:I{1,3}V?|VI{0,3}|IX|X{1,3})\.\s"  # Roman numerals
    r"|DŮVODOVÁ ZPRÁVA"
    r"|ZVLÁŠTNÍ ČÁST"
    r"|OBECNÁ ČÁST"
    r"|§\s*\d+"  # Paragraph markers (§ 1, § 123a)
    r")",
    re.MULTILINE,
)


def truncate_legislative_text(
    text: str,
    verbatim_chars: int = LLM_VERBATIM_CHARS,
    max_chars: int = LLM_MAX_TEXT_CHARS,
) -> str:
    """Truncate Czech legislative text via structural extraction for LLM processing.

    When TISK_SHORTENER is disabled (0), returns the full text unmodified.

    Strategy (when enabled and text exceeds max_chars):
    1. First `verbatim_chars` kept verbatim (covers explanatory report + most content)
    2. From remainder: extract section headings + first 500 chars of each section
    3. Hard cap at `max_chars` total
    """
    if not TISK_SHORTENER:
        return text

    if len(text) <= max_chars:
        return text

    # Clamp verbatim to not exceed max (defensive for comparison calls)
    verbatim_chars = min(verbatim_chars, max_chars)

    result = text[:verbatim_chars]
    remainder = text[verbatim_chars:]

    # Extract structural highlights from the remainder
    highlights: list[str] = []
    for match in _HEADING_RE.finditer(remainder):
        start = match.start()
        snippet = remainder[start : start + 500 + len(match.group())]
        highlights.append(snippet.strip())

    if highlights:
        result += "\n\n[...]\n\n" + "\n\n".join(highlights)

    return result[:max_chars]


def create_llm_client() -> LLMClient:
    """Factory: return the configured LLM backend client.

    Reads LLM_PROVIDER from config:
    - "ollama" (default) -> LLMClient(provider="ollama", ...)
    - "openai" -> LLMClient(provider="openai", ...) (fails fast if OPENAI_API_KEY is empty)

    Raises ValueError for unknown provider.
    """
    from pspcz_analyzer.services.llm.client import LLMClient

    match LLM_PROVIDER.lower().strip():
        case "ollama":
            return LLMClient(
                provider="ollama",
                base_url=OLLAMA_BASE_URL,
                model=OLLAMA_MODEL,
                api_key=OLLAMA_API_KEY,
                structured_output=LLM_STRUCTURED_OUTPUT,
            )
        case "openai":
            if not OPENAI_API_KEY:
                msg = (
                    "LLM_PROVIDER=openai but OPENAI_API_KEY is not set. "
                    "Set OPENAI_API_KEY in your .env or environment."
                )
                raise ValueError(msg)
            return LLMClient(
                provider="openai",
                base_url=OPENAI_BASE_URL,
                model=OPENAI_MODEL,
                api_key=OPENAI_API_KEY,
                structured_output=LLM_STRUCTURED_OUTPUT,
            )
        case _:
            msg = f"Unknown LLM_PROVIDER={LLM_PROVIDER!r}. Use 'ollama' or 'openai'."
            raise ValueError(msg)
