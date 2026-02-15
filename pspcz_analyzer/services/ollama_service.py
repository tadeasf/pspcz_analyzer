"""Ollama integration for AI topic classification and tisk summarization.

When Ollama is running locally with the configured model, provides:
- Free-form topic classification (1-3 Czech topic labels per tisk)
- Czech-language summaries explaining what each proposed law changes

Falls back gracefully to keyword classification when Ollama is unavailable.
"""

import json
import re

import httpx
from loguru import logger

from pspcz_analyzer.config import (
    OLLAMA_BASE_URL,
    OLLAMA_HEALTH_TIMEOUT,
    OLLAMA_MAX_TEXT_CHARS,
    OLLAMA_MODEL,
    OLLAMA_TIMEOUT,
    OLLAMA_VERBATIM_CHARS,
)

_CLASSIFICATION_SYSTEM = (
    "Jsi analytik ceskeho parlamentu. Analyzujes parlamentni tisky a priradujes jim tematicke stitky. "
    "Odpovez POUZE ve formatu 'TOPICS: tema1, tema2, tema3' kde temata jsou 1-3 kratke ceske nazvy "
    "tematickych oblasti (napr. 'Dane a poplatky', 'Socialni pojisteni', 'Trestni pravo'). "
    "Pouzivej strucne a konkretni nazvy temat. Zadny dalsi text."
)

_CLASSIFICATION_PROMPT_TEMPLATE = (
    "Urc 1-3 hlavni temata nasledujiciho parlamentniho tisku. "
    "Pouzij kratke ceske nazvy temat (2-4 slova). "
    "Bud konkretni - napr. misto 'Pravo' napis 'Trestni pravo' nebo 'Obcanske pravo'.\n\n"
    "Nazev tisku: {title}\n\n"
    "Text tisku:\n{text}\n\n"
    "Odpovez POUZE: TOPICS: tema1, tema2, tema3"
)

_SUMMARY_SYSTEM = (
    "Jsi analytik ceskeho parlamentu. Pises strucne a srozumitelne shrnutí navrhu zakonu v cestine. "
    "Zamer se na prakticke dopady - co zakon meni a proc. 2-3 vety."
)

_SUMMARY_PROMPT_TEMPLATE = (
    "Shrn co nasledujici parlamentni tisk navrhuje, co meni a proc. "
    "2-3 vety v cestine. Zamer se na prakticke dopady pro obcany.\n\n"
    "Nazev: {title}\n\n"
    "Text:\n{text}"
)

# Pattern for heading lines in Czech legislative texts
_HEADING_RE = re.compile(
    r"^(?:"
    r"[A-ZÁČĎÉĚÍŇÓŘŠŤÚŮÝŽ\s]{10,}"  # ALL CAPS lines (10+ chars)
    r"|(?:Část|ČÁST|Hlava|HLAVA|Článek|ČLÁNEK|Díl|DÍL)\s"  # Section markers
    r"|(?:I{1,3}V?|VI{0,3}|IX|X{1,3})\.\s"  # Roman numerals
    r"|DŮVODOVÁ ZPRÁVA"
    r"|ZVLÁŠTNÍ ČÁST"
    r"|OBECNÁ ČÁST"
    r")",
    re.MULTILINE,
)


def truncate_legislative_text(
    text: str,
    verbatim_chars: int = OLLAMA_VERBATIM_CHARS,
    max_chars: int = OLLAMA_MAX_TEXT_CHARS,
) -> str:
    """Truncate Czech legislative text intelligently for LLM processing.

    Strategy:
    1. First `verbatim_chars` characters verbatim (captures explanatory report)
    2. From remainder: extract heading lines + first 200 chars after each heading
    3. Hard cap at `max_chars` total
    """
    if len(text) <= max_chars:
        return text

    result = text[:verbatim_chars]
    remainder = text[verbatim_chars:]

    # Extract structural highlights from the remainder
    highlights: list[str] = []
    for match in _HEADING_RE.finditer(remainder):
        start = match.start()
        # Grab heading + 200 chars after it
        snippet = remainder[start : start + 200 + len(match.group())]
        highlights.append(snippet.strip())

    if highlights:
        result += "\n\n[...]\n\n" + "\n\n".join(highlights)

    return result[:max_chars]


class OllamaClient:
    """Client for local Ollama LLM integration."""

    def __init__(
        self,
        base_url: str = OLLAMA_BASE_URL,
        model: str = OLLAMA_MODEL,
        timeout: float = OLLAMA_TIMEOUT,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.timeout = timeout
        self._available: bool | None = None

    def is_available(self) -> bool:
        """Check if Ollama is running and the model is available.

        Caches result after first call.
        """
        if self._available is not None:
            return self._available

        try:
            resp = httpx.get(
                f"{self.base_url}/api/tags",
                timeout=OLLAMA_HEALTH_TIMEOUT,
            )
            resp.raise_for_status()
            models = [m.get("name", "") for m in resp.json().get("models", [])]
            # Match model name with or without tag suffix
            self._available = any(
                m == self.model or m.startswith(f"{self.model}:")
                or self.model.startswith(f"{m}:")
                or m == self.model.split(":")[0]
                for m in models
            )
            if self._available:
                logger.info("[ollama] Available with model {}", self.model)
            else:
                logger.info(
                    "[ollama] Running but model {} not found (available: {})",
                    self.model, ", ".join(models),
                )
        except Exception:
            self._available = False
            logger.info("[ollama] Not available (connection failed)")

        return self._available

    def classify_topics(self, text: str, title: str) -> list[str]:
        """Classify a tisk into 1-3 free-form topic labels using the LLM.

        Returns list of Czech topic labels, or empty list on failure.
        """
        truncated = truncate_legislative_text(text)
        prompt = _CLASSIFICATION_PROMPT_TEMPLATE.format(
            title=title or "(bez názvu)",
            text=truncated,
        )
        response = self._generate(prompt, _CLASSIFICATION_SYSTEM)
        if response is None:
            return []
        return self._parse_topics_response(response)

    def summarize(self, text: str, title: str) -> str:
        """Generate a Czech-language summary of what a proposed law changes.

        Returns summary text or empty string on failure.
        """
        truncated = truncate_legislative_text(text)
        prompt = _SUMMARY_PROMPT_TEMPLATE.format(
            title=title or "(bez názvu)",
            text=truncated,
        )
        response = self._generate(prompt, _SUMMARY_SYSTEM)
        return response.strip() if response else ""

    def _generate(self, prompt: str, system: str) -> str | None:
        """Send a generation request to Ollama. Returns response text or None."""
        try:
            resp = httpx.post(
                f"{self.base_url}/api/generate",
                json={
                    "model": self.model,
                    "prompt": prompt,
                    "system": system,
                    "stream": False,
                },
                timeout=self.timeout,
            )
            resp.raise_for_status()
            return resp.json().get("response")
        except Exception:
            logger.opt(exception=True).debug("[ollama] Generation request failed")
            return None

    def _parse_topics_response(self, response: str) -> list[str]:
        """Extract topic labels from LLM response.

        Expects format: TOPICS: topic1, topic2, topic3
        """
        match = re.search(r"TOPICS?:\s*(.+)", response, re.IGNORECASE)
        if not match:
            logger.debug("[ollama] Could not parse topics from response: {}", response[:200])
            return []

        raw = match.group(1).strip()
        # Split on comma, clean up each topic
        topics = [t.strip().strip(".,;:-–") for t in raw.split(",")]
        # Filter empty strings and "none"
        topics = [t for t in topics if t and t.lower() != "none"]

        if not topics:
            logger.debug("[ollama] No valid topics parsed from: {}", raw[:200])
            return []

        # Cap at 3 topics
        return topics[:3]


def serialize_topics(topics: list[str]) -> str:
    """Serialize topic list for parquet storage."""
    return json.dumps(topics, ensure_ascii=False)


def deserialize_topics(raw: str) -> list[str]:
    """Deserialize topic list from parquet storage.

    Handles both new JSON format and old single-topic-ID format.
    """
    if not raw:
        return []
    # Try JSON first (new format: '["topic1", "topic2"]')
    if raw.startswith("["):
        try:
            topics = json.loads(raw)
            return [t for t in topics if isinstance(t, str) and t]
        except (json.JSONDecodeError, TypeError):
            pass
    # Old format: single topic ID like "finance" or "justice"
    return [raw]
