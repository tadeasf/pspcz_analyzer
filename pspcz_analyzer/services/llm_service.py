"""LLM integration for AI topic classification and tisk summarization.

Supports two backends:
- Ollama (local/remote, default) — uses /api/generate endpoint
- OpenAI-compatible (OpenAI, Azure, Together, Groq, vLLM) — uses /chat/completions

When the configured LLM is available, provides:
- Free-form topic classification (1-3 Czech topic labels per tisk)
- Czech-language summaries explaining what each proposed law changes

Falls back gracefully to keyword classification when LLM is unavailable.
"""

from __future__ import annotations

import json
import re
from abc import ABC, abstractmethod
from typing import Any

import httpx
from loguru import logger

from pspcz_analyzer.config import (
    LLM_PROVIDER,
    OLLAMA_API_KEY,
    OLLAMA_BASE_URL,
    OLLAMA_HEALTH_TIMEOUT,
    OLLAMA_MAX_TEXT_CHARS,
    OLLAMA_MODEL,
    OLLAMA_TIMEOUT,
    OLLAMA_VERBATIM_CHARS,
    OPENAI_API_KEY,
    OPENAI_BASE_URL,
    OPENAI_MODEL,
    TISK_SHORTENER,
)

# ── Shared formatting / language enforcement constants ────────────────────

_LANG_CS = " VŽDY odpovídej POUZE v češtině. Nikdy nepoužívej angličtinu."
_LANG_EN = " ALWAYS respond ONLY in English. Never use Czech or any other language."

_FORMAT_CS = (
    " Formátuj výstup jako validní markdown. Nepoužívej emoji. "
    "Tabulky piš ve formátu markdown. Používej **tučné** pro klíčové pojmy, "
    "odrážky pro seznamy. Nepoužívej HTML tagy."
)
_FORMAT_EN = (
    " Format output as valid markdown. No emojis. "
    "Use markdown tables. Use **bold** for key terms, bullet points for lists. "
    "No HTML tags."
)

# ── Czech system prompts ─────────────────────────────────────────────────

_CLASSIFICATION_SYSTEM = (
    "Jsi analytik českého parlamentu. Analyzuješ parlamentní tisky a přiřazuješ jim tematické štítky. "
    "Odpověz POUZE ve formátu 'TOPICS: téma1, téma2, téma3' kde témata jsou 1–3 krátké české názvy "
    "tematických oblastí (např. 'Daně a poplatky', 'Sociální pojištění', 'Trestní právo'). "
    "Používej stručné a konkrétní názvy témat. Žádný další text." + _LANG_CS + _FORMAT_CS
)

_CLASSIFICATION_PROMPT_TEMPLATE = (
    "Urči 1–3 hlavní témata následujícího parlamentního tisku. "
    "Použij krátké české názvy témat (2–4 slova). "
    "Buď konkrétní – např. místo 'Právo' napiš 'Trestní právo' nebo 'Občanské právo'.\n\n"
    "Název tisku:\n---BEGIN USER TEXT---\n{title}\n---END USER TEXT---\n\n"
    "Text tisku:\n---BEGIN USER TEXT---\n{text}\n---END USER TEXT---\n\n"
    "Odpověz POUZE: TOPICS: téma1, téma2, téma3"
)

_SUMMARY_SYSTEM = (
    "Jsi kriticko-analytický komentátor českého parlamentu. Píšeš ostře, věcně a bez přikrašlování. "
    "Odhaluješ skryté dopady zákonů, rizika zneužití, a kdo z novely skutečně profituje. "
    "Neboj se pojmenovat problémy přímo — např. oslabení nezávislosti úředníků, rozšíření pravomocí "
    "bez kontroly, skryté privatizace, nebo omezení občanských práv. 3–4 věty."
    + _LANG_CS
    + _FORMAT_CS
)

_SUMMARY_PROMPT_TEMPLATE = (
    "Analyzuj následující parlamentní tisk KRITICKY. Nestačí říct 'co mění' — vysvětli:\n"
    "1. Co KONKRÉTNĚ se mění (žádné vágní formulace)\n"
    "2. Komu to prospívá a komu škodí\n"
    "3. Jaké je RIZIKO zneužití nebo nezamýšlený důsledek\n"
    "Buď přímý a kriticko-analytický. Pokud zákon oslabuje kontrolu, nezávislost nebo práva, řekni to jasně.\n"
    "3–4 věty v češtině.\n\n"
    "Název:\n---BEGIN USER TEXT---\n{title}\n---END USER TEXT---\n\n"
    "Text:\n---BEGIN USER TEXT---\n{text}\n---END USER TEXT---"
)

_CONSOLIDATION_SYSTEM = (
    "Jsi analytik českého parlamentu. Dostaneš seznam tematických štítků. "
    "Sjednoť podobná/překrývající se témata pod jeden kanonický název." + _LANG_CS + _FORMAT_CS
)

_CONSOLIDATION_PROMPT_TEMPLATE = (
    "Zde je seznam {n} témat z parlamentních tisků. Sjednoť podobná a překrývající se témata.\n"
    "Pro každé téma napiš mapování ve formátu: staré_téma -> kanonický_název\n"
    "Pokud je téma už trefné, mapuj ho samo na sebe.\n\n"
    "Témata:\n{topics_list}\n\n"
    "Odpověz POUZE mapováním, jeden řádek na téma."
)

_COMPARISON_SYSTEM = (
    "Jsi analyticko-právní expert na českou legislativu. Srovnáváš verze parlamentních tisků "
    "a identifikuješ KONKRÉTNÍ změny mezi nimi — čísla paragrafů, co bylo přidáno, odebráno či změněno."
    + _LANG_CS
    + _FORMAT_CS
)

_COMPARISON_PROMPT_TEMPLATE = (
    "Porovnej následující dvě verze parlamentního tisku a popiš KONKRÉTNÍ rozdíly:\n"
    "1. Které paragrafy/články se změnily a jak\n"
    "2. Co bylo přidáno nebo odebráno\n"
    "3. Jaký je celkový charakter změn (zpřísnění/zmírnění/technická úprava)\n"
    "3–4 věty v češtině. Buď konkrétní — cituj čísla paragrafů.\n\n"
    "VERZE {ct1_old} ({label_old}):\n---BEGIN USER TEXT---\n{text_old}\n---END USER TEXT---\n\n"
    "VERZE {ct1_new} ({label_new}):\n---BEGIN USER TEXT---\n{text_new}\n---END USER TEXT---"
)

# ── English system prompts ───────────────────────────────────────────────

_CLASSIFICATION_SYSTEM_EN = (
    "You are a Czech Parliament analyst. You analyze parliamentary bills and assign topic labels. "
    "Respond ONLY in format 'TOPICS: topic1, topic2, topic3' where topics are 1-3 short English names "
    "of thematic areas (e.g. 'Taxes & Fees', 'Social Insurance', 'Criminal Law'). "
    "Use concise and specific topic names. No other text." + _LANG_EN + _FORMAT_EN
)

_CLASSIFICATION_PROMPT_TEMPLATE_EN = (
    "Identify 1-3 main topics of the following Czech parliamentary bill. "
    "Use short English topic names (2-4 words). "
    "Be specific — e.g. instead of 'Law' write 'Criminal Law' or 'Civil Law'.\n\n"
    "Bill title:\n---BEGIN USER TEXT---\n{title}\n---END USER TEXT---\n\n"
    "Bill text:\n---BEGIN USER TEXT---\n{text}\n---END USER TEXT---\n\n"
    "Respond ONLY: TOPICS: topic1, topic2, topic3"
)

_CONSOLIDATION_SYSTEM_EN = (
    "You are a Czech Parliament analyst. You will receive a list of topic labels. "
    "Unify similar/overlapping topics under one canonical English name." + _LANG_EN + _FORMAT_EN
)

_CONSOLIDATION_PROMPT_TEMPLATE_EN = (
    "Here is a list of {n} topics from parliamentary bills. Unify similar and overlapping topics.\n"
    "For each topic write a mapping in format: old_topic -> canonical_name\n"
    "If a topic is already good, map it to itself.\n\n"
    "Topics:\n{topics_list}\n\n"
    "Respond ONLY with mappings, one line per topic."
)

_SUMMARY_SYSTEM_EN = (
    "You are a critical analyst of the Czech Parliament. You write sharp, factual assessments "
    "without embellishment. You expose hidden impacts of laws, risks of abuse, and who truly "
    "benefits from amendments. Don't hesitate to name problems directly — e.g. weakening of "
    "official independence, expanding powers without oversight, hidden privatizations, or "
    "restrictions on civil rights. 3-4 sentences." + _LANG_EN + _FORMAT_EN
)

_SUMMARY_PROMPT_TEMPLATE_EN = (
    "Analyze the following Czech parliamentary bill CRITICALLY. Don't just say 'what it changes' — explain:\n"
    "1. What SPECIFICALLY changes (no vague formulations)\n"
    "2. Who benefits and who is harmed\n"
    "3. What is the RISK of abuse or unintended consequence\n"
    "Be direct and critical. If the law weakens oversight, independence, or rights, say it clearly.\n"
    "3-4 sentences in English.\n\n"
    "Title:\n---BEGIN USER TEXT---\n{title}\n---END USER TEXT---\n\n"
    "Text:\n---BEGIN USER TEXT---\n{text}\n---END USER TEXT---"
)

_COMPARISON_SYSTEM_EN = (
    "You are a legal expert on Czech legislation. You compare versions of parliamentary bills "
    "and identify SPECIFIC changes between them — paragraph numbers, what was added, removed, or modified."
    + _LANG_EN
    + _FORMAT_EN
)

_COMPARISON_PROMPT_TEMPLATE_EN = (
    "Compare the following two versions of a Czech parliamentary bill and describe SPECIFIC differences:\n"
    "1. Which paragraphs/articles changed and how\n"
    "2. What was added or removed\n"
    "3. What is the overall character of changes (tightening/loosening/technical adjustment)\n"
    "3-4 sentences in English. Be specific — cite paragraph numbers.\n\n"
    "VERSION {ct1_old} ({label_old}):\n---BEGIN USER TEXT---\n{text_old}\n---END USER TEXT---\n\n"
    "VERSION {ct1_new} ({label_new}):\n---BEGIN USER TEXT---\n{text_new}\n---END USER TEXT---"
)

# ── Security & text processing ───────────────────────────────────────────

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
    r")",
    re.MULTILINE,
)


def truncate_legislative_text(
    text: str,
    verbatim_chars: int = OLLAMA_VERBATIM_CHARS,
    max_chars: int = OLLAMA_MAX_TEXT_CHARS,
) -> str:
    """Truncate Czech legislative text intelligently for LLM processing.

    When TISK_SHORTENER is disabled (0), returns the full text unmodified.

    Strategy (when enabled):
    1. First `verbatim_chars` characters verbatim (captures explanatory report)
    2. From remainder: extract heading lines + first 200 chars after each heading
    3. Hard cap at `max_chars` total
    """
    if not TISK_SHORTENER:
        return text

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


# ── Base class ───────────────────────────────────────────────────────────


class BaseLLMClient(ABC):
    """Abstract base class for LLM backend clients."""

    base_url: str
    model: str
    _log_prefix: str

    @abstractmethod
    def is_available(self) -> bool:
        """Check if the LLM backend is reachable."""
        ...

    @abstractmethod
    def _generate(
        self, prompt: str, system: str, *, reasoning_effort: str | None = None
    ) -> str | None:
        """Send a generation request to the LLM. Returns response text or None."""
        ...

    def classify_topics(self, text: str, title: str) -> list[str]:
        """Classify a tisk into 1-3 free-form Czech topic labels using the LLM."""
        truncated = truncate_legislative_text(text)
        prompt = _CLASSIFICATION_PROMPT_TEMPLATE.format(
            title=_sanitize_llm_input(title or "(bez názvu)"),
            text=_sanitize_llm_input(truncated),
        )
        response = self._generate(prompt, _CLASSIFICATION_SYSTEM, reasoning_effort="low")
        if response is None:
            return []
        return self._parse_topics_response(response)

    def summarize(self, text: str, title: str) -> str:
        """Generate a Czech-language summary of what a proposed law changes."""
        truncated = truncate_legislative_text(text)
        prompt = _SUMMARY_PROMPT_TEMPLATE.format(
            title=_sanitize_llm_input(title or "(bez názvu)"),
            text=_sanitize_llm_input(truncated),
        )
        response = self._generate(prompt, _SUMMARY_SYSTEM, reasoning_effort="medium")
        if not response:
            return ""
        return self._strip_think(response)

    def summarize_en(self, text: str, title: str) -> str:
        """Generate an English-language critical summary of a proposed law."""
        truncated = truncate_legislative_text(text)
        prompt = _SUMMARY_PROMPT_TEMPLATE_EN.format(
            title=_sanitize_llm_input(title or "(no title)"),
            text=_sanitize_llm_input(truncated),
        )
        response = self._generate(prompt, _SUMMARY_SYSTEM_EN, reasoning_effort="medium")
        if not response:
            return ""
        return self._strip_think(response)

    def consolidate_topics(self, all_topics: list[str]) -> dict[str, str]:
        """Consolidate/deduplicate topic labels via the LLM."""
        topics_list = "\n".join(f"- {t}" for t in all_topics)
        prompt = _CONSOLIDATION_PROMPT_TEMPLATE.format(
            n=len(all_topics),
            topics_list=topics_list,
        )
        response = self._generate(prompt, _CONSOLIDATION_SYSTEM, reasoning_effort="low")
        if not response:
            return {t: t for t in all_topics}
        return self._parse_consolidation_response(response, all_topics)

    def classify_topics_en(self, text: str, title: str) -> list[str]:
        """Classify a tisk into 1-3 free-form English topic labels using the LLM."""
        truncated = truncate_legislative_text(text)
        prompt = _CLASSIFICATION_PROMPT_TEMPLATE_EN.format(
            title=_sanitize_llm_input(title or "(no title)"),
            text=_sanitize_llm_input(truncated),
        )
        response = self._generate(prompt, _CLASSIFICATION_SYSTEM_EN, reasoning_effort="low")
        if response is None:
            return []
        return self._parse_topics_response(response)

    def classify_topics_bilingual(self, text: str, title: str) -> tuple[list[str], list[str]]:
        """Classify a tisk into topic labels in both Czech and English."""
        topics_cs = self.classify_topics(text, title)
        topics_en = self.classify_topics_en(text, title)
        return topics_cs, topics_en

    def consolidate_topics_en(self, all_topics: list[str]) -> dict[str, str]:
        """Consolidate/deduplicate English topic labels via the LLM."""
        topics_list = "\n".join(f"- {t}" for t in all_topics)
        prompt = _CONSOLIDATION_PROMPT_TEMPLATE_EN.format(
            n=len(all_topics),
            topics_list=topics_list,
        )
        response = self._generate(prompt, _CONSOLIDATION_SYSTEM_EN, reasoning_effort="low")
        if not response:
            return {t: t for t in all_topics}
        return self._parse_consolidation_response(response, all_topics)

    def consolidate_topics_bilingual(
        self,
        all_topics_cs: list[str],
        all_topics_en: list[str],
    ) -> tuple[dict[str, str], dict[str, str]]:
        """Consolidate topic labels in both Czech and English."""
        mapping_cs = self.consolidate_topics(all_topics_cs)
        mapping_en = self.consolidate_topics_en(all_topics_en)
        return mapping_cs, mapping_en

    def compare_versions(
        self,
        text_old: str,
        text_new: str,
        ct1_old: int,
        ct1_new: int,
        label_old: str = "",
        label_new: str = "",
    ) -> str:
        """Compare two versions of a tisk and return a Czech-language diff summary."""
        trunc_old = truncate_legislative_text(text_old)
        trunc_new = truncate_legislative_text(text_new)
        prompt = _COMPARISON_PROMPT_TEMPLATE.format(
            ct1_old=ct1_old,
            ct1_new=ct1_new,
            label_old=label_old or f"CT1={ct1_old}",
            label_new=label_new or f"CT1={ct1_new}",
            text_old=_sanitize_llm_input(trunc_old),
            text_new=_sanitize_llm_input(trunc_new),
        )
        response = self._generate(prompt, _COMPARISON_SYSTEM, reasoning_effort="medium")
        if not response:
            return ""
        return self._strip_think(response)

    def summarize_bilingual(self, text: str, title: str) -> dict[str, str]:
        """Generate both Czech and English summaries."""
        cs = self.summarize(text, title)
        en = self.summarize_en(text, title)
        return {"cs": cs, "en": en}

    def compare_versions_bilingual(
        self,
        text_old: str,
        text_new: str,
        ct1_old: int,
        ct1_new: int,
        label_old: str = "",
        label_new: str = "",
    ) -> dict[str, str]:
        """Compare two versions and return bilingual diff summaries."""
        cs = self.compare_versions(text_old, text_new, ct1_old, ct1_new, label_old, label_new)
        trunc_old = truncate_legislative_text(text_old)
        trunc_new = truncate_legislative_text(text_new)
        prompt = _COMPARISON_PROMPT_TEMPLATE_EN.format(
            ct1_old=ct1_old,
            ct1_new=ct1_new,
            label_old=label_old or f"CT1={ct1_old}",
            label_new=label_new or f"CT1={ct1_new}",
            text_old=_sanitize_llm_input(trunc_old),
            text_new=_sanitize_llm_input(trunc_new),
        )
        response = self._generate(prompt, _COMPARISON_SYSTEM_EN, reasoning_effort="medium")
        en = self._strip_think(response) if response else ""
        return {"cs": cs, "en": en}

    @staticmethod
    def _strip_think(text: str) -> str:
        """Remove <think>...</think> blocks from responses."""
        return _THINK_RE.sub("", text).strip()

    def _parse_topics_response(self, response: str) -> list[str]:
        """Extract topic labels from LLM response.

        Expects format: TOPICS: topic1, topic2, topic3
        """
        response = self._strip_think(response)
        match = re.search(r"TOPICS?:\s*(.+)", response, re.IGNORECASE)
        if not match:
            logger.debug(
                "{} Could not parse topics from response: {}",
                self._log_prefix,
                response[:200],
            )
            return []

        raw = match.group(1).strip()
        # Split on comma, clean up each topic
        topics = [t.strip().strip(".,;:-–") for t in raw.split(",")]
        # Filter empty strings and "none"
        topics = [t for t in topics if t and t.lower() != "none"]

        if not topics:
            logger.debug(
                "{} No valid topics parsed from: {}",
                self._log_prefix,
                raw[:200],
            )
            return []

        # Cap at 3 topics
        return topics[:3]

    @staticmethod
    def _parse_consolidation_response(response: str, all_topics: list[str]) -> dict[str, str]:
        """Parse consolidation mapping from LLM response."""
        response = _THINK_RE.sub("", response).strip()
        mapping: dict[str, str] = {}
        for line in response.splitlines():
            line = line.strip()
            if " -> " not in line:
                continue
            parts = line.split(" -> ", 1)
            old = parts[0].strip().strip("- ")
            new = parts[1].strip()
            if old and new:
                mapping[old] = new
        for t in all_topics:
            if t not in mapping:
                mapping[t] = t
        return mapping


# ── Ollama backend ───────────────────────────────────────────────────────


class OllamaClient(BaseLLMClient):
    """Client for local Ollama LLM integration."""

    def __init__(
        self,
        base_url: str = OLLAMA_BASE_URL,
        model: str = OLLAMA_MODEL,
        timeout: float = OLLAMA_TIMEOUT,
        api_key: str = OLLAMA_API_KEY,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.timeout = timeout
        self._available: bool | None = None
        self._headers: dict[str, str] = {}
        self._log_prefix = "[ollama]"
        if api_key:
            self._headers["Authorization"] = f"Bearer {api_key}"

    def is_available(self) -> bool:
        """Check if Ollama is running and the model is available.

        Caches result after first call.
        """
        if self._available is not None:
            return self._available

        try:
            resp = httpx.get(
                f"{self.base_url}/api/tags",
                headers=self._headers,
                timeout=OLLAMA_HEALTH_TIMEOUT,
            )
            resp.raise_for_status()
            models = [m.get("name", "") for m in resp.json().get("models", [])]
            # Match model name with or without tag suffix
            self._available = any(
                m == self.model
                or m.startswith(f"{self.model}:")
                or self.model.startswith(f"{m}:")
                or m == self.model.split(":")[0]
                for m in models
            )
            if self._available:
                logger.info("[ollama] Available with model {}", self.model)
            else:
                logger.info(
                    "[ollama] Running but model {} not found (available: {})",
                    self.model,
                    ", ".join(models),
                )
        except Exception:
            self._available = False
            logger.info("[ollama] Not available (connection failed)")

        return self._available

    def _generate(
        self, prompt: str, system: str, *, reasoning_effort: str | None = None
    ) -> str | None:
        """Send a generation request to Ollama. Returns response text or None.

        The reasoning_effort parameter is accepted but ignored (Ollama doesn't support it).
        """
        try:
            resp = httpx.post(
                f"{self.base_url}/api/generate",
                headers=self._headers,
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


# ── OpenAI-compatible backend ────────────────────────────────────────────


class OpenAIClient(BaseLLMClient):
    """Client for OpenAI-compatible LLM APIs (OpenAI, Azure, Together, Groq, vLLM, etc.)."""

    def __init__(
        self,
        base_url: str = OPENAI_BASE_URL,
        model: str = OPENAI_MODEL,
        timeout: float = OLLAMA_TIMEOUT,
        api_key: str = OPENAI_API_KEY,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.timeout = timeout
        self._available: bool | None = None
        self._headers: dict[str, str] = {"Content-Type": "application/json"}
        self._log_prefix = "[openai]"
        if api_key:
            self._headers["Authorization"] = f"Bearer {api_key}"

    def is_available(self) -> bool:
        """Check if the OpenAI-compatible API is reachable.

        Caches result after first call.
        """
        if self._available is not None:
            return self._available

        try:
            resp = httpx.get(
                f"{self.base_url}/models",
                headers=self._headers,
                timeout=OLLAMA_HEALTH_TIMEOUT,
            )
            resp.raise_for_status()
            self._available = True
            logger.info("[openai] Available at {} with model {}", self.base_url, self.model)
        except Exception:
            self._available = False
            logger.info("[openai] Not available (connection to {} failed)", self.base_url)

        return self._available

    def _generate(
        self, prompt: str, system: str, *, reasoning_effort: str | None = None
    ) -> str | None:
        """Send a chat completion request. Returns response text or None.

        When reasoning_effort is provided, includes it in the request payload
        for models that support it (e.g. gpt-oss-120b).
        """
        payload: dict[str, Any] = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": prompt},
            ],
            "stream": False,
        }
        if reasoning_effort is not None:
            payload["reasoning_effort"] = reasoning_effort

        try:
            resp = httpx.post(
                f"{self.base_url}/chat/completions",
                headers=self._headers,
                json=payload,
                timeout=self.timeout,
            )
            resp.raise_for_status()
            data = resp.json()
            choices = data.get("choices", [])
            if choices:
                return choices[0].get("message", {}).get("content")
            return None
        except Exception:
            logger.opt(exception=True).debug("[openai] Chat completion request failed")
            return None


# ── Serialization helpers ────────────────────────────────────────────────


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
            logger.debug("Failed to parse topics JSON: {}", raw)
    # Old format: single topic ID like "finance" or "justice"
    return [raw]


# ── Factory ──────────────────────────────────────────────────────────────


def create_llm_client() -> BaseLLMClient:
    """Factory: return the configured LLM backend client.

    Reads LLM_PROVIDER from config:
    - "ollama" (default) -> OllamaClient
    - "openai" -> OpenAIClient (fails fast if OPENAI_API_KEY is empty)

    Raises ValueError for unknown provider.
    """
    match LLM_PROVIDER.lower().strip():
        case "ollama":
            return OllamaClient()
        case "openai":
            if not OPENAI_API_KEY:
                msg = (
                    "LLM_PROVIDER=openai but OPENAI_API_KEY is not set. "
                    "Set OPENAI_API_KEY in your .env or environment."
                )
                raise ValueError(msg)
            return OpenAIClient()
        case _:
            msg = f"Unknown LLM_PROVIDER={LLM_PROVIDER!r}. Use 'ollama' or 'openai'."
            raise ValueError(msg)
