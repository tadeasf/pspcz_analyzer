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
    OLLAMA_API_KEY,
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
    "Odpovez POUZE: TOPICS: tema1, tema2, tema3 /no_think"
)

_SUMMARY_SYSTEM = (
    "Jsi kriticko-analyticky komentator ceskeho parlamentu. Pises ostre, vecne a bez prikraslovani. "
    "Odhalujes skryte dopady zakonu, rizika zneuziti, a kdo z novely skutecne profituje. "
    "Neboj se pojmenovat problemy primo — napr. oslabeni nezavislosti uredniku, rozsireni pravomoci "
    "bez kontroly, skryte privatizace, nebo omeze obcanskych prav. 3-4 vety."
)

_SUMMARY_PROMPT_TEMPLATE = (
    "Analyzuj nasledujici parlamentni tisk KRITICKY. Nestaci rict 'co meni' — vysvetli:\n"
    "1. Co KONKRETNE se meni (zadne vague formulace)\n"
    "2. Komu to prospiva a komu skodi\n"
    "3. Jake je RIZIKO zneuziti nebo nezamysleny dusledek\n"
    "Bud primy a kriticko-analyticky. Pokud zakon oslabuje kontrolu, nezavislost nebo prava, rekni to jasne.\n"
    "3-4 vety v cestine.\n\n"
    "Nazev: {title}\n\n"
    "Text:\n{text} /no_think"
)

_CONSOLIDATION_SYSTEM = (
    "Jsi analytik ceskeho parlamentu. Dostanes seznam tematickych stitku. "
    "Sjednot podobna/prekryvajici se temata pod jeden kanonicky nazev."
)

_CONSOLIDATION_PROMPT_TEMPLATE = (
    "Zde je seznam {n} temat z parlamentnich tisku. Sjednot podobna a prekryvajici se temata.\n"
    "Pro kazde tema napis mapovani ve formatu: stare_tema -> kanonicky_nazev\n"
    "Pokud je tema uz dobre, mapuj ho samo na sebe.\n\n"
    "Temata:\n{topics_list}\n\n"
    "Odpovez POUZE mapovanim, jeden radek na tema. /no_think"
)

_COMPARISON_SYSTEM = (
    "Jsi analyticko-pravni expert na ceskou legislativu. Srovnavas verze parlamentnich tisku "
    "a identifikujes KONKRETNI zmeny mezi nimi — cisla paragrafu, co bylo pridano, odebrano ci zmeneno."
)

_COMPARISON_PROMPT_TEMPLATE = (
    "Porovnej nasledujici dve verze parlamentniho tisku a popis KONKRETNI rozdily:\n"
    "1. Ktere paragrafy/clanky se zmenily a jak\n"
    "2. Co bylo pridano nebo odebrano\n"
    "3. Jaky je celkovy charakter zmen (zprisneni/zmireni/technicka uprava)\n"
    "3-4 vety v cestine. Bud konkretni — cituj cisla paragrafu.\n\n"
    "VERZE {ct1_old} ({label_old}):\n{text_old}\n\n"
    "VERZE {ct1_new} ({label_new}):\n{text_new} /no_think"
)

# ── English prompts for bilingual output ──────────────────────────────────

_SUMMARY_SYSTEM_EN = (
    "You are a critical analyst of the Czech Parliament. You write sharp, factual assessments "
    "without embellishment. You expose hidden impacts of laws, risks of abuse, and who truly "
    "benefits from amendments. Don't hesitate to name problems directly — e.g. weakening of "
    "official independence, expanding powers without oversight, hidden privatizations, or "
    "restrictions on civil rights. 3-4 sentences."
)

_SUMMARY_PROMPT_TEMPLATE_EN = (
    "Analyze the following Czech parliamentary bill CRITICALLY. Don't just say 'what it changes' — explain:\n"
    "1. What SPECIFICALLY changes (no vague formulations)\n"
    "2. Who benefits and who is harmed\n"
    "3. What is the RISK of abuse or unintended consequence\n"
    "Be direct and critical. If the law weakens oversight, independence, or rights, say it clearly.\n"
    "3-4 sentences in English.\n\n"
    "Title: {title}\n\n"
    "Text:\n{text} /no_think"
)

_COMPARISON_SYSTEM_EN = (
    "You are a legal expert on Czech legislation. You compare versions of parliamentary bills "
    "and identify SPECIFIC changes between them — paragraph numbers, what was added, removed, or modified."
)

_COMPARISON_PROMPT_TEMPLATE_EN = (
    "Compare the following two versions of a Czech parliamentary bill and describe SPECIFIC differences:\n"
    "1. Which paragraphs/articles changed and how\n"
    "2. What was added or removed\n"
    "3. What is the overall character of changes (tightening/loosening/technical adjustment)\n"
    "3-4 sentences in English. Be specific — cite paragraph numbers.\n\n"
    "VERSION {ct1_old} ({label_old}):\n{text_old}\n\n"
    "VERSION {ct1_new} ({label_new}):\n{text_new} /no_think"
)

# Strip <think>...</think> blocks from Qwen3 responses (defensive)
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
        api_key: str = OLLAMA_API_KEY,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.timeout = timeout
        self._available: bool | None = None
        self._headers: dict[str, str] = {}
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
        if not response:
            return ""
        return self._strip_think(response)

    def consolidate_topics(self, all_topics: list[str]) -> dict[str, str]:
        """Consolidate/deduplicate topic labels via the LLM.

        Sends all unique topics to the model and asks it to merge
        similar/overlapping ones under canonical names.

        Returns dict mapping old_name -> canonical_name.
        Topics not in the response keep their original name.
        """
        topics_list = "\n".join(f"- {t}" for t in all_topics)
        prompt = _CONSOLIDATION_PROMPT_TEMPLATE.format(
            n=len(all_topics),
            topics_list=topics_list,
        )
        response = self._generate(prompt, _CONSOLIDATION_SYSTEM)
        if not response:
            return {t: t for t in all_topics}

        response = self._strip_think(response)
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

        # Any topics not in the mapping keep their original name
        for t in all_topics:
            if t not in mapping:
                mapping[t] = t

        return mapping

    def compare_versions(
        self,
        text_old: str,
        text_new: str,
        ct1_old: int,
        ct1_new: int,
        label_old: str = "",
        label_new: str = "",
    ) -> str:
        """Compare two versions of a tisk and return a Czech-language diff summary.

        Returns 3-4 sentence summary or empty string on failure.
        """
        trunc_old = truncate_legislative_text(text_old)
        trunc_new = truncate_legislative_text(text_new)
        prompt = _COMPARISON_PROMPT_TEMPLATE.format(
            ct1_old=ct1_old,
            ct1_new=ct1_new,
            label_old=label_old or f"CT1={ct1_old}",
            label_new=label_new or f"CT1={ct1_new}",
            text_old=trunc_old,
            text_new=trunc_new,
        )
        response = self._generate(prompt, _COMPARISON_SYSTEM)
        if not response:
            return ""
        return self._strip_think(response)

    def summarize_bilingual(self, text: str, title: str) -> dict[str, str]:
        """Generate both Czech and English summaries.

        Returns {"cs": ..., "en": ...}. Either may be empty on failure.
        """
        cs = self.summarize(text, title)
        truncated = truncate_legislative_text(text)
        prompt = _SUMMARY_PROMPT_TEMPLATE_EN.format(
            title=title or "(no title)",
            text=truncated,
        )
        response = self._generate(prompt, _SUMMARY_SYSTEM_EN)
        en = self._strip_think(response) if response else ""
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
        """Compare two versions and return bilingual diff summaries.

        Returns {"cs": ..., "en": ...}. Either may be empty on failure.
        """
        cs = self.compare_versions(text_old, text_new, ct1_old, ct1_new, label_old, label_new)
        trunc_old = truncate_legislative_text(text_old)
        trunc_new = truncate_legislative_text(text_new)
        prompt = _COMPARISON_PROMPT_TEMPLATE_EN.format(
            ct1_old=ct1_old,
            ct1_new=ct1_new,
            label_old=label_old or f"CT1={ct1_old}",
            label_new=label_new or f"CT1={ct1_new}",
            text_old=trunc_old,
            text_new=trunc_new,
        )
        response = self._generate(prompt, _COMPARISON_SYSTEM_EN)
        en = self._strip_think(response) if response else ""
        return {"cs": cs, "en": en}

    def _generate(self, prompt: str, system: str) -> str | None:
        """Send a generation request to Ollama. Returns response text or None."""
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

    @staticmethod
    def _strip_think(text: str) -> str:
        """Remove <think>...</think> blocks from Qwen3 responses."""
        return _THINK_RE.sub("", text).strip()

    def _parse_topics_response(self, response: str) -> list[str]:
        """Extract topic labels from LLM response.

        Expects format: TOPICS: topic1, topic2, topic3
        """
        response = self._strip_think(response)
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
            logger.debug("Failed to parse topics JSON: {}", raw)
    # Old format: single topic ID like "finance" or "justice"
    return [raw]
