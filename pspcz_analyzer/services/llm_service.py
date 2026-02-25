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
    LLM_HEALTH_TIMEOUT,
    LLM_MAX_COMPARISON_CHARS,
    LLM_MAX_TEXT_CHARS,
    LLM_PROVIDER,
    LLM_TIMEOUT,
    LLM_VERBATIM_CHARS,
    OLLAMA_API_KEY,
    OLLAMA_BASE_URL,
    OLLAMA_MODEL,
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

# ── Combined classify + summarize free-text prompts (Ollama fallback) ────

_COMBINED_SYSTEM_CS = (
    "Jsi kriticko-analytický komentátor českého parlamentu. Analyzuješ parlamentní tisky, "
    "přiřazuješ jim tematické štítky a píšeš ostré, věcné komentáře. "
    "Odpověz PŘESNĚ ve formátu:\n"
    "TOPICS: téma1, téma2, téma3\n"
    "CHANGES: co se konkrétně mění\n"
    "IMPACT: komu to prospívá a komu škodí\n"
    "RISKS: riziko zneužití nebo nezamýšlený důsledek\n"
    "Žádný další text." + _LANG_CS + _FORMAT_CS
)

_COMBINED_PROMPT_TEMPLATE_CS = (
    "Analyzuj následující parlamentní tisk.\n\n"
    "1. Urči 1–3 hlavní témata (krátké české názvy, 2–4 slova, konkrétní).\n"
    "2. Napiš kritickou analýzu: co se mění, komu to prospívá/škodí, jaká jsou rizika.\n\n"
    "Název:\n---BEGIN USER TEXT---\n{title}\n---END USER TEXT---\n\n"
    "Text:\n---BEGIN USER TEXT---\n{text}\n---END USER TEXT---\n\n"
    "Odpověz PŘESNĚ v tomto formátu:\n"
    "TOPICS: téma1, téma2, téma3\n"
    "CHANGES: co se konkrétně mění (1–2 věty)\n"
    "IMPACT: komu to prospívá a komu škodí (1–2 věty)\n"
    "RISKS: riziko zneužití (1–2 věty)"
)

_COMBINED_SYSTEM_EN = (
    "You are a critical analyst of the Czech Parliament. You analyze parliamentary bills, "
    "assign topic labels, and write sharp, factual assessments. "
    "Respond EXACTLY in this format:\n"
    "TOPICS: topic1, topic2, topic3\n"
    "CHANGES: what specifically changes\n"
    "IMPACT: who benefits and who is harmed\n"
    "RISKS: risk of abuse or unintended consequence\n"
    "No other text." + _LANG_EN + _FORMAT_EN
)

_COMBINED_PROMPT_TEMPLATE_EN = (
    "Analyze the following Czech parliamentary bill.\n\n"
    "1. Identify 1-3 main topics (short English names, 2-4 words, specific).\n"
    "2. Write a critical analysis: what changes, who benefits/is harmed, what are the risks.\n\n"
    "Title:\n---BEGIN USER TEXT---\n{title}\n---END USER TEXT---\n\n"
    "Text:\n---BEGIN USER TEXT---\n{text}\n---END USER TEXT---\n\n"
    "Respond EXACTLY in this format:\n"
    "TOPICS: topic1, topic2, topic3\n"
    "CHANGES: what specifically changes (1-2 sentences)\n"
    "IMPACT: who benefits and who is harmed (1-2 sentences)\n"
    "RISKS: risk of abuse (1-2 sentences)"
)

# ── Security & text processing ───────────────────────────────────────────


# ── JSON schemas for structured output (OpenAI-compatible) ──────────────

_CLASSIFICATION_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "topics": {
            "type": "array",
            "items": {"type": "string"},
            "description": "1-3 short topic labels",
        }
    },
    "required": ["topics"],
    "additionalProperties": False,
}

_SUMMARY_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "changes": {"type": "string", "description": "What specifically changes"},
        "impact": {"type": "string", "description": "Who benefits and who is harmed"},
        "risks": {"type": "string", "description": "Risk of abuse or unintended consequences"},
    },
    "required": ["changes", "impact", "risks"],
    "additionalProperties": False,
}

_COMPARISON_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "changed_paragraphs": {
            "type": "string",
            "description": "Which paragraphs/articles changed and how",
        },
        "additions_removals": {
            "type": "string",
            "description": "What was added or removed",
        },
        "overall_character": {
            "type": "string",
            "description": "Overall character: tightening/loosening/technical",
        },
    },
    "required": ["changed_paragraphs", "additions_removals", "overall_character"],
    "additionalProperties": False,
}

_CONSOLIDATION_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "mappings": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "old": {"type": "string"},
                    "canonical": {"type": "string"},
                },
                "required": ["old", "canonical"],
                "additionalProperties": False,
            },
        }
    },
    "required": ["mappings"],
    "additionalProperties": False,
}

_CLASSIFY_AND_SUMMARIZE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "topics": {
            "type": "array",
            "items": {"type": "string"},
            "description": "1-3 short topic labels",
        },
        "changes": {"type": "string", "description": "What specifically changes"},
        "impact": {"type": "string", "description": "Who benefits and who is harmed"},
        "risks": {"type": "string", "description": "Risk of abuse or unintended consequences"},
    },
    "required": ["topics", "changes", "impact", "risks"],
    "additionalProperties": False,
}

# ── Structured output prompts (no format instructions) ──────────────────

_STRUCTURED_CLASSIFICATION_SYSTEM_CS = (
    "Jsi analytik českého parlamentu. Analyzuješ parlamentní tisky a přiřazuješ jim tematické štítky. "
    "Používej stručné a konkrétní české názvy tematických oblastí." + _LANG_CS
)

_STRUCTURED_CLASSIFICATION_PROMPT_CS = (
    "Urči 1–3 hlavní témata následujícího parlamentního tisku. "
    "Použij krátké české názvy témat (2–4 slova). "
    "Buď konkrétní – např. místo 'Právo' napiš 'Trestní právo' nebo 'Občanské právo'.\n\n"
    "Název tisku:\n---BEGIN USER TEXT---\n{title}\n---END USER TEXT---\n\n"
    "Text tisku:\n---BEGIN USER TEXT---\n{text}\n---END USER TEXT---"
)

_STRUCTURED_CLASSIFICATION_SYSTEM_EN = (
    "You are a Czech Parliament analyst. You analyze parliamentary bills and assign topic labels. "
    "Use concise and specific English topic names." + _LANG_EN
)

_STRUCTURED_CLASSIFICATION_PROMPT_EN = (
    "Identify 1-3 main topics of the following Czech parliamentary bill. "
    "Use short English topic names (2-4 words). "
    "Be specific — e.g. instead of 'Law' write 'Criminal Law' or 'Civil Law'.\n\n"
    "Bill title:\n---BEGIN USER TEXT---\n{title}\n---END USER TEXT---\n\n"
    "Bill text:\n---BEGIN USER TEXT---\n{text}\n---END USER TEXT---"
)

_STRUCTURED_SUMMARY_SYSTEM_CS = (
    "Jsi kriticko-analytický komentátor českého parlamentu. Píšeš ostře, věcně a bez přikrašlování. "
    "Odhaluješ skryté dopady zákonů, rizika zneužití, a kdo z novely skutečně profituje. "
    "Neboj se pojmenovat problémy přímo — např. oslabení nezávislosti úředníků, rozšíření pravomocí "
    "bez kontroly, skryté privatizace, nebo omezení občanských práv." + _LANG_CS
)

_STRUCTURED_SUMMARY_PROMPT_CS = (
    "Analyzuj následující parlamentní tisk KRITICKY.\n"
    "Pro pole 'changes': Co KONKRÉTNĚ se mění (žádné vágní formulace)\n"
    "Pro pole 'impact': Komu to prospívá a komu škodí\n"
    "Pro pole 'risks': Jaké je RIZIKO zneužití nebo nezamýšlený důsledek\n"
    "Buď přímý a kriticko-analytický. 1–2 věty na pole.\n\n"
    "Název:\n---BEGIN USER TEXT---\n{title}\n---END USER TEXT---\n\n"
    "Text:\n---BEGIN USER TEXT---\n{text}\n---END USER TEXT---"
)

_STRUCTURED_SUMMARY_SYSTEM_EN = (
    "You are a critical analyst of the Czech Parliament. You write sharp, factual assessments "
    "without embellishment. You expose hidden impacts of laws, risks of abuse, and who truly "
    "benefits from amendments. Don't hesitate to name problems directly." + _LANG_EN
)

_STRUCTURED_SUMMARY_PROMPT_EN = (
    "Analyze the following Czech parliamentary bill CRITICALLY.\n"
    "For 'changes': What SPECIFICALLY changes (no vague formulations)\n"
    "For 'impact': Who benefits and who is harmed\n"
    "For 'risks': What is the RISK of abuse or unintended consequence\n"
    "Be direct and critical. 1-2 sentences per field.\n\n"
    "Title:\n---BEGIN USER TEXT---\n{title}\n---END USER TEXT---\n\n"
    "Text:\n---BEGIN USER TEXT---\n{text}\n---END USER TEXT---"
)

_STRUCTURED_CONSOLIDATION_SYSTEM_CS = (
    "Jsi analytik českého parlamentu. Dostaneš seznam tematických štítků. "
    "Sjednoť podobná/překrývající se témata pod jeden kanonický název." + _LANG_CS
)

_STRUCTURED_CONSOLIDATION_PROMPT_CS = (
    "Zde je seznam {n} témat z parlamentních tisků. Sjednoť podobná a překrývající se témata.\n"
    "Pro každé téma vrať mapování z původního názvu na kanonický název.\n"
    "Pokud je téma už trefné, mapuj ho samo na sebe.\n\n"
    "Témata:\n{topics_list}"
)

_STRUCTURED_CONSOLIDATION_SYSTEM_EN = (
    "You are a Czech Parliament analyst. You will receive a list of topic labels. "
    "Unify similar/overlapping topics under one canonical English name." + _LANG_EN
)

_STRUCTURED_CONSOLIDATION_PROMPT_EN = (
    "Here is a list of {n} topics from parliamentary bills. Unify similar and overlapping topics.\n"
    "For each topic return a mapping from original name to canonical name.\n"
    "If a topic is already good, map it to itself.\n\n"
    "Topics:\n{topics_list}"
)

_STRUCTURED_COMPARISON_SYSTEM_CS = (
    "Jsi analyticko-právní expert na českou legislativu. Srovnáváš verze parlamentních tisků "
    "a identifikuješ KONKRÉTNÍ změny mezi nimi — čísla paragrafů, co bylo přidáno, odebráno či změněno."
    + _LANG_CS
)

_STRUCTURED_COMPARISON_PROMPT_CS = (
    "Porovnej následující dvě verze parlamentního tisku a popiš KONKRÉTNÍ rozdíly.\n"
    "Pro pole 'changed_paragraphs': Které paragrafy/články se změnily a jak\n"
    "Pro pole 'additions_removals': Co bylo přidáno nebo odebráno\n"
    "Pro pole 'overall_character': Celkový charakter změn (zpřísnění/zmírnění/technická úprava)\n"
    "Buď konkrétní — cituj čísla paragrafů. 1–2 věty na pole.\n\n"
    "VERZE {ct1_old} ({label_old}):\n---BEGIN USER TEXT---\n{text_old}\n---END USER TEXT---\n\n"
    "VERZE {ct1_new} ({label_new}):\n---BEGIN USER TEXT---\n{text_new}\n---END USER TEXT---"
)

_STRUCTURED_COMPARISON_SYSTEM_EN = (
    "You are a legal expert on Czech legislation. You compare versions of parliamentary bills "
    "and identify SPECIFIC changes between them — paragraph numbers, what was added, removed, or modified."
    + _LANG_EN
)

_STRUCTURED_COMPARISON_PROMPT_EN = (
    "Compare the following two versions of a Czech parliamentary bill and describe SPECIFIC differences.\n"
    "For 'changed_paragraphs': Which paragraphs/articles changed and how\n"
    "For 'additions_removals': What was added or removed\n"
    "For 'overall_character': Overall character of changes (tightening/loosening/technical adjustment)\n"
    "Be specific — cite paragraph numbers. 1-2 sentences per field.\n\n"
    "VERSION {ct1_old} ({label_old}):\n---BEGIN USER TEXT---\n{text_old}\n---END USER TEXT---\n\n"
    "VERSION {ct1_new} ({label_new}):\n---BEGIN USER TEXT---\n{text_new}\n---END USER TEXT---"
)

# ── Combined classify + summarize structured prompts ─────────────────────

_STRUCTURED_CLASSIFY_AND_SUMMARIZE_SYSTEM_CS = (
    "Jsi kriticko-analytický komentátor českého parlamentu. Analyzuješ parlamentní tisky, "
    "přiřazuješ jim tematické štítky a píšeš ostré, věcné komentáře bez přikrašlování. "
    "Odhaluješ skryté dopady zákonů, rizika zneužití, a kdo z novely skutečně profituje. "
    "Neboj se pojmenovat problémy přímo." + _LANG_CS
)

_STRUCTURED_CLASSIFY_AND_SUMMARIZE_PROMPT_CS = (
    "Analyzuj následující parlamentní tisk.\n\n"
    "ÚKOL 1 — TÉMATA: Urči 1–3 hlavní témata. Použij krátké české názvy (2–4 slova). "
    "Buď konkrétní — např. 'Trestní právo', ne 'Právo'.\n\n"
    "ÚKOL 2 — KRITICKÁ ANALÝZA:\n"
    "Pro pole 'changes': Co KONKRÉTNĚ se mění (žádné vágní formulace)\n"
    "Pro pole 'impact': Komu to prospívá a komu škodí\n"
    "Pro pole 'risks': Jaké je RIZIKO zneužití nebo nezamýšlený důsledek\n"
    "Buď přímý a kriticko-analytický. 1–2 věty na pole.\n\n"
    "Název:\n---BEGIN USER TEXT---\n{title}\n---END USER TEXT---\n\n"
    "Text:\n---BEGIN USER TEXT---\n{text}\n---END USER TEXT---"
)

_STRUCTURED_CLASSIFY_AND_SUMMARIZE_SYSTEM_EN = (
    "You are a critical analyst of the Czech Parliament. You analyze parliamentary bills, "
    "assign topic labels, and write sharp, factual assessments without embellishment. "
    "You expose hidden impacts of laws, risks of abuse, and who truly benefits from amendments. "
    "Don't hesitate to name problems directly." + _LANG_EN
)

_STRUCTURED_CLASSIFY_AND_SUMMARIZE_PROMPT_EN = (
    "Analyze the following Czech parliamentary bill.\n\n"
    "TASK 1 — TOPICS: Identify 1-3 main topics. Use short English names (2-4 words). "
    "Be specific — e.g. 'Criminal Law', not 'Law'.\n\n"
    "TASK 2 — CRITICAL ANALYSIS:\n"
    "For 'changes': What SPECIFICALLY changes (no vague formulations)\n"
    "For 'impact': Who benefits and who is harmed\n"
    "For 'risks': What is the RISK of abuse or unintended consequence\n"
    "Be direct and critical. 1-2 sentences per field.\n\n"
    "Title:\n---BEGIN USER TEXT---\n{title}\n---END USER TEXT---\n\n"
    "Text:\n---BEGIN USER TEXT---\n{text}\n---END USER TEXT---"
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


# ── Base class ───────────────────────────────────────────────────────────


# ── Markdown rendering helpers for structured output ────────────────────


def _render_summary_markdown_cs(data: dict[str, Any]) -> str:
    """Render structured summary JSON to Czech markdown."""
    parts: list[str] = []
    if data.get("changes"):
        parts.append(f"**Co se mění:** {data['changes']}")
    if data.get("impact"):
        parts.append(f"**Dopady:** {data['impact']}")
    if data.get("risks"):
        parts.append(f"**Rizika:** {data['risks']}")
    return "\n\n".join(parts)


def _render_summary_markdown_en(data: dict[str, Any]) -> str:
    """Render structured summary JSON to English markdown."""
    parts: list[str] = []
    if data.get("changes"):
        parts.append(f"**Changes:** {data['changes']}")
    if data.get("impact"):
        parts.append(f"**Impact:** {data['impact']}")
    if data.get("risks"):
        parts.append(f"**Risks:** {data['risks']}")
    return "\n\n".join(parts)


def _render_comparison_markdown_cs(data: dict[str, Any]) -> str:
    """Render structured comparison JSON to Czech markdown."""
    parts: list[str] = []
    if data.get("changed_paragraphs"):
        parts.append(f"**Změněné paragrafy:** {data['changed_paragraphs']}")
    if data.get("additions_removals"):
        parts.append(f"**Přidáno/odebráno:** {data['additions_removals']}")
    if data.get("overall_character"):
        parts.append(f"**Charakter změn:** {data['overall_character']}")
    return "\n\n".join(parts)


def _render_comparison_markdown_en(data: dict[str, Any]) -> str:
    """Render structured comparison JSON to English markdown."""
    parts: list[str] = []
    if data.get("changed_paragraphs"):
        parts.append(f"**Changed paragraphs:** {data['changed_paragraphs']}")
    if data.get("additions_removals"):
        parts.append(f"**Additions/removals:** {data['additions_removals']}")
    if data.get("overall_character"):
        parts.append(f"**Overall character:** {data['overall_character']}")
    return "\n\n".join(parts)


def _parse_consolidation_json(data: dict[str, Any], all_topics: list[str]) -> dict[str, str]:
    """Parse structured consolidation JSON into a topic mapping dict."""
    mapping: dict[str, str] = {}
    for item in data.get("mappings", []):
        old = item.get("old", "").strip()
        canonical = item.get("canonical", "").strip()
        if old and canonical:
            mapping[old] = canonical
    for t in all_topics:
        if t not in mapping:
            mapping[t] = t
    return mapping


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
        self,
        prompt: str,
        system: str,
        *,
        response_format: dict[str, Any] | None = None,
    ) -> str | None:
        """Send a generation request to the LLM. Returns response text or None."""
        ...

    @property
    def supports_structured_output(self) -> bool:
        """Whether this backend supports structured output (JSON schema constraint)."""
        return False

    def _generate_json(
        self,
        prompt: str,
        system: str,
        schema: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Generate a structured JSON response using response_format.

        Args:
            prompt: The user prompt.
            system: The system prompt.
            schema: JSON schema dict for the response format.

        Returns:
            Parsed dict on success, None on failure.
        """
        response_format = {
            "type": "json_schema",
            "json_schema": {"name": "response", "strict": True, "schema": schema},
        }
        raw = self._generate(
            prompt,
            system,
            response_format=response_format,
        )
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            logger.debug("{} Failed to parse JSON response: {}", self._log_prefix, raw[:200])
            return None

    def classify_topics(self, text: str, title: str, *, _truncated: bool = False) -> list[str]:
        """Classify a tisk into 1-3 free-form Czech topic labels using the LLM."""
        truncated = text if _truncated else truncate_legislative_text(text)
        sanitized_title = _sanitize_llm_input(title or "(bez názvu)")
        sanitized_text = _sanitize_llm_input(truncated)

        if self.supports_structured_output:
            prompt = _STRUCTURED_CLASSIFICATION_PROMPT_CS.format(
                title=sanitized_title, text=sanitized_text
            )
            data = self._generate_json(
                prompt,
                _STRUCTURED_CLASSIFICATION_SYSTEM_CS,
                _CLASSIFICATION_SCHEMA,
            )
            if data is None:
                return []
            topics = [t.strip() for t in data.get("topics", []) if isinstance(t, str) and t.strip()]
            return topics[:3]

        prompt = _CLASSIFICATION_PROMPT_TEMPLATE.format(title=sanitized_title, text=sanitized_text)
        response = self._generate(prompt, _CLASSIFICATION_SYSTEM)
        if response is None:
            return []
        return self._parse_topics_response(response)

    def summarize(self, text: str, title: str, *, _truncated: bool = False) -> str:
        """Generate a Czech-language summary of what a proposed law changes."""
        truncated = text if _truncated else truncate_legislative_text(text)
        sanitized_title = _sanitize_llm_input(title or "(bez názvu)")
        sanitized_text = _sanitize_llm_input(truncated)

        if self.supports_structured_output:
            prompt = _STRUCTURED_SUMMARY_PROMPT_CS.format(
                title=sanitized_title, text=sanitized_text
            )
            data = self._generate_json(
                prompt,
                _STRUCTURED_SUMMARY_SYSTEM_CS,
                _SUMMARY_SCHEMA,
            )
            if data is None:
                return ""
            return _render_summary_markdown_cs(data)

        prompt = _SUMMARY_PROMPT_TEMPLATE.format(title=sanitized_title, text=sanitized_text)
        response = self._generate(prompt, _SUMMARY_SYSTEM)
        if not response:
            return ""
        return self._strip_think(response)

    def summarize_en(self, text: str, title: str, *, _truncated: bool = False) -> str:
        """Generate an English-language critical summary of a proposed law."""
        truncated = text if _truncated else truncate_legislative_text(text)
        sanitized_title = _sanitize_llm_input(title or "(no title)")
        sanitized_text = _sanitize_llm_input(truncated)

        if self.supports_structured_output:
            prompt = _STRUCTURED_SUMMARY_PROMPT_EN.format(
                title=sanitized_title, text=sanitized_text
            )
            data = self._generate_json(
                prompt,
                _STRUCTURED_SUMMARY_SYSTEM_EN,
                _SUMMARY_SCHEMA,
            )
            if data is None:
                return ""
            return _render_summary_markdown_en(data)

        prompt = _SUMMARY_PROMPT_TEMPLATE_EN.format(title=sanitized_title, text=sanitized_text)
        response = self._generate(prompt, _SUMMARY_SYSTEM_EN)
        if not response:
            return ""
        return self._strip_think(response)

    def consolidate_topics(self, all_topics: list[str]) -> dict[str, str]:
        """Consolidate/deduplicate topic labels via the LLM."""
        topics_list = "\n".join(f"- {t}" for t in all_topics)

        if self.supports_structured_output:
            prompt = _STRUCTURED_CONSOLIDATION_PROMPT_CS.format(
                n=len(all_topics), topics_list=topics_list
            )
            data = self._generate_json(
                prompt,
                _STRUCTURED_CONSOLIDATION_SYSTEM_CS,
                _CONSOLIDATION_SCHEMA,
            )
            if data is None:
                return {t: t for t in all_topics}
            return _parse_consolidation_json(data, all_topics)

        prompt = _CONSOLIDATION_PROMPT_TEMPLATE.format(n=len(all_topics), topics_list=topics_list)
        response = self._generate(prompt, _CONSOLIDATION_SYSTEM)
        if not response:
            return {t: t for t in all_topics}
        return self._parse_consolidation_response(response, all_topics)

    def classify_topics_en(self, text: str, title: str, *, _truncated: bool = False) -> list[str]:
        """Classify a tisk into 1-3 free-form English topic labels using the LLM."""
        truncated = text if _truncated else truncate_legislative_text(text)
        sanitized_title = _sanitize_llm_input(title or "(no title)")
        sanitized_text = _sanitize_llm_input(truncated)

        if self.supports_structured_output:
            prompt = _STRUCTURED_CLASSIFICATION_PROMPT_EN.format(
                title=sanitized_title, text=sanitized_text
            )
            data = self._generate_json(
                prompt,
                _STRUCTURED_CLASSIFICATION_SYSTEM_EN,
                _CLASSIFICATION_SCHEMA,
            )
            if data is None:
                return []
            topics = [t.strip() for t in data.get("topics", []) if isinstance(t, str) and t.strip()]
            return topics[:3]

        prompt = _CLASSIFICATION_PROMPT_TEMPLATE_EN.format(
            title=sanitized_title, text=sanitized_text
        )
        response = self._generate(prompt, _CLASSIFICATION_SYSTEM_EN)
        if response is None:
            return []
        return self._parse_topics_response(response)

    def classify_topics_bilingual(self, text: str, title: str) -> tuple[list[str], list[str]]:
        """Classify a tisk into topic labels in both Czech and English."""
        truncated = truncate_legislative_text(text)
        topics_cs = self.classify_topics(truncated, title, _truncated=True)
        topics_en = self.classify_topics_en(truncated, title, _truncated=True)
        return topics_cs, topics_en

    def consolidate_topics_en(self, all_topics: list[str]) -> dict[str, str]:
        """Consolidate/deduplicate English topic labels via the LLM."""
        topics_list = "\n".join(f"- {t}" for t in all_topics)

        if self.supports_structured_output:
            prompt = _STRUCTURED_CONSOLIDATION_PROMPT_EN.format(
                n=len(all_topics), topics_list=topics_list
            )
            data = self._generate_json(
                prompt,
                _STRUCTURED_CONSOLIDATION_SYSTEM_EN,
                _CONSOLIDATION_SCHEMA,
            )
            if data is None:
                return {t: t for t in all_topics}
            return _parse_consolidation_json(data, all_topics)

        prompt = _CONSOLIDATION_PROMPT_TEMPLATE_EN.format(
            n=len(all_topics), topics_list=topics_list
        )
        response = self._generate(prompt, _CONSOLIDATION_SYSTEM_EN)
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
        *,
        _truncated: bool = False,
    ) -> str:
        """Compare two versions of a tisk and return a Czech-language diff summary."""
        trunc_old = (
            text_old
            if _truncated
            else truncate_legislative_text(text_old, max_chars=LLM_MAX_COMPARISON_CHARS)
        )
        trunc_new = (
            text_new
            if _truncated
            else truncate_legislative_text(text_new, max_chars=LLM_MAX_COMPARISON_CHARS)
        )
        fmt_kwargs = {
            "ct1_old": ct1_old,
            "ct1_new": ct1_new,
            "label_old": label_old or f"CT1={ct1_old}",
            "label_new": label_new or f"CT1={ct1_new}",
            "text_old": _sanitize_llm_input(trunc_old),
            "text_new": _sanitize_llm_input(trunc_new),
        }

        if self.supports_structured_output:
            prompt = _STRUCTURED_COMPARISON_PROMPT_CS.format(**fmt_kwargs)
            data = self._generate_json(
                prompt,
                _STRUCTURED_COMPARISON_SYSTEM_CS,
                _COMPARISON_SCHEMA,
            )
            if data is None:
                return ""
            return _render_comparison_markdown_cs(data)

        prompt = _COMPARISON_PROMPT_TEMPLATE.format(**fmt_kwargs)
        response = self._generate(prompt, _COMPARISON_SYSTEM)
        if not response:
            return ""
        return self._strip_think(response)

    def summarize_bilingual(self, text: str, title: str) -> dict[str, str]:
        """Generate both Czech and English summaries."""
        truncated = truncate_legislative_text(text)
        cs = self.summarize(truncated, title, _truncated=True)
        en = self.summarize_en(truncated, title, _truncated=True)
        return {"cs": cs, "en": en}

    def _parse_combined_response(self, response: str) -> tuple[list[str], str]:
        """Parse combined classify+summarize free-text response.

        Expects format:
            TOPICS: topic1, topic2, topic3
            CHANGES: ...
            IMPACT: ...
            RISKS: ...

        Returns:
            (topics, summary_markdown) — partial success is acceptable.
        """
        response = self._strip_think(response)

        # Extract topics
        topics: list[str] = []
        topics_match = re.search(r"TOPICS?:\s*(.+)", response, re.IGNORECASE)
        if topics_match:
            raw = topics_match.group(1).strip()
            topics = [t.strip().strip(".,;:-–") for t in raw.split(",")]
            topics = [t for t in topics if t and t.lower() != "none"]
            topics = topics[:3]

        # Extract summary fields
        changes = ""
        impact = ""
        risks = ""

        changes_match = re.search(
            r"CHANGES?:\s*(.+?)(?=\n(?:IMPACT|RISKS?)\s*:|$)", response, re.IGNORECASE | re.DOTALL
        )
        if changes_match:
            changes = changes_match.group(1).strip()

        impact_match = re.search(
            r"IMPACT:\s*(.+?)(?=\n(?:RISKS?)\s*:|$)", response, re.IGNORECASE | re.DOTALL
        )
        if impact_match:
            impact = impact_match.group(1).strip()

        risks_match = re.search(r"RISKS?:\s*(.+)", response, re.IGNORECASE | re.DOTALL)
        if risks_match:
            risks = risks_match.group(1).strip()

        return topics, {"changes": changes, "impact": impact, "risks": risks}

    def classify_and_summarize(
        self, text: str, title: str, *, _truncated: bool = False
    ) -> tuple[list[str], str]:
        """Classify topics + summarize in a single LLM call (Czech).

        Returns:
            (topics, summary_markdown) — ([], "") on failure.
        """
        truncated = text if _truncated else truncate_legislative_text(text)
        sanitized_title = _sanitize_llm_input(title or "(bez názvu)")
        sanitized_text = _sanitize_llm_input(truncated)

        if self.supports_structured_output:
            prompt = _STRUCTURED_CLASSIFY_AND_SUMMARIZE_PROMPT_CS.format(
                title=sanitized_title, text=sanitized_text
            )
            data = self._generate_json(
                prompt,
                _STRUCTURED_CLASSIFY_AND_SUMMARIZE_SYSTEM_CS,
                _CLASSIFY_AND_SUMMARIZE_SCHEMA,
            )
            if data is None:
                return [], ""
            topics = [t.strip() for t in data.get("topics", []) if isinstance(t, str) and t.strip()]
            return topics[:3], _render_summary_markdown_cs(data)

        prompt = _COMBINED_PROMPT_TEMPLATE_CS.format(title=sanitized_title, text=sanitized_text)
        response = self._generate(prompt, _COMBINED_SYSTEM_CS)
        if response is None:
            return [], ""
        topics, summary_data = self._parse_combined_response(response)
        return topics, _render_summary_markdown_cs(summary_data)

    def classify_and_summarize_en(
        self, text: str, title: str, *, _truncated: bool = False
    ) -> tuple[list[str], str]:
        """Classify topics + summarize in a single LLM call (English).

        Returns:
            (topics, summary_markdown) — ([], "") on failure.
        """
        truncated = text if _truncated else truncate_legislative_text(text)
        sanitized_title = _sanitize_llm_input(title or "(no title)")
        sanitized_text = _sanitize_llm_input(truncated)

        if self.supports_structured_output:
            prompt = _STRUCTURED_CLASSIFY_AND_SUMMARIZE_PROMPT_EN.format(
                title=sanitized_title, text=sanitized_text
            )
            data = self._generate_json(
                prompt,
                _STRUCTURED_CLASSIFY_AND_SUMMARIZE_SYSTEM_EN,
                _CLASSIFY_AND_SUMMARIZE_SCHEMA,
            )
            if data is None:
                return [], ""
            topics = [t.strip() for t in data.get("topics", []) if isinstance(t, str) and t.strip()]
            return topics[:3], _render_summary_markdown_en(data)

        prompt = _COMBINED_PROMPT_TEMPLATE_EN.format(title=sanitized_title, text=sanitized_text)
        response = self._generate(prompt, _COMBINED_SYSTEM_EN)
        if response is None:
            return [], ""
        topics, summary_data = self._parse_combined_response(response)
        return topics, _render_summary_markdown_en(summary_data)

    def classify_and_summarize_bilingual(
        self, text: str, title: str
    ) -> tuple[list[str], list[str], str, str]:
        """Classify + summarize in both languages (2 LLM calls instead of 4).

        Returns:
            (topics_cs, topics_en, summary_cs, summary_en)
        """
        truncated = truncate_legislative_text(text)
        topics_cs, summary_cs = self.classify_and_summarize(truncated, title, _truncated=True)
        topics_en, summary_en = self.classify_and_summarize_en(truncated, title, _truncated=True)
        return topics_cs, topics_en, summary_cs, summary_en

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
        trunc_old = truncate_legislative_text(text_old, max_chars=LLM_MAX_COMPARISON_CHARS)
        trunc_new = truncate_legislative_text(text_new, max_chars=LLM_MAX_COMPARISON_CHARS)
        cs = self.compare_versions(
            trunc_old, trunc_new, ct1_old, ct1_new, label_old, label_new, _truncated=True
        )
        fmt_kwargs = {
            "ct1_old": ct1_old,
            "ct1_new": ct1_new,
            "label_old": label_old or f"CT1={ct1_old}",
            "label_new": label_new or f"CT1={ct1_new}",
            "text_old": _sanitize_llm_input(trunc_old),
            "text_new": _sanitize_llm_input(trunc_new),
        }

        if self.supports_structured_output:
            prompt = _STRUCTURED_COMPARISON_PROMPT_EN.format(**fmt_kwargs)
            data = self._generate_json(
                prompt,
                _STRUCTURED_COMPARISON_SYSTEM_EN,
                _COMPARISON_SCHEMA,
            )
            en = _render_comparison_markdown_en(data) if data else ""
        else:
            prompt = _COMPARISON_PROMPT_TEMPLATE_EN.format(**fmt_kwargs)
            response = self._generate(prompt, _COMPARISON_SYSTEM_EN)
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
        timeout: float = LLM_TIMEOUT,
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
                timeout=LLM_HEALTH_TIMEOUT,
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
        self,
        prompt: str,
        system: str,
        *,
        response_format: dict[str, Any] | None = None,
    ) -> str | None:
        """Send a generation request to Ollama. Returns response text or None.

        The response_format parameter is accepted but ignored
        (Ollama uses free-text prompts with regex parsing).
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
        timeout: float = LLM_TIMEOUT,
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
                timeout=LLM_HEALTH_TIMEOUT,
            )
            resp.raise_for_status()
            self._available = True
            logger.info("[openai] Available at {} with model {}", self.base_url, self.model)
        except Exception:
            self._available = False
            logger.info("[openai] Not available (connection to {} failed)", self.base_url)

        return self._available

    @property
    def supports_structured_output(self) -> bool:
        """OpenAI-compatible APIs support structured output via response_format."""
        return True

    def _generate(
        self,
        prompt: str,
        system: str,
        *,
        response_format: dict[str, Any] | None = None,
    ) -> str | None:
        """Send a chat completion request. Returns response text or None.

        When response_format is provided, constrains the output to the given JSON schema.
        """
        payload: dict[str, Any] = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": prompt},
            ],
            "stream": False,
        }
        if response_format is not None:
            payload["response_format"] = response_format

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
