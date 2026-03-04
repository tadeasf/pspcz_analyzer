"""LLM response parsing and rendering helpers."""

from __future__ import annotations

import json
import re
from typing import Any

from loguru import logger

from pspcz_analyzer.services.llm.helpers import _THINK_RE

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


def _format_amendments_list(amendments: list[dict[str, str]]) -> str:
    """Format amendment metadata into a prompt-friendly list.

    Args:
        amendments: List of dicts with 'letter', 'submitter', 'description' keys.

    Returns:
        Formatted string like "- A (Berkovce): legislativně-technická oprava".
    """
    lines: list[str] = []
    for a in amendments:
        letter = a.get("letter", "?")
        submitter = a.get("submitter", "")
        description = a.get("description", "")
        parts = [f"- {letter}"]
        if submitter:
            parts.append(f"({submitter})")
        if description:
            parts.append(f": {description}")
        lines.append(" ".join(parts))
    return "\n".join(lines)


def _parse_amendment_summaries_json(data: dict[str, Any]) -> dict[str, str]:
    """Extract {letter: summary} from structured JSON response.

    Args:
        data: Parsed JSON matching _AMENDMENT_SUMMARIES_SCHEMA.

    Returns:
        Dict mapping normalized (uppercase stripped) amendment letter to summary text.
    """
    result: dict[str, str] = {}
    for item in data.get("amendments", []):
        letter = item.get("letter", "").strip().upper()
        summary = item.get("summary", "").strip()
        if letter and summary:
            result[letter] = summary
    return result


def _parse_amendment_summaries_text(response: str) -> dict[str, str]:
    """Parse 'LETTER: summary' lines from free-text response.

    Args:
        response: Raw LLM response text.

    Returns:
        Dict mapping normalized (uppercase stripped) amendment letter to summary text.
    """
    response = _THINK_RE.sub("", response).strip()
    result: dict[str, str] = {}
    for line in response.splitlines():
        line = line.strip()
        if not line:
            continue
        # Match patterns like "A: summary", "B1: summary", "- A: summary", "* b2 – summary"
        match = re.match(r"^[-*•]?\s*([A-Za-z]\d*(?:\s*[aA]\s*[A-Za-z]\d*)?)\s*[:–—-]\s*(.+)", line)
        if match:
            letter = match.group(1).strip().upper()
            summary = match.group(2).strip()
            if letter and summary:
                result[letter] = summary
    return result


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
