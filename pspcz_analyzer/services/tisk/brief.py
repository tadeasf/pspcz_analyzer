"""Short descriptive AI brief per bill (#109) with an automatic output check (#97).

One JSON file per tisk in ``tisky_meta/{period}/briefs/{ct}.json``: a plain
title, a one/two-sentence summary, who it concerns, amounts and the proposed
start date, in Czech and English, plus provenance (model, date, source text
hash) and the issues found by :func:`brief_issues`.
"""

import hashlib
import json
import re
import unicodedata
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl
from loguru import logger

from pspcz_analyzer.config import (
    LLM_EMPTY_RETRIES,
    PARQUET_DIR,
    PERIOD_ORGAN_IDS,
    TISKY_META_DIR,
    TISKY_TEXT_DIR,
)
from pspcz_analyzer.fileio import write_json_atomic
from pspcz_analyzer.services.llm import LLMClient, create_llm_client, llm_map
from pspcz_analyzer.services.llm.helpers import _sanitize_llm_input

BRIEFS_DIR = "briefs"
BRIEF_VERSION = 2  # bump when the prompt or schema changes: all briefs regenerate
BRIEF_DRUHY = (1, 2, 3)  # vládní návrh zákona, návrh zákona, státní rozpočet
# ~3 chars per token for Czech: 480k chars stays well inside a 262k-token context
BRIEF_MAX_CHARS = 480_000

FIELDS = ("nazev", "shrnuti", "koho", "kolik", "od_kdy")

_SYSTEM = (
    "Píšeš krátké věcné popisy návrhů zákonů pro běžné čtenáře. Jen popisuješ, co návrh "
    "mění. Nehodnotíš, nekritizuješ, nepíšeš o rizicích, motivech ani o tom, komu to "
    "prospěje. Používáš jen údaje, které stojí v textu tisku; nic nedomýšlíš. Píšeš "
    "srozumitelně, bez právnického žargonu, bez markdownu a bez odkazů na paragrafy. "
    "Česká pole píšeš česky, pole končící _en anglicky."
)

_PROMPT = """Z textu sněmovního tisku vyplň pole:
- nazev: věcný název o 2 až 6 slovech, co návrh dělá (např. "Zmrazení ceny dálniční známky"). Ne oficiální název.
- shrnuti: jedna, nejvýš dvě věty (do 45 slov): co se má změnit.
- koho: komu se změna týká, krátká fráze ve 2. pádě navazující na "Týká se ..." (např. "řidičů, kteří potřebují dálniční známku").
- kolik: hlavní částky nebo sazby, které se čtenáře přímo týkají (ceny, poplatky, dávky, daně, pokuty), nejvýš 8, každá s tím, čeho se týká, čísla přesně jak stojí v textu. Když návrh stanoví jednu sazbu ve více variantách (např. na rok, 30 dní, 10 dní a 1 den), uveď všechny varianty, např. "roční 2 570 Kč, 30denní 480 Kč, 10denní 300 Kč, jednodenní 230 Kč". Ne lhůty, počty členů ani délky funkčních období. Když žádné nejsou, prázdný řetězec.
- od_kdy: navržená účinnost (např. "od 1. 1. 2027"). Když ji text neuvádí, prázdný řetězec.
- nazev_en, shrnuti_en, koho_en, kolik_en, od_kdy_en: totéž anglicky.

Název tisku:
---BEGIN USER TEXT---
{title}
---END USER TEXT---

Text tisku:
---BEGIN USER TEXT---
{text}
---END USER TEXT---"""

_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {f: {"type": "string"} for f in (*FIELDS, *(f"{f}_en" for f in FIELDS))},
    "required": [*FIELDS, *(f"{f}_en" for f in FIELDS)],
    "additionalProperties": False,
}

_NUMBER_RE = re.compile(r"\d+(?:[  .]\d{3})*")
_ENGLISH_RE = re.compile(r"\b(?:the|and|of|is|will|with|for)\b", re.IGNORECASE)
# English word forms Czech never has ("definition", "awareness"); one is enough (#97)
_ENGLISH_FORM_RE = re.compile(r"\b[a-z]+(?:tion|tions|ness)\b")
_STRAY_SIGN_RE = re.compile(r"×(?!\s?\d)|[¤�]")  # "×" not in "3 × 2"
_SENTENCE_END_RE = re.compile(r"[.!?…\"“”)»]\s*$")


def _english(text: str) -> bool:
    """English drift: two common English words or one English-only word form."""
    return len(_ENGLISH_RE.findall(text)) >= 2 or bool(_ENGLISH_FORM_RE.search(text))


def _numbers(text: str) -> set[str]:
    """Numbers in ``text`` with thousands separators removed ("2 570" -> "2570")."""
    return {re.sub(r"\D", "", m) for m in _NUMBER_RE.findall(text)}


def _non_latin(text: str) -> bool:
    """True if ``text`` has a letter outside the Latin script (e.g. Cyrillic drift)."""
    return any(ch.isalpha() and not unicodedata.name(ch, "").startswith("LATIN") for ch in text)


def text_issues(cs: str, en: str, source: str) -> list[str]:
    """Check one Czech/English AI text pair (e.g. an amendment summary) against its source.

    Args:
        cs: Czech text.
        en: English text.
        source: Text it was generated from.

    Returns:
        Issue codes: ``script``, ``english``, ``sign``, ``number:<n>``; empty when it passes.
    """
    issues = ["script"] if _non_latin(cs) or _non_latin(en) else []
    if _english(cs):
        issues.append("english")
    if _STRAY_SIGN_RE.search(cs):
        issues.append("sign")
    return issues + [f"number:{n}" for n in sorted(_numbers(cs) - _numbers(source))]


def brief_issues(cs: dict[str, str], en: dict[str, str], source: str) -> list[str]:
    """Check a generated brief against its source text.

    Args:
        cs: Czech fields.
        en: English fields.
        source: Text the brief was generated from.

    Returns:
        Issue codes, empty when the brief passes: ``empty``, ``script:<field>``,
        ``english:<field>``, ``sign:<field>``, ``unfinished:shrnuti``, ``number:<n>``.
    """
    issues: list[str] = []
    if not cs.get("nazev") or not cs.get("shrnuti"):
        issues.append("empty")
    for lang, fields in (("cs", cs), ("en", en)):
        issues += [f"script:{f}_{lang}" for f, v in fields.items() if _non_latin(v)]
    issues += [f"english:{f}" for f, v in cs.items() if _english(v)]
    issues += [f"sign:{f}" for f, v in cs.items() if _STRAY_SIGN_RE.search(v)]
    if cs.get("shrnuti") and not _SENTENCE_END_RE.search(cs["shrnuti"]):
        issues.append("unfinished:shrnuti")
    known = _numbers(source)
    cs_text = " ".join(cs.get(f, "") for f in ("shrnuti", "kolik", "od_kdy"))
    issues += [f"number:{n}" for n in sorted(_numbers(cs_text) - known)]
    return issues


def _fit(text: str) -> tuple[str, bool]:
    """Keep head and tail of an over-long text (the explanatory report is at the end)."""
    if len(text) <= BRIEF_MAX_CHARS:
        return text, False
    half = BRIEF_MAX_CHARS // 2
    return text[:half] + "\n\n[...]\n\n" + text[-half:], True


def generate_brief(llm: LLMClient, text: str, title: str) -> dict[str, Any] | None:
    """Generate and check one brief, retrying while the check finds issues.

    Args:
        llm: LLM client.
        text: Full tisk text.
        title: Official title.

    Returns:
        Brief record without the ``ct`` key, or None if the LLM gave nothing.
    """
    fitted, truncated = _fit(text)
    prompt = _PROMPT.format(
        title=_sanitize_llm_input(title or "(bez názvu)"), text=_sanitize_llm_input(fitted)
    )
    best: tuple[list[str], dict[str, str], dict[str, str]] | None = None
    for _ in range(1 + LLM_EMPTY_RETRIES):
        data = llm._generate_json(prompt, _SYSTEM, _SCHEMA)
        if not data:
            continue
        cs = {f: str(data.get(f, "")).strip() for f in FIELDS}
        en = {f: str(data.get(f"{f}_en", "")).strip() for f in FIELDS}
        issues = brief_issues(cs, en, fitted)
        if best is None or len(issues) < len(best[0]):
            best = (issues, cs, en)
        if not issues:
            break
    if best is None:
        return None
    issues, cs, en = best
    return {
        "version": BRIEF_VERSION,
        "model": llm.model,
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "source": {
            "ct1": 0,
            "chars": len(text),
            "truncated": truncated,
            "sha256": hashlib.sha256(text.encode()).hexdigest(),
        },
        "status": "warning" if issues else "ok",
        "issues": issues,
        "cs": cs,
        "en": en,
    }


def _brief_path(cache_dir: Path, period: int, ct: int) -> Path:
    return cache_dir / TISKY_META_DIR / str(period) / BRIEFS_DIR / f"{ct}.json"


def load_briefs(cache_dir: Path, period: int) -> dict[int, dict]:
    """Read all briefs of a period from disk.

    Args:
        cache_dir: Base cache directory.
        period: Electoral period.

    Returns:
        ``{ct: brief}``.
    """
    briefs: dict[int, dict] = {}
    for path in (cache_dir / TISKY_META_DIR / str(period) / BRIEFS_DIR).glob("*.json"):
        try:
            briefs[int(path.stem)] = json.loads(path.read_text(encoding="utf-8"))
        except (ValueError, OSError):
            logger.opt(exception=True).warning("Unreadable brief {}", path)
    return briefs


def _bill_titles(cache_dir: Path, period: int) -> dict[int, str]:
    """``{ct: official title}`` for the bill kinds that get a brief."""
    df = pl.read_parquet(cache_dir / PARQUET_DIR / "tisky_v2.parquet").filter(
        pl.col("id_obdobi") == PERIOD_ORGAN_IDS[period], pl.col("id_druh").is_in(BRIEF_DRUHY)
    )
    return {
        int(r["ct"]): str(r["nazev_cely"] or r["nazev_tisku"] or "")
        for r in df.select("ct", "nazev_cely", "nazev_tisku").to_dicts()
    }


def _is_current(path: Path, text: str) -> bool:
    """True if the stored brief is from this prompt version and this text."""
    if not path.exists():
        return False
    try:
        old = json.loads(path.read_text(encoding="utf-8"))
    except (ValueError, OSError):
        return False
    sha = hashlib.sha256(text.encode()).hexdigest()
    return old.get("version") == BRIEF_VERSION and old.get("source", {}).get("sha256") == sha


def generate_briefs_sync(
    period: int,
    cache_dir: Path,
    progress_callback: Callable[[int, int], None] | None = None,
    cancel_check: Callable[[], None] | None = None,
) -> int:
    """Generate missing or outdated briefs for all bills of a period that have text.

    Args:
        period: Electoral period.
        cache_dir: Base cache directory.
        progress_callback: Called with (done, total).
        cancel_check: Raises PipelineCancelled when the run is stopped.

    Returns:
        Number of briefs written.
    """
    text_dir = cache_dir / TISKY_TEXT_DIR / str(period)
    todo: list[tuple[int, str, str]] = []
    for ct, title in sorted(_bill_titles(cache_dir, period).items()):
        text_path = text_dir / f"{ct}.txt"
        if not text_path.exists():
            continue
        text = text_path.read_text(encoding="utf-8")
        if text.strip() and not _is_current(_brief_path(cache_dir, period, ct), text):
            todo.append((ct, title, text))
    if not todo:
        return 0
    llm = create_llm_client()
    if not llm.is_available():
        logger.warning("[briefs] LLM not available, skipping {} briefs", len(todo))
        return 0

    def _one(item: tuple[int, str, str]) -> dict[str, Any] | None:
        if cancel_check:
            cancel_check()
        return generate_brief(llm, item[2], item[1])

    written = 0
    for i, ((ct, _title, _text), brief) in enumerate(
        zip(todo, llm_map(_one, todo), strict=True), 1
    ):
        if brief is not None:
            write_json_atomic({"ct": ct, **brief}, _brief_path(cache_dir, period, ct))
            written += 1
            logger.info(
                "[briefs] {}/{} ct={} {} {}", i, len(todo), ct, brief["status"], brief["issues"]
            )
        if progress_callback:
            progress_callback(i, len(todo))
    return written


def brief_view(brief: dict | None, lang: str) -> dict | None:
    """Fields of a stored brief in the UI language plus its check status.

    Args:
        brief: Stored brief record (``TiskInfo.brief``).
        lang: UI language; English falls back to Czech when missing.

    Returns:
        Dict with the FIELDS plus ``status``, ``generated_at`` and ``edited``
        (admin review, see services/overrides), or None.
    """
    if not brief:
        return None
    fields = brief.get("en") if lang == "en" else None
    if not fields or not fields.get("shrnuti"):
        fields = brief.get("cs") or {}
    return {
        **{f: fields.get(f, "") for f in FIELDS},
        "status": brief.get("status", "warning"),
        "generated_at": brief.get("generated_at", ""),
        "edited": brief.get("edited"),
    }
