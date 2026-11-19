"""What each vote was about, from the steno record (#102).

Every vote in a steno page is anchored as ``<a href="/sqw/hlasy.sqw?G=<id>" id="hN">``,
so the text between the previous vote's anchor and this one is the chair's and
rapporteur's lead-in to the vote. Rules classify its kind; the LLM writes the
question plus a verbatim quote, which must be found in the steno text (and pass the
#97 text check), otherwise the question stays unknown.

Output: ``vote_steno/{period}/{schuze}.json`` = ``{id_hlasovani: record}``.
"""

import hashlib
import html
import json
import re
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl
from loguru import logger

from pspcz_analyzer.config import PARQUET_DIR, PERIOD_YEARS, PSP_REQUEST_DELAY
from pspcz_analyzer.fileio import write_json_atomic
from pspcz_analyzer.services.amendments.steno_scraper import (
    _DAY_PAGE_LINK_RE,
    _SUBPAGE_LINK_RE,
    _SUBPAGE_NUM_RE,
    PSP_STENO_BASE_TEMPLATE,
    _download_day_page,
    _download_index,
    _download_subpage,
    _steno_cache_dir,
)
from pspcz_analyzer.services.llm import LLMClient, create_llm_client
from pspcz_analyzer.services.llm.helpers import _sanitize_llm_input
from pspcz_analyzer.services.tisk.brief import text_issues

VOTE_STENO_DIR = "vote_steno"
VERSION = 1  # bump when rules or prompt change: all records regenerate
WINDOW_CHARS = 2500
RECENT_DAYS = 21  # sittings this fresh get their index and day pages re-fetched
_LLM_WORKERS = 4

_VOTE_ANCHOR_RE = re.compile(r'<a\b[^>]*hlasy\.sqw\?G=(\d+)[^>]*\bid="h(\d+)"[^>]*>', re.I)
_TAG_RE = re.compile(r"<[^>]+>")

# Kind -> pattern; the kind whose last match is closest to the vote wins.
_KIND_PATTERNS: dict[str, re.Pattern[str]] = {
    "final": re.compile(
        r"celém návrhu zákona|návrh(?:u|em)? zákona jako celku|o zákonu jako celku"
    ),
    "amendment": re.compile(
        r"pozměňovací|\bnávrh(?:u|em)? [A-Z]\d*(?:\.\d+)?\b|legislativně technick|návrh(?:u)? výboru|"
        r"stanovisko (?:výboru|navrhovatele|ministr)"
    ),
    "reject": re.compile(r"zamítnutí|zamítnout|zamítl"),
    "return": re.compile(r"vrácení|vrátit (?:návrh )?(?:navrhovateli|k dopracování)"),
    "objection": re.compile(r"námitk"),
    "challenge": re.compile(r"zpochybn"),
    "procedural": re.compile(
        r"pořad|přestávk|zkrác|lhůt|procedur|protinávrh|sloučen|jednat (?:a hlasovat )?i po|"
        r"přikáza|en bloc|po \d+\. hodině|zařazení|vyřazení|přerušení"
    ),
}

_SYSTEM = (
    "Z úryvku stenozáznamu Poslanecké sněmovny zjistíš, o čem se hlasovalo v hlasování, "
    "kterým úryvek končí. Jen popisuješ, nehodnotíš. Používáš jen to, co v úryvku stojí. "
    "Česká pole píšeš česky, pole končící _en anglicky."
)
_PROMPT = """Vyplň pole:
- otazka: o čem se hlasovalo, jako otázka o jedné větě, např. "Přijmout pozměňovací návrh B1 poslance Papajanovského (kraje by dostaly 6 % daně z minerálních olejů)?"
- otazka_en: totéž anglicky.
- citace: jedna věta z úryvku, ze které to plyne, zkopírovaná doslova.
Když se to z úryvku nedá poznat, vrať všechna pole prázdná.

Hlasování č. {cislo}, bod: {bod}
---BEGIN USER TEXT---
{window}
---END USER TEXT---"""
_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {k: {"type": "string"} for k in ("otazka", "otazka_en", "citace")},
    "required": ["otazka", "otazka_en", "citace"],
    "additionalProperties": False,
}


def _plain(fragment: str) -> str:
    """HTML fragment -> plain text with collapsed whitespace."""
    return " ".join(html.unescape(_TAG_RE.sub(" ", fragment)).split())


def vote_windows(pages: list[tuple[str, str]]) -> dict[int, dict[str, Any]]:
    """Lead-in text of every vote anchored in a sitting's steno pages.

    Args:
        pages: ``(page name, html)`` in page order.

    Returns:
        ``{id_hlasovani: {"cislo", "page", "window"}}``.
    """
    out: dict[int, dict[str, Any]] = {}
    carry = ""
    for name, page in pages:
        pos = 0
        for m in _VOTE_ANCHOR_RE.finditer(page):
            text = (carry + " " + _plain(page[pos : m.start()])).strip()
            out[int(m.group(1))] = {
                "cislo": int(m.group(2)),
                "page": name,
                "window": text[-WINDOW_CHARS:],
            }
            carry, pos = "", m.end()
        carry = (carry + " " + _plain(page[pos:]))[-WINDOW_CHARS:]
    return out


def vote_kind(window: str) -> str:
    """Rule-based kind of a vote from its lead-in text (``other`` if none matches)."""
    best, best_pos = "other", -1
    for kind, pattern in _KIND_PATTERNS.items():
        hits = [m.end() for m in pattern.finditer(window)]
        if hits and hits[-1] > best_pos:
            best, best_pos = kind, hits[-1]
    return best


def _norm(text: str) -> str:
    return " ".join(text.replace("„", '"').replace("“", '"').split()).strip(' "').lower()


def _ask(llm: LLMClient, window: str, cislo: int, bod: str) -> dict[str, str]:
    """LLM question + quote, kept only when the quote is in the steno and the check passes."""
    prompt = _PROMPT.format(
        cislo=cislo, bod=_sanitize_llm_input(bod or "-"), window=_sanitize_llm_input(window)
    )
    data = llm._generate_json(prompt, _SYSTEM, _SCHEMA) or {}
    q, q_en, quote = (str(data.get(k, "")).strip() for k in ("otazka", "otazka_en", "citace"))
    nq = _norm(quote)
    # the chair's opening formula says nothing about the subject
    ok = bool(q) and len(nq) >= 15 and nq in _norm(window) and not nq.startswith("zahajuji")
    if ok and not text_issues(q, q_en, window):
        return {"question": q, "question_en": q_en, "quote": quote, "status": "ok"}
    return {"question": "", "question_en": "", "quote": "", "status": "unknown"}


def _is_recent(datum: str) -> bool:
    try:
        return datetime.strptime(datum, "%d.%m.%Y").date() >= date.today() - timedelta(
            days=RECENT_DAYS
        )
    except ValueError:
        return False


def sitting_pages(
    period: int, schuze: int, cache_dir: Path, refresh: bool = False
) -> list[tuple[str, str]]:
    """Download (or read cached) all steno sub-pages of a sitting, in order.

    Day-page TOCs skip pages without a speaker anchor, so the whole numeric range
    between the first and last linked page is fetched.

    Args:
        period: Electoral period.
        schuze: Sitting number.
        cache_dir: Base cache directory.
        refresh: Re-fetch the index and day pages (sitting may still grow).

    Returns:
        ``(page name, html)`` list; empty when the steno is not published.
    """
    sdir = _steno_cache_dir(cache_dir, period)
    if refresh:
        for stale in [sdir / f"index_{schuze}.html", *sdir.glob(f"{schuze}_day_*")]:
            stale.unlink(missing_ok=True)
    index = _download_index(period, schuze, cache_dir)
    if not index:
        return []
    base = PSP_STENO_BASE_TEMPLATE.format(year=PERIOD_YEARS[period], session=schuze)
    nums: set[int] = set()
    for day in sorted({m.group(2) + ".html" for m in _DAY_PAGE_LINK_RE.finditer(index)}):
        day_html = _download_day_page(base, day, period, schuze, cache_dir) or ""
        for link in _SUBPAGE_LINK_RE.findall(day_html):
            if m := _SUBPAGE_NUM_RE.match(link):
                nums.add(int(m.group(1)))
    pages: list[tuple[str, str]] = []
    for n in range(min(nums), max(nums) + 1) if nums else ():
        name = f"s{n:06d}.htm"
        cached = (sdir / f"{schuze}_sub_{name.replace('.htm', '.html')}").exists()
        page = _download_subpage(base, name, period, schuze, cache_dir)
        if not cached:
            time.sleep(PSP_REQUEST_DELAY)
        if page:
            pages.append((name, page))
    return pages


def _sitting_records(
    llm: LLMClient,
    period: int,
    schuze: int,
    votes: pl.DataFrame,
    cache_dir: Path,
    out_path: Path,
) -> dict[str, dict]:
    """Records for one sitting, reusing unchanged ones from ``out_path``."""
    old: dict[str, dict] = json.loads(out_path.read_text()) if out_path.exists() else {}
    refresh = any(_is_recent(d) for d in votes["datum"].to_list())
    windows = vote_windows(sitting_pages(period, schuze, cache_dir, refresh=refresh))
    bods = dict(zip(votes["id_hlasovani"].to_list(), votes["nazev_dlouhy"].to_list(), strict=True))
    base = PSP_STENO_BASE_TEMPLATE.format(year=PERIOD_YEARS[period], session=schuze)
    records: dict[str, dict] = {}
    todo: list[tuple[str, dict]] = []
    for vid, w in windows.items():
        if vid not in bods:
            continue
        sha = hashlib.sha256(w["window"].encode()).hexdigest()[:16]
        prev = old.get(str(vid))
        if prev and prev.get("version") == VERSION and prev.get("sha") == sha:
            records[str(vid)] = prev
            continue
        rec = {
            "version": VERSION,
            "sha": sha,
            "cislo": w["cislo"],
            "kind": vote_kind(w["window"]),
            "url": f"{base}{w['page']}#h{w['cislo']}",
            "model": llm.model,
        }
        todo.append((str(vid), rec))
        records[str(vid)] = rec

    def _fill(item: tuple[str, dict]) -> None:
        vid, rec = item
        rec.update(_ask(llm, windows[int(vid)]["window"], rec["cislo"], bods[int(vid)] or ""))

    with ThreadPoolExecutor(_LLM_WORKERS) as pool:
        list(pool.map(_fill, todo))
    return records


def generate_vote_steno_sync(
    period: int,
    cache_dir: Path,
    progress_callback: Callable[[int, int], None] | None = None,
    cancel_check: Callable[[], None] | None = None,
) -> int:
    """Crawl the steno of every sitting with votes and write the vote records.

    Args:
        period: Electoral period.
        cache_dir: Base cache directory.
        progress_callback: Called with (sittings done, total).
        cancel_check: Raises PipelineCancelled when the run is stopped.

    Returns:
        Number of votes with a record.
    """
    votes = pl.read_parquet(cache_dir / PARQUET_DIR / f"hl_hlasovani_{period}.parquet")
    llm = create_llm_client()
    if not llm.is_available():
        logger.warning("[vote steno] LLM not available, skipping period {}", period)
        return 0
    sittings = sorted(votes["schuze"].unique().to_list())
    total = 0
    for i, schuze in enumerate(sittings, 1):
        if cancel_check:
            cancel_check()
        out_path = cache_dir / VOTE_STENO_DIR / str(period) / f"{schuze}.json"
        sitting_votes = votes.filter(pl.col("schuze") == schuze)
        records = _sitting_records(llm, period, schuze, sitting_votes, cache_dir, out_path)
        if records:
            write_json_atomic(records, out_path)
        ok = sum(r.get("status") == "ok" for r in records.values())
        total += len(records)
        logger.info(
            "[vote steno] period {} sitting {}: {}/{} votes, {} with question",
            period,
            schuze,
            len(records),
            sitting_votes.height,
            ok,
        )
        if progress_callback:
            progress_callback(i, len(sittings))
    return total


def load_vote_steno(cache_dir: Path, period: int) -> dict[int, dict]:
    """All vote records of a period: ``{id_hlasovani: record}``."""
    out: dict[int, dict] = {}
    for path in (cache_dir / VOTE_STENO_DIR / str(period)).glob("*.json"):
        try:
            out.update({int(k): v for k, v in json.loads(path.read_text()).items()})
        except (ValueError, OSError):
            logger.opt(exception=True).warning("Unreadable vote steno file {}", path)
    return out
