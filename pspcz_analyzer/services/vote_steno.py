"""What each vote was about, from the steno record (#102).

Votes are anchored at their announcement in the steno (``vote_steno_text``); the text
since the previous announcement is the lead-in. The LLM extracts kind, question, what
"Pro" meant, the proposer and a verbatim quote. The quote must be in the steno and the
#97 text check must pass, otherwise the question stays unknown. When the lead-in only
points at an earlier proposal ("o mém návrhu"), earlier proposals of the sitting are
added and a found answer is stored as ``uncertain``. A deterministic pass then links
challenged and repeated votes.

Output: ``vote_steno/{period}/{schuze}.json`` = ``{id_hlasovani: record}`` and
``vote_steno/{period}/checks/{schuze}.json`` = agenda items whose anchored vote count
differs from ``hl_hlasovani``.
"""

import hashlib
import json
import re
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import polars as pl
from loguru import logger

from pspcz_analyzer.config import PARQUET_DIR, PERIOD_YEARS
from pspcz_analyzer.fileio import write_json_atomic

# module import: speeches -> amendments -> tisk -> vote_steno is a cycle when speeches loads first
from pspcz_analyzer.services import speeches
from pspcz_analyzer.services.amendments.steno_scraper import PSP_STENO_BASE_TEMPLATE
from pspcz_analyzer.services.llm import LLMClient, create_llm_client
from pspcz_analyzer.services.llm.helpers import _sanitize_llm_input
from pspcz_analyzer.services.llm.prompts import (
    VOTE_KINDS,
    VOTE_STENO_EARLIER,
    VOTE_STENO_PROMPT,
    VOTE_STENO_SCHEMA,
    VOTE_STENO_STR_FIELDS,
    VOTE_STENO_SYSTEM,
)
from pspcz_analyzer.services.tisk.brief import text_issues
from pspcz_analyzer.services.vote_steno_pages import is_recent, sitting_pages
from pspcz_analyzer.services.vote_steno_text import (
    WINDOW_CHARS,
    anchor_check,
    challengers,
    clean_proposer,
    earlier_proposals,
    is_referential,
    is_result_formula,
    link_repeats,
    norm_quote,
    quote_named,
    quote_speaker,
    rule_kind,
    sitting_text,
    speaker_before,
)

_SHORTEN_RE = re.compile(r"\bzkr[áa]t\w* (?:\w+ )?lhůt", re.I)
VERSION = 2  # bump when rules or prompt change: all records regenerate
CHECKS_DIR = "checks"
SHORT_LEAD_IN = 700
CONTEXT_CHARS = 1800
_PREV_LABEL = "Souvislost před předchozím hlasováním"
_RECENT_LABEL = "Hlasování těsně předtím (už proběhla)"
_RECENT_VOTES = 6
_LLM_WORKERS = 4

_UNKNOWN: dict[str, str] = {
    "question": "",
    "question_en": "",
    "pro": "",
    "pro_en": "",
    "proposer": "",
    "quote": "",
    "status": "unknown",
}


def _ask_once(llm: LLMClient, window: str, snips: list[str], cislo: int, bod: str) -> dict:
    """One LLM extraction; ``status`` ok/uncertain/unknown after the quote checks."""
    earlier = (
        VOTE_STENO_EARLIER.format(snips=_sanitize_llm_input("\n\n".join(snips))) if snips else ""
    )
    prompt = VOTE_STENO_PROMPT.format(
        cislo=cislo,
        bod=_sanitize_llm_input(bod or "-"),
        earlier=earlier,
        window=_sanitize_llm_input(window),
    )
    data = llm._generate_json(prompt, VOTE_STENO_SYSTEM, VOTE_STENO_SCHEMA) or {}
    f = {k: str(data.get(k, "")).strip() for k in ("druh", "podtyp", *VOTE_STENO_STR_FIELDS)}
    nq = norm_quote(f["citace"])
    context = window + " " + " ".join(snips)
    in_window, in_context = nq in norm_quote(window), nq in norm_quote(context)
    # the chair's opening formula and result announcements say nothing about the subject
    ok = bool(f["otazka"]) and len(nq) >= 15 and in_context
    ok = ok and not nq.startswith("zahajuji") and not is_result_formula(nq)
    ok = ok and not (len(nq) < 80 and is_referential(nq))  # "o mém návrhu" names no subject
    cs = f"{f['otazka']} {f['pro_znamena']}"
    if not ok or text_issues(cs, f"{f['otazka_en']} {f['pro_znamena_en']}", context):
        return dict(_UNKNOWN)
    out = {
        "question": f["otazka"],
        "question_en": f["otazka_en"],
        "pro": f["pro_znamena"],
        "pro_en": f["pro_znamena_en"],
        "proposer": clean_proposer(f["navrhovatel"], context),
        "quote": f["citace"],
        "status": "ok" if in_window else "uncertain",
    }
    if f["druh"] in VOTE_KINDS:
        out["kind"] = f["druh"]
        out["subkind"] = f["podtyp"] if f["druh"] == "procedural" and f["podtyp"] else ""
    return out


def _ask(
    llm: LLMClient,
    text: str,
    span: dict[str, Any],
    cislo: int,
    bod: str,
    recent: list[str] | None = None,
) -> dict:
    """Extract the subject, adding earlier proposals when the lead-in only refers to one.

    Args:
        llm: LLM client.
        text: Plain steno text of the sitting.
        span: The vote's ``sitting_text`` span.
        cislo: Vote number.
        bod: Agenda item name.
        recent: Questions of the votes just before (second pass for unknown votes).

    Returns:
        The extracted fields with ``status``.
    """
    start = max(span["start"], span["end"] - WINDOW_CHARS)
    window = text[start : span["end"]].strip()
    prev = ""
    if len(window) < SHORT_LEAD_IN and span["start"] > 0:
        # a bare "a teď o mém návrhu" lead-in: the vote series context sits before it
        prev = text[max(0, span["start"] - CONTEXT_CHARS) : span["start"]].strip()
    snips = earlier_proposals(text, start, window, prev) if is_referential(window) else []
    out = _ask_once(llm, window, snips, cislo, bod)
    if out["status"] == "unknown":
        more = [f"{_RECENT_LABEL}: " + " ".join(recent)] if recent else []
        more += [f"{_PREV_LABEL}: {prev}"] if prev else []
        more += snips or earlier_proposals(text, start, window, prev)
        if more:
            out = _ask_once(llm, window, more, cislo, bod)
    if out.get("kind") == "committee" and _SHORTEN_RE.search(out.get("question", "")):
        out["kind"], out["subkind"] = "procedural", "other"  # shortening a deadline, no referral
    if out.get("kind") == "committee" and out["proposer"] == speaker_before(text, span["end"]):
        out["proposer"] = ""  # the chair only puts the referral to the vote
    quote = out.get("quote", "")
    out["proposer"] = quote_speaker(text, span["end"], quote) or quote_named(out["proposer"], quote)
    if out.get("subkind") == "challenge" and not out["proposer"]:
        out["proposer"] = challengers(text, span["start"], span["end"])
    return out


def _sitting_records(
    llm: LLMClient,
    period: int,
    schuze: int,
    votes: pl.DataFrame,
    cache_dir: Path,
    out_path: Path,
) -> tuple[dict[str, dict], set[int]]:
    """Records for one sitting, reusing unchanged ones from ``out_path``.

    Returns:
        ``(records, anchored vote ids)``.
    """
    old: dict[str, dict] = json.loads(out_path.read_text()) if out_path.exists() else {}
    refresh = any(is_recent(d) for d in votes["datum"].to_list())
    text, spans = sitting_text(sitting_pages(period, schuze, cache_dir, refresh=refresh))
    if spans and not refresh and not set(votes["id_hlasovani"].to_list()) <= set(spans):
        # a cached index from before the last days were published: fetch it again
        text, spans = sitting_text(sitting_pages(period, schuze, cache_dir, refresh=True))
    bods = dict(zip(votes["id_hlasovani"].to_list(), votes["nazev_dlouhy"].to_list(), strict=True))
    base = PSP_STENO_BASE_TEMPLATE.format(year=PERIOD_YEARS[period], session=schuze)
    records: dict[str, dict] = {}
    todo: list[tuple[str, dict]] = []
    for vid, span in spans.items():
        if vid not in bods:
            continue
        window = text[span["start"] : span["end"]]
        sha = hashlib.sha256(window[-WINDOW_CHARS:].encode()).hexdigest()[:16]
        prev = old.get(str(vid))
        if prev and prev.get("version") == VERSION and prev.get("sha") == sha:
            records[str(vid)] = {
                k: v
                for k, v in prev.items()
                if k not in ("invalid_by", "repeated_as", "repeats", "chair_repeated_as")
            }
            continue
        kind, subkind = rule_kind(window[-WINDOW_CHARS:])
        rec = {
            "version": VERSION,
            "sha": sha,
            "cislo": span["cislo"],
            "kind": kind,
            "subkind": subkind,
            "url": f"{base}{span['page']}#h{span['cislo']}",
            "model": llm.model,
        }
        todo.append((str(vid), rec))
        records[str(vid)] = rec

    def _fill(item: tuple[str, dict]) -> None:
        vid, rec = item
        rec.update(_ask(llm, text, spans[int(vid)], rec["cislo"], bods[int(vid)] or ""))

    with ThreadPoolExecutor(_LLM_WORKERS) as pool:
        list(pool.map(_fill, todo))
    _second_pass(llm, text, spans, records, [vid for vid, _ in todo], bods)
    link_repeats(records, votes, text, spans)
    return records, set(spans)


def _second_pass(
    llm: LLMClient,
    text: str,
    spans: dict[int, dict[str, Any]],
    records: dict[str, dict],
    asked: list[str],
    bods: dict[int, str],
) -> None:
    """Re-ask unknown votes with the questions of the votes just before them.

    "A teď o mém návrhu" after a series of votes is answerable once the model knows
    which proposals were already voted on.
    """
    order = sorted(records, key=lambda v: records[v]["cislo"])
    for vid in sorted(asked, key=lambda v: records[v]["cislo"]):
        rec = records[vid]
        if rec.get("status") != "unknown":
            continue
        before = order[: order.index(vid)][-_RECENT_VOTES:]
        recent = [
            f"č. {records[b]['cislo']}: {records[b]['question']}"
            for b in before
            if records[b].get("status") in ("ok", "uncertain")
        ]
        if recent:
            rec.update(_ask(llm, text, spans[int(vid)], rec["cislo"], bods[int(vid)] or "", recent))


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
        out_dir = cache_dir / speeches.VOTE_STENO_DIR / str(period)
        sitting_votes = votes.filter(pl.col("schuze") == schuze)
        records, anchored = _sitting_records(
            llm, period, schuze, sitting_votes, cache_dir, out_dir / f"{schuze}.json"
        )
        if records:
            write_json_atomic(records, out_dir / f"{schuze}.json")
            _write_check(out_dir, schuze, anchor_check(sitting_votes, anchored), period)
        counts = speeches.sitting_speeches(period, schuze, cache_dir)
        if counts:
            write_json_atomic(counts, out_dir / speeches.SPEECHES_DIR / f"{schuze}.json")
        known = sum(r.get("status") in ("ok", "uncertain") for r in records.values())
        total += len(records)
        logger.info(
            "[vote steno] period {} sitting {}: {}/{} votes, {} with question",
            period,
            schuze,
            len(records),
            sitting_votes.height,
            known,
        )
        if progress_callback:
            progress_callback(i, len(sittings))
    return total


def _write_check(out_dir: Path, schuze: int, check: dict[str, dict[str, int]], period: int) -> None:
    """Store (and log) agenda items whose anchored vote count is off."""
    for bod, c in check.items():
        logger.warning(
            "[vote steno] period {} sitting {} bod {}: {} of {} votes anchored in the steno",
            period,
            schuze,
            bod,
            c["anchored"],
            c["votes"],
        )
    write_json_atomic(check, out_dir / CHECKS_DIR / f"{schuze}.json")


def load_vote_steno(cache_dir: Path, period: int) -> dict[int, dict]:
    """All vote records of a period: ``{id_hlasovani: record}``."""
    out: dict[int, dict] = {}
    for path in (cache_dir / speeches.VOTE_STENO_DIR / str(period)).glob("*.json"):
        try:
            out.update({int(k): v for k, v in json.loads(path.read_text()).items()})
        except (ValueError, OSError):
            logger.opt(exception=True).warning("Unreadable vote steno file {}", path)
    return out


def load_anchor_checks(cache_dir: Path, period: int) -> list[dict[str, int]]:
    """Agenda items with an anchoring mismatch: ``[{schuze, bod, votes, anchored}]``."""
    out: list[dict[str, int]] = []
    for path in sorted(
        (cache_dir / speeches.VOTE_STENO_DIR / str(period) / CHECKS_DIR).glob("*.json")
    ):
        try:
            for bod, c in json.loads(path.read_text()).items():
                out.append({"schuze": int(path.stem), "bod": int(bod), **c})
        except (ValueError, OSError):
            logger.opt(exception=True).warning("Unreadable anchor check file {}", path)
    return sorted(out, key=lambda c: (c["schuze"], c["bod"]))
