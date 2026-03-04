"""Amendment pipeline orchestrator.

Coordinates the full amendment analysis workflow:
  IDENTIFY → PDF_DOWNLOAD_PARSE → STENO_DOWNLOAD_PARSE → MERGE
  → RESOLVE_IDS → RESOLVE_SUBMITTERS → LLM_SUMMARIZE → CACHE

PDF text is the primary source for amendment structure (letters, submitter
names, per-amendment text). Steno records provide vote linkage (vote numbers,
results, stances). The MERGE stage combines both data sources.

Follows the same async pattern as TiskPipelineService.
"""

import asyncio
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path

import polars as pl
from loguru import logger

from pspcz_analyzer.config import TISKY_HISTORIE_DIR, TISKY_META_DIR
from pspcz_analyzer.models.amendment_models import AmendmentVote, BillAmendmentData
from pspcz_analyzer.models.pipeline_progress import AmendmentMode
from pspcz_analyzer.models.tisk_models import PeriodData
from pspcz_analyzer.services.amendments.cache_manager import load_amendments, save_amendments
from pspcz_analyzer.services.amendments.pdf_parser import PdfAmendment, parse_amendment_pdf
from pspcz_analyzer.services.amendments.steno_parser import (
    cross_validate_amendments,
    parse_steno_amendments,
)
from pspcz_analyzer.services.amendments.steno_scraper import StenoFailure, find_steno_for_bod
from pspcz_analyzer.services.amendments.submitter_resolver import resolve_submitter_ids
from pspcz_analyzer.services.llm import LLMClient, create_llm_client
from pspcz_analyzer.services.tisk.cache_manager import TiskCacheManager
from pspcz_analyzer.services.tisk.io import (
    download_subtisk_pdf,
    extract_text_from_pdf,
    scrape_all_subtisk_documents,
)
from pspcz_analyzer.services.tisk.io.history_scraper import TiskHistory
from pspcz_analyzer.services.tisk.metadata_scraper import scrape_histories_sync


class AmendmentStage(StrEnum):
    """Pipeline stage identifiers."""

    SCRAPE_HISTORIES = "scrape_histories"
    IDENTIFY = "identify"
    PDF_DOWNLOAD_PARSE = "pdf_download_parse"
    STENO_DOWNLOAD_PARSE = "steno_download_parse"
    MERGE = "merge"
    RESOLVE_IDS = "resolve_ids"
    RESOLVE_SUBMITTERS = "resolve_submitters"
    LLM_SUMMARIZE = "llm_summarize"
    CACHE = "cache"
    COMPLETED = "completed"
    FAILED = "failed"


class AmendmentStatus(StrEnum):
    """Pipeline status for a period."""

    IDLE = "idle"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class AmendmentProgress:
    """Progress tracking for the amendment pipeline."""

    status: AmendmentStatus = AmendmentStatus.IDLE
    stage: AmendmentStage = AmendmentStage.IDENTIFY
    total_items: int = 0
    done_items: int = 0
    bills_found: int = 0
    bills_parsed: int = 0
    started_at: float = 0.0
    summaries_completed: int = 0
    summaries_failed: int = 0
    amendment_summaries_completed: int = 0

    @property
    def elapsed(self) -> float:
        """Seconds elapsed since pipeline started."""
        if self.started_at <= 0:
            return 0.0
        return time.monotonic() - self.started_at

    @property
    def rate(self) -> float:
        """Items processed per second."""
        elapsed = self.elapsed
        if elapsed <= 0 or self.done_items <= 0:
            return 0.0
        return self.done_items / elapsed

    @property
    def percent(self) -> float | None:
        """Completion percentage, or None if total unknown."""
        if self.total_items <= 0:
            return None
        return (self.done_items / self.total_items) * 100

    @property
    def eta_seconds(self) -> float | None:
        """Estimated seconds remaining, or None if unknown."""
        r = self.rate
        if r <= 0 or self.total_items <= 0:
            return None
        remaining = self.total_items - self.done_items
        if remaining <= 0:
            return 0.0
        return remaining / r


def _ensure_tisk_histories(
    period: int,
    period_data: PeriodData,
    cache_dir: Path,
) -> None:
    """Ensure tisk history data exists and is loaded into memory.

    The amendment candidate identification requires tisk.history.stages
    to detect third-reading bills. If the tisk pipeline hasn't scraped
    histories yet, do it now as a prerequisite. Always loads the cached
    histories into in-memory TiskInfo objects.

    Args:
        period: Electoral period number.
        period_data: Loaded period data with tisk_lookup.
        cache_dir: Base cache directory.
    """
    hist_dir = cache_dir / TISKY_META_DIR / str(period) / TISKY_HISTORIE_DIR
    needs_scrape = not hist_dir.exists() or not any(hist_dir.glob("*.json"))

    if needs_scrape:
        ct_numbers = sorted({t.ct for t in period_data.tisk_lookup.values() if t.ct})
        if not ct_numbers:
            return

        logger.info(
            "[amendment pipeline] No tisk history data found for period {}, "
            "scraping {} tisk histories as prerequisite...",
            period,
            len(ct_numbers),
        )
        scrape_histories_sync(period, ct_numbers, cache_dir)

    # Always load histories into in-memory TiskInfo objects
    cache_mgr = TiskCacheManager(cache_dir)
    cache_mgr.invalidate(period)
    history_map = cache_mgr.load_history_cache(period)

    # Detect stale histories missing amendment sub-tisk data and re-scrape
    stale_cts = [ct for ct, h in history_map.items() if h.amendment_tisk_ct1 is None and h.stages]
    if stale_cts:
        logger.info(
            "[amendment pipeline] Re-scraping {} stale tisk histories for period {} "
            "(missing amendment sub-tisk data)",
            len(stale_cts),
            period,
        )
        refreshed = scrape_histories_sync(period, stale_cts, cache_dir)
        history_map.update(refreshed)

    loaded = 0
    for tisk in period_data.tisk_lookup.values():
        hist = history_map.get(tisk.ct)
        if hist is not None:
            tisk.history = hist
            loaded += 1

    logger.info(
        "[amendment pipeline] Tisk histories: {} loaded into memory for period {}",
        loaded,
        period,
    )


def _identify_third_reading_bods(
    period_data: PeriodData,
) -> list[tuple[int, int, int, str]]:
    """Identify agenda items that likely had third-reading amendment votes.

    Looks for (schuze, bod) pairs where:
    - There's a tisk with legislative history showing 3. čtení
    - There are multiple votes on the same (schuze, bod)

    Args:
        period_data: Loaded period data with tisk_lookup.

    Returns:
        List of (schuze, bod, ct, tisk_nazev) tuples.
    """
    candidates: list[tuple[int, int, int, str]] = []

    # Group votes by (schuze, bod) and find those with multiple votes
    void_ids = period_data.void_votes.get_column("id_hlasovani")
    votes = period_data.votes.filter(~pl.col("id_hlasovani").is_in(void_ids))

    vote_counts = (
        votes.group_by(["schuze", "bod"])
        .agg(pl.col("id_hlasovani").count().alias("n_votes"))
        .filter(pl.col("n_votes") > 1)  # Multiple votes = likely amendments
    )

    for row in vote_counts.iter_rows(named=True):
        schuze = row["schuze"]
        bod = row["bod"]
        tisk = period_data.get_tisk(schuze, bod)
        if tisk is None:
            continue

        # Check if tisk history shows 3. čtení
        has_third_reading = False
        if tisk.history and tisk.history.stages:
            for stage in tisk.history.stages:
                if stage.stage_type == "3_cteni":
                    has_third_reading = True
                    break

        if has_third_reading:
            candidates.append((schuze, bod, tisk.ct, tisk.nazev))

    logger.info(
        "[amendment pipeline] Identified {} candidates from {} tisk entries for period {}",
        len(candidates),
        len(period_data.tisk_lookup),
        period_data.period,
    )
    return candidates


def _resolve_vote_ids(
    amendments: list[BillAmendmentData],
    period_data: PeriodData,
) -> None:
    """Resolve vote_number (cislo) to id_hlasovani via the votes DataFrame.

    Mutates the AmendmentVote objects in-place.

    Args:
        amendments: List of bill amendment data with parsed votes.
        period_data: Period data containing the votes DataFrame.
    """
    for bill in amendments:
        schuze_votes = period_data.votes.filter(pl.col("schuze") == bill.schuze)

        all_amends = list(bill.amendments)
        if bill.final_vote:
            all_amends.append(bill.final_vote)

        for amend in all_amends:
            if amend.vote_number == 0:
                continue
            match = schuze_votes.filter(pl.col("cislo") == amend.vote_number)
            if match.height > 0:
                amend.id_hlasovani = match.item(0, "id_hlasovani")


def _download_amendment_pdf(
    period: int,
    ct: int,
    amendment_ct1: int | None,
    amendment_idd: int | None,
    cache_dir: Path,
) -> str:
    """Download amendment sub-tisk PDF and extract text.

    Uses the known CT1/idd from history scraping if available,
    otherwise falls back to iterating sub-tisk versions.

    Args:
        period: Electoral period number.
        ct: Tisk number.
        amendment_ct1: Known amendment sub-tisk CT1 (from history).
        amendment_idd: Known idd for direct download (from history).
        cache_dir: Base cache directory.

    Returns:
        Extracted PDF text, or empty string on failure.
    """
    # Fast path: download directly using known CT1/idd
    if amendment_ct1 is not None and amendment_idd is not None:
        pdf_path = download_subtisk_pdf(period, ct, amendment_ct1, amendment_idd, cache_dir)
        if pdf_path is not None:
            text = extract_text_from_pdf(pdf_path)
            if text.strip():
                return text

    # Fallback: iterate sub-tisk versions
    try:
        versions = scrape_all_subtisk_documents(period, ct)
    except Exception:
        logger.warning("[amendment pipeline] Failed to scrape sub-tisk versions for ct={}", ct)
        return ""

    # Filter for amendment sub-versions (CT1 >= 2) with downloadable PDFs
    amendment_versions = [v for v in versions if v.ct1 >= 2 and v.idd is not None]
    if not amendment_versions:
        return ""

    texts: list[str] = []
    for ver in amendment_versions:
        pdf_path = download_subtisk_pdf(period, ct, ver.ct1, ver.idd, cache_dir)  # type: ignore[arg-type]
        if pdf_path is None:
            continue
        text = extract_text_from_pdf(pdf_path)
        if text.strip():
            texts.append(text)

    return "\n\n---\n\n".join(texts)


def _pdf_download_and_parse(
    bills: list[BillAmendmentData],
    period: int,
    cache_dir: Path,
    period_data: PeriodData,
) -> tuple[dict[int, list[PdfAmendment]], dict[int, str]]:
    """Download amendment PDFs and parse their structure.

    Groups bills by CT to avoid duplicate downloads. Populates
    bill.amendment_tisk_ct1 and bill.amendment_tisk_idd.

    Args:
        bills: List of bill amendment data.
        period: Electoral period number.
        cache_dir: Base cache directory.
        period_data: Period data with tisk_lookup for history access.

    Returns:
        Tuple of (ct -> list[PdfAmendment], ct -> raw PDF text).
    """
    pdf_data: dict[int, list[PdfAmendment]] = {}
    ct_texts: dict[int, str] = {}

    # Build ct -> TiskHistory lookup from period_data
    ct_history: dict[int, TiskHistory] = {}
    for tisk in period_data.tisk_lookup.values():
        if tisk.history is not None:
            ct_history[tisk.ct] = tisk.history

    # Collect unique CTs
    unique_cts = sorted({bill.ct for bill in bills})

    for ct in unique_cts:
        history = ct_history.get(ct)
        amendment_ct1 = history.amendment_tisk_ct1 if history else None
        amendment_idd = history.amendment_tisk_idd if history else None

        text = _download_amendment_pdf(period, ct, amendment_ct1, amendment_idd, cache_dir)
        ct_texts[ct] = text

        if text:
            parsed = parse_amendment_pdf(text)
            pdf_data[ct] = parsed
        else:
            pdf_data[ct] = []

    # Populate bill-level metadata fields (not the large PDF text)
    for bill in bills:
        history = ct_history.get(bill.ct)
        if history:
            bill.amendment_tisk_ct1 = history.amendment_tisk_ct1
            bill.amendment_tisk_idd = history.amendment_tisk_idd

    extracted = sum(1 for t in ct_texts.values() if t)
    parsed_count = sum(1 for p in pdf_data.values() if p)
    logger.info(
        "[amendment pipeline] PDFs: {}/{} CTs have text, {}/{} parsed into amendments",
        extracted,
        len(unique_cts),
        parsed_count,
        len(unique_cts),
    )

    return pdf_data, ct_texts


def _pop_numeric_variants(
    steno_by_letter: dict[str, AmendmentVote],
    pdf_letter: str,
) -> list[AmendmentVote]:
    """Pop ALL steno amendments matching numeric variants of a PDF letter.

    E.g., PDF letter "A" matches steno "A1", "A2", "A3" — all are returned.

    Args:
        steno_by_letter: Mutable dict of steno amendments keyed by letter.
        pdf_letter: PDF amendment letter (e.g. "A").

    Returns:
        List of matching steno AmendmentVote objects (may be empty).
    """
    matches: list[AmendmentVote] = []
    keys_to_pop = [
        key
        for key in sorted(steno_by_letter.keys())
        if key.startswith(pdf_letter) and key[len(pdf_letter) :].isdigit()
    ]
    for key in keys_to_pop:
        matches.append(steno_by_letter.pop(key))
    return matches


def _merge_pdf_and_steno(
    bills: list[BillAmendmentData],
    pdf_data: dict[int, list[PdfAmendment]],
) -> None:
    """Merge PDF-parsed amendment structure with steno-parsed vote data.

    For each bill:
    - PDF amendments with matching steno data: copy vote info from steno,
      text/name from PDF
    - PDF-only amendments (no matching steno): create unvoted AmendmentVote
    - Steno-only leftovers (oral amendments): keep as-is

    Args:
        bills: List of bill amendment data (mutated in-place).
        pdf_data: Mapping of ct -> list[PdfAmendment].
    """
    total_pdf_matched = 0
    total_pdf_only = 0
    total_steno_only = 0

    for bill in bills:
        pdf_amendments = pdf_data.get(bill.ct, [])
        if not pdf_amendments:
            continue

        # Build steno lookup by letter (uppercase, stripped)
        steno_by_letter: dict[str, AmendmentVote] = {}
        for amend in bill.amendments:
            key = amend.letter.strip().upper()
            steno_by_letter[key] = amend

        merged: list[AmendmentVote] = []

        for pdf_amend in pdf_amendments:
            pdf_key = pdf_amend.letter.strip().upper()

            # Try exact match first
            steno = steno_by_letter.pop(pdf_key, None)
            steno_matches: list[AmendmentVote] = [steno] if steno else []

            # Try numeric variants: "A" matches steno "A1", "A2", "A3"
            if not steno_matches:
                steno_matches = _pop_numeric_variants(steno_by_letter, pdf_key)

            # Also check grouped_with: if steno has grouped_with letters,
            # try to match those PDF letters too
            for s in list(steno_matches):
                for grouped_letter in s.grouped_with:
                    gl = grouped_letter.strip().upper()
                    # Extract base letter (e.g. "F2" -> "F")
                    base = gl.rstrip("0123456789")
                    if base and base in steno_by_letter:
                        steno_by_letter.pop(base)

            if steno_matches:
                # Enrich all matching steno entries with PDF data.
                # All matches get raw_text — steno "E1" means the entire
                # letter E section, typically voted en bloc.
                for steno_item in steno_matches:
                    steno_item.amendment_text = pdf_amend.raw_text
                    steno_item.pdf_submitter_name = pdf_amend.submitter_name
                    if not steno_item.submitter_names and pdf_amend.submitter_name:
                        steno_item.submitter_names = [pdf_amend.submitter_name]
                    merged.append(steno_item)
                    total_pdf_matched += 1
            else:
                # PDF-only: no steno match → no vote linkage, skip
                total_pdf_only += 1

        # Append steno-only leftovers (oral amendments not in PDF)
        for steno_leftover in steno_by_letter.values():
            merged.append(steno_leftover)
            total_steno_only += 1

        bill.amendments = merged

    logger.info(
        "[amendment pipeline] Merge: {} PDF+steno matched, {} PDF-only, {} steno-only",
        total_pdf_matched,
        total_pdf_only,
        total_steno_only,
    )


def _summarize_bill_text(
    llm: LLMClient,
    text: str,
    title: str,
    bill_index: int,
    total_bills: int,
) -> tuple[str, str]:
    """Generate CS and EN summaries for a bill's amendment documents.

    Uses summarize_bilingual to truncate text once and generate both
    language summaries in sequence, respecting LLM_MAX_TEXT_CHARS.

    Args:
        llm: LLMClient instance.
        text: Combined amendment PDF text for the bill.
        title: Bill title for context.
        bill_index: 1-based bill index.
        total_bills: Total number of bills.

    Returns:
        (cs_summary, en_summary), empty strings on failure.
    """
    try:
        logger.info(
            "[amendment pipeline] [{}/{}] bill '{}' ({} chars) summarizing...",
            bill_index,
            total_bills,
            title[:60],
            len(text),
        )
        result = llm.summarize_bilingual(text, title)
        cs = result.get("cs", "")
        en = result.get("en", "")
        logger.info(
            "[amendment pipeline] [{}/{}] bill '{}' -> summary={}chars summary_en={}chars",
            bill_index,
            total_bills,
            title[:60],
            len(cs),
            len(en),
        )
        return cs, en
    except Exception:
        logger.warning(
            "[amendment pipeline] [{}/{}] bill '{}' LLM failed",
            bill_index,
            total_bills,
            title[:60],
        )
        return "", ""


def _summarize_per_amendment(
    llm: LLMClient,
    bill: BillAmendmentData,
    bill_index: int,
    total_bills: int,
    bill_context: str = "",
    pdf_text: str = "",
) -> int:
    """Generate per-amendment AI summaries for a single bill.

    Uses a single batched LLM call per language (2 calls total) instead
    of 2×N calls, making this efficient even for bills with many amendments.

    Args:
        llm: LLMClient instance.
        bill: Bill with amendments to summarize.
        bill_index: 1-based bill index for logging.
        total_bills: Total number of bills for logging.
        bill_context: Optional AI-generated summary of the original bill.
        pdf_text: Raw PDF text for this bill (from transient ct_to_pdf_text).

    Returns:
        Number of amendments that received summaries.
    """
    # Use provided PDF text or fall back to per-amendment text
    text = pdf_text or next((a.amendment_text for a in bill.amendments if a.amendment_text), "")
    if not text:
        return 0

    # Build metadata list for non-final, non-withdrawn amendments
    amendments_meta: list[dict[str, str]] = []
    for amend in bill.amendments:
        if amend.is_final_vote or amend.is_withdrawn:
            continue
        if not amend.letter:
            continue
        # Prefer PDF submitter name (nominative), fall back to steno names
        submitter = amend.pdf_submitter_name or (
            ", ".join(amend.submitter_names) if amend.submitter_names else ""
        )
        amendments_meta.append(
            {
                "letter": amend.letter,
                "submitter": submitter,
                "description": amend.description,
            }
        )

    if not amendments_meta:
        return 0

    try:
        logger.info(
            "[amendment pipeline] [{}/{}] per-amendment summaries for '{}' ({} amendments)...",
            bill_index,
            total_bills,
            bill.tisk_nazev[:60],
            len(amendments_meta),
        )
        cs_map, en_map = llm.summarize_amendments_bilingual(
            text,
            bill.tisk_nazev,
            amendments_meta,
            bill_context=bill_context,
        )
    except Exception:
        logger.warning(
            "[amendment pipeline] [%d/%d] per-amendment LLM failed for '%s'",
            bill_index,
            total_bills,
            bill.tisk_nazev[:60],
        )
        return 0

    logger.debug(
        "[amendment pipeline] cs_map keys=%s en_map keys=%s amend letters=%s",
        list(cs_map.keys()),
        list(en_map.keys()),
        [
            a.letter
            for a in bill.amendments
            if not a.is_final_vote and not a.is_withdrawn and a.letter
        ],
    )

    # Assign summaries to amendment objects
    count = 0
    for amend in bill.amendments:
        key = amend.letter.strip().upper()
        cs_summary = cs_map.get(key, "")
        en_summary = en_map.get(key, "")
        if cs_summary:
            amend.summary = cs_summary
            count += 1
        if en_summary:
            amend.summary_en = en_summary

    logger.info(
        "[amendment pipeline] [%d/%d] per-amendment summaries: %d/%d amendments got summaries",
        bill_index,
        total_bills,
        count,
        len(amendments_meta),
    )
    return count


def _summarize_amendments(
    bills: list[BillAmendmentData],
    cache_dir: Path,
    period: int,
    progress: AmendmentProgress,
    on_progress: Callable[[int, list[BillAmendmentData]], None] | None = None,
    period_data: PeriodData | None = None,
    ct_to_pdf_text: dict[int, str] | None = None,
) -> None:
    """Generate bill summaries (reusing tisk summaries) and per-amendment LLM summaries.

    Tries to reuse tisk pipeline summaries for bill_summary/bill_summary_en.
    Falls back to LLM-based _summarize_bill_text() when no tisk summary exists.
    Per-amendment summaries still require LLM but now receive bill context.

    Args:
        bills: List of bill amendment data with amendment_text populated.
        cache_dir: Base cache directory for intermediate saves.
        period: Electoral period number.
        progress: Progress tracker (mutated in-place).
        on_progress: Optional callback(period, bills) for incremental UI refresh.
        period_data: Loaded period data for tisk summary lookup.
        ct_to_pdf_text: Mapping of ct -> raw PDF text (transient, not cached).
    """
    llm = create_llm_client()
    if not llm.is_available():
        logger.info("[amendment pipeline] LLM unavailable, skipping amendment summarization")
        return

    logger.info(
        "[amendment pipeline] LLM summarization starting: provider={} model={} bills={}",
        llm.provider,
        llm.model,
        len(bills),
    )

    progress.started_at = time.monotonic()
    progress.done_items = 0
    progress.total_items = len(bills)
    progress.summaries_completed = 0
    progress.summaries_failed = 0

    pdf_text_map = ct_to_pdf_text or {}
    tisk_reused = 0
    llm_generated = 0

    for i, bill in enumerate(bills):
        # Use transient PDF text dict or fall back to per-amendment text
        text = pdf_text_map.get(bill.ct, "") or next(
            (a.amendment_text for a in bill.amendments if a.amendment_text), ""
        )
        has_text = bool(text)

        # --- Bill summary: try tisk reuse first, then LLM fallback ---
        tisk = period_data.get_tisk(bill.schuze, bill.bod) if period_data else None

        if tisk and tisk.summary:
            bill.bill_summary = tisk.summary
            bill.bill_summary_en = tisk.summary_en or ""
            tisk_reused += 1
            logger.info(
                "[amendment pipeline] [{}/{}] bill ct={} '{}' reused tisk summary "
                "({}chars+{}chars)",
                i + 1,
                len(bills),
                bill.ct,
                bill.tisk_nazev[:60],
                len(bill.bill_summary),
                len(bill.bill_summary_en),
            )
        elif has_text:
            cs, en = _summarize_bill_text(
                llm,
                text,
                bill.tisk_nazev,
                bill_index=i + 1,
                total_bills=len(bills),
            )
            bill.bill_summary = cs
            bill.bill_summary_en = en
            if cs:
                llm_generated += 1
            else:
                progress.summaries_failed += 1
        else:
            # No tisk summary and no amendment text — skip
            progress.done_items = i + 1
            continue

        # --- Per-amendment summaries (require amendment text) ---
        if bill.bill_summary and has_text:
            progress.summaries_completed += 1
            amend_count = _summarize_per_amendment(
                llm,
                bill,
                i + 1,
                len(bills),
                bill_context=bill.bill_summary,
                pdf_text=text,
            )
            progress.amendment_summaries_completed += amend_count
        elif bill.bill_summary:
            # Have bill summary (from tisk) but no amendment text for per-amendment
            progress.summaries_completed += 1
        else:
            progress.summaries_failed += 1

        progress.done_items = i + 1

        # Save + notify after each bill so UI updates incrementally
        if bill.bill_summary:
            save_amendments(cache_dir, period, bills)
            if on_progress:
                on_progress(period, bills)

    elapsed = progress.elapsed
    logger.info(
        "[amendment pipeline] LLM summarization complete: {}/{} bill summaries "
        "({} tisk-reused, {} LLM-generated), {} failed, "
        "{} per-amendment summaries ({:.1f}s elapsed)",
        progress.summaries_completed,
        len(bills),
        tisk_reused,
        llm_generated,
        progress.summaries_failed,
        progress.amendment_summaries_completed,
        elapsed,
    )


def _run_pipeline_sync(
    period: int,
    period_data: PeriodData,
    cache_dir: Path,
    progress: AmendmentProgress,
    on_progress: Callable[[int, list[BillAmendmentData]], None] | None = None,
    mode: AmendmentMode = AmendmentMode.FULL,
) -> list[BillAmendmentData]:
    """Run the amendment pipeline synchronously with mode-based stage selection.

    Args:
        period: Electoral period number.
        period_data: Loaded period data.
        cache_dir: Base cache directory.
        progress: Progress tracker (mutated in-place).
        on_progress: Optional callback(period, bills) for incremental UI refresh.
        mode: Pipeline execution mode.

    Returns:
        List of parsed BillAmendmentData.
    """
    logger.info(
        "[amendment pipeline] === Starting period {} (mode={}) ===",
        period,
        mode.value,
    )

    run_parse = mode in (AmendmentMode.FULL, AmendmentMode.PARSE)
    run_summarize = mode in (AmendmentMode.FULL, AmendmentMode.SUMMARIZE)

    bills: list[BillAmendmentData] = []
    ct_to_pdf_text: dict[int, str] = {}

    if run_parse:
        bills, ct_to_pdf_text = _run_parse_stages(
            period, period_data, cache_dir, progress, on_progress
        )
    elif run_summarize:
        # Load cached bills from disk
        bills = list(load_amendments(cache_dir, period).values())
        if not bills:
            logger.warning(
                "[amendment pipeline] No cached amendment data for period {} — "
                "run PARSE mode first",
                period,
            )
            return []
        progress.bills_found = len(bills)
        progress.total_items = len(bills)

    if run_summarize and bills:
        # LLM summarization
        progress.stage = AmendmentStage.LLM_SUMMARIZE
        logger.info("[amendment pipeline] Starting LLM summarization for {} bills...", len(bills))
        _summarize_amendments(
            bills, cache_dir, period, progress, on_progress, period_data, ct_to_pdf_text
        )

        # Final cache
        progress.stage = AmendmentStage.CACHE
        save_amendments(cache_dir, period, bills)

    total_amendments = sum(b.amendment_count for b in bills)
    with_submitters = sum(1 for b in bills for a in b.amendments if a.submitter_ids)
    with_committee = sum(1 for b in bills for a in b.amendments if a.committee_stance)
    with_proposer = sum(1 for b in bills for a in b.amendments if a.proposer_stance)
    logger.info(
        "[amendment pipeline] Period {} complete (mode={}): {} candidates, {} bills, "
        "{} amendments, {} with submitters, {} with committee stance, {} with proposer stance",
        period,
        mode.value,
        progress.total_items,
        len(bills),
        total_amendments,
        with_submitters,
        with_committee,
        with_proposer,
    )

    return bills


def _run_parse_stages(
    period: int,
    period_data: PeriodData,
    cache_dir: Path,
    progress: AmendmentProgress,
    on_progress: Callable[[int, list[BillAmendmentData]], None] | None = None,
) -> tuple[list[BillAmendmentData], dict[int, str]]:
    """Run parse stages of the amendment pipeline.

    Stage order:
      0. SCRAPE_HISTORIES — ensure tisk histories exist
      1. IDENTIFY — find third-reading amendment candidates
      2. PDF_DOWNLOAD_PARSE — download/parse amendment PDFs
      3. STENO_DOWNLOAD_PARSE — download/parse steno records
      4. MERGE — combine PDF structure with steno vote data
      5. RESOLVE_IDS — link vote numbers to vote IDs
      6. RESOLVE_SUBMITTERS — resolve names to MP IDs
      7. (LLM_SUMMARIZE is handled separately)

    Args:
        period: Electoral period number.
        period_data: Loaded period data.
        cache_dir: Base cache directory.
        progress: Progress tracker (mutated in-place).
        on_progress: Optional callback for UI refresh.

    Returns:
        Tuple of (list of parsed BillAmendmentData, ct -> PDF text mapping).
    """
    # Stage 0: Ensure tisk histories exist (scrape if needed)
    progress.stage = AmendmentStage.SCRAPE_HISTORIES
    logger.info("[amendment pipeline] Ensuring tisk histories exist...")
    _ensure_tisk_histories(period, period_data, cache_dir)

    # Stage 1: Identify candidates
    progress.stage = AmendmentStage.IDENTIFY
    logger.info("[amendment pipeline] Identifying third-reading candidates...")
    candidates = _identify_third_reading_bods(period_data)
    progress.bills_found = len(candidates)
    progress.total_items = len(candidates)

    if not candidates:
        logger.info(
            "[amendment pipeline] No third-reading amendment bills found for period {}",
            period,
        )
        return [], {}

    # Stage 2: Download and parse amendment PDFs (before steno)
    progress.stage = AmendmentStage.PDF_DOWNLOAD_PARSE
    logger.info(
        "[amendment pipeline] Downloading & parsing amendment PDFs for {} candidates...",
        len(candidates),
    )

    # Build temporary bills to group by CT for PDF download
    ct_set = {ct for _, _, ct, _ in candidates}
    temp_bills_for_pdf = [BillAmendmentData(period=period, schuze=0, bod=0, ct=ct) for ct in ct_set]
    pdf_data, ct_to_pdf_text = _pdf_download_and_parse(
        temp_bills_for_pdf, period, cache_dir, period_data
    )

    # Stage 3: Download steno and parse
    progress.stage = AmendmentStage.STENO_DOWNLOAD_PARSE
    logger.info(
        "[amendment pipeline] Downloading & parsing steno for {} candidates...",
        len(candidates),
    )
    bills: list[BillAmendmentData] = []
    failure_counts: Counter[StenoFailure] = Counter()
    no_amendments_count = 0

    for i, (schuze, bod, ct, nazev) in enumerate(candidates):
        progress.done_items = i

        # Progress checkpoint every 50 candidates
        if (i + 1) % 50 == 0:
            logger.info(
                "[amendment pipeline] Steno download/parse: {}/{} candidates processed",
                i + 1,
                len(candidates),
            )

        html, steno_url, failure = find_steno_for_bod(period, schuze, bod, nazev, cache_dir)
        if html is None:
            if failure is not None:
                failure_counts[failure] += 1
            logger.debug(
                "[amendment pipeline] No steno found for period={} schuze={} bod={} ct={} reason={}",
                period,
                schuze,
                bod,
                ct,
                failure,
            )
            # Without steno, there's no vote linkage — skip.
            # The tisk pipeline handles raw PDF content separately.
            continue

        amendments, confidence, warnings = parse_steno_amendments(
            html, period=period, schuze=schuze, bod=bod
        )
        if not amendments and not pdf_data.get(ct):
            no_amendments_count += 1
            logger.debug(
                "[amendment pipeline] Steno found but no amendments parsed for "
                "period={} schuze={} bod={} ct={}",
                period,
                schuze,
                bod,
                ct,
            )
            continue

        # Cross-validate against official vote data
        schuze_bod_votes = period_data.votes.filter(
            (pl.col("schuze") == schuze) & (pl.col("bod") == bod)
        )
        if amendments:
            amendments, xval_warnings = cross_validate_amendments(
                amendments, schuze_bod_votes, schuze, bod
            )
            warnings.extend(xval_warnings)

        # Separate final vote from amendments
        regular = [a for a in amendments if not a.is_final_vote]
        final = next((a for a in amendments if a.is_final_vote), None)

        bill = BillAmendmentData(
            period=period,
            schuze=schuze,
            bod=bod,
            ct=ct,
            tisk_nazev=nazev,
            steno_url=steno_url,
            amendments=regular,
            final_vote=final,
            parse_confidence=confidence,
            parse_warnings=warnings,
            amendment_tisk_ct1=next(
                (b.amendment_tisk_ct1 for b in temp_bills_for_pdf if b.ct == ct), None
            ),
            amendment_tisk_idd=next(
                (b.amendment_tisk_idd for b in temp_bills_for_pdf if b.ct == ct), None
            ),
        )
        bills.append(bill)
        progress.bills_parsed += 1

    progress.done_items = len(candidates)

    failure_detail = ", ".join(f"{r.value}={c}" for r, c in failure_counts.most_common())
    logger.info(
        "[amendment pipeline] Steno download/parse: {}/{} candidates yielded bills "
        "({}; {} steno found but empty)",
        len(bills),
        len(candidates),
        failure_detail or "no failures",
        no_amendments_count,
    )

    # Field extraction rate summary
    total_amendments = sum(b.amendment_count for b in bills)
    with_submitter_names = sum(1 for b in bills for a in b.amendments if a.submitter_names)
    with_committee = sum(1 for b in bills for a in b.amendments if a.committee_stance)
    with_proposer = sum(1 for b in bills for a in b.amendments if a.proposer_stance)
    logger.info(
        "[amendment pipeline] Field extraction rates: {}/{} with submitter names, "
        "{}/{} with committee stance, {}/{} with proposer stance",
        with_submitter_names,
        total_amendments,
        with_committee,
        total_amendments,
        with_proposer,
        total_amendments,
    )

    # Stage 4: Merge PDF and steno data
    progress.stage = AmendmentStage.MERGE
    logger.info("[amendment pipeline] Merging PDF and steno amendment data...")
    _merge_pdf_and_steno(bills, pdf_data)

    # Stage 5: Resolve vote IDs
    progress.stage = AmendmentStage.RESOLVE_IDS
    logger.info("[amendment pipeline] Resolving vote IDs...")
    _resolve_vote_ids(bills, period_data)
    linked_votes = sum(1 for b in bills for a in b.amendments if a.id_hlasovani)
    total_after_merge = sum(b.amendment_count for b in bills)
    logger.info(
        "[amendment pipeline] Vote IDs: {}/{} amendments linked",
        linked_votes,
        total_after_merge,
    )

    # Stage 6: Resolve submitter names to MP IDs
    progress.stage = AmendmentStage.RESOLVE_SUBMITTERS
    logger.info("[amendment pipeline] Resolving submitter names...")
    resolve_submitter_ids(bills, period_data.mp_info)

    # Intermediate save: results visible before LLM
    save_amendments(cache_dir, period, bills)
    if on_progress:
        on_progress(period, bills)

    return bills, ct_to_pdf_text


@dataclass
class AmendmentPipelineService:
    """Orchestrates the amendment analysis pipeline for all periods.

    Follows the same pattern as TiskPipelineService:
    - Background async tasks
    - Per-period progress tracking
    - Cancellation support

    Attributes:
        cache_dir: Base cache directory.
        _progress: Per-period progress tracking.
        _tasks: Per-period asyncio tasks.
    """

    cache_dir: Path
    _progress: dict[int, AmendmentProgress] = field(default_factory=dict)
    _tasks: dict[int, asyncio.Task] = field(default_factory=dict)  # type: ignore[type-arg]

    @property
    def progress(self) -> dict[int, AmendmentProgress]:
        """Current progress for all periods."""
        return self._progress

    def start_period(
        self,
        period: int,
        period_data: PeriodData,
        on_complete: Callable | None = None,
        on_progress: Callable[[int, list[BillAmendmentData]], None] | None = None,
        mode: AmendmentMode = AmendmentMode.FULL,
    ) -> None:
        """Start the amendment pipeline for a single period.

        Args:
            period: Electoral period number.
            period_data: Loaded period data with tisk_lookup populated.
            on_complete: Optional callback(period, bills) on completion.
            on_progress: Optional callback(period, bills) for incremental UI refresh.
            mode: Pipeline execution mode.
        """
        if period in self._tasks and not self._tasks[period].done():
            logger.info("[amendment pipeline] Already running for period {}", period)
            return

        prog = AmendmentProgress(status=AmendmentStatus.RUNNING)
        self._progress[period] = prog

        async def _run() -> None:
            try:
                bills = await asyncio.to_thread(
                    _run_pipeline_sync,
                    period,
                    period_data,
                    self.cache_dir,
                    prog,
                    on_progress,
                    mode,
                )
                prog.status = AmendmentStatus.COMPLETED
                prog.stage = AmendmentStage.COMPLETED
                if on_complete:
                    on_complete(period, bills)
            except asyncio.CancelledError:
                logger.info("[amendment pipeline] Cancelled for period {}", period)
                raise
            except Exception:
                prog.status = AmendmentStatus.FAILED
                prog.stage = AmendmentStage.FAILED
                logger.opt(exception=True).error(
                    "[amendment pipeline] Failed for period {}", period
                )

        self._tasks[period] = asyncio.create_task(_run())

    def is_running(self, period: int) -> bool:
        """Check if the pipeline is currently running for a period."""
        task = self._tasks.get(period)
        return task is not None and not task.done()

    def get_task(self, period: int) -> asyncio.Task | None:
        """Get the running asyncio.Task for a period, or None if not running."""
        task = self._tasks.get(period)
        if task is not None and not task.done():
            return task
        return None

    def cancel_period(self, period: int) -> bool:
        """Cancel the amendment pipeline for a single period.

        Each period runs as an independent asyncio.Task, so we can just
        cancel it directly.

        Returns True if a task was found and cancelled.
        """
        task = self._tasks.get(period)
        if task is None or task.done():
            return False
        task.cancel()
        logger.info("[amendment pipeline] Cancelling for period {}", period)
        return True

    def cancel_all(self) -> None:
        """Cancel all running pipeline tasks."""
        for period, task in self._tasks.items():
            if not task.done():
                task.cancel()
                logger.info("[amendment pipeline] Cancelling for period {}", period)
