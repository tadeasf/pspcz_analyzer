"""Amendment LLM summarization.

Functions for generating bill-level and per-amendment AI summaries
using the configured LLM provider.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from pathlib import Path

from loguru import logger

from pspcz_analyzer.models.amendment_models import BillAmendmentData
from pspcz_analyzer.models.tisk_models import PeriodData
from pspcz_analyzer.services.amendments.cache_manager import save_amendments
from pspcz_analyzer.services.amendments.progress import AmendmentProgress
from pspcz_analyzer.services.llm import LLMClient, create_llm_client


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
        # Prefer PDF submitter names (nominative), fall back to steno names
        submitter = (
            ", ".join(amend.pdf_submitter_names)
            if amend.pdf_submitter_names
            else ", ".join(amend.submitter_names)
            if amend.submitter_names
            else ""
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
