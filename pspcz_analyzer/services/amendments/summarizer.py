"""Amendment LLM summarization.

Functions for generating bill-level and per-amendment AI summaries
using the configured LLM provider.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from pathlib import Path

from loguru import logger

from pspcz_analyzer.models.amendment_models import AmendmentVote, BillAmendmentData
from pspcz_analyzer.models.tisk_models import PeriodData, TiskInfo
from pspcz_analyzer.services.amendments.cache_manager import save_amendments
from pspcz_analyzer.services.amendments.progress import AmendmentProgress
from pspcz_analyzer.services.llm import LLMClient, LLMUnavailable, create_llm_client, llm_map
from pspcz_analyzer.services.tisk.brief import text_issues

# How many summarized bills to accumulate before flushing the parquet +
# notifying the UI. Saving after every single bill rewrites the whole period
# file – O(n²) I/O – while batching by 10 keeps the admin UI's incremental
# refresh responsive.
_SAVE_EVERY_BILLS = 10


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
    except LLMUnavailable:
        raise
    except Exception:
        logger.warning(
            "[amendment pipeline] [{}/{}] bill '{}' LLM failed",
            bill_index,
            total_bills,
            title[:60],
        )
        return "", ""


def _build_amendment_meta(amend: AmendmentVote) -> dict[str, str]:
    """Build metadata dict for a single amendment, including per-amendment text.

    Args:
        amend: Amendment vote object.

    Returns:
        Dict with letter, submitter, description, amendment_text, and grouped_with.
    """
    submitter = (
        ", ".join(amend.pdf_submitter_names)
        if amend.pdf_submitter_names
        else ", ".join(amend.submitter_names)
        if amend.submitter_names
        else ""
    )
    meta: dict[str, str] = {
        "letter": amend.letter,
        "submitter": submitter,
        "description": amend.description,
        "amendment_text": amend.amendment_text,
    }
    if amend.grouped_with:
        meta["grouped_with"] = ", ".join(amend.grouped_with)
    return meta


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

    # Letters to summarize: non-final, non-withdrawn, with their own text. A letter
    # without its own text gets no summary (#100): from the whole document the LLM
    # would only guess what that letter says.
    todo: list[AmendmentVote] = []
    for amend in bill.amendments:
        if amend.is_final_vote or amend.is_withdrawn or not amend.letter:
            continue
        if not amend.amendment_text:
            logger.warning(
                "[amendment pipeline] amendment {} in '{}' has no per-amendment text, "
                "not summarized",
                amend.letter,
                bill.tisk_nazev[:60],
            )
            continue
        todo.append(amend)

    if not todo:
        return 0
    amendments_meta = [_build_amendment_meta(a) for a in todo]
    if all(a.summary for a in todo):
        return len(todo)  # all carried over from the previous run (carry_summaries)

    amendments_with_text = sum(1 for a in amendments_meta if a.get("amendment_text"))
    logger.debug(
        "[amendment pipeline] [{}/{}] '{}' {}/{} amendments have per-amendment text",
        bill_index,
        total_bills,
        bill.tisk_nazev[:60],
        amendments_with_text,
        len(amendments_meta),
    )

    try:
        logger.info(
            "[amendment pipeline] [{}/{}] per-amendment summaries for '{}' ({} amendments, {} with text)...",
            bill_index,
            total_bills,
            bill.tisk_nazev[:60],
            len(amendments_meta),
            amendments_with_text,
        )
        cs_map, en_map = llm.summarize_amendments_bilingual(
            text,
            bill.tisk_nazev,
            amendments_meta,
            bill_context=bill_context,
        )
    except LLMUnavailable:
        raise
    except Exception:
        logger.warning(
            "[amendment pipeline] [{}/{}] per-amendment LLM failed for '{}'",
            bill_index,
            total_bills,
            bill.tisk_nazev[:60],
        )
        return 0

    logger.debug(
        "[amendment pipeline] cs_map keys={} en_map keys={} amend letters={}",
        list(cs_map.keys()),
        list(en_map.keys()),
        [
            a.letter
            for a in bill.amendments
            if not a.is_final_vote and not a.is_withdrawn and a.letter
        ],
    )

    # Assign summaries to the letters that were asked about
    count = 0
    for amend in todo:
        key = amend.letter.strip().upper()
        cs_summary = cs_map.get(key, "")
        en_summary = en_map.get(key, "")
        if cs_summary:
            amend.summary = cs_summary
            count += 1
        if en_summary:
            amend.summary_en = en_summary
        amend.summary_warning = bool(
            cs_summary and text_issues(cs_summary, en_summary, amend.amendment_text or text)
        )

    logger.info(
        "[amendment pipeline] [{}/{}] per-amendment summaries: {}/{} amendments got summaries",
        bill_index,
        total_bills,
        count,
        len(amendments_meta),
    )
    return count


def _summarize_bill(
    llm: LLMClient,
    bill: BillAmendmentData,
    i: int,
    total: int,
    tisk: TiskInfo | None,
    text: str,
) -> tuple[str, int]:
    """Bill summary (brief or tisk summary reused, else LLM) and per-amendment summaries.

    Args:
        llm: LLM client.
        bill: Bill to fill in place.
        i: 0-based bill index for logging.
        total: Number of bills for logging.
        tisk: The bill's tisk, when known.
        text: Amendment PDF text (or the first amendment text), "" when none.

    Returns:
        ``(source, amendments summarized)``; source is ``tisk``, ``llm``, ``llm_failed``
        or ``skip`` (no summary to reuse and no text).
    """
    brief = (tisk.brief or {}) if tisk else {}
    if brief.get("cs", {}).get("shrnuti"):
        bill.bill_summary = brief["cs"]["shrnuti"]
        bill.bill_summary_en = brief.get("en", {}).get("shrnuti", "")
        source = "tisk"
    elif tisk and tisk.summary:
        bill.bill_summary = tisk.summary
        bill.bill_summary_en = tisk.summary_en or ""
        source = "tisk"
        logger.info(
            "[amendment pipeline] [{}/{}] bill ct={} '{}' reused tisk summary ({}chars+{}chars)",
            i + 1,
            total,
            bill.ct,
            bill.tisk_nazev[:60],
            len(bill.bill_summary),
            len(bill.bill_summary_en),
        )
    elif text:
        cs, en = _summarize_bill_text(
            llm, text, bill.tisk_nazev, bill_index=i + 1, total_bills=total
        )
        bill.bill_summary = cs
        bill.bill_summary_en = en
        source = "llm" if cs else "llm_failed"
    else:
        return "skip", 0
    if not (bill.bill_summary and text):
        return source, 0
    count = _summarize_per_amendment(
        llm, bill, i + 1, total, bill_context=bill.bill_summary, pdf_text=text
    )
    return source, count


def _summarize_amendments(
    bills: list[BillAmendmentData],
    cache_dir: Path,
    period: int,
    progress: AmendmentProgress,
    on_progress: Callable[[int, list[BillAmendmentData]], None] | None = None,
    period_data: PeriodData | None = None,
    ct_to_pdf_text: dict[int, str] | None = None,
    cancel_check: Callable[[], None] | None = None,
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
        on_progress: Optional callback(period, bills) for incremental UI
            refresh, fired after each periodic save (every
            _SAVE_EVERY_BILLS summarized bills and once at the end).
        period_data: Loaded period data for tisk summary lookup.
        ct_to_pdf_text: Mapping of ct -> raw PDF text (transient, not cached).
        cancel_check: Optional cancellation hook raised between bills.
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
    summarized_since_save = 0

    def _one(item: tuple[int, BillAmendmentData]) -> tuple[str, int]:
        i, bill = item
        if cancel_check:
            cancel_check()
        text = pdf_text_map.get(bill.ct, "") or next(
            (a.amendment_text for a in bill.amendments if a.amendment_text), ""
        )
        tisk = period_data.get_tisk(bill.schuze, bill.bod) if period_data else None
        return _summarize_bill(llm, bill, i, len(bills), tisk, text)

    for (i, bill), (source, amend_count) in zip(
        enumerate(bills), llm_map(_one, list(enumerate(bills))), strict=True
    ):
        progress.done_items = i + 1
        match source:
            case "skip":
                continue
            case "tisk":
                tisk_reused += 1
            case "llm":
                llm_generated += 1
            case _:
                progress.summaries_failed += 1
        if bill.bill_summary:
            progress.summaries_completed += 1
            progress.amendment_summaries_completed += amend_count
        else:
            progress.summaries_failed += 1

        # Save + notify periodically so the UI updates incrementally without
        # rewriting the whole period parquet after every single bill
        if bill.bill_summary:
            summarized_since_save += 1
            if summarized_since_save >= _SAVE_EVERY_BILLS:
                save_amendments(cache_dir, period, bills)
                if on_progress:
                    on_progress(period, bills)
                summarized_since_save = 0

    # Flush any summaries not yet written
    if summarized_since_save:
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
