"""Amendment pipeline orchestrator.

Coordinates the full amendment analysis workflow:
  IDENTIFY → DOWNLOAD_PARSE → RESOLVE_IDS → RESOLVE_SUBMITTERS
  → PDF_EXTRACT → LLM_SUMMARIZE → CACHE

Follows the same async pattern as TiskPipelineService.
"""

import asyncio
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path

import polars as pl
from loguru import logger

from pspcz_analyzer.data.steno_scraper import StenoFailure, find_steno_for_bod
from pspcz_analyzer.data.tisk_downloader import download_subtisk_pdf
from pspcz_analyzer.data.tisk_extractor import extract_text_from_pdf
from pspcz_analyzer.data.tisk_scraper import scrape_all_subtisk_documents
from pspcz_analyzer.models.amendment_models import BillAmendmentData
from pspcz_analyzer.models.tisk_models import PeriodData
from pspcz_analyzer.services.amendments.cache_manager import save_amendments
from pspcz_analyzer.services.amendments.steno_parser import parse_steno_amendments
from pspcz_analyzer.services.amendments.submitter_resolver import resolve_submitter_ids
from pspcz_analyzer.services.llm_service import LLMClient, create_llm_client


class AmendmentStage(StrEnum):
    """Pipeline stage identifiers."""

    IDENTIFY = "identify"
    RESOLVE_URLS = "resolve_urls"
    DOWNLOAD_PARSE = "download_parse"
    RESOLVE_IDS = "resolve_ids"
    RESOLVE_SUBMITTERS = "resolve_submitters"
    PDF_EXTRACT = "pdf_extract"
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
        "Identified {} candidate third-reading bills for period {}",
        len(candidates),
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


def _download_and_extract_for_ct(
    period: int,
    ct: int,
    cache_dir: Path,
) -> str:
    """Download amendment sub-tisk PDFs and extract their text.

    Looks for CT1 >= 2 sub-versions (amendment documents) and combines
    their text content.

    Args:
        period: Electoral period number.
        ct: Tisk number.
        cache_dir: Base cache directory.

    Returns:
        Combined extracted text, or empty string on failure.
    """
    try:
        versions = scrape_all_subtisk_documents(period, ct)
    except Exception:
        logger.warning("Failed to scrape sub-tisk versions for ct={}", ct)
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


def _extract_amendment_pdfs(
    bills: list[BillAmendmentData],
    period: int,
    cache_dir: Path,
) -> None:
    """Extract text from amendment PDFs for all bills.

    Groups bills by CT to avoid duplicate downloads. Assigns combined
    text to AmendmentVote.amendment_text (not final_vote).

    Args:
        bills: List of bill amendment data.
        period: Electoral period number.
        cache_dir: Base cache directory.
    """
    # Group by CT to avoid duplicate downloads
    ct_texts: dict[int, str] = {}
    for bill in bills:
        if bill.ct not in ct_texts:
            ct_texts[bill.ct] = _download_and_extract_for_ct(period, bill.ct, cache_dir)

    # Assign text to amendments
    for bill in bills:
        text = ct_texts.get(bill.ct, "")
        if not text:
            continue
        for amend in bill.amendments:
            amend.amendment_text = text

    extracted = sum(1 for ct, t in ct_texts.items() if t)
    logger.info("Extracted PDF text for {}/{} tisk numbers", extracted, len(ct_texts))


def _summarize_single_amendment(
    llm: LLMClient,
    text: str,
    title: str,
) -> tuple[str, str]:
    """Generate CS and EN summaries for a single amendment.

    Uses summarize_bilingual to truncate text once and generate both
    language summaries in sequence, respecting LLM_MAX_TEXT_CHARS.

    Args:
        llm: LLMClient instance.
        text: Amendment text to summarize.
        title: Bill title for context.

    Returns:
        (cs_summary, en_summary), empty strings on failure.
    """
    try:
        logger.debug(
            "Summarizing amendment text ({} chars) for '{}'",
            len(text),
            title[:50],
        )
        result = llm.summarize_bilingual(text, title)
        return result.get("cs", ""), result.get("en", "")
    except Exception:
        logger.warning("LLM summarization failed for '{}'", title[:50])
        return "", ""


def _summarize_amendments(
    bills: list[BillAmendmentData],
    cache_dir: Path,
    period: int,
    on_progress: Callable[[int, list[BillAmendmentData]], None] | None = None,
) -> None:
    """Generate LLM summaries for amendments with extracted text.

    Silently skips if LLM is unavailable. Saves to parquet and calls
    on_progress after each bill so the UI updates incrementally.

    Args:
        bills: List of bill amendment data with amendment_text populated.
        cache_dir: Base cache directory for intermediate saves.
        period: Electoral period number.
        on_progress: Optional callback(period, bills) for incremental UI refresh.
    """
    llm = create_llm_client()
    if not llm.is_available():
        logger.info("LLM unavailable, skipping amendment summarization")
        return

    summarized = 0
    for i, bill in enumerate(bills):
        if (i + 1) % 10 == 0 or i == 0:
            logger.info(
                "[amendment pipeline] LLM summarizing bill {}/{}",
                i + 1,
                len(bills),
            )
        bill_had_summary = False
        for amend in bill.amendments:
            if not amend.amendment_text:
                continue
            cs, en = _summarize_single_amendment(llm, amend.amendment_text, bill.tisk_nazev)
            amend.summary = cs
            amend.summary_en = en
            if cs:
                summarized += 1
                bill_had_summary = True

        # Save + notify after each bill so UI updates incrementally
        if bill_had_summary:
            save_amendments(cache_dir, period, bills)
            if on_progress:
                on_progress(period, bills)

    logger.info("Generated LLM summaries for {} amendments", summarized)


def _run_pipeline_sync(
    period: int,
    period_data: PeriodData,
    cache_dir: Path,
    progress: AmendmentProgress,
    on_progress: Callable[[int, list[BillAmendmentData]], None] | None = None,
) -> list[BillAmendmentData]:
    """Run the full amendment pipeline synchronously.

    Args:
        period: Electoral period number.
        period_data: Loaded period data.
        cache_dir: Base cache directory.
        progress: Progress tracker (mutated in-place).
        on_progress: Optional callback(period, bills) for incremental UI refresh.

    Returns:
        List of parsed BillAmendmentData.
    """
    # Stage 1: Identify candidates
    progress.stage = AmendmentStage.IDENTIFY
    candidates = _identify_third_reading_bods(period_data)
    progress.bills_found = len(candidates)
    progress.total_items = len(candidates)

    if not candidates:
        logger.info("No third-reading amendment bills found for period {}", period)
        return []

    # Stage 2+3: Download steno and parse
    progress.stage = AmendmentStage.DOWNLOAD_PARSE
    bills: list[BillAmendmentData] = []
    failure_counts: Counter[StenoFailure] = Counter()
    no_amendments_count = 0

    for i, (schuze, bod, ct, nazev) in enumerate(candidates):
        progress.done_items = i

        html, steno_url, failure = find_steno_for_bod(period, schuze, bod, nazev, cache_dir)
        if html is None:
            if failure is not None:
                failure_counts[failure] += 1
            logger.debug(
                "No steno found for period={} schuze={} bod={} ct={} reason={}",
                period,
                schuze,
                bod,
                ct,
                failure,
            )
            continue

        amendments, confidence, warnings = parse_steno_amendments(
            html, period=period, schuze=schuze, bod=bod
        )
        if not amendments:
            no_amendments_count += 1
            logger.debug(
                "Steno found but no amendments parsed for period={} schuze={} bod={} ct={}",
                period,
                schuze,
                bod,
                ct,
            )
            continue

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
        )
        bills.append(bill)
        progress.bills_parsed += 1

    progress.done_items = len(candidates)

    failure_detail = ", ".join(f"{r.value}={c}" for r, c in failure_counts.most_common())
    logger.info(
        "[amendment pipeline] Period {} download/parse: {}/{} candidates yielded bills "
        "({}; {} parsed but empty)",
        period,
        len(bills),
        len(candidates),
        failure_detail or "no failures",
        no_amendments_count,
    )

    # Stage 4: Resolve vote IDs
    progress.stage = AmendmentStage.RESOLVE_IDS
    _resolve_vote_ids(bills, period_data)

    # Stage 5: Resolve submitter names to MP IDs
    progress.stage = AmendmentStage.RESOLVE_SUBMITTERS
    resolve_submitter_ids(bills, period_data.mp_info)

    # Stage 6: Extract amendment PDF text
    progress.stage = AmendmentStage.PDF_EXTRACT
    _extract_amendment_pdfs(bills, period, cache_dir)

    # Intermediate save: results visible before LLM
    save_amendments(cache_dir, period, bills)
    if on_progress:
        on_progress(period, bills)

    # Stage 7: LLM summarization (saves incrementally per bill)
    progress.stage = AmendmentStage.LLM_SUMMARIZE
    _summarize_amendments(bills, cache_dir, period, on_progress)

    # Stage 8: Final cache (ensures everything is persisted)
    progress.stage = AmendmentStage.CACHE
    save_amendments(cache_dir, period, bills)

    logger.info(
        "Amendment pipeline complete for period {}: {} bills, {} total amendments",
        period,
        len(bills),
        sum(b.amendment_count for b in bills),
    )

    return bills


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
    ) -> None:
        """Start the amendment pipeline for a single period.

        Args:
            period: Electoral period number.
            period_data: Loaded period data with tisk_lookup populated.
            on_complete: Optional callback(period, bills) on completion.
            on_progress: Optional callback(period, bills) for incremental UI refresh.
        """
        if period in self._tasks and not self._tasks[period].done():
            logger.info("Amendment pipeline already running for period {}", period)
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
                )
                prog.status = AmendmentStatus.COMPLETED
                prog.stage = AmendmentStage.COMPLETED
                if on_complete:
                    on_complete(period, bills)
            except asyncio.CancelledError:
                logger.info("Amendment pipeline cancelled for period {}", period)
                raise
            except Exception:
                prog.status = AmendmentStatus.FAILED
                prog.stage = AmendmentStage.FAILED
                logger.opt(exception=True).error("Amendment pipeline failed for period {}", period)

        self._tasks[period] = asyncio.create_task(_run())

    def is_running(self, period: int) -> bool:
        """Check if the pipeline is currently running for a period."""
        task = self._tasks.get(period)
        return task is not None and not task.done()

    def cancel_all(self) -> None:
        """Cancel all running pipeline tasks."""
        for period, task in self._tasks.items():
            if not task.done():
                task.cancel()
                logger.info("Cancelling amendment pipeline for period {}", period)
