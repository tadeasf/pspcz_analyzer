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
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import polars as pl
from loguru import logger

from pspcz_analyzer.models.amendment_models import BillAmendmentData
from pspcz_analyzer.models.pipeline_progress import AmendmentMode
from pspcz_analyzer.models.tisk_models import PeriodData
from pspcz_analyzer.services.amendments.cache_manager import load_amendments, save_amendments
from pspcz_analyzer.services.amendments.identifier import (
    _ensure_tisk_histories,
    _identify_third_reading_bods,
    _resolve_vote_ids,
)
from pspcz_analyzer.services.amendments.merger import (
    _merge_pdf_and_steno,
    _pdf_download_and_parse,
)
from pspcz_analyzer.services.amendments.progress import (
    AmendmentProgress,
    AmendmentStage,
    AmendmentStatus,
)
from pspcz_analyzer.services.amendments.steno_parser import (
    cross_validate_amendments,
    parse_steno_amendments,
)
from pspcz_analyzer.services.amendments.steno_scraper import StenoFailure, find_steno_for_bod
from pspcz_analyzer.services.amendments.submitter_resolver import resolve_submitter_ids
from pspcz_analyzer.services.amendments.summarizer import (
    _summarize_amendments,
)
from pspcz_analyzer.services.cancellation import CancellationFlag, PipelineCancelled


def _run_pipeline_sync(
    period: int,
    period_data: PeriodData,
    cache_dir: Path,
    progress: AmendmentProgress,
    on_progress: Callable[[int, list[BillAmendmentData]], None] | None = None,
    mode: AmendmentMode = AmendmentMode.FULL,
    refresh_active: bool = False,
    cancel_check: Callable[[], None] | None = None,
) -> list[BillAmendmentData]:
    """Run the amendment pipeline synchronously with mode-based stage selection.

    Args:
        period: Electoral period number.
        period_data: Loaded period data.
        cache_dir: Base cache directory.
        progress: Progress tracker (mutated in-place).
        on_progress: Optional callback(period, bills) for incremental UI refresh.
        mode: Pipeline execution mode.
        refresh_active: When True, re-scrape active bills' histories so newly
            third-read bills are picked up (daily refresh).
        cancel_check: Optional cancellation hook raised between stages/items;
            the service passes its per-period CancellationFlag.check here.

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
            period, period_data, cache_dir, progress, on_progress, refresh_active, cancel_check
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
        if cancel_check:
            cancel_check()
        # LLM summarization
        progress.stage = AmendmentStage.LLM_SUMMARIZE
        logger.info("[amendment pipeline] Starting LLM summarization for {} bills...", len(bills))
        _summarize_amendments(
            bills,
            cache_dir,
            period,
            progress,
            on_progress,
            period_data,
            ct_to_pdf_text,
            cancel_check,
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
    refresh_active: bool = False,
    cancel_check: Callable[[], None] | None = None,
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
        cancel_check: Optional cancellation hook raised between stages/items.

    Returns:
        Tuple of (list of parsed BillAmendmentData, ct -> PDF text mapping).
    """
    if cancel_check:
        cancel_check()

    # Stage 0: Ensure tisk histories exist (scrape if needed)
    progress.stage = AmendmentStage.SCRAPE_HISTORIES
    logger.info("[amendment pipeline] Ensuring tisk histories exist...")
    _ensure_tisk_histories(
        period, period_data, cache_dir, refresh_active=refresh_active, cancel_check=cancel_check
    )

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
        temp_bills_for_pdf, period, cache_dir, period_data, cancel_check
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
        try:
            if cancel_check:
                cancel_check()

            # Progress checkpoint every 50 candidates
            if (i + 1) % 50 == 0:
                logger.info(
                    "[amendment pipeline] Steno download/parse: {}/{} candidates processed",
                    i + 1,
                    len(candidates),
                )

            html, steno_url, failure = find_steno_for_bod(period, schuze, bod, cache_dir)
            if html is None:
                if failure is not None:
                    failure_counts[failure] += 1
                logger.debug(
                    "[amendment pipeline] No steno found for period={} schuze={} bod={} "
                    "ct={} reason={}",
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
        finally:
            # Count the candidate as done regardless of outcome (previously
            # set to `i` before processing — the bar lagged one item behind).
            progress.done_items = i + 1

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

    if cancel_check:
        cancel_check()

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
    _flags: dict[int, CancellationFlag] = field(default_factory=dict)

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
        refresh_active: bool = False,
    ) -> None:
        """Start the amendment pipeline for a single period.

        Args:
            period: Electoral period number.
            period_data: Loaded period data with tisk_lookup populated.
            on_complete: Optional callback(period, bills) on completion.
            on_progress: Optional callback(period, bills) for incremental UI refresh.
            mode: Pipeline execution mode.
            refresh_active: When True, re-scrape active bills' histories so newly
                third-read bills are picked up (daily refresh).
        """
        if period in self._tasks and not self._tasks[period].done():
            logger.info("[amendment pipeline] Already running for period {}", period)
            return

        prog = AmendmentProgress(status=AmendmentStatus.RUNNING)
        self._progress[period] = prog
        flag = CancellationFlag(period)
        self._flags[period] = flag

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
                    refresh_active,
                    flag.check,
                )
                prog.status = AmendmentStatus.COMPLETED
                prog.stage = AmendmentStage.COMPLETED
                if on_complete:
                    on_complete(period, bills)
            except PipelineCancelled:
                prog.status = AmendmentStatus.CANCELLED
                logger.info("[amendment pipeline] Cancelled for period {}", period)
            except asyncio.CancelledError:
                prog.status = AmendmentStatus.CANCELLED
                logger.info("[amendment pipeline] Cancelled for period {}", period)
                raise
            except Exception:
                prog.status = AmendmentStatus.FAILED
                prog.stage = AmendmentStage.FAILED
                logger.opt(exception=True).error(
                    "[amendment pipeline] Failed for period {}", period
                )
            finally:
                self._flags.pop(period, None)

        task = asyncio.create_task(_run())
        task.add_done_callback(lambda t: self._on_task_done(period, t))
        self._tasks[period] = task

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

    def _on_task_done(self, period: int, task: asyncio.Task) -> None:
        """Prune a finished period's task + flag.

        Only removes the dict entries if this task is still the current
        one — a restart for the same period may have replaced them
        before this done-callback fired.
        """
        if self._tasks.get(period) is task:
            self._tasks.pop(period, None)
            self._flags.pop(period, None)

    def cancel_period(self, period: int) -> bool:
        """Cancel the amendment pipeline for a single period.

        Flips the period's cancellation flag; the worker thread stops
        cooperatively at its next checkpoint. (task.cancel() alone cannot
        interrupt work running inside asyncio.to_thread — it only
        releases the awaiting coroutine.)

        Returns True if a cancellation was requested.
        """
        flag = self._flags.get(period)
        if flag is None or not self.is_running(period):
            return False
        flag.cancel()
        logger.info("[amendment pipeline] Cancellation requested for period {}", period)
        return True

    def cancel_all(self) -> None:
        """Request cancellation of all running amendment pipelines.

        Flips every period flag; running stages stop cooperatively at
        their next checkpoint. Await wait_stopped() to let tasks drain.
        """
        for flag in self._flags.values():
            flag.cancel()
        logger.info("[amendment pipeline] Cancellation requested for all periods")

    async def wait_stopped(self, timeout: float = 10.0) -> None:
        """Wait for running tasks to drain after cancel_all().

        Tasks still running after *timeout* are hard-cancelled — that
        releases the event-loop side immediately; worker threads exit
        at their next cancellation checkpoint.
        """
        tasks = [t for t in self._tasks.values() if not t.done()]
        if tasks:
            _, pending = await asyncio.wait(tasks, timeout=timeout)
            if pending:
                logger.warning(
                    "[amendment pipeline] {} tasks still draining after {}s — forcing cancellation",
                    len(pending),
                    timeout,
                )
                for t in pending:
                    t.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
        self._tasks.clear()
        self._flags.clear()
