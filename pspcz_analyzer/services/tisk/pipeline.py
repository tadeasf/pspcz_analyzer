"""Background pipeline orchestrator: coordinates tisk processing stages.

Runs as an asyncio background task so the web server stays responsive.
"""

import asyncio
import threading
import time
from collections.abc import Callable
from pathlib import Path

import polars as pl
from loguru import logger

from pspcz_analyzer.config import (
    DEFAULT_CACHE_DIR,
    DEV_SKIP_CLASSIFY_AND_SUMMARIZE,
    DEV_SKIP_VERSION_DIFFS,
    TISKY_META_DIR,
    TISKY_TEXT_DIR,
)
from pspcz_analyzer.models.pipeline_progress import (
    PeriodProgress,
    PeriodStatus,
    PipelineProgress,
    PipelineStage,
    StageProgress,
    TiskMode,
)
from pspcz_analyzer.services.llm import deserialize_topics
from pspcz_analyzer.services.tisk.classifier import classify_and_save, consolidate_topics
from pspcz_analyzer.services.tisk.downloader_pipeline import process_period_sync
from pspcz_analyzer.services.tisk.metadata_scraper import (
    scrape_histories_sync,
    scrape_law_changes_sync,
)
from pspcz_analyzer.services.tisk.version_service import (
    analyze_version_diffs_sync,
    download_subtisk_versions_sync,
)


class PeriodCancelled(Exception):
    """Raised when a running period is cancelled at a stage boundary."""

    def __init__(self, period: int) -> None:
        self.period = period
        super().__init__(f"Period {period} cancelled")


class TiskPipelineService:
    """Manages background tisk processing for loaded periods."""

    def __init__(self, cache_dir: Path = DEFAULT_CACHE_DIR) -> None:
        self.cache_dir = cache_dir
        self._tasks: dict[int, asyncio.Task] = {}
        self._all_task: asyncio.Task | None = None
        self._progress = PipelineProgress()
        self._progress_lock = threading.Lock()
        self._skip_periods: set[int] = set()
        self._cancel_current: int | None = None
        self._cancel_all_flag: bool = False

    @property
    def progress(self) -> PipelineProgress:
        """Current pipeline progress (thread-safe read)."""
        return self._progress

    def start_period(
        self,
        period: int,
        ct_numbers: list[int],
        on_complete: Callable | None = None,
        mode: TiskMode = TiskMode.FULL,
    ) -> None:
        """Start background processing for a period. Idempotent — skips if already running."""
        if period in self._tasks and not self._tasks[period].done():
            logger.debug("Tisk pipeline already running for period {}", period)
            return

        task = asyncio.create_task(
            self._run_period(period, ct_numbers, on_complete, mode),
            name=f"tisk-pipeline-{period}",
        )
        self._tasks[period] = task
        logger.info(
            "[tisk pipeline] Started background processing for period {} ({} tisky, mode={})",
            period,
            len(ct_numbers),
            mode.value,
        )

    def start_all_periods(
        self,
        period_ct_numbers: list[tuple[int, list[int]]],
        on_complete: Callable | None = None,
        mode: TiskMode = TiskMode.FULL,
    ) -> None:
        """Process all periods sequentially in one background task (newest first).

        Args:
            period_ct_numbers: List of (period, ct_numbers) tuples, ordered by priority.
            on_complete: Callback invoked after each period finishes.
            mode: Pipeline execution mode.
        """
        if self._all_task is not None and not self._all_task.done():
            logger.debug("All-periods pipeline already running")
            return

        self._init_progress(period_ct_numbers)

        self._all_task = asyncio.create_task(
            self._run_all_periods(period_ct_numbers, on_complete, mode),
            name="tisk-pipeline-all",
        )
        total_tisky = sum(len(cts) for _, cts in period_ct_numbers)
        logger.info(
            "[tisk pipeline] Started sequential processing of {} periods ({} tisky total, mode={})",
            len(period_ct_numbers),
            total_tisky,
            mode.value,
        )

    def _init_progress(self, period_ct_numbers: list[tuple[int, list[int]]]) -> None:
        """Initialize progress tracking for a new pipeline run."""
        self._skip_periods.clear()
        self._cancel_current = None
        with self._progress_lock:
            self._progress = PipelineProgress(
                running=True,
                started_at=time.monotonic(),
                periods={
                    period: PeriodProgress(period=period, tisky_count=len(cts))
                    for period, cts in period_ct_numbers
                },
            )

    def _set_stage(self, period: int, stage: PipelineStage, total: int = 0) -> None:
        """Update the current stage for a period (thread-safe)."""
        with self._progress_lock:
            pp = self._progress.periods.get(period)
            if pp is None:
                return
            pp.current_stage = StageProgress(
                stage=stage,
                items_total=total,
                started_at=time.monotonic(),
            )

    def _update_stage_items(self, period: int, done: int, total: int) -> None:
        """Update items_done/items_total for the current stage (thread-safe)."""
        with self._progress_lock:
            pp = self._progress.periods.get(period)
            if pp is None or pp.current_stage is None:
                return
            pp.current_stage.items_done = done
            pp.current_stage.items_total = total

    def _set_period_status(self, period: int, status: PeriodStatus) -> None:
        """Update a period's status (thread-safe)."""
        with self._progress_lock:
            pp = self._progress.periods.get(period)
            if pp is None:
                return
            pp.status = status
            if status in (
                PeriodStatus.COMPLETED,
                PeriodStatus.FAILED,
                PeriodStatus.CANCELLED,
            ):
                pp.current_stage = None

    def _check_period_cancelled(self, period: int) -> None:
        """Check if the current period was cancelled and raise if so.

        Called between stages in _run_period(). Safe to call from the event
        loop thread — _cancel_current is only written from HTTP handlers
        and read at await boundaries.
        """
        if self._cancel_all_flag or self._cancel_current == period:
            self._cancel_current = None
            raise PeriodCancelled(period)

    def _make_cancel_check(self, period: int) -> Callable[[], None]:
        """Create a cancellation checker for use inside worker threads.

        Returns a closure that reads _cancel_current and raises
        PeriodCancelled if the given period was cancelled. Safe to call
        from worker threads — single-variable reads are atomic under the GIL.
        """

        def check() -> None:
            if self._cancel_all_flag or self._cancel_current == period:
                self._cancel_current = None
                raise PeriodCancelled(period)

        return check

    def remove_pending_period(self, period: int) -> bool:
        """Remove a pending period from the queue before it starts.

        Returns True if the period was pending and is now marked SKIPPED.
        """
        with self._progress_lock:
            pp = self._progress.periods.get(period)
            if pp is None or pp.status != PeriodStatus.PENDING:
                return False
            pp.status = PeriodStatus.SKIPPED
            pp.current_stage = None
        self._skip_periods.add(period)
        logger.info("[tisk pipeline] Removed pending period {} from queue", period)
        return True

    def cancel_period(self, period: int) -> bool:
        """Cancel a single period — pending or in-progress.

        For pending periods, marks SKIPPED immediately.
        For in-progress periods, sets _cancel_current flag checked at next
        stage boundary.

        Returns True if a cancellation action was taken.
        """
        with self._progress_lock:
            pp = self._progress.periods.get(period)
            if pp is None:
                return False
            match pp.status:
                case PeriodStatus.PENDING:
                    pp.status = PeriodStatus.SKIPPED
                    pp.current_stage = None
                    self._skip_periods.add(period)
                    logger.info("[tisk pipeline] Removed pending period {}", period)
                    return True
                case PeriodStatus.IN_PROGRESS:
                    self._cancel_current = period
                    logger.info(
                        "[tisk pipeline] Cancellation requested for running period {}",
                        period,
                    )
                    return True
                case _:
                    return False

    async def _run_all_periods(
        self,
        period_ct_numbers: list[tuple[int, list[int]]],
        on_complete: Callable | None,
        mode: TiskMode = TiskMode.FULL,
    ) -> None:
        """Process periods one by one, sequentially."""
        try:
            for period, ct_numbers in period_ct_numbers:
                # Check if period was removed from queue
                if period in self._skip_periods:
                    self._skip_periods.discard(period)
                    self._set_period_status(period, PeriodStatus.SKIPPED)
                    logger.info("[tisk pipeline] Skipping removed period {}", period)
                    continue
                if not ct_numbers:
                    self._set_period_status(period, PeriodStatus.SKIPPED)
                    continue
                logger.info(
                    "[tisk pipeline] === Starting period {} ({} tisky, mode={}) ===",
                    period,
                    len(ct_numbers),
                    mode.value,
                )
                self._set_period_status(period, PeriodStatus.IN_PROGRESS)
                try:
                    await self._run_period(period, ct_numbers, on_complete, mode)
                except PeriodCancelled:
                    self._set_period_status(period, PeriodStatus.CANCELLED)
                    logger.info("[tisk pipeline] Period {} cancelled, continuing to next", period)
                    continue
            completed = sum(
                1 for pp in self._progress.periods.values() if pp.status == PeriodStatus.COMPLETED
            )
            cancelled = sum(
                1 for pp in self._progress.periods.values() if pp.status == PeriodStatus.CANCELLED
            )
            logger.info(
                "[tisk pipeline] === All periods processed ({} completed, {} cancelled) ===",
                completed,
                cancelled,
            )
        except asyncio.CancelledError:
            logger.info("[tisk pipeline] All-periods pipeline cancelled")
            raise
        finally:
            with self._progress_lock:
                self._progress.running = False

    def _load_cached_topic_data(
        self,
        period: int,
    ) -> tuple[dict[int, list[str]], dict[int, str], dict[int, str]]:
        """Load previously cached topic/summary data from parquet (no LLM).

        Returns (topic_map, summary_map, summary_en_map) — empty dicts if
        no cache exists yet.
        """
        parquet_path = (
            self.cache_dir / TISKY_META_DIR / str(period) / "topic_classifications.parquet"
        )
        if not parquet_path.exists():
            return {}, {}, {}
        df = pl.read_parquet(parquet_path)
        topic_map: dict[int, list[str]] = {}
        summary_map: dict[int, str] = {}
        summary_en_map: dict[int, str] = {}
        for row in df.iter_rows(named=True):
            ct = row["ct"]
            parsed = deserialize_topics(row.get("topic", ""))
            if parsed:
                topic_map[ct] = parsed
            if row.get("summary"):
                summary_map[ct] = row["summary"]
            if row.get("summary_en"):
                summary_en_map[ct] = row["summary_en"]
        return topic_map, summary_map, summary_en_map

    def _load_cached_text_paths(self, period: int) -> dict[int, Path]:
        """Load text file paths from cache directory for CLASSIFY mode.

        Returns:
            Map of ct -> text file path for existing cached text files.
        """
        text_dir = self.cache_dir / TISKY_TEXT_DIR / str(period)
        if not text_dir.exists():
            return {}
        return {
            int(p.stem): p
            for p in text_dir.glob("*.txt")
            if p.stem.isdigit() and p.stat().st_size > 0
        }

    async def _run_period(
        self,
        period: int,
        ct_numbers: list[int],
        on_complete: Callable | None,
        mode: TiskMode = TiskMode.FULL,
    ) -> None:
        """Run pipeline stages based on mode. Runs heavy work in threads."""
        n = len(ct_numbers)
        cancel_check = self._make_cancel_check(period)
        try:
            histories: dict = {}
            pdf_paths: dict = {}
            text_paths: dict[int, Path] = {}
            topic_map: dict[int, list[str]] = {}
            summary_map: dict[int, str] = {}
            summary_en_map: dict[int, str] = {}
            law_changes_map: dict = {}
            subtisk_map: dict = {}
            version_diffs: dict = {}

            run_download = mode in (TiskMode.FULL, TiskMode.DOWNLOAD)
            run_classify = mode in (TiskMode.FULL, TiskMode.CLASSIFY)
            run_diffs = mode in (TiskMode.FULL, TiskMode.DIFFS)

            # ── Phase A: Download & Scrape ──
            if run_download:

                def _progress_cb(done: int, total: int) -> None:
                    self._update_stage_items(period, done, total)

                self._check_period_cancelled(period)
                self._set_stage(period, PipelineStage.SCRAPE_HISTORIES, n)
                histories = await asyncio.to_thread(
                    scrape_histories_sync,
                    period,
                    ct_numbers,
                    self.cache_dir,
                    cancel_check=cancel_check,
                    progress_callback=_progress_cb,
                )
                self._check_period_cancelled(period)
                self._set_stage(period, PipelineStage.DOWNLOAD_PDFS, n)
                pdf_paths, text_paths = await asyncio.to_thread(
                    process_period_sync,
                    period,
                    ct_numbers,
                    self.cache_dir,
                    cancel_check=cancel_check,
                    progress_callback=_progress_cb,
                )
                self._check_period_cancelled(period)
                self._set_stage(period, PipelineStage.SCRAPE_LAW_CHANGES, n)
                law_changes_map = await asyncio.to_thread(
                    scrape_law_changes_sync,
                    period,
                    ct_numbers,
                    self.cache_dir,
                    cancel_check=cancel_check,
                    progress_callback=_progress_cb,
                )
                self._check_period_cancelled(period)
                self._set_stage(period, PipelineStage.DOWNLOAD_VERSIONS, n)
                subtisk_map = await asyncio.to_thread(
                    download_subtisk_versions_sync,
                    period,
                    ct_numbers,
                    self.cache_dir,
                    cancel_check=cancel_check,
                    progress_callback=_progress_cb,
                )

            # ── Phase B: AI Classify + Summarize ──
            if run_classify:
                self._check_period_cancelled(period)
                if not text_paths:
                    text_paths = self._load_cached_text_paths(period)
                if not text_paths:
                    logger.warning(
                        "[tisk pipeline] No cached text files for period {} — skipping classify",
                        period,
                    )
                elif DEV_SKIP_CLASSIFY_AND_SUMMARIZE:
                    logger.info(
                        "[tisk pipeline] DEV_SKIP: skipping CLASSIFY + CONSOLIDATE for period {}",
                        period,
                    )
                    topic_map, summary_map, summary_en_map = self._load_cached_topic_data(period)
                else:

                    def _classify_cb(done: int, total: int) -> None:
                        self._update_stage_items(period, done, total)

                    self._set_stage(period, PipelineStage.CLASSIFY, len(text_paths))
                    topic_map, summary_map, summary_en_map = await asyncio.to_thread(
                        classify_and_save,
                        period,
                        text_paths,
                        self.cache_dir,
                        progress_callback=_classify_cb,
                        cancel_check=cancel_check,
                    )
                    self._check_period_cancelled(period)
                    self._set_stage(period, PipelineStage.CONSOLIDATE_TOPICS)
                    topic_map, summary_map, summary_en_map = await asyncio.to_thread(
                        consolidate_topics,
                        period,
                        self.cache_dir,
                        cancel_check=cancel_check,
                    )

            # ── Phase C: AI Version Diffs ──
            if run_diffs:
                self._check_period_cancelled(period)
                if DEV_SKIP_VERSION_DIFFS:
                    logger.info(
                        "[tisk pipeline] DEV_SKIP: skipping VERSION_DIFFS for period {}",
                        period,
                    )
                else:

                    def _diffs_cb(done: int, total: int) -> None:
                        self._update_stage_items(period, done, total)

                    self._set_stage(period, PipelineStage.ANALYZE_DIFFS, 0)
                    version_diffs, _version_diffs_en = await asyncio.to_thread(
                        analyze_version_diffs_sync,
                        period,
                        ct_numbers,
                        self.cache_dir,
                        progress_callback=_diffs_cb,
                        cancel_check=cancel_check,
                    )

            self._set_period_status(period, PeriodStatus.COMPLETED)
            logger.info(
                "[tisk pipeline] Period {} complete (mode={}): {} histories, {} texts, "
                "{} topics, {} law changes, {} sub-tisk, {} diffs",
                period,
                mode.value,
                len(histories),
                len(text_paths),
                len(topic_map),
                len(law_changes_map),
                len(subtisk_map),
                len(version_diffs),
            )
            if on_complete:
                on_complete(
                    period,
                    text_paths,
                    topic_map,
                    summary_map,
                    histories,
                    law_changes_map,
                    subtisk_map,
                    version_diffs,
                )
        except PeriodCancelled:
            raise
        except asyncio.CancelledError:
            logger.info("[tisk pipeline] Pipeline cancelled for period {}", period)
            raise
        except Exception:
            self._set_period_status(period, PeriodStatus.FAILED)
            logger.opt(exception=True).error("[tisk pipeline] Failed for period {}", period)

    def is_running(self, period: int) -> bool:
        """Check whether the pipeline is running for a given period."""
        task = self._tasks.get(period)
        return task is not None and not task.done()

    def get_task(self, period: int) -> asyncio.Task | None:
        """Get the running asyncio.Task for a period, or None if not running."""
        task = self._tasks.get(period)
        if task is not None and not task.done():
            return task
        return None

    async def cancel_all(self) -> None:
        """Cancel all running pipeline tasks and wait for them to finish."""
        self._skip_periods.clear()
        self._cancel_current = None
        self._cancel_all_flag = True

        tasks_to_cancel: list[asyncio.Task] = []

        if self._all_task is not None and not self._all_task.done():
            self._all_task.cancel()
            tasks_to_cancel.append(self._all_task)

        for task in self._tasks.values():
            if not task.done():
                task.cancel()
                tasks_to_cancel.append(task)

        if tasks_to_cancel:
            logger.info("[tisk pipeline] Cancelling {} tasks ...", len(tasks_to_cancel))
            await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
            logger.info("[tisk pipeline] All tasks cancelled")

        # Grace period: keep _cancel_all_flag set so thread pool threads
        # still running their current LLM call will notice it at the next
        # cancel_check() and exit cleanly.
        await asyncio.sleep(0.5)
        self._cancel_all_flag = False
        self._tasks.clear()
        self._all_task = None

        with self._progress_lock:
            self._progress.running = False
