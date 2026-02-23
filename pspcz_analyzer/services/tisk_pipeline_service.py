"""Background pipeline orchestrator: coordinates tisk processing stages.

Runs as an asyncio background task so the web server stays responsive.
"""

import asyncio
import threading
import time
from collections.abc import Callable
from pathlib import Path

from loguru import logger

from pspcz_analyzer.config import DEFAULT_CACHE_DIR
from pspcz_analyzer.models.pipeline_progress import (
    PeriodProgress,
    PeriodStatus,
    PipelineProgress,
    PipelineStage,
    StageProgress,
)
from pspcz_analyzer.services.tisk_classifier import classify_and_save, consolidate_topics
from pspcz_analyzer.services.tisk_downloader_pipeline import process_period_sync
from pspcz_analyzer.services.tisk_metadata_scraper import (
    scrape_histories_sync,
    scrape_law_changes_sync,
)
from pspcz_analyzer.services.tisk_version_service import (
    analyze_version_diffs_sync,
    download_subtisk_versions_sync,
)


class TiskPipelineService:
    """Manages background tisk processing for loaded periods."""

    def __init__(self, cache_dir: Path = DEFAULT_CACHE_DIR) -> None:
        self.cache_dir = cache_dir
        self._tasks: dict[int, asyncio.Task] = {}
        self._all_task: asyncio.Task | None = None
        self._progress = PipelineProgress()
        self._progress_lock = threading.Lock()

    @property
    def progress(self) -> PipelineProgress:
        """Current pipeline progress (thread-safe read)."""
        return self._progress

    def start_period(
        self,
        period: int,
        ct_numbers: list[int],
        on_complete: Callable | None = None,
    ) -> None:
        """Start background processing for a period. Idempotent — skips if already running."""
        if period in self._tasks and not self._tasks[period].done():
            logger.debug("Tisk pipeline already running for period {}", period)
            return

        task = asyncio.create_task(
            self._run_period(period, ct_numbers, on_complete),
            name=f"tisk-pipeline-{period}",
        )
        self._tasks[period] = task
        logger.info(
            "[tisk pipeline] Started background processing for period {} ({} tisky)",
            period,
            len(ct_numbers),
        )

    def start_all_periods(
        self,
        period_ct_numbers: list[tuple[int, list[int]]],
        on_complete: Callable | None = None,
    ) -> None:
        """Process all periods sequentially in one background task (newest first).

        Args:
            period_ct_numbers: List of (period, ct_numbers) tuples, ordered by priority.
            on_complete: Callback invoked after each period finishes.
        """
        if self._all_task is not None and not self._all_task.done():
            logger.debug("All-periods pipeline already running")
            return

        self._init_progress(period_ct_numbers)

        self._all_task = asyncio.create_task(
            self._run_all_periods(period_ct_numbers, on_complete),
            name="tisk-pipeline-all",
        )
        total_tisky = sum(len(cts) for _, cts in period_ct_numbers)
        logger.info(
            "[tisk pipeline] Started sequential processing of {} periods ({} tisky total)",
            len(period_ct_numbers),
            total_tisky,
        )

    def _init_progress(self, period_ct_numbers: list[tuple[int, list[int]]]) -> None:
        """Initialize progress tracking for a new pipeline run."""
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
            if status in (PeriodStatus.COMPLETED, PeriodStatus.FAILED):
                pp.current_stage = None

    async def _run_all_periods(
        self,
        period_ct_numbers: list[tuple[int, list[int]]],
        on_complete: Callable | None,
    ) -> None:
        """Process periods one by one, sequentially."""
        try:
            for period, ct_numbers in period_ct_numbers:
                if not ct_numbers:
                    self._set_period_status(period, PeriodStatus.SKIPPED)
                    continue
                logger.info(
                    "[tisk pipeline] === Starting period {} ({} tisky) ===",
                    period,
                    len(ct_numbers),
                )
                self._set_period_status(period, PeriodStatus.IN_PROGRESS)
                await self._run_period(period, ct_numbers, on_complete)
            logger.info("[tisk pipeline] === All periods processed ===")
        except asyncio.CancelledError:
            logger.info("[tisk pipeline] All-periods pipeline cancelled")
            raise
        finally:
            with self._progress_lock:
                self._progress.running = False

    async def _run_period(
        self,
        period: int,
        ct_numbers: list[int],
        on_complete: Callable | None,
    ) -> None:
        """Run the full pipeline in a thread to avoid blocking the event loop."""
        n = len(ct_numbers)
        try:
            self._set_stage(period, PipelineStage.SCRAPE_HISTORIES, n)
            histories = await asyncio.to_thread(
                scrape_histories_sync,
                period,
                ct_numbers,
                self.cache_dir,
            )
            self._set_stage(period, PipelineStage.DOWNLOAD_PDFS, n)
            pdf_paths, text_paths = await asyncio.to_thread(
                process_period_sync,
                period,
                ct_numbers,
                self.cache_dir,
            )

            # Build a progress callback for classify stage
            def _classify_cb(done: int, total: int) -> None:
                self._update_stage_items(period, done, total)

            self._set_stage(period, PipelineStage.CLASSIFY, len(text_paths))
            topic_map, summary_map, summary_en_map = await asyncio.to_thread(
                classify_and_save,
                period,
                text_paths,
                self.cache_dir,
                progress_callback=_classify_cb,
            )
            self._set_stage(period, PipelineStage.CONSOLIDATE_TOPICS)
            topic_map, summary_map, summary_en_map = await asyncio.to_thread(
                consolidate_topics,
                period,
                self.cache_dir,
            )
            self._set_stage(period, PipelineStage.SCRAPE_LAW_CHANGES, n)
            law_changes_map = await asyncio.to_thread(
                scrape_law_changes_sync,
                period,
                ct_numbers,
                self.cache_dir,
            )
            self._set_stage(period, PipelineStage.DOWNLOAD_VERSIONS, n)
            subtisk_map = await asyncio.to_thread(
                download_subtisk_versions_sync,
                period,
                ct_numbers,
                self.cache_dir,
            )

            def _diffs_cb(done: int, total: int) -> None:
                self._update_stage_items(period, done, total)

            self._set_stage(period, PipelineStage.ANALYZE_DIFFS, 0)
            version_diffs, version_diffs_en = await asyncio.to_thread(
                analyze_version_diffs_sync,
                period,
                ct_numbers,
                self.cache_dir,
                progress_callback=_diffs_cb,
            )
            self._set_period_status(period, PeriodStatus.COMPLETED)
            logger.info(
                "[tisk pipeline] Period {} complete: {} histories, {} PDFs, {} texts, "
                "{} topics, {} law changes, {} sub-tisk, {} diffs",
                period,
                len(histories),
                len(pdf_paths),
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

    async def cancel_all(self) -> None:
        """Cancel all running pipeline tasks and wait for them to finish."""
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

        self._tasks.clear()
        self._all_task = None

        with self._progress_lock:
            self._progress.running = False
