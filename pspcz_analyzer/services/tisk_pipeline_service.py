"""Background pipeline orchestrator: coordinates tisk processing stages.

Runs as an asyncio background task so the web server stays responsive.
"""

import asyncio
from pathlib import Path

from loguru import logger

from pspcz_analyzer.config import DEFAULT_CACHE_DIR
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

    def start_period(
        self,
        period: int,
        ct_numbers: list[int],
        on_complete=None,
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
        on_complete=None,
    ) -> None:
        """Process all periods sequentially in one background task (newest first).

        period_ct_numbers: list of (period, ct_numbers) tuples, ordered by priority.
        """
        if self._all_task is not None and not self._all_task.done():
            logger.debug("All-periods pipeline already running")
            return

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

    async def _run_all_periods(
        self,
        period_ct_numbers: list[tuple[int, list[int]]],
        on_complete,
    ) -> None:
        """Process periods one by one, sequentially."""
        for period, ct_numbers in period_ct_numbers:
            if not ct_numbers:
                continue
            logger.info(
                "[tisk pipeline] === Starting period {} ({} tisky) ===",
                period,
                len(ct_numbers),
            )
            await self._run_period(period, ct_numbers, on_complete)
        logger.info("[tisk pipeline] === All periods processed ===")

    async def _run_period(self, period: int, ct_numbers: list[int], on_complete) -> None:
        """Run the full pipeline in a thread to avoid blocking the event loop."""
        try:
            histories = await asyncio.to_thread(
                scrape_histories_sync,
                period,
                ct_numbers,
                self.cache_dir,
            )
            pdf_paths, text_paths = await asyncio.to_thread(
                process_period_sync,
                period,
                ct_numbers,
                self.cache_dir,
            )
            topic_map, summary_map = await asyncio.to_thread(
                classify_and_save,
                period,
                text_paths,
                self.cache_dir,
            )
            topic_map, summary_map = await asyncio.to_thread(
                consolidate_topics,
                period,
                self.cache_dir,
            )
            law_changes_map = await asyncio.to_thread(
                scrape_law_changes_sync,
                period,
                ct_numbers,
                self.cache_dir,
            )
            subtisk_map = await asyncio.to_thread(
                download_subtisk_versions_sync,
                period,
                ct_numbers,
                self.cache_dir,
            )
            version_diffs = await asyncio.to_thread(
                analyze_version_diffs_sync,
                period,
                ct_numbers,
                self.cache_dir,
            )
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
        except Exception:
            logger.opt(exception=True).error("[tisk pipeline] Failed for period {}", period)

    def is_running(self, period: int) -> bool:
        task = self._tasks.get(period)
        return task is not None and not task.done()
