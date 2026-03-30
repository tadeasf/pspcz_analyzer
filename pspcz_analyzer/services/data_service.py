"""Data service: orchestrates download, parsing, caching, and holds DataFrames.

DataService extends DataReader with pipeline orchestration
(tisk, amendment, daily refresh).
"""

import asyncio
from pathlib import Path

import polars as pl
from loguru import logger

from pspcz_analyzer.config import (
    AI_PERIODS_LIMIT,
    AMENDMENTS_ENABLED,
    DEFAULT_CACHE_DIR,
    DEFAULT_PERIOD,
    DEV_SKIP_AMENDMENTS,
    PERIOD_ORGAN_IDS,
)
from pspcz_analyzer.data.cache import invalidate_parquet
from pspcz_analyzer.data.downloader import (
    download_poslanci_data,
    download_schuze_data,
    download_tisky_data,
    download_voting_data,
)
from pspcz_analyzer.models.pipeline_progress import AmendmentMode, TiskMode
from pspcz_analyzer.models.tisk_models import PeriodData
from pspcz_analyzer.services.amendments.cache_manager import load_amendments
from pspcz_analyzer.services.amendments.pipeline import AmendmentPipelineService
from pspcz_analyzer.services.analysis_cache import analysis_cache
from pspcz_analyzer.services.data_reader import DataReader
from pspcz_analyzer.services.tisk import TiskPipelineService

__all__ = ["DataService", "PeriodData"]


class DataService(DataReader):
    """Full data service with pipeline orchestration (tisk, amendment, refresh)."""

    def __init__(self, cache_dir: Path = DEFAULT_CACHE_DIR) -> None:
        super().__init__(cache_dir)
        self.tisk_pipeline = TiskPipelineService(cache_dir)
        self.amendment_pipeline = AmendmentPipelineService(cache_dir=cache_dir)
        self._refresh_lock = asyncio.Lock()

    def start_tisk_pipeline(self, period: int, mode: TiskMode = TiskMode.FULL) -> bool:
        """Kick off background tisk processing for a period.

        Extracts the list of ct numbers from the already-loaded tisky table
        and starts the pipeline. On completion, updates in-memory tisk_lookup
        entries with fresh topics, summaries, and has_text flags.

        Returns:
            True if a background task was created, False if preconditions not met.
        """
        if self._tisky is None:
            logger.warning("[tisk pipeline] Cannot start: tisky table not loaded")
            return False

        organ_id = PERIOD_ORGAN_IDS[period]
        period_tisky = self._tisky.filter(
            (pl.col("id_obdobi") == organ_id) & pl.col("ct").is_not_null()
        )
        ct_numbers = sorted(period_tisky.get_column("ct").unique().to_list())
        if not ct_numbers:
            logger.warning("[tisk pipeline] Cannot start: no ct numbers for period {}", period)
            return False

        def _on_complete(
            p: int,
            _text_paths: dict,
            _topic_map: dict,
            _summary_map: dict,
            *_args: object,
            **_kwargs: object,
        ) -> None:
            """Callback: refresh in-memory tisk data after pipeline finishes."""
            self._cache_mgr.invalidate(p)
            period_data = self._periods.get(p)
            if period_data is None:
                return
            self._refresh_tisk_data(p)
            logger.info(
                "[tisk pipeline] Updated in-memory tisk data for period {}",
                p,
            )
            # Start amendment pipeline after tisk completes
            self.start_amendment_pipeline(p)

        self.tisk_pipeline.start_period(period, ct_numbers, on_complete=_on_complete, mode=mode)
        return True

    def start_amendment_pipeline(
        self, period: int, mode: AmendmentMode = AmendmentMode.FULL
    ) -> bool:
        """Kick off background amendment parsing for a period.

        Should be called after tisk pipeline completes (needs tisk_lookup).

        Returns:
            True if a background task was created, False if preconditions not met.
        """
        if not AMENDMENTS_ENABLED:
            logger.info(
                "[amendment pipeline] Skipped for period {} (AMENDMENTS_ENABLED=0)",
                period,
            )
            return False
        if DEV_SKIP_AMENDMENTS:
            logger.info(
                "[amendment pipeline] Skipped for period {} (DEV_SKIP_AMENDMENTS=1)",
                period,
            )
            return False
        pd = self._periods.get(period)
        if pd is None:
            logger.info("[amendment pipeline] Period {} not loaded, loading on demand", period)
            try:
                self._load_period(period)
            except Exception:
                logger.opt(exception=True).warning(
                    "[amendment pipeline] Cannot start: failed to load period {}", period
                )
                return False
            pd = self._periods.get(period)
            if pd is None:
                logger.warning(
                    "[amendment pipeline] Cannot start: period {} still not available after load",
                    period,
                )
                return False

        def _on_progress(p: int, _bills: list) -> None:
            """Callback: reload in-memory amendment data on incremental saves."""
            period_data = self._periods.get(p)
            if period_data is None:
                return
            period_data.amendment_data = load_amendments(self.cache_dir, p)
            analysis_cache.invalidate(f"amendments:{p}:")
            analysis_cache.invalidate(f"amendment-coalitions:{p}:")

        def _on_complete(
            p: int,
            bills: list,
        ) -> None:
            """Callback: refresh in-memory amendment data after pipeline finishes."""
            period_data = self._periods.get(p)
            if period_data is None:
                return
            period_data.amendment_data = load_amendments(self.cache_dir, p)
            analysis_cache.invalidate(f"amendments:{p}:")
            analysis_cache.invalidate(f"amendment-coalitions:{p}:")
            logger.info(
                "[amendment pipeline] Updated in-memory amendment data for period {}: {} bills",
                p,
                len(period_data.amendment_data),
            )

        self.amendment_pipeline.start_period(
            period, pd, on_complete=_on_complete, on_progress=_on_progress, mode=mode
        )
        return True

    def start_all_tisk_pipelines(self) -> None:
        """Kick off sequential background tisk processing for ALL periods (newest first).

        Does not require periods to be loaded — uses the shared tisky table
        to get ct numbers. When a period completes, updates in-memory data
        if that period happens to be loaded.
        """
        if self._tisky is None:
            return

        period_ct: list[tuple[int, list[int]]] = []
        for period in sorted(PERIOD_ORGAN_IDS.keys(), reverse=True):
            organ_id = PERIOD_ORGAN_IDS[period]
            period_tisky = self._tisky.filter(
                (pl.col("id_obdobi") == organ_id) & pl.col("ct").is_not_null()
            )
            ct_numbers = sorted(period_tisky.get_column("ct").unique().to_list())
            if ct_numbers:
                period_ct.append((period, ct_numbers))

        if not period_ct:
            return

        if AI_PERIODS_LIMIT > 0 and len(period_ct) > AI_PERIODS_LIMIT:
            skipped = len(period_ct) - AI_PERIODS_LIMIT
            period_ct = period_ct[:AI_PERIODS_LIMIT]
            logger.info(
                "[tisk pipeline] AI limited to {} newest periods ({} skipped)",
                AI_PERIODS_LIMIT,
                skipped,
            )

        def _on_complete(
            p: int,
            _text_paths: dict,
            _topic_map: dict,
            _summary_map: dict,
            *_args: object,
            **_kwargs: object,
        ) -> None:
            self._cache_mgr.invalidate(p)
            if p not in self._periods:
                self._load_period(p)
            self._refresh_tisk_data(p)
            logger.info("[tisk pipeline] Updated in-memory tisk data for period {}", p)
            # Start amendment pipeline after tisk completes
            self.start_amendment_pipeline(p)

        self.tisk_pipeline.start_all_periods(period_ct, on_complete=_on_complete)

    def cancel_period_pipeline(self, period: int) -> dict:
        """Cancel a single period in both tisk and amendment pipelines.

        Returns a dict with cancellation results for each pipeline.
        """
        tisk_cancelled = self.tisk_pipeline.cancel_period(period)
        amend_cancelled = self.amendment_pipeline.cancel_period(period)
        return {"tisk": tisk_cancelled, "amendment": amend_cancelled}

    def remove_pending_period(self, period: int) -> bool:
        """Remove a pending period from the tisk pipeline queue.

        Returns True if the period was pending and is now skipped.
        """
        return self.tisk_pipeline.remove_pending_period(period)

    def _force_reload_shared_tables(self) -> None:
        """Re-download and re-parse all shared tables (MPs, organs, sessions, tisky)."""
        self._persons = None
        self._mps = None
        self._organs = None
        self._memberships = None
        self._poslanci_dir = None
        self._schuze = None
        self._bod_schuze = None
        self._tisky = None

        for table in (
            "osoby",
            "poslanec",
            "organy",
            "zarazeni",
            "schuze",
            "bod_schuze",
            "tisky",
        ):
            invalidate_parquet(table, self.cache_dir)

        download_poslanci_data(self.cache_dir, force=True)
        download_schuze_data(self.cache_dir, force=True)
        download_tisky_data(self.cache_dir, force=True)
        self._load_shared_tables()

    def _force_reload_period(self, period: int) -> None:
        """Re-download and re-parse voting data for a single period."""
        for table in (
            f"hl_hlasovani_{period}",
            f"hl_poslanec_{period}",
            f"zmatecne_{period}",
        ):
            invalidate_parquet(table, self.cache_dir)

        download_voting_data(period, self.cache_dir, force=True)
        if period in self._periods:
            self._load_period(period)

    async def refresh_all_data(self) -> None:
        """Re-download all data from psp.cz and reload in-memory state.

        Pauses the tisk AI pipeline, refreshes data, then restarts the pipeline.
        Safe for concurrent HTTP requests — old data stays valid until swapped.
        """
        if self._refresh_lock.locked():
            logger.warning("[daily-refresh] Refresh already in progress, skipping")
            return

        async with self._refresh_lock:
            logger.info("[daily-refresh] Starting full data refresh ...")

            # 1. Cancel tisk and amendment pipelines
            await self.tisk_pipeline.cancel_all()
            self.amendment_pipeline.cancel_all()

            # 2. Re-download and reload shared tables
            try:
                await asyncio.to_thread(self._force_reload_shared_tables)
                logger.info("[daily-refresh] Shared tables reloaded")
            except Exception:
                logger.opt(exception=True).error("[daily-refresh] Failed to reload shared tables")

            # 3. Re-download and reload current period only
            try:
                await asyncio.to_thread(self._force_reload_period, DEFAULT_PERIOD)
                logger.info("[daily-refresh] Period {} reloaded", DEFAULT_PERIOD)
            except Exception:
                logger.opt(exception=True).error(
                    "[daily-refresh] Failed to reload period {}", DEFAULT_PERIOD
                )

            # 4. Invalidate analysis caches
            analysis_cache.invalidate()
            for period in self._periods:
                self._cache_mgr.invalidate(period)

            # 5. Restart tisk pipeline with fresh data
            self.start_all_tisk_pipelines()

            logger.info("[daily-refresh] Full data refresh complete")
