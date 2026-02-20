"""Data service: orchestrates download, parsing, caching, and holds DataFrames."""

import asyncio
from pathlib import Path

import polars as pl
from loguru import logger

from pspcz_analyzer.config import (
    DEFAULT_CACHE_DIR,
    DEFAULT_PERIOD,
    PERIOD_LABELS,
    PERIOD_ORGAN_IDS,
    PERIOD_YEARS,
)
from pspcz_analyzer.data.cache import get_or_parse
from pspcz_analyzer.data.downloader import (
    download_poslanci_data,
    download_schuze_data,
    download_tisky_data,
    download_voting_data,
)
from pspcz_analyzer.data.parser import parse_unl, parse_unl_multi
from pspcz_analyzer.models.schemas import (
    BOD_SCHUZE_COLUMNS,
    BOD_SCHUZE_DTYPES,
    HL_HLASOVANI_COLUMNS,
    HL_HLASOVANI_DTYPES,
    HL_POSLANEC_COLUMNS,
    HL_POSLANEC_DTYPES,
    ORGANY_COLUMNS,
    ORGANY_DTYPES,
    OSOBY_COLUMNS,
    OSOBY_DTYPES,
    POSLANEC_COLUMNS,
    POSLANEC_DTYPES,
    SCHUZE_COLUMNS,
    SCHUZE_DTYPES,
    TISKY_COLUMNS,
    TISKY_DTYPES,
    ZARAZENI_COLUMNS,
    ZARAZENI_DTYPES,
    ZMATECNE_COLUMNS,
    ZMATECNE_DTYPES,
)
from pspcz_analyzer.models.tisk_models import PeriodData
from pspcz_analyzer.services.analysis_cache import analysis_cache
from pspcz_analyzer.services.mp_builder import build_mp_info
from pspcz_analyzer.services.tisk_cache_manager import TiskCacheManager
from pspcz_analyzer.services.tisk_lookup_builder import build_tisk_lookup
from pspcz_analyzer.services.tisk_pipeline_service import TiskPipelineService
from pspcz_analyzer.services.tisk_text_service import TiskTextService


class DataService:
    """Manages data for multiple electoral periods, loading on demand."""

    def __init__(self, cache_dir: Path = DEFAULT_CACHE_DIR) -> None:
        self.cache_dir = cache_dir
        self._periods: dict[int, PeriodData] = {}
        self.tisk_text = TiskTextService(cache_dir)
        self.tisk_pipeline = TiskPipelineService(cache_dir)
        self._cache_mgr = TiskCacheManager(cache_dir)
        self._refresh_lock = asyncio.Lock()

        # Shared tables (not period-specific), populated by _load_shared_tables
        self._persons: pl.DataFrame | None = None
        self._mps: pl.DataFrame | None = None
        self._organs: pl.DataFrame | None = None
        self._memberships: pl.DataFrame | None = None
        self._poslanci_dir: Path | None = None

        # Session/tisk tables (shared across all periods)
        self._schuze: pl.DataFrame | None = None
        self._bod_schuze: pl.DataFrame | None = None
        self._tisky: pl.DataFrame | None = None

    @property
    def available_periods(self) -> list[dict]:
        """All periods available for selection, sorted descending."""
        return [
            {"number": p, "label": PERIOD_LABELS.get(p, y), "loaded": p in self._periods}
            for p, y in sorted(PERIOD_YEARS.items(), reverse=True)
        ]

    @property
    def loaded_periods(self) -> list[int]:
        return sorted(self._periods.keys(), reverse=True)

    def get_period(self, period: int) -> PeriodData:
        """Get data for a period, loading it on demand if needed.

        Also refreshes topic/summary data from the parquet cache if the
        pipeline has written new data since last access.
        """
        if period not in self._periods:
            self._load_period(period)
        self._refresh_tisk_data(period)
        return self._periods[period]

    def _refresh_tisk_data(self, period: int) -> None:
        """Update in-memory tisk_lookup from the latest parquet cache.

        Called on every get_period — only re-reads if the parquet mtime changed.
        """
        pd = self._periods.get(period)
        if pd is None:
            return
        topic_map = self._cache_mgr.load_topic_cache(period)
        summary_map = self._cache_mgr.summary_cache.get(period, {})
        summary_en_map = self._cache_mgr.summary_en_cache.get(period, {})
        history_map = self._cache_mgr.load_history_cache(period)
        law_changes_map = self._cache_mgr.load_law_changes_cache(period)
        subtisk_map = self._cache_mgr.load_subtisk_versions_cache(period)
        diffs_map, diffs_en_map = self._cache_mgr.load_version_diffs_cache(period)
        for tisk in pd.tisk_lookup.values():
            tisk.topics = topic_map.get(tisk.ct, [])
            tisk.summary = summary_map.get(tisk.ct, "")
            tisk.summary_en = summary_en_map.get(tisk.ct, "")
            tisk.has_text = self.tisk_text.has_text(period, tisk.ct)
            tisk.history = history_map.get(tisk.ct)
            tisk.law_changes = law_changes_map.get(tisk.ct, [])
            # Populate sub_versions with diff summaries
            versions = subtisk_map.get(tisk.ct, [])
            for v in versions:
                diff_key = f"{tisk.ct}_{v.get('ct1', '')}"
                v["llm_diff_summary"] = diffs_map.get(diff_key, "")
                v["llm_diff_summary_en"] = diffs_en_map.get(diff_key, "")
            tisk.sub_versions = versions

    def initialize(self, period: int = DEFAULT_PERIOD) -> None:
        """Pre-load shared data and the default period."""
        self._load_shared_tables()
        self._load_period(period)

    def _load_shared_tables(self) -> None:
        """Load MP/party data (shared across all periods)."""
        if self._persons is not None:
            return

        poslanci_dir = download_poslanci_data(self.cache_dir)
        self._poslanci_dir = poslanci_dir

        self._persons = get_or_parse(
            "osoby",
            poslanci_dir,
            lambda: parse_unl(
                self._find_file(poslanci_dir, "osoby.unl"),
                OSOBY_COLUMNS,
                OSOBY_DTYPES,
            ),
            self.cache_dir,
        )

        self._mps = get_or_parse(
            "poslanec",
            poslanci_dir,
            lambda: parse_unl(
                self._find_file(poslanci_dir, "poslanec.unl"),
                POSLANEC_COLUMNS,
                POSLANEC_DTYPES,
            ),
            self.cache_dir,
        )

        self._organs = get_or_parse(
            "organy",
            poslanci_dir,
            lambda: parse_unl(
                self._find_file(poslanci_dir, "organy.unl"),
                ORGANY_COLUMNS,
                ORGANY_DTYPES,
            ),
            self.cache_dir,
        )

        self._memberships = get_or_parse(
            "zarazeni",
            poslanci_dir,
            lambda: parse_unl(
                self._find_file(poslanci_dir, "zarazeni.unl"),
                ZARAZENI_COLUMNS,
                ZARAZENI_DTYPES,
            ),
            self.cache_dir,
        )

        # Session and tisk data
        self._load_schuze_tisky()

    def _load_schuze_tisky(self) -> None:
        """Load session agenda and parliamentary prints data."""
        schuze_dir = download_schuze_data(self.cache_dir)
        tisky_dir = download_tisky_data(self.cache_dir)

        self._schuze = get_or_parse(
            "schuze",
            schuze_dir,
            lambda: parse_unl(
                self._find_file(schuze_dir, "schuze.unl"),
                SCHUZE_COLUMNS,
                SCHUZE_DTYPES,
            ),
            self.cache_dir,
        )

        self._bod_schuze = get_or_parse(
            "bod_schuze",
            schuze_dir,
            lambda: parse_unl(
                self._find_file(schuze_dir, "bod_schuze.unl"),
                BOD_SCHUZE_COLUMNS,
                BOD_SCHUZE_DTYPES,
            ),
            self.cache_dir,
        )

        self._tisky = get_or_parse(
            "tisky",
            tisky_dir,
            lambda: parse_unl(
                self._find_file(tisky_dir, "tisky.unl"),
                TISKY_COLUMNS,
                TISKY_DTYPES,
            ),
            self.cache_dir,
        )

        logger.info(
            "Loaded schuze ({}), bod_schuze ({}), tisky ({})",
            self._schuze.height,
            self._bod_schuze.height,
            self._tisky.height,
        )

    def _ensure_shared_loaded(self) -> None:
        """Assert that shared tables have been loaded (narrows Optional types)."""
        assert self._schuze is not None, "Call _load_shared_tables first"
        assert self._bod_schuze is not None
        assert self._tisky is not None
        assert self._persons is not None
        assert self._mps is not None
        assert self._organs is not None
        assert self._memberships is not None

    def _load_period(self, period: int) -> None:
        """Load voting data for a specific period."""
        if period not in PERIOD_YEARS:
            msg = f"Unknown period {period}. Available: {list(PERIOD_YEARS.keys())}"
            raise ValueError(msg)

        self._load_shared_tables()

        year = PERIOD_YEARS[period]
        logger.info("Loading data for period {} ({}) ...", period, year)

        voting_dir = download_voting_data(period, self.cache_dir)

        votes = get_or_parse(
            f"hl_hlasovani_{period}",
            voting_dir,
            lambda: parse_unl(
                self._find_file(voting_dir, f"hl{year}s.unl"),
                HL_HLASOVANI_COLUMNS,
                HL_HLASOVANI_DTYPES,
            ),
            self.cache_dir,
        )

        mp_votes = get_or_parse(
            f"hl_poslanec_{period}",
            voting_dir,
            lambda: parse_unl_multi(
                voting_dir,
                f"hl{year}h*.unl",
                HL_POSLANEC_COLUMNS,
                HL_POSLANEC_DTYPES,
            ),
            self.cache_dir,
        )

        try:
            zmatecne_file = self._find_file(voting_dir, f"hl{year}z.unl")
            void_votes = get_or_parse(
                f"zmatecne_{period}",
                voting_dir,
                lambda: parse_unl(zmatecne_file, ZMATECNE_COLUMNS, ZMATECNE_DTYPES),
                self.cache_dir,
            )
        except FileNotFoundError:
            logger.info("No void votes file for period {}", period)
            void_votes = pl.DataFrame({"id_hlasovani": pl.Series([], dtype=pl.Int64)})

        self._ensure_shared_loaded()
        assert self._mps is not None
        assert self._persons is not None
        assert self._organs is not None
        assert self._memberships is not None
        assert self._schuze is not None
        assert self._bod_schuze is not None
        assert self._tisky is not None

        mp_info = build_mp_info(period, self._mps, self._persons, self._organs, self._memberships)

        # Pre-load topic cache for tisk lookup builder
        self._cache_mgr.load_topic_cache(period)
        tisk_lookup = build_tisk_lookup(
            period,
            votes,
            self._schuze,
            self._bod_schuze,
            self._tisky,
            self.tisk_text,
            self._cache_mgr.topic_cache,
            self._cache_mgr.summary_cache,
            self._cache_mgr.summary_en_cache,
        )

        pd = PeriodData(
            period=period,
            votes=votes,
            mp_votes=mp_votes,
            void_votes=void_votes,
            mp_info=mp_info,
            tisk_lookup=tisk_lookup,
        )
        self._periods[period] = pd

        logger.info(
            "Period {} ready: {} votes, {} vote records, {} MPs, {} tisk links",
            period,
            votes.height,
            mp_votes.height,
            mp_info.height,
            len(tisk_lookup),
        )

    def _find_file(self, directory: Path, filename: str) -> Path:
        """Find a file in directory tree (case-insensitive search)."""
        for f in directory.rglob(filename):
            return f
        for f in directory.rglob("*"):
            if f.name.lower() == filename.lower():
                return f
        msg = f"File {filename} not found in {directory}"
        raise FileNotFoundError(msg)

    def start_tisk_pipeline(self, period: int) -> None:
        """Kick off background tisk processing for a period.

        Extracts the list of ct numbers from the already-loaded tisky table
        and starts the pipeline. On completion, updates in-memory tisk_lookup
        entries with fresh topics, summaries, and has_text flags.
        """
        if self._tisky is None:
            return

        organ_id = PERIOD_ORGAN_IDS[period]
        period_tisky = self._tisky.filter(
            (pl.col("id_obdobi") == organ_id) & pl.col("ct").is_not_null()
        )
        ct_numbers = sorted(period_tisky.get_column("ct").unique().to_list())
        if not ct_numbers:
            return

        def _on_complete(
            p: int,
            text_paths: dict,
            topic_map: dict,
            summary_map: dict,
            *_args: object,
            **_kwargs: object,
        ) -> None:
            """Callback: refresh in-memory tisk data after pipeline finishes."""
            self._cache_mgr.invalidate(p)
            pd = self._periods.get(p)
            if pd is None:
                return
            self._refresh_tisk_data(p)
            logger.info(
                "[tisk pipeline] Updated in-memory tisk data for period {}",
                p,
            )

        self.tisk_pipeline.start_period(period, ct_numbers, on_complete=_on_complete)

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

        def _on_complete(
            p: int,
            text_paths: dict,
            topic_map: dict,
            summary_map: dict,
            *_args: object,
            **_kwargs: object,
        ) -> None:
            self._cache_mgr.invalidate(p)
            pd = self._periods.get(p)
            if pd is None:
                return
            self._refresh_tisk_data(p)
            logger.info("[tisk pipeline] Updated in-memory tisk data for period {}", p)

        self.tisk_pipeline.start_all_periods(period_ct, on_complete=_on_complete)

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

        download_poslanci_data(self.cache_dir, force=True)
        download_schuze_data(self.cache_dir, force=True)
        download_tisky_data(self.cache_dir, force=True)
        self._load_shared_tables()

    def _force_reload_period(self, period: int) -> None:
        """Re-download and re-parse voting data for a single period."""
        download_voting_data(period, self.cache_dir, force=True)
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

            # 1. Cancel tisk pipeline
            await self.tisk_pipeline.cancel_all()

            # 2. Re-download and reload shared tables
            try:
                await asyncio.to_thread(self._force_reload_shared_tables)
                logger.info("[daily-refresh] Shared tables reloaded")
            except Exception:
                logger.opt(exception=True).error("[daily-refresh] Failed to reload shared tables")

            # 3. Re-download and reload each loaded period
            for period in list(self._periods.keys()):
                try:
                    await asyncio.to_thread(self._force_reload_period, period)
                    logger.info("[daily-refresh] Period {} reloaded", period)
                except Exception:
                    logger.opt(exception=True).error(
                        "[daily-refresh] Failed to reload period {}", period
                    )

            # 4. Invalidate analysis caches
            analysis_cache.invalidate()
            for period in self._periods:
                self._cache_mgr.invalidate(period)

            # 5. Restart tisk pipeline with fresh data
            self.start_all_tisk_pipelines()

            logger.info("[daily-refresh] Full data refresh complete")
