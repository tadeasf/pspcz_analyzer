"""Read-only data service: loads cached data, watches for file changes."""

import asyncio
import contextlib
from pathlib import Path

import polars as pl
from loguru import logger

from pspcz_analyzer.config import (
    AMENDMENTS_ENABLED,
    DEFAULT_CACHE_DIR,
    DEFAULT_PERIOD,
    PERIOD_LABELS,
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
from pspcz_analyzer.services.amendments.cache_manager import load_amendments
from pspcz_analyzer.services.analysis_cache import analysis_cache
from pspcz_analyzer.services.mp_builder import build_mp_info
from pspcz_analyzer.services.tisk import (
    TiskCacheManager,
    TiskTextService,
    build_tisk_lookup,
)

_WATCH_INTERVAL_S = 30


def _collect_parquet_mtimes(cache_dir: Path) -> dict[str, float]:
    """Collect mtime of all parquet files in cache dir."""
    parquet_dir = cache_dir / "parquet"
    if not parquet_dir.exists():
        return {}
    return {f.name: f.stat().st_mtime for f in parquet_dir.glob("*.parquet")}


def _collect_amendment_mtimes(cache_dir: Path) -> dict[int, float]:
    """Collect mtime of amendment parquet files, keyed by period number."""
    amendments_dir = cache_dir / "amendments"
    if not amendments_dir.exists():
        return {}
    result: dict[int, float] = {}
    for period_dir in amendments_dir.iterdir():
        if not period_dir.is_dir():
            continue
        pq = period_dir / "amendments.parquet"
        if pq.exists():
            with contextlib.suppress(ValueError, OSError):
                result[int(period_dir.name)] = pq.stat().st_mtime
    return result


class DataReader:
    """Reads cached data (parquet/JSON) — no pipeline orchestration."""

    def __init__(self, cache_dir: Path = DEFAULT_CACHE_DIR) -> None:
        self.cache_dir = cache_dir
        self._periods: dict[int, PeriodData] = {}
        self.tisk_text = TiskTextService(cache_dir)
        self._cache_mgr = TiskCacheManager(cache_dir)

        # Shared tables (not period-specific)
        self._persons: pl.DataFrame | None = None
        self._mps: pl.DataFrame | None = None
        self._organs: pl.DataFrame | None = None
        self._memberships: pl.DataFrame | None = None
        self._poslanci_dir: Path | None = None

        # Session/tisk tables (shared across all periods)
        self._schuze: pl.DataFrame | None = None
        self._bod_schuze: pl.DataFrame | None = None
        self._tisky: pl.DataFrame | None = None

        # For file-watcher: last known parquet mtimes
        self._last_mtimes: dict[str, float] = {}
        self._last_amendment_mtimes: dict[int, float] = {}
        self._watcher_task: asyncio.Task | None = None

    @property
    def available_periods(self) -> list[dict]:
        """All periods available for selection, sorted descending."""
        return [
            {"number": p, "label": PERIOD_LABELS.get(p, y), "loaded": p in self._periods}
            for p, y in sorted(PERIOD_YEARS.items(), reverse=True)
        ]

    @property
    def loaded_periods(self) -> list[int]:
        """Period numbers currently loaded in memory."""
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
            self._cache_mgr.topic_en_cache,
        )

        # Load cached amendment data
        amendment_data = load_amendments(self.cache_dir, period) if AMENDMENTS_ENABLED else {}

        pd = PeriodData(
            period=period,
            votes=votes,
            mp_votes=mp_votes,
            void_votes=void_votes,
            mp_info=mp_info,
            tisk_lookup=tisk_lookup,
            amendment_data=amendment_data,
        )
        pd.build_amendment_vote_index()
        self._periods[period] = pd

        logger.info(
            "Period {} ready: {} votes, {} vote records, {} MPs, {} tisk links, {} amendment bills",
            period,
            votes.height,
            mp_votes.height,
            mp_info.height,
            len(tisk_lookup),
            len(amendment_data),
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

    # ── File watcher ─────────────────────────────────────────────

    def start_watcher(self) -> None:
        """Start background coroutine that polls parquet mtimes and reloads changed periods."""
        if self._watcher_task is not None:
            return
        self._last_mtimes = _collect_parquet_mtimes(self.cache_dir)
        self._last_amendment_mtimes = _collect_amendment_mtimes(self.cache_dir)
        self._watcher_task = asyncio.create_task(self._watch_loop())
        logger.info("[file-watcher] Started (polling every {}s)", _WATCH_INTERVAL_S)

    async def stop_watcher(self) -> None:
        """Cancel the file-watcher background task."""
        if self._watcher_task is not None:
            self._watcher_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._watcher_task
            self._watcher_task = None
            logger.info("[file-watcher] Stopped")

    async def _watch_loop(self) -> None:
        """Poll parquet file mtimes and reload periods with changed data."""
        while True:
            await asyncio.sleep(_WATCH_INTERVAL_S)
            try:
                self._check_for_updates()
                self._check_amendment_updates()
            except Exception:
                logger.opt(exception=True).warning("[file-watcher] Error during check")

    def _check_for_updates(self) -> None:
        """Compare current parquet mtimes with last known, reload changed periods."""
        current = _collect_parquet_mtimes(self.cache_dir)
        changed_files = {
            name for name, mtime in current.items() if self._last_mtimes.get(name) != mtime
        }
        # Also detect new files
        new_files = set(current.keys()) - set(self._last_mtimes.keys())
        changed_files |= new_files

        if not changed_files:
            return

        self._last_mtimes = current
        logger.info("[file-watcher] Detected {} changed parquet files", len(changed_files))

        # Determine which loaded periods need reloading
        periods_to_reload = self._identify_changed_periods(changed_files)

        for period in periods_to_reload:
            logger.info("[file-watcher] Reloading period {}", period)
            self._cache_mgr.invalidate(period)
            self._load_period(period)

        # Invalidate analysis cache for reloaded periods
        if periods_to_reload:
            analysis_cache.invalidate()
            logger.info(
                "[file-watcher] Invalidated analysis cache for {} periods",
                len(periods_to_reload),
            )

    def _check_amendment_updates(self) -> None:
        """Check for changed amendment parquets and reload affected periods."""
        current = _collect_amendment_mtimes(self.cache_dir)
        changed_periods: list[int] = []
        for period, mtime in current.items():
            if period in self._periods and self._last_amendment_mtimes.get(period) != mtime:
                changed_periods.append(period)
        # Detect new files for loaded periods
        for period in set(current.keys()) - set(self._last_amendment_mtimes.keys()):
            if period in self._periods and period not in changed_periods:
                changed_periods.append(period)

        self._last_amendment_mtimes = current

        for period in changed_periods:
            pd = self._periods[period]
            pd.amendment_data = load_amendments(self.cache_dir, period)
            pd.build_amendment_vote_index()
            analysis_cache.invalidate(f"amendments:{period}:")
            analysis_cache.invalidate(f"amendment-coalitions:{period}:")
            logger.info(
                "[file-watcher] Reloaded amendment data for period {} ({} bills)",
                period,
                len(pd.amendment_data),
            )

    def _identify_changed_periods(self, changed_files: set[str]) -> list[int]:
        """Map changed parquet filenames back to period numbers."""
        periods: set[int] = set()
        for fname in changed_files:
            # Period-specific files: hl_hlasovani_10.parquet, hl_poslanec_9.parquet, etc.
            for prefix in ("hl_hlasovani_", "hl_poslanec_", "zmatecne_"):
                if fname.startswith(prefix):
                    try:
                        p = int(fname.removeprefix(prefix).removesuffix(".parquet"))
                        if p in self._periods:
                            periods.add(p)
                    except ValueError:
                        pass

            # Shared table changes affect all loaded periods
            shared_names = (
                "osoby",
                "poslanec",
                "organy",
                "zarazeni",
                "schuze",
                "bod_schuze",
                "tisky",
            )
            base = fname.removesuffix(".parquet")
            if base in shared_names:
                # Need to reload shared tables + all periods
                self._persons = None
                self._load_shared_tables()
                periods.update(self._periods.keys())
                break

        return sorted(periods, reverse=True)
