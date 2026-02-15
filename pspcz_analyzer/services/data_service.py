"""Data service: orchestrates download, parsing, caching, and holds DataFrames."""

from dataclasses import dataclass, field
from pathlib import Path

import polars as pl
from loguru import logger

from pspcz_analyzer.config import (
    DEFAULT_CACHE_DIR,
    DEFAULT_PERIOD,
    PERIOD_LABELS,
    PERIOD_ORGAN_IDS,
    PERIOD_YEARS,
    TISKY_HISTORIE_DIR,
    TISKY_LAW_CHANGES_DIR,
    TISKY_META_DIR,
    TISKY_PDF_DIR,
    TISKY_TEXT_DIR,
    TISKY_VERSION_DIFFS_DIR,
)
from pspcz_analyzer.services.tisk_pipeline_service import TiskPipelineService
from pspcz_analyzer.services.tisk_text_service import TiskTextService
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


@dataclass
class TiskInfo:
    """Info about a parliamentary print linked to a vote."""

    id_tisk: int
    ct: int  # tisk number
    nazev: str
    period: int
    topics: list[str] = field(default_factory=list)
    has_text: bool = False
    summary: str = ""
    history: object | None = None  # TiskHistory from data.history_scraper
    law_changes: list[dict] = field(default_factory=list)
    sub_versions: list[dict] = field(default_factory=list)

    @property
    def url(self) -> str:
        return f"https://www.psp.cz/sqw/historie.sqw?o={self.period}&t={self.ct}"


@dataclass
class PeriodData:
    """All DataFrames for a single electoral period."""

    period: int
    votes: pl.DataFrame
    mp_votes: pl.DataFrame
    void_votes: pl.DataFrame
    mp_info: pl.DataFrame
    # Lookup: (schuze_num, bod_num) -> TiskInfo
    tisk_lookup: dict[tuple[int, int], TiskInfo] = field(default_factory=dict)

    @property
    def stats(self) -> dict:
        date_col = self.votes.get_column("datum")
        dates = date_col.drop_nulls().str.strip_chars()
        return {
            "period": self.period,
            "label": PERIOD_LABELS.get(self.period, PERIOD_YEARS.get(self.period, "?")),
            "total_votes": self.votes.height,
            "total_mp_records": self.mp_votes.height,
            "total_mps": self.mp_info.height,
            "date_min": dates.sort().head(1).to_list()[0] if dates.len() > 0 else "N/A",
            "date_max": dates.sort().tail(1).to_list()[0] if dates.len() > 0 else "N/A",
            "void_votes": self.void_votes.height,
        }

    def get_tisk(self, schuze: int, bod: int) -> TiskInfo | None:
        """Get tisk info for a vote given its session and agenda item numbers."""
        return self.tisk_lookup.get((schuze, bod))

    def get_all_topic_labels(self) -> list[str]:
        """Collect all unique topic labels across all tisky, sorted."""
        labels: set[str] = set()
        for tisk in self.tisk_lookup.values():
            labels.update(tisk.topics)
        return sorted(labels)


class DataService:
    """Manages data for multiple electoral periods, loading on demand."""

    def __init__(self, cache_dir: Path = DEFAULT_CACHE_DIR) -> None:
        self.cache_dir = cache_dir
        self._periods: dict[int, PeriodData] = {}
        self.tisk_text = TiskTextService(cache_dir)
        self.tisk_pipeline = TiskPipelineService(cache_dir)

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

        # Topic classification cache: period -> {ct -> [topic_labels]}
        self._topic_cache: dict[int, dict[int, list[str]]] = {}
        # Summary cache: period -> {ct -> summary_text}
        self._summary_cache: dict[int, dict[int, str]] = {}
        # Track parquet mtime to detect incremental updates
        self._topic_cache_mtime: dict[int, float] = {}
        # Legislative history cache: period -> {ct -> TiskHistory}
        self._history_cache: dict[int, dict] = {}
        # Note: law_changes, subtisk_versions, and version_diffs are always
        # read from disk (no in-memory cache) for incremental UI updates.

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
        topic_map = self._load_topic_cache(period)
        summary_map = self._summary_cache.get(period, {})
        history_map = self._load_history_cache(period)
        law_changes_map = self._load_law_changes_cache(period)
        subtisk_map = self._load_subtisk_versions_cache(period)
        diffs_map = self._load_version_diffs_cache(period)
        for tisk in pd.tisk_lookup.values():
            tisk.topics = topic_map.get(tisk.ct, [])
            tisk.summary = summary_map.get(tisk.ct, "")
            tisk.has_text = self.tisk_text.has_text(period, tisk.ct)
            tisk.history = history_map.get(tisk.ct)
            tisk.law_changes = law_changes_map.get(tisk.ct, [])
            # Populate sub_versions with diff summaries
            versions = subtisk_map.get(tisk.ct, [])
            for v in versions:
                diff_key = f"{tisk.ct}_{v.get('ct1', '')}"
                v["llm_diff_summary"] = diffs_map.get(diff_key, "")
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
            "osoby", poslanci_dir,
            lambda: parse_unl(
                self._find_file(poslanci_dir, "osoby.unl"),
                OSOBY_COLUMNS, OSOBY_DTYPES,
            ),
            self.cache_dir,
        )

        self._mps = get_or_parse(
            "poslanec", poslanci_dir,
            lambda: parse_unl(
                self._find_file(poslanci_dir, "poslanec.unl"),
                POSLANEC_COLUMNS, POSLANEC_DTYPES,
            ),
            self.cache_dir,
        )

        self._organs = get_or_parse(
            "organy", poslanci_dir,
            lambda: parse_unl(
                self._find_file(poslanci_dir, "organy.unl"),
                ORGANY_COLUMNS, ORGANY_DTYPES,
            ),
            self.cache_dir,
        )

        self._memberships = get_or_parse(
            "zarazeni", poslanci_dir,
            lambda: parse_unl(
                self._find_file(poslanci_dir, "zarazeni.unl"),
                ZARAZENI_COLUMNS, ZARAZENI_DTYPES,
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
            "schuze", schuze_dir,
            lambda: parse_unl(
                self._find_file(schuze_dir, "schuze.unl"),
                SCHUZE_COLUMNS, SCHUZE_DTYPES,
            ),
            self.cache_dir,
        )

        self._bod_schuze = get_or_parse(
            "bod_schuze", schuze_dir,
            lambda: parse_unl(
                self._find_file(schuze_dir, "bod_schuze.unl"),
                BOD_SCHUZE_COLUMNS, BOD_SCHUZE_DTYPES,
            ),
            self.cache_dir,
        )

        self._tisky = get_or_parse(
            "tisky", tisky_dir,
            lambda: parse_unl(
                self._find_file(tisky_dir, "tisky.unl"),
                TISKY_COLUMNS, TISKY_DTYPES,
            ),
            self.cache_dir,
        )

        logger.info(
            "Loaded schuze ({}), bod_schuze ({}), tisky ({})",
            self._schuze.height, self._bod_schuze.height, self._tisky.height,
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

    def _build_tisk_lookup(
        self, period: int, votes: pl.DataFrame,
    ) -> dict[tuple[int, int], TiskInfo]:
        """Build a mapping from (schuze_num, bod_num) -> TiskInfo for a given period.

        Primary path: schuze -> bod_schuze -> tisky (reliable, full coverage).
        Fallback: if schuze data is missing for this period, match vote
        descriptions directly to tisk names (covers new periods where
        schuze.zip hasn't been updated yet).
        """
        self._ensure_shared_loaded()
        assert self._schuze is not None  # for type narrowing
        organ_id = PERIOD_ORGAN_IDS[period]

        # Try primary path via schuze -> bod_schuze
        sessions = self._schuze.filter(pl.col("id_org") == organ_id)
        if sessions.height > 0:
            return self._build_tisk_lookup_via_schuze(period, sessions)

        # Fallback: text matching for periods without schuze data
        logger.info(
            "No session data for period {} (organ {}), using text-match fallback",
            period, organ_id,
        )
        return self._build_tisk_lookup_via_text(period, votes)

    def _build_tisk_lookup_via_schuze(
        self, period: int, sessions: pl.DataFrame,
    ) -> dict[tuple[int, int], TiskInfo]:
        """Build lookup using the schuze -> bod_schuze -> tisky chain."""
        assert self._bod_schuze is not None
        assert self._tisky is not None

        session_map = dict(zip(
            sessions.get_column("id_schuze").to_list(),
            sessions.get_column("schuze").to_list(),
        ))
        session_ids = set(session_map.keys())

        bods = self._bod_schuze.filter(
            pl.col("id_schuze").is_in(session_ids)
            & pl.col("id_tisk").is_not_null()
            & (pl.col("id_tisk") != 0)
        )

        if bods.height == 0:
            return {}

        # Load topic classifications, summaries, and text availability
        topic_map = self._load_topic_cache(period)
        summary_map = self._summary_cache.get(period, {})

        tisk_ids = set(bods.get_column("id_tisk").to_list())
        relevant_tisky = self._tisky.filter(pl.col("id_tisk").is_in(tisk_ids))
        tisk_map = {}
        for row in relevant_tisky.iter_rows(named=True):
            ct = row.get("ct")
            if ct:
                tisk_map[row["id_tisk"]] = TiskInfo(
                    id_tisk=row["id_tisk"],
                    ct=ct,
                    nazev=row.get("nazev_tisku") or "",
                    period=period,
                    topics=topic_map.get(ct, []),
                    has_text=self.tisk_text.has_text(period, ct),
                    summary=summary_map.get(ct, ""),
                )

        lookup: dict[tuple[int, int], TiskInfo] = {}
        for row in bods.iter_rows(named=True):
            id_schuze = row["id_schuze"]
            schuze_num = session_map.get(id_schuze)
            bod_num = row.get("bod")
            id_tisk = row["id_tisk"]
            if schuze_num is not None and bod_num is not None and id_tisk in tisk_map:
                lookup[(schuze_num, bod_num)] = tisk_map[id_tisk]

        logger.info(
            "Period {}: built tisk lookup with {} entries (via schuze)",
            period, len(lookup),
        )
        return lookup

    def _build_tisk_lookup_via_text(
        self, period: int, votes: pl.DataFrame,
    ) -> dict[tuple[int, int], TiskInfo]:
        """Fallback: match vote descriptions to tisk names for this period.

        Used when schuze.zip hasn't been updated for a new period yet.
        """
        assert self._tisky is not None

        organ_id = PERIOD_ORGAN_IDS[period]
        period_tisky = self._tisky.filter(pl.col("id_obdobi") == organ_id)
        if period_tisky.height == 0:
            return {}

        # Load topic classifications, summaries, and text availability
        topic_map = self._load_topic_cache(period)
        summary_map = self._summary_cache.get(period, {})

        # Build list of tisk names for matching (longest first for greedy match)
        tisk_entries = []
        for row in period_tisky.iter_rows(named=True):
            ct = row.get("ct")
            nazev = (row.get("nazev_tisku") or "").strip()
            if ct and nazev:
                tisk_entries.append(TiskInfo(
                    id_tisk=row["id_tisk"],
                    ct=ct,
                    nazev=nazev,
                    period=period,
                    topics=topic_map.get(ct, []),
                    has_text=self.tisk_text.has_text(period, ct),
                    summary=summary_map.get(ct, ""),
                ))
        tisk_entries.sort(key=lambda t: len(t.nazev), reverse=True)

        # Get unique (schuze, bod) combinations with descriptions
        vote_bods = votes.filter(
            pl.col("nazev_dlouhy").is_not_null() & (pl.col("bod") > 0)
        ).select("schuze", "bod", "nazev_dlouhy").unique(subset=["schuze", "bod"])

        lookup: dict[tuple[int, int], TiskInfo] = {}
        for row in vote_bods.iter_rows(named=True):
            desc = (row["nazev_dlouhy"] or "").strip()
            if not desc:
                continue
            for tisk in tisk_entries:
                if desc.startswith(tisk.nazev) or tisk.nazev.startswith(desc):
                    lookup[(row["schuze"], row["bod"])] = tisk
                    break

        logger.info(
            "Period {}: built tisk lookup with {} entries (via text match, {} tisky available)",
            period, len(lookup), len(tisk_entries),
        )
        return lookup

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
            f"hl_hlasovani_{period}", voting_dir,
            lambda: parse_unl(
                self._find_file(voting_dir, f"hl{year}s.unl"),
                HL_HLASOVANI_COLUMNS, HL_HLASOVANI_DTYPES,
            ),
            self.cache_dir,
        )

        mp_votes = get_or_parse(
            f"hl_poslanec_{period}", voting_dir,
            lambda: parse_unl_multi(
                voting_dir, f"hl{year}h*.unl",
                HL_POSLANEC_COLUMNS, HL_POSLANEC_DTYPES,
            ),
            self.cache_dir,
        )

        try:
            zmatecne_file = self._find_file(voting_dir, f"hl{year}z.unl")
            void_votes = get_or_parse(
                f"zmatecne_{period}", voting_dir,
                lambda: parse_unl(zmatecne_file, ZMATECNE_COLUMNS, ZMATECNE_DTYPES),
                self.cache_dir,
            )
        except FileNotFoundError:
            logger.info("No void votes file for period {}", period)
            void_votes = pl.DataFrame({"id_hlasovani": pl.Series([], dtype=pl.Int64)})

        mp_info = self._build_mp_info(period)
        tisk_lookup = self._build_tisk_lookup(period, votes)

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
            period, votes.height, mp_votes.height, mp_info.height, len(tisk_lookup),
        )

    def _build_mp_info(self, period: int) -> pl.DataFrame:
        """Build MP lookup table: id_poslanec -> name, party for a given period."""
        assert self._mps is not None
        assert self._persons is not None
        assert self._organs is not None
        assert self._memberships is not None

        organ_id = PERIOD_ORGAN_IDS[period]
        period_mps = self._mps.filter(pl.col("id_obdobi") == organ_id)

        mp_persons = period_mps.join(
            self._persons.select("id_osoba", "jmeno", "prijmeni"),
            on="id_osoba",
            how="left",
        )

        clubs = self._organs.filter(pl.col("id_typ_organu") == 1).select(
            "id_organ", "zkratka"
        )

        club_memberships = (
            self._memberships
            .join(clubs, left_on="id_of", right_on="id_organ", how="inner")
            .select("id_osoba", "zkratka", "od_o", "do_o")
        )

        club_memberships = club_memberships.sort("od_o", descending=True).unique(
            subset=["id_osoba"], keep="first"
        )

        mp_info = mp_persons.join(
            club_memberships.select("id_osoba", pl.col("zkratka").alias("party")),
            on="id_osoba",
            how="left",
        ).select("id_poslanec", "id_osoba", "jmeno", "prijmeni", "party")

        # Normalize party abbreviations from psp.cz to commonly used names.
        # "ANO2011" is the official registration name but everyone calls it "ANO".
        # "Nezařaz" is the truncated abbreviation for independent MPs ("Nezařazení").
        party_aliases = {
            "ANO2011": "ANO",
            "Nezařaz": "Nezařazení",
        }
        return mp_info.with_columns(
            pl.col("party").replace(party_aliases).alias("party")
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

    def _load_topic_cache(self, period: int) -> dict[int, list[str]]:
        """Load topic classifications (and summaries) from parquet cache.

        Re-reads the parquet if it's been modified since last load (picks up
        incremental pipeline updates).
        """
        from pspcz_analyzer.services.ollama_service import deserialize_topics

        meta_path = self.cache_dir / TISKY_META_DIR / str(period) / "topic_classifications.parquet"
        if not meta_path.exists():
            self._topic_cache[period] = {}
            self._summary_cache[period] = {}
            self._topic_cache_mtime[period] = 0
            return {}

        # Check if we need to re-read (new file or modified since last load)
        current_mtime = meta_path.stat().st_mtime
        cached_mtime = self._topic_cache_mtime.get(period, 0)
        if period in self._topic_cache and current_mtime == cached_mtime:
            return self._topic_cache[period]

        df = pl.read_parquet(meta_path)
        topics: dict[int, list[str]] = {}
        summaries: dict[int, str] = {}
        for row in df.iter_rows(named=True):
            ct = row["ct"]
            raw_topic = row.get("topic", "")
            parsed = deserialize_topics(raw_topic)
            if parsed:
                topics[ct] = parsed
            summary = row.get("summary", "")
            if summary:
                summaries[ct] = summary
        self._topic_cache[period] = topics
        self._summary_cache[period] = summaries
        self._topic_cache_mtime[period] = current_mtime
        logger.debug(
            "Loaded topic classifications for period {}: {} tisky, {} summaries",
            period, len(topics), len(summaries),
        )
        return topics

    def _load_history_cache(self, period: int) -> dict:
        """Load legislative history JSON files for a period.

        Returns {ct: TiskHistory} dict. Caches in memory.
        """
        if period in self._history_cache:
            return self._history_cache[period]

        from pspcz_analyzer.data.history_scraper import load_history_json

        hist_dir = self.cache_dir / TISKY_META_DIR / str(period) / TISKY_HISTORIE_DIR
        histories: dict = {}
        if not hist_dir.exists():
            self._history_cache[period] = histories
            return histories

        for json_path in hist_dir.glob("*.json"):
            try:
                ct = int(json_path.stem)
            except ValueError:
                continue
            h = load_history_json(json_path)
            if h:
                histories[ct] = h

        self._history_cache[period] = histories
        if histories:
            logger.debug(
                "Loaded {} tisk histories for period {}", len(histories), period,
            )
        return histories

    def _load_law_changes_cache(self, period: int) -> dict[int, list[dict]]:
        """Load law changes JSON files for a period.

        Always reads from disk (no in-memory cache) so incremental pipeline
        results are visible immediately in the UI.
        Returns {ct: [law_change_dicts]}.
        """
        lc_dir = self.cache_dir / TISKY_META_DIR / str(period) / TISKY_LAW_CHANGES_DIR
        changes: dict[int, list[dict]] = {}
        if not lc_dir.exists():
            return changes

        import json

        for json_path in lc_dir.glob("*.json"):
            try:
                ct = int(json_path.stem)
            except ValueError:
                continue
            try:
                data = json.loads(json_path.read_text(encoding="utf-8"))
                if data:  # only store non-empty
                    changes[ct] = data
            except Exception:
                logger.opt(exception=True).warning(
                    "Failed to load law changes from {}", json_path,
                )

        return changes

    def _load_subtisk_versions_cache(self, period: int) -> dict[int, list[dict]]:
        """Load sub-tisk version info from JSON cache.

        Always reads from disk (no in-memory cache) so incremental pipeline
        results are visible immediately in the UI.
        Returns {ct: [version_dicts]}.
        """
        import json

        scan_dir = self.cache_dir / TISKY_META_DIR / str(period) / "subtisk_versions"
        versions: dict[int, list[dict]] = {}

        if not scan_dir.exists():
            return versions

        for json_path in scan_dir.glob("*.json"):
            try:
                ct = int(json_path.stem)
            except ValueError:
                continue
            try:
                data = json.loads(json_path.read_text(encoding="utf-8"))
                if data:  # skip empty (means no sub-versions)
                    versions[ct] = data
            except Exception:
                logger.opt(exception=True).warning(
                    "Failed to load subtisk cache from {}", json_path,
                )

        return versions

    def _load_version_diffs_cache(self, period: int) -> dict[str, str]:
        """Load LLM version diff summaries for a period.

        Always reads from disk (no in-memory cache) so incremental pipeline
        results are visible immediately in the UI.
        Returns {"{ct}_{ct1}": summary_text}.
        """
        diff_dir = (
            self.cache_dir / TISKY_META_DIR / str(period) / TISKY_VERSION_DIFFS_DIR
        )
        diffs: dict[str, str] = {}
        if not diff_dir.exists():
            return diffs

        for txt_path in diff_dir.glob("*.txt"):
            key = txt_path.stem  # "{ct}_{ct1}"
            try:
                diffs[key] = txt_path.read_text(encoding="utf-8")
            except Exception:
                logger.opt(exception=True).warning(
                    "Failed to load version diff from {}", txt_path,
                )

        return diffs

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
            p: int, text_paths: dict, topic_map: dict, summary_map: dict,
            histories: dict | None = None,
            law_changes_map: dict | None = None,
            subtisk_map: dict | None = None,
            version_diffs: dict | None = None,
        ) -> None:
            """Callback: refresh in-memory tisk data after pipeline finishes."""
            # Invalidate caches so next lookup picks up new data
            self._topic_cache.pop(p, None)
            self._summary_cache.pop(p, None)
            self._history_cache.pop(p, None)

            # Update existing tisk_lookup entries
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
            p: int, text_paths: dict, topic_map: dict, summary_map: dict,
            histories: dict | None = None,
            law_changes_map: dict | None = None,
            subtisk_map: dict | None = None,
            version_diffs: dict | None = None,
        ) -> None:
            self._topic_cache.pop(p, None)
            self._summary_cache.pop(p, None)
            self._history_cache.pop(p, None)
            pd = self._periods.get(p)
            if pd is None:
                return
            self._refresh_tisk_data(p)
            logger.info("[tisk pipeline] Updated in-memory tisk data for period {}", p)

        self.tisk_pipeline.start_all_periods(period_ct, on_complete=_on_complete)
