"""Cache manager for tisk metadata: topics, histories, law changes, versions."""

import json
from pathlib import Path

import polars as pl
from loguru import logger

from pspcz_analyzer.config import (
    TISKY_HISTORIE_DIR,
    TISKY_LAW_CHANGES_DIR,
    TISKY_META_DIR,
    TISKY_VERSION_DIFFS_DIR,
)
from pspcz_analyzer.data.history_scraper import load_history_json
from pspcz_analyzer.services.ollama_service import deserialize_topics


class TiskCacheManager:
    """Loads and caches tisk metadata from the filesystem."""

    def __init__(self, cache_dir: Path) -> None:
        self.cache_dir = cache_dir
        # Topic classification cache: period -> {ct -> [topic_labels]}
        self._topic_cache: dict[int, dict[int, list[str]]] = {}
        # Summary cache: period -> {ct -> summary_text}
        self._summary_cache: dict[int, dict[int, str]] = {}
        # English summary cache: period -> {ct -> summary_en_text}
        self._summary_en_cache: dict[int, dict[int, str]] = {}
        # Track parquet mtime to detect incremental updates
        self._topic_cache_mtime: dict[int, float] = {}
        # Legislative history cache: period -> {ct -> TiskHistory}
        self._history_cache: dict[int, dict] = {}

    @property
    def topic_cache(self) -> dict[int, dict[int, list[str]]]:
        return self._topic_cache

    @property
    def summary_cache(self) -> dict[int, dict[int, str]]:
        return self._summary_cache

    @property
    def summary_en_cache(self) -> dict[int, dict[int, str]]:
        return self._summary_en_cache

    def invalidate(self, period: int) -> None:
        """Invalidate all caches for a period so next access re-reads from disk."""
        self._topic_cache.pop(period, None)
        self._summary_cache.pop(period, None)
        self._summary_en_cache.pop(period, None)
        self._history_cache.pop(period, None)

    def load_topic_cache(self, period: int) -> dict[int, list[str]]:
        """Load topic classifications (and summaries) from parquet cache.

        Re-reads the parquet if it's been modified since last load (picks up
        incremental pipeline updates).
        """
        meta_path = self.cache_dir / TISKY_META_DIR / str(period) / "topic_classifications.parquet"
        if not meta_path.exists():
            self._topic_cache[period] = {}
            self._summary_cache[period] = {}
            self._summary_en_cache[period] = {}
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
        summaries_en: dict[int, str] = {}
        for row in df.iter_rows(named=True):
            ct = row["ct"]
            raw_topic = row.get("topic", "")
            parsed = deserialize_topics(raw_topic)
            if parsed:
                topics[ct] = parsed
            summary = row.get("summary", "")
            if summary:
                summaries[ct] = summary
            summary_en = row.get("summary_en", "")
            if summary_en:
                summaries_en[ct] = summary_en
        self._topic_cache[period] = topics
        self._summary_cache[period] = summaries
        self._summary_en_cache[period] = summaries_en
        self._topic_cache_mtime[period] = current_mtime
        logger.debug(
            "Loaded topic classifications for period {}: {} tisky, {} summaries, {} EN summaries",
            period,
            len(topics),
            len(summaries),
            len(summaries_en),
        )
        return topics

    def load_history_cache(self, period: int) -> dict:
        """Load legislative history JSON files for a period.

        Returns {ct: TiskHistory} dict. Caches in memory.
        """
        if period in self._history_cache:
            return self._history_cache[period]

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
                "Loaded {} tisk histories for period {}",
                len(histories),
                period,
            )
        return histories

    def load_law_changes_cache(self, period: int) -> dict[int, list[dict]]:
        """Load law changes JSON files for a period.

        Always reads from disk (no in-memory cache) so incremental pipeline
        results are visible immediately in the UI.
        Returns {ct: [law_change_dicts]}.
        """
        lc_dir = self.cache_dir / TISKY_META_DIR / str(period) / TISKY_LAW_CHANGES_DIR
        changes: dict[int, list[dict]] = {}
        if not lc_dir.exists():
            return changes

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
                    "Failed to load law changes from {}",
                    json_path,
                )

        return changes

    def load_subtisk_versions_cache(self, period: int) -> dict[int, list[dict]]:
        """Load sub-tisk version info from JSON cache.

        Always reads from disk (no in-memory cache) so incremental pipeline
        results are visible immediately in the UI.
        Returns {ct: [version_dicts]}.
        """
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
                    "Failed to load subtisk cache from {}",
                    json_path,
                )

        return versions

    def load_version_diffs_cache(self, period: int) -> tuple[dict[str, str], dict[str, str]]:
        """Load LLM version diff summaries for a period.

        Always reads from disk (no in-memory cache) so incremental pipeline
        results are visible immediately in the UI.
        Returns ({"{ct}_{ct1}": summary_cs}, {"{ct}_{ct1}": summary_en}).
        """
        diff_dir = self.cache_dir / TISKY_META_DIR / str(period) / TISKY_VERSION_DIFFS_DIR
        diffs: dict[str, str] = {}
        diffs_en: dict[str, str] = {}
        if not diff_dir.exists():
            return diffs, diffs_en

        for txt_path in diff_dir.glob("*.txt"):
            stem = txt_path.stem
            # English diff files end with _en
            if stem.endswith("_en"):
                key = stem[:-3]  # strip _en
                try:
                    diffs_en[key] = txt_path.read_text(encoding="utf-8")
                except Exception:
                    logger.opt(exception=True).warning(
                        "Failed to load EN version diff from {}",
                        txt_path,
                    )
            else:
                try:
                    diffs[stem] = txt_path.read_text(encoding="utf-8")
                except Exception:
                    logger.opt(exception=True).warning(
                        "Failed to load version diff from {}",
                        txt_path,
                    )

        return diffs, diffs_en
