"""Tests for data-freshness helpers and DataReader.last_updated."""

from pathlib import Path

from pspcz_analyzer.i18n import set_locale
from pspcz_analyzer.routes.context import _absolute_time, _relative_time
from pspcz_analyzer.services.data_reader import DataReader

NOW = 1_000_000.0


class TestRelativeTime:
    def test_just_now(self):
        assert _relative_time(NOW - 10, NOW) == "právě teď"

    def test_minutes(self):
        set_locale("en")
        assert _relative_time(NOW - 5 * 60, NOW) == "5 min ago"
        set_locale("cs")

    def test_hours(self):
        assert _relative_time(NOW - 3 * 3600, NOW) == "před 3 h"

    def test_days(self):
        assert _relative_time(NOW - 2 * 86400, NOW) == "před 2 dny"

    def test_future_timestamp_clamped(self):
        assert _relative_time(NOW + 500, NOW) == "právě teď"


class TestAbsoluteTime:
    def test_cs_format(self):
        set_locale("cs")
        # 2001-09-09 03:46:40 UTC+2 (Europe/Prague assumed by CI locale);
        # only assert the date part pattern is day-first.
        out = _absolute_time(1_000_000_000.0)
        assert "." in out and "," in out

    def test_en_format(self):
        set_locale("en")
        out = _absolute_time(1_000_000_000.0)
        assert "," in out
        set_locale("cs")


class TestLastUpdated:
    def test_fallback_scan_of_parquet_dir(self, tmp_path: Path):
        parquet_dir = tmp_path / "parquet"
        parquet_dir.mkdir()
        f = parquet_dir / "x.parquet"
        f.write_bytes(b"x")

        reader = DataReader(cache_dir=tmp_path)
        assert reader.last_updated() == f.stat().st_mtime

    def test_none_when_no_cache(self, tmp_path: Path):
        reader = DataReader(cache_dir=tmp_path)
        assert reader.last_updated() is None

    def test_period_amendment_mtime_included(self, tmp_path: Path):
        amend_dir = tmp_path / "amendments" / "10"
        amend_dir.mkdir(parents=True)
        f = amend_dir / "amendments.parquet"
        f.write_bytes(b"x")

        reader = DataReader(cache_dir=tmp_path)
        assert reader.last_updated(period=10) == f.stat().st_mtime
        assert reader.last_updated(period=9) is None
