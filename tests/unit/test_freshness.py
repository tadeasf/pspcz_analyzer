"""Tests for the footer dates (#105)."""

from datetime import date
from pathlib import Path
from types import SimpleNamespace

from pspcz_analyzer.i18n import set_locale
from pspcz_analyzer.routes.context import _histories_loaded, format_date


class TestFormatDate:
    def test_cs_format(self):
        set_locale("cs")
        assert format_date(date(2026, 10, 4)) == "4. 10. 2026"

    def test_en_format(self):
        set_locale("en")
        try:
            assert format_date(date(2026, 10, 4)) == "Oct 4, 2026"
        finally:
            set_locale("cs")


class TestHistoriesLoaded:
    def test_none_without_histories(self, tmp_path: Path):
        assert _histories_loaded(SimpleNamespace(cache_dir=tmp_path), 10) is None

    def test_newest_history_file(self, tmp_path: Path):
        hist = tmp_path / "tisky_meta" / "10" / "tisky_historie"
        hist.mkdir(parents=True)
        (hist / "1.json").write_text("{}")
        set_locale("cs")
        out = _histories_loaded(SimpleNamespace(cache_dir=tmp_path), 10)
        assert out == format_date(date.today())
