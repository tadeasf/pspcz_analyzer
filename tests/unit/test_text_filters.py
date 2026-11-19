"""Tests for the number, percent and date filters (#106)."""

from datetime import date

import pytest

from pspcz_analyzer.i18n import format_date_cz, format_number, format_percent, set_locale


@pytest.fixture(autouse=True)
def _restore_locale():
    yield
    set_locale("cs")


def test_czech_formats() -> None:
    set_locale("cs")
    assert format_number(1377) == "1 377"
    assert format_number(2198.6, 1) == "2 198,6"
    assert format_percent(43.0) == "43,0 %"
    assert format_date_cz("23.09.2026") == "23. 9. 2026"
    assert format_date_cz(date(2026, 9, 23), short=True) == "23. 9."
    assert format_date_cz("2026-09-23 00") == "23. 9. 2026"


def test_english_formats() -> None:
    set_locale("en")
    assert format_number(1377) == "1,377"
    assert format_percent(29.2) == "29.2%"
    assert format_date_cz("23.09.2026") == "Sep 23, 2026"


def test_bad_input_passes_through() -> None:
    assert format_date_cz("zatím neznáme") == "zatím neznáme"
    assert format_number(None) == ""
