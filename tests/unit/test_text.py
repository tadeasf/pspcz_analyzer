"""Tests for text normalization and status classification utilities."""

import pytest

from pspcz_analyzer.utils.text import normalize_czech, status_category, strip_diacritics


@pytest.mark.parametrize(
    "status",
    [
        "Vyhlášeno",
        "Podepsáno prezidentem",
        "Schváleno sněmovnou",
        "návrh byl přijat",
        "Výbor doporučuje",
        "schválen ve 3. čtení",
        "podepsal prezident",
    ],
)
def test_status_category_pass(status: str) -> None:
    assert status_category(status) == "pass"


@pytest.mark.parametrize(
    "status",
    [
        "Zamítnuto",
        "Staženo",
        "zamítnut sněmovnou",
        # "nedoporučuje" contains "doporučuje" — fail must win (template
        # substring checks used to style this as approved)
        "Výbor nedoporučuje",
    ],
)
def test_status_category_fail(status: str) -> None:
    assert status_category(status) == "fail"


@pytest.mark.parametrize(
    "status",
    ["Projednáváno", "1. čtení", "Vláda projednala", ""],
)
def test_status_category_active(status: str) -> None:
    assert status_category(status) == "active"


def test_status_category_none() -> None:
    assert status_category(None) == "active"


def test_strip_diacritics() -> None:
    assert strip_diacritics("Bartošem") == "Bartosem"
    assert strip_diacritics("řžč") == "rzc"


def test_normalize_czech() -> None:
    assert normalize_czech("Bartošem") == "bartosem"
