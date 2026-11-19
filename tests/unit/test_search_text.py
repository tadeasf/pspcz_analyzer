"""Tests for the shared search matcher (#95)."""

import pytest

from pspcz_analyzer.utils.text import matches_tokens, search_tokens, tisk_number_query


@pytest.mark.parametrize(
    ("query", "number"),
    [
        ("246", 246),
        ("tisk 246", 246),
        ("ST 246", 246),
        ("st. 246", 246),
        (" Tisk246 ", 246),
        ("246 televize", None),
        ("dálnice", None),
    ],
)
def test_tisk_number_query(query: str, number: int | None) -> None:
    assert tisk_number_query(query) == number


@pytest.mark.parametrize(
    ("query", "text"),
    [
        ("poplatky", "Novela z. o rozhlasových a televizních poplatcích"),
        ("televize", "Novela z. o České televizi a o Českém rozhlasu"),
        ("česká televize", "Novela z. o České televizi"),
        ("dalnicni znamka", "zmrazení ceny dálniční známky"),
    ],
)
def test_inflection_and_diacritics(query: str, text: str) -> None:
    assert matches_tokens(search_tokens(query), text)


def test_all_tokens_must_match() -> None:
    assert not matches_tokens(search_tokens("televize rozpočet"), "Novela z. o České televizi")
