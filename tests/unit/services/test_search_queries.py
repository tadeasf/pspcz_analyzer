"""#95: the issue's search queries over a small stored sample (laws and votes lists)."""

import polars as pl
import pytest

from pspcz_analyzer.models.tisk_models import PeriodData, TiskInfo
from pspcz_analyzer.services.law_service import list_laws
from pspcz_analyzer.services.votes_service import list_votes
from tests.fixtures.sample_data import make_mp_info, make_mp_votes, make_void_votes, make_votes

# (ct, short title) as in period 10's tisky.unl; each is linked to agenda item (1, ct)
_TISKY = [
    (82, "Novela z. o podpoře bydlení"),
    (201, "Novela z. o rozhlasových a televizních poplatcích"),
    (231, "Novela z. o České televizi a o Českém rozhlasu"),
    (311, "Novela z. o daních z příjmů"),
]


def _sample() -> PeriodData:
    """Period data with one vote on each sample tisk's agenda item."""
    votes = make_votes(len(_TISKY)).with_columns(
        pl.Series("bod", [ct for ct, _ in _TISKY], dtype=pl.Int32),
        pl.Series("nazev_dlouhy", [name for _, name in _TISKY]),
        pl.Series("nazev_kratky", [name[:20] for _, name in _TISKY]),
    )
    lookup = {
        (1, ct): TiskInfo(id_tisk=1000 + ct, ct=ct, nazev=name, period=10, id_druh=2)
        for ct, name in _TISKY
    }
    return PeriodData(
        period=10,
        votes=votes,
        mp_votes=make_mp_votes(),
        void_votes=make_void_votes(),
        mp_info=make_mp_info(),
        tisk_lookup=lookup,
    )


def _law_cts(search: str) -> list[int]:
    return sorted(row["ct"] for row in list_laws(_sample(), search=search)["rows"])


def _vote_bods(search: str) -> list[int]:
    return sorted(row["bod"] for row in list_votes(_sample(), search=search)["rows"])


_QUERIES = [
    ("bydlení", [82]),
    ("bydleni", [82]),
    ("82", [82]),
    ("tisk 82", [82]),
    ("České televizi", [231]),
    ("Česká televize", [231]),
    ("ceske televizi", [231]),
    ("poplatky", [201]),
]


@pytest.mark.parametrize(("search", "expected"), _QUERIES)
def test_laws_search(search: str, expected: list[int]) -> None:
    assert _law_cts(search) == expected


@pytest.mark.parametrize(("search", "expected"), _QUERIES)
def test_votes_search(search: str, expected: list[int]) -> None:
    # The vote's bod equals the linked tisk number in the sample
    assert _vote_bods(search) == expected


def test_diacritics_and_inflection_give_the_same_results() -> None:
    assert _law_cts("bydleni") == _law_cts("bydlení")
    assert _law_cts("ceske televizi") == _law_cts("České televizi") == _law_cts("Česká televize")
    assert _vote_bods("ceske televizi") == _vote_bods("Česká televize")
