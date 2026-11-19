"""Tests for the (schuze, bod) -> tisk lookup: approved agenda only (#85)."""

from unittest.mock import MagicMock

import polars as pl

from pspcz_analyzer.services.tisk.lookup_builder import build_tisk_lookup_via_schuze

_SESSIONS = pl.DataFrame(
    {
        "id_schuze": [100, 100],
        "schuze": [10, 10],
        "pozvanka": [None, "1"],
    }
)

# Shapes of the real mismatches (votes 86822, 86479, 87746):
#  bod 17: proposed and approved agenda carry different tisky -> approved wins
#  bod 51: approved item has no tisk, proposed one does -> must stay unlinked
#  bod 137: undocumented pozvanka "0" interpellation row collides -> ignored
_BODS = pl.DataFrame(
    {
        "id_schuze": [100, 100, 100, 100, 100, 100],
        "bod": [17, 17, 51, 51, 137, 137],
        "id_tisk": [1, 2, None, 3, 4, 5],
        "pozvanka": [None, "1", None, "1", None, "0"],
    }
)

_TISKY = pl.DataFrame(
    {
        "id_tisk": [1, 2, 3, 4, 5],
        "ct": [11, 22, 33, 44, 55],
        "nazev_tisku": ["approved", "proposed", "proposed-only", "real", "interpellation"],
        "id_druh": [1, 2, 2, 5, 6],
    }
)


def _build(bods: pl.DataFrame) -> dict[tuple[int, int], int]:
    text = MagicMock()
    text.has_text.return_value = False
    lookup = build_tisk_lookup_via_schuze(10, _SESSIONS, bods, _TISKY, text, {}, {})
    return {key: info.ct for key, info in lookup.items()}


def test_lookup_uses_only_approved_agenda() -> None:
    assert _build(_BODS) == {(10, 17): 11, (10, 137): 44}


def test_proposed_rows_never_overwrite_regardless_of_order() -> None:
    assert _build(_BODS.reverse()) == {(10, 17): 11, (10, 137): 44}


def test_lookup_carries_document_type() -> None:
    text = MagicMock()
    text.has_text.return_value = False
    lookup = build_tisk_lookup_via_schuze(10, _SESSIONS, _BODS, _TISKY, text, {}, {})
    assert {key: info.id_druh for key, info in lookup.items()} == {(10, 17): 1, (10, 137): 5}
