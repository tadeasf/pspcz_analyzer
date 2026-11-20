"""Tests for per-period clubs and the club at the vote date (#101)."""

import polars as pl

from pspcz_analyzer.services.mp_builder import add_vote_parties, build_mp_info

# Period 10 = organ 174. Clubs 1750 (ANO2011) and 1894 (Nezařaz) belong to it, 1537 to period 9.
ORGANS = pl.DataFrame(
    {
        "id_organ": [1750, 1894, 1537, 500],
        "id_typ_organu": [1, 1, 1, 7],
        "organ_id_organ": [174, 174, 173, 174],
        "zkratka": ["ANO2011", "Nezařaz", "ANO", "VÝBOR"],
    },
    schema_overrides={"id_organ": pl.Int32, "organ_id_organ": pl.Int32},
)
MEMBERSHIPS = pl.DataFrame(
    {
        "id_osoba": [1, 1, 1, 1],
        "id_of": [1537, 1750, 1894, 1894],
        # last row is a function row whose id_of collides with a club id
        "cl_funkce": ["0", "0", "0", "1"],
        "od_o": ["2021-10-12 00", "2025-10-08 00", "2026-11-01 00", "2030-01-01 00"],
        "do_o": ["2025-10-08 00", "2026-11-01 00", None, None],
    },
    schema_overrides={"id_of": pl.Int32},
)
MPS = pl.DataFrame({"id_poslanec": [11], "id_osoba": [1], "id_obdobi": [174]})
PERSONS = pl.DataFrame({"id_osoba": [1], "jmeno": ["David"], "prijmeni": ["Pražák"]})


def test_mp_info_uses_latest_club_of_the_period() -> None:
    info = build_mp_info(10, MPS, PERSONS, ORGANS, MEMBERSHIPS)
    assert info.get_column("party").to_list() == ["Nezařazení"]


def test_party_at_vote_date() -> None:
    info = build_mp_info(10, MPS, PERSONS, ORGANS, MEMBERSHIPS)
    votes = pl.DataFrame(
        {"id_hlasovani": [1, 2, 3], "datum": ["24.10.2026", "01.11.2026", "05.11.2026"]}
    )
    mp_votes = pl.DataFrame(
        {"id_poslanec": [11, 11, 11], "id_hlasovani": [3, 1, 2], "vysledek": ["A", "M", "B"]}
    )
    out = add_vote_parties(10, mp_votes, votes, info, ORGANS, MEMBERSHIPS)
    assert out.get_column("id_hlasovani").to_list() == [3, 1, 2]
    assert out.get_column("party").to_list() == ["Nezařazení", "ANO", "Nezařazení"]
