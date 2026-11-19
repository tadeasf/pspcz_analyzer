"""Tests for similarity (PCA map + agreement pairs)."""

import polars as pl

from pspcz_analyzer.models.tisk_models import PeriodData
from pspcz_analyzer.services.similarity_service import (
    compute_cross_party_similarity,
    compute_pca_coords,
)
from tests.fixtures.sample_data import make_period_data


class TestComputePcaCoords:
    def test_returns_list_of_dicts(self):
        data = make_period_data()
        result = compute_pca_coords(data)
        assert isinstance(result, list)
        assert all(isinstance(r, dict) for r in result)

    def test_2d_coordinates(self):
        """Each result should have x and y coordinates."""
        data = make_period_data()
        result = compute_pca_coords(data)
        for r in result:
            assert "x" in r and "y" in r
            assert isinstance(r["x"], float)
            assert isinstance(r["y"], float)

    def test_mp_name_and_party(self):
        """Each result should have mp_name and party."""
        data = make_period_data()
        result = compute_pca_coords(data)
        for r in result:
            assert "mp_name" in r
            assert "party" in r
            assert isinstance(r["mp_name"], str)

    def test_one_result_per_mp(self):
        """Should return one coordinate per MP in the data."""
        data = make_period_data()
        result = compute_pca_coords(data)
        # We have 6 MPs in our fixture
        assert len(result) == 6


def _two_mp_data(n_votes: int, second: list[str]) -> PeriodData:
    """Two MPs from different clubs; MP 1 votes YES on every vote."""
    data = make_period_data()
    ids = list(range(1000, 1000 + n_votes))
    data.votes = pl.DataFrame(
        {"id_hlasovani": ids, "datum": ["01.05.2023"] * n_votes},
        schema_overrides={"id_hlasovani": pl.Int64},
    )
    data.void_votes = pl.DataFrame({"id_hlasovani": []}, schema={"id_hlasovani": pl.Int64})
    data.mp_votes = pl.DataFrame(
        {
            "id_poslanec": [1] * n_votes + [2] * n_votes,
            "id_hlasovani": ids + ids,
            "vysledek": ["A"] * n_votes + second,
            "party": ["GOV"] * n_votes + ["OPP"] * n_votes,
        },
        schema_overrides={"id_poslanec": pl.Int64, "id_hlasovani": pl.Int64},
    )
    data.mp_info = pl.DataFrame(
        {
            "id_poslanec": [1, 2],
            "id_osoba": [1, 2],
            "jmeno": ["A", "B"],
            "prijmeni": ["X", "Y"],
            "party": ["GOV", "OPP"],
        },
        schema_overrides={"id_poslanec": pl.Int64, "id_osoba": pl.Int64},
    )
    data.coalition_parties = frozenset({"GOV"})
    return data


class TestAgreement:
    """Real agreement instead of cosine similarity (#104)."""

    def test_identical_votes_are_100_percent(self):
        out = compute_cross_party_similarity(_two_mp_data(100, ["A"] * 100))
        pair = out["cross_party"][0]
        assert (pair["same"], pair["shared"], pair["agreement"]) == (100, 100, 1.0)
        assert out["gov_opp"][0]["agreement"] == 1.0

    def test_absent_and_excused_do_not_count(self):
        second = ["A"] * 100 + ["@"] * 20 + ["M"] * 20 + ["B"] * 10
        out = compute_cross_party_similarity(_two_mp_data(150, second))
        pair = out["cross_party"][0]
        assert (pair["same"], pair["shared"]) == (100, 110)

    def test_minimum_shared_votes(self):
        out = compute_cross_party_similarity(_two_mp_data(99, ["A"] * 99))
        assert out == {"cross_party": [], "gov_opp": []}

    def test_unknown_government_gives_no_gov_opp_pairs(self):
        data = _two_mp_data(100, ["A"] * 100)
        data.coalition_parties = frozenset()
        assert compute_cross_party_similarity(data)["gov_opp"] == []
