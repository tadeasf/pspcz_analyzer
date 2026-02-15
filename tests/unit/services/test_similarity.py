"""Tests for similarity (PCA + cosine) computation."""

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


class TestComputeCrossPartySimilarity:
    def test_returns_list_of_dicts(self):
        data = make_period_data()
        result = compute_cross_party_similarity(data)
        assert isinstance(result, list)

    def test_cross_party_only(self):
        """All pairs should be from different parties."""
        data = make_period_data()
        result = compute_cross_party_similarity(data)
        for pair in result:
            assert pair["mp1_party"] != pair["mp2_party"]

    def test_similarity_range(self):
        """Cosine similarity should be between -1 and 1."""
        data = make_period_data()
        result = compute_cross_party_similarity(data)
        for pair in result:
            assert -1.0 <= pair["similarity"] <= 1.0

    def test_sorted_by_similarity_descending(self):
        """Results should be sorted by similarity descending."""
        data = make_period_data()
        result = compute_cross_party_similarity(data)
        sims = [p["similarity"] for p in result]
        assert sims == sorted(sims, reverse=True)

    def test_top_limits_results(self):
        data = make_period_data()
        result = compute_cross_party_similarity(data, top=2)
        assert len(result) <= 2
