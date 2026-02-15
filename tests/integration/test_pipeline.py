"""Integration tests: end-to-end pipeline from download to analysis."""

import pytest

from pspcz_analyzer.services.data_service import DataService

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def data_service(integration_cache_dir):
    """Initialize DataService with real data for period 1."""
    svc = DataService(cache_dir=integration_cache_dir)
    svc.initialize(period=1)
    return svc


@pytest.fixture(scope="module")
def period_data(data_service):
    """Get period data for period 1."""
    return data_service.get_period(1)


class TestFullPipeline:
    def test_period_loads(self, period_data):
        """Period 1 should load successfully with real data."""
        assert period_data is not None
        assert period_data.period == 1

    def test_votes_non_empty(self, period_data):
        assert period_data.votes.height > 0

    def test_mp_votes_non_empty(self, period_data):
        assert period_data.mp_votes.height > 0

    def test_mp_info_non_empty(self, period_data):
        assert period_data.mp_info.height > 0

    def test_mp_info_has_parties(self, period_data):
        """MP info should have non-null party values for most MPs."""
        null_pct = period_data.mp_info["party"].null_count() / period_data.mp_info.height
        assert null_pct < 0.5  # at most 50% null parties

    def test_stats_output(self, period_data):
        """Stats should produce a sensible dict."""
        stats = period_data.stats
        assert stats["period"] == 1
        assert stats["total_votes"] > 0
        assert stats["total_mps"] > 0


class TestAnalysisOnRealData:
    def test_loyalty_produces_results(self, period_data):
        from pspcz_analyzer.services.loyalty_service import compute_loyalty

        result = compute_loyalty(period_data, top=10)
        assert len(result) > 0
        for r in result:
            assert 0 <= r["rebellion_pct"] <= 100

    def test_attendance_produces_results(self, period_data):
        from pspcz_analyzer.services.attendance_service import compute_attendance

        result = compute_attendance(period_data, top=10)
        assert len(result) > 0

    def test_similarity_produces_results(self, period_data):
        from pspcz_analyzer.services.similarity_service import compute_pca_coords

        result = compute_pca_coords(period_data)
        assert len(result) > 0

    def test_attendance_most_active_sort(self, period_data):
        from pspcz_analyzer.services.attendance_service import compute_attendance

        result = compute_attendance(period_data, top=10, sort="most_active")
        assert len(result) > 0

    def test_votes_list_produces_results(self, period_data):
        from pspcz_analyzer.services.votes_service import list_votes

        result = list_votes(period_data)
        assert result["total"] > 0
        assert len(result["rows"]) > 0
