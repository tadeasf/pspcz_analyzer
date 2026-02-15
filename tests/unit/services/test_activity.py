"""Tests for activity (vote participation ranking) computation."""

from pspcz_analyzer.services.activity_service import compute_activity
from tests.fixtures.sample_data import make_period_data


class TestComputeActivity:
    def test_returns_list_of_dicts(self):
        data = make_period_data()
        result = compute_activity(data)
        assert isinstance(result, list)
        assert all(isinstance(r, dict) for r in result)

    def test_sorted_by_active_count_descending(self):
        """Results should be sorted by active vote count descending."""
        data = make_period_data()
        result = compute_activity(data, top=50)
        actives = [r["active"] for r in result]
        assert actives == sorted(actives, reverse=True)

    def test_party_filter(self):
        """Filtering by party should only return MPs from that party."""
        data = make_period_data()
        result = compute_activity(data, party_filter="ANO")
        assert all(r["party"] == "ANO" for r in result)

    def test_party_filter_case_insensitive(self):
        data = make_period_data()
        result = compute_activity(data, party_filter="ano")
        assert all(r["party"] == "ANO" for r in result)

    def test_top_limits_results(self):
        data = make_period_data()
        result = compute_activity(data, top=2)
        assert len(result) <= 2

    def test_expected_fields(self):
        """Each result should have the expected vote breakdown keys."""
        data = make_period_data()
        result = compute_activity(data, top=1)
        assert len(result) >= 1
        expected_keys = {
            "jmeno",
            "prijmeni",
            "party",
            "active",
            "yes_votes",
            "no_votes",
            "abstained",
            "passive",
            "absent",
            "excused",
            "total",
            "attendance_pct",
        }
        assert expected_keys.issubset(result[0].keys())

    def test_active_count_matches_data(self):
        """MP 1 (Jan Novák, ANO) votes YES on all 5 votes = 5 active."""
        data = make_period_data()
        result = compute_activity(data, top=50)
        jan = [r for r in result if r["prijmeni"] == "Novák"]
        assert len(jan) == 1
        assert jan[0]["active"] == 5
        assert jan[0]["yes_votes"] == 5
