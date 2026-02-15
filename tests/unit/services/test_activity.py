"""Tests for attendance service — vote breakdown and party filter (merged from activity)."""

from pspcz_analyzer.services.attendance_service import compute_attendance
from tests.fixtures.sample_data import make_period_data


class TestAttendanceVoteBreakdown:
    def test_includes_vote_breakdown_fields(self):
        """Each result should have YES/NO/ABSTAINED breakdown keys."""
        data = make_period_data()
        result = compute_attendance(data, top=1)
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
            "attendance_pct",
        }
        assert expected_keys.issubset(result[0].keys())

    def test_party_filter(self):
        """Filtering by party should only return MPs from that party."""
        data = make_period_data()
        result = compute_attendance(data, party_filter="ANO")
        assert all(r["party"] == "ANO" for r in result)

    def test_party_filter_case_insensitive(self):
        data = make_period_data()
        result = compute_attendance(data, party_filter="ano")
        assert all(r["party"] == "ANO" for r in result)

    def test_sort_most_active(self):
        """sort=most_active should sort by active vote count descending."""
        data = make_period_data()
        result = compute_attendance(data, top=50, sort="most_active")
        actives = [r["active"] for r in result]
        assert actives == sorted(actives, reverse=True)

    def test_active_count_matches_data(self):
        """MP 1 (Jan Novák, ANO) votes YES on all 5 votes = 5 active."""
        data = make_period_data()
        result = compute_attendance(data, top=50, sort="most_active")
        jan = [r for r in result if r["prijmeni"] == "Novák"]
        assert len(jan) == 1
        assert jan[0]["active"] == 5
        assert jan[0]["yes_votes"] == 5
