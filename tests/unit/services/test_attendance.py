"""Tests for attendance computation."""

from pspcz_analyzer.services.attendance_service import compute_attendance
from tests.fixtures.sample_data import make_period_data


class TestComputeAttendance:
    def test_returns_list_of_dicts(self):
        data = make_period_data()
        result = compute_attendance(data)
        assert isinstance(result, list)
        assert all(isinstance(r, dict) for r in result)

    def test_attendance_pct_formula(self):
        """Verify: attendance = active / (total - excused) * 100.

        MP 5 (Marie Nová): 1 YES + 1 ABSTAINED = 2 active, 1 ABSENT, 1 EXCUSED, 1 PASSIVE
        total=5, excused=1, attendance = 2 / (5-1) * 100 = 50%
        """
        data = make_period_data()
        result = compute_attendance(data, top=50)
        marie = [r for r in result if r["prijmeni"] == "Nová"]
        assert len(marie) == 1
        assert marie[0]["attendance_pct"] == 50.0
        assert marie[0]["active"] == 2
        assert marie[0]["excused"] == 1

    def test_sort_worst(self):
        """sort='worst' should put lowest attendance first."""
        data = make_period_data()
        result = compute_attendance(data, sort="worst", top=50)
        pcts = [r["attendance_pct"] for r in result]
        assert pcts == sorted(pcts)

    def test_sort_best(self):
        """sort='best' should put highest attendance first."""
        data = make_period_data()
        result = compute_attendance(data, sort="best", top=50)
        pcts = [r["attendance_pct"] for r in result]
        assert pcts == sorted(pcts, reverse=True)

    def test_top_limits_results(self):
        data = make_period_data()
        result = compute_attendance(data, top=2)
        assert len(result) <= 2

    def test_expected_fields(self):
        """Each result should have the expected keys."""
        data = make_period_data()
        result = compute_attendance(data, top=1)
        assert len(result) >= 1
        expected_keys = {
            "jmeno",
            "prijmeni",
            "party",
            "active",
            "passive",
            "absent",
            "excused",
            "attendance_pct",
        }
        assert expected_keys.issubset(result[0].keys())
