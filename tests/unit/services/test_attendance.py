"""Tests for attendance computation."""

import polars as pl

from pspcz_analyzer.models.enums import VoteResult
from pspcz_analyzer.models.tisk_models import PeriodData
from pspcz_analyzer.services.attendance_service import compute_attendance
from tests.fixtures.sample_data import make_period_data


def _with_fully_excused_mp() -> PeriodData:
    """PeriodData extended with MP 99, excused from every single vote."""
    data = make_period_data()
    excused_rows = pl.DataFrame(
        {
            "id_poslanec": [99] * 5,
            "id_hlasovani": list(range(1, 6)),
            "vysledek": [VoteResult.EXCUSED] * 5,
        },
        schema={"id_poslanec": pl.Int64, "id_hlasovani": pl.Int64, "vysledek": pl.Utf8},
    )
    data.mp_votes = pl.concat([data.mp_votes, excused_rows])
    extra_info = pl.DataFrame(
        {
            "id_poslanec": [99],
            "id_osoba": [199],
            "jmeno": ["Věčně"],
            "prijmeni": ["Omluvený"],
            "party": ["TOP09"],
        },
        schema={
            "id_poslanec": pl.Int64,
            "id_osoba": pl.Int64,
            "jmeno": pl.Utf8,
            "prijmeni": pl.Utf8,
            "party": pl.Utf8,
        },
    )
    data.mp_info = pl.concat([data.mp_info, extra_info])
    return data


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

    def test_fully_excused_mp_has_null_rate(self):
        """All-excused MPs get a null rate, not inf (audit fix B6).

        Regression: active / (total - excused) = 0/0 = inf used to rank a
        permanently-excused MP as having the BEST attendance.
        """
        data = _with_fully_excused_mp()
        result = compute_attendance(data, top=50, sort="worst")
        excused = [r for r in result if r["prijmeni"] == "Omluvený"]
        assert len(excused) == 1
        assert excused[0]["attendance_pct"] is None
        assert excused[0]["excused"] == 5

    def test_fully_excused_mp_sorts_last_worst(self):
        """nulls_last: an undefined rate never tops the 'worst attendance' list."""
        data = _with_fully_excused_mp()
        result = compute_attendance(data, top=50, sort="worst")
        assert result[-1]["prijmeni"] == "Omluvený"

    def test_fully_excused_mp_sorts_last_best(self):
        """nulls_last: an undefined rate never tops the 'best attendance' list."""
        data = _with_fully_excused_mp()
        result = compute_attendance(data, top=50, sort="best")
        assert result[-1]["prijmeni"] == "Omluvený"

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
            "yes_votes",
            "no_votes",
            "abstained",
            "passive",
            "absent",
            "excused",
            "attendance_pct",
        }
        assert expected_keys.issubset(result[0].keys())
