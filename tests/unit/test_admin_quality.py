"""Tests for the admin data quality report (#100)."""

from pspcz_analyzer.admin.quality import quality_report
from tests.fixtures.sample_data import make_period_data


def test_report_counts_votes_and_flagged_briefs() -> None:
    data = make_period_data()
    tisk = next(iter(data.tisk_lookup.values()), None)
    if tisk is not None:
        tisk.brief = {"status": "warning", "issues": ["number:9"]}
    data.vote_steno = {1: {"status": "ok"}, 2: {"status": "unknown"}}
    r = quality_report(data)
    assert r["votes_steno"] == 2 and r["votes_question"] == 1
    assert r["votes_total"] == data.votes.height
    assert len(r["briefs_flagged"]) == (1 if tisk else 0)
