"""scripts/check_statuses.py: the cache-vs-psp.cz comparison (#86, #90), no network."""

from pspcz_analyzer.services.tisk.io.history_scraper import TiskHistory
from scripts.check_statuses import compare_history


def test_equal_histories_have_no_differences():
    cached = TiskHistory(ct=82, period=10, current_status="vyhlášeno", law_number="129/2026 Sb.")
    fresh = TiskHistory(ct=82, period=10, current_status="vyhlášeno", law_number="129/2026 Sb.")
    assert compare_history(cached, fresh) == []


def test_stale_status_and_boilerplate_law_number_are_reported():
    # #86: tisk 231 was cached as "staženo" + 106/1999 Sb.
    cached = TiskHistory(ct=231, period=10, current_status="staženo", law_number="106/1999 Sb.")
    fresh = TiskHistory(ct=231, period=10, current_status="projednáváno", law_number=None)
    assert compare_history(cached, fresh) == [
        ("current_status", "staženo", "projednáváno"),
        ("law_number", "106/1999 Sb.", None),
    ]


def test_other_fields_are_ignored():
    cached = TiskHistory(ct=1, period=10, submitter="Vláda", scraped_at="2026-01-01")
    fresh = TiskHistory(ct=1, period=10, submitter="", scraped_at="2026-11-20")
    assert compare_history(cached, fresh) == []
