"""Tests for the homepage phase stepper (#108)."""

from pspcz_analyzer.models.tisk_models import TiskInfo
from pspcz_analyzer.services.legislation_overview import phase_steps
from pspcz_analyzer.services.tisk.io.history_scraper import TiskHistory
from pspcz_analyzer.services.tisk.io.history_stages import TiskHistoryStage


def test_committee_stage_is_now_after_first_reading() -> None:
    h = TiskHistory(ct=324, period=10, current_status="projednáváno")
    h.submitter_date = "30. 9. 2026"
    h.stages = [TiskHistoryStage(stage_type="1_cteni", label="1. čtení", date="22. 10. 2026")]
    steps = phase_steps(TiskInfo(id_tisk=1, ct=324, nazev="Rozpočet", period=10, history=h))
    assert [s["state"] for s in steps] == ["done", "done", "now"] + ["next"] * 4
    assert steps[0]["date"] == "30. 9. 2026" and steps[1]["date"] == "22. 10. 2026"
