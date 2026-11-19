"""Tests for the laws list tabs, phases and submitter kinds (#110)."""

from pspcz_analyzer.models.tisk_models import TiskInfo
from pspcz_analyzer.services.law_filters import last_step, phase_of, submitter_kind, tab_of
from pspcz_analyzer.services.tisk.io.history_scraper import TiskHistory, TiskHistoryStage


def _tisk(
    id_druh: int | None = None, submitter: str = "", stages: list[TiskHistoryStage] | None = None
) -> TiskInfo:
    history = (
        TiskHistory(ct=1, period=10, submitter=submitter, stages=stages or [])
        if submitter or stages
        else None
    )
    return TiskInfo(id_tisk=1, ct=1, nazev="x", period=10, id_druh=id_druh, history=history)


def test_tabs_by_document_type() -> None:
    assert [tab_of(_tisk(d)) for d in (1, 2, 3, 4, 5, 6, None)] == [
        "navrhy",
        "navrhy",
        "navrhy",
        "smlouvy",
        "zpravy",
        "interpelace",
        "zpravy",
    ]


def test_phases() -> None:
    assert phase_of("u prezidenta") == "president"
    assert phase_of("vyhlášeno") == "valid"
    assert phase_of("vzato zpět") == "failed"
    assert phase_of("vráceno Senátem") == "chamber"


def test_submitter_kind() -> None:
    assert submitter_kind(_tisk(1)) == "vlada"
    assert submitter_kind(_tisk(2, "Poslanci Jan Novák a další")) == "poslanci"
    assert submitter_kind(_tisk(2, "Senát")) == "senat"
    assert submitter_kind(_tisk(2, "Zastupitelstvo Jihomoravského kraje")) == "kraje"
    assert submitter_kind(_tisk(2)) is None


def test_last_step_picks_latest_dated_stage() -> None:
    stages = [
        TiskHistoryStage(
            stage_type="3_cteni", label="3. čtení", date="3. 9. 2026", outcome="Schválen"
        ),
        TiskHistoryStage(stage_type="senat", label="Senát"),
        TiskHistoryStage(
            stage_type="prezident", label="Zákon doručen prezidentovi", date="30. 10. 2026"
        ),
    ]
    step = last_step(_tisk(1, "Vláda", stages))
    assert step == {
        "sort": (2026, 10, 30),
        "date": "30. 10. 2026",
        "text": "Zákon doručen prezidentovi",
    }
