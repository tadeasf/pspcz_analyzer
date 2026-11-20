"""Step sentences, phase labels and law detail parts (#106, #110, #111)."""

from datetime import date, timedelta

import polars as pl

from pspcz_analyzer.models.tisk_models import TiskInfo
from pspcz_analyzer.services.law_detail import amended_laws, documents, published_law_url, timeline
from pspcz_analyzer.services.law_filters import last_step
from pspcz_analyzer.services.legislation_overview import _first_sentence
from pspcz_analyzer.services.step_text import phase_key, president_deadline, step_sentence
from pspcz_analyzer.services.tisk.io.history_scraper import TiskHistory
from pspcz_analyzer.services.tisk.io.history_stages import TiskHistoryStage
from pspcz_analyzer.services.tisk.lookup_builder import UNSCHEDULED_SCHUZE, add_unscheduled


def _tisk(stages: list[TiskHistoryStage], status: str = "projednáváno", druh: int = 1) -> TiskInfo:
    t = TiskInfo(id_tisk=1, ct=246, nazev="Novela", period=10, id_druh=druh)
    t.history = TiskHistory(ct=246, period=10, stages=stages, current_status=status)
    return t


def _st(stype: str, d: str, outcome: str | None = None, details: str = "") -> TiskHistoryStage:
    return TiskHistoryStage(stage_type=stype, label=stype, date=d, outcome=outcome, details=details)


def test_sentences_follow_document_type() -> None:
    assert (
        step_sentence("vybor", "doporučuje schválit", "", "navrhy")
        == "Výbor doporučil návrh schválit."
    )
    assert step_sentence("senat", "schválil", "", "navrhy") == "Senát zákon schválil."
    assert step_sentence("vybor", "", "", "zpravy") == "Výbor zprávu projednal."
    assert step_sentence("senat", "", "Senát dává souhlas s ratifikací", "smlouvy") == (
        "Senát dal souhlas s ratifikací."
    )
    assert step_sentence("1_cteni", "přikázán výborům", "", "smlouvy") == ""  # bill wording only
    assert step_sentence("ps", "", "rozeslán poslancům", "navrhy", submitter="vlada") == (
        "Vláda předložila návrh."
    )


def test_last_step_sentence_with_deadline_and_number() -> None:
    delivered = date.today() - timedelta(days=2)
    d = f"{delivered.day}. {delivered.month}. {delivered.year}"
    t = _tisk(
        [_st("prezident", d, "doručeno prezidentovi", f"Zákon doručen prezidentovi {d}.")],
        "u prezidenta",
    )
    deadline = delivered + timedelta(days=15)
    assert president_deadline(t) == deadline
    step = last_step(t)
    assert step and step["text"] == (
        f"Zákon dostal k podpisu prezident. Rozhodnout musí do {deadline.day}. {deadline.month}."
    )
    published = _tisk([_st("vyhlaseni", "23. 7. 2026", "vyhlášen")], "vyhlášeno")
    assert published.history is not None
    published.history.law_number = "129/2026 Sb."
    step = last_step(published)
    assert step and step["text"] == "Zákon vyšel ve Sbírce zákonů pod číslem 129/2026 Sb."


def test_phase_labels() -> None:
    assert phase_key(_tisk([_st("1_cteni", "1. 1. 2026", "přikázán výborům")])) == "phl.committees"
    assert phase_key(_tisk([_st("2_cteni", "1. 1. 2026")])) == "phl.before_3rd"
    assert phase_key(_tisk([], "vyhlášeno")) == "phl.valid"
    assert phase_key(_tisk([], "vzato zpět")) == "phl.withdrawn"


def test_timeline_has_future_step_and_documents() -> None:
    t = _tisk(
        [
            _st(
                "ps",
                "30. 6. 2026",
                None,
                "Návrh zákona rozeslán poslancům jako tisk 246/0 dne 30. 6. 2026.",
            ),
            _st("3_cteni", "23. 9. 2026", "schválen"),
            _st("senat_postoupeno", "29. 9. 2026", "postoupeno Senátu", "Senátu jako tisk 294 ."),
        ],
        "v Senátu",
    )
    secs = {s["key"]: s for s in timeline(t, [])}
    assert secs["ld.sec_senate"]["future"] == "ld.next_senate"
    assert [d["kind"] for d in documents(t)] == ["original", "senate"]


def test_amended_and_published_law_links() -> None:
    laws = amended_laws(
        "Vládní návrh zákona, kterým se mění zákon č. 13/1997 Sb., o pozemních komunikacích"
    )
    assert laws == [
        {
            "label": "č. 13/1997 Sb., o pozemních komunikacích",
            "url": "https://www.e-sbirka.cz/sb/1997/13",
        }
    ]
    assert published_law_url("129/2026 Sb.") == "https://www.e-sbirka.cz/sb/2026/129"
    assert published_law_url(None) == ""


def test_first_sentence_keeps_abbreviations() -> None:
    text = "Příjmy 2 198,6 mld. Kč a výdaje 2 484 mld. Kč. Schodek 286 mld. Kč."
    assert _first_sentence(text) == "Příjmy 2 198,6 mld. Kč a výdaje 2 484 mld. Kč."


class _NoText:
    def has_text(self, period: int, ct: int) -> bool:
        return False


def test_tisky_without_agenda_item_are_added() -> None:
    lookup = {(30, 5): TiskInfo(id_tisk=1, ct=246, nazev="A", period=10)}
    rows = pl.DataFrame(
        {
            "id_tisk": [1, 2],
            "ct": [246, 369],
            "nazev_tisku": ["A", "B"],
            "id_druh": [1, 1],
            "nazev_cely": ["", ""],
        }
    )
    add_unscheduled(lookup, 10, rows, _NoText(), ({}, {}), ({}, {}))  # type: ignore[arg-type]
    assert lookup[(UNSCHEDULED_SCHUZE, 369)].ct == 369
    assert len(lookup) == 2
