"""Regression tests for legislative status / law-number parsing.

Guards the psp.cz history parser against reading page boilerplate as legislative
content (the bug that marked every current-period bill as staženo/vyhlášeno).
"""

from bs4 import BeautifulSoup

from pspcz_analyzer.services.tisk.io.history_scraper import (
    TiskHistoryStage,
    _determine_status,
    _law_number_from_stages,
    _parse_stages,
)


def _stage(stage_type: str, outcome: str | None = None, details: str = "") -> TiskHistoryStage:
    return TiskHistoryStage(
        stage_type=stage_type, label=stage_type, outcome=outcome, details=details
    )


class TestDetermineStatus:
    def test_government_opinion_only_is_in_progress(self):
        # "VL" = Vláda (government opinion), NOT publication → still in progress
        stages = [_stage("ps"), _stage("vlada"), _stage("organizacni")]
        assert _determine_status(stages) == "projednáváno"

    def test_sbirka_stage_is_published(self):
        assert _determine_status([_stage("ps"), _stage("sbirka")]) == "vyhlášeno"

    def test_third_reading_passed(self):
        stages = [_stage("1_cteni"), _stage("3_cteni", outcome="schválen")]
        assert _determine_status(stages) == "schváleno sněmovnou"

    def test_president_signed(self):
        assert _determine_status([_stage("prezident", outcome="podepsal schválen")]) == "podepsáno"

    def test_rejected(self):
        assert _determine_status([_stage("3_cteni", outcome="zamítnut")]) == "zamítnuto"

    def test_withdrawn(self):
        assert _determine_status([_stage("1_cteni", outcome="vzat zpět")]) == "staženo"

    def test_empty_is_in_progress(self):
        assert _determine_status([]) == "projednáváno"


class TestLawNumberFromStages:
    def test_reads_from_sbirka_stage_only(self):
        stages = [
            _stage("ps", details="ke stažení 106/1999 Sb."),  # boilerplate-like, ignored
            _stage("sbirka", details="vyhlášen pod číslem 246/2022 Sb."),
        ]
        assert _law_number_from_stages(stages) == "246/2022 Sb."

    def test_none_without_sbirka(self):
        stages = [_stage("ps", details="106/1999 Sb. footer"), _stage("vlada")]
        assert _law_number_from_stages(stages) is None


class TestParseStagesMarkMap:
    def test_vl_mark_is_government_not_published(self):
        # A page section with the VL mark must parse as the 'vlada' stage,
        # so _determine_status does NOT report it as published.
        html = """
        <div class="section"><h2>Vláda</h2>
          <div class="section-content"><ul class="document-log">
            <li class="document-log-item"><span class="mark">VL</span>
            <p>Vláda zaslala stanovisko 1. 1. 2026</p></li>
          </ul></div>
        </div>
        """
        stages = _parse_stages(BeautifulSoup(html, "html.parser"))
        assert [s.stage_type for s in stages] == ["vlada"]
        assert _determine_status(stages) == "projednáváno"
