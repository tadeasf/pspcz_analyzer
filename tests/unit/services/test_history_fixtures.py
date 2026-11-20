"""Parse saved psp.cz history pages (tests/fixtures/history, fetched 19. 11. 2026).

The snapshots are the original windows-1250 pages of historie.sqw?o=<period>&t=<ct>.
Statuses asserted here are the truth on the snapshot date, which can be later
than the state described in the GitHub issues (e.g. tisk 246 has since been
signed and sent for publication).
"""

import re
from pathlib import Path

from pspcz_analyzer.config import TERMINAL_TISK_STATUSES
from pspcz_analyzer.models.tisk_models import TiskInfo
from pspcz_analyzer.services.step_text import president_deadline
from pspcz_analyzer.services.tisk.io.history_scraper import TiskHistory, parse_tisk_history
from pspcz_analyzer.services.tisk.io.history_stages import _stage_status

_FIXTURES = Path(__file__).parents[2] / "fixtures" / "history"

_SIGNED_RE = re.compile(r"podepsal\s+(\d{1,2}\. \d{1,2}\. \d{4})")

_PS_CHAMBER = {"ps", "organizacni", "1_cteni", "vybor", "2_cteni", "garant", "3_cteni"}


def _parse(period: int, ct: int) -> TiskHistory:
    html = (_FIXTURES / f"p{period}_t{ct}.html").read_bytes().decode("cp1250")
    return parse_tisk_history(html, period, ct)


def _dates(h: TiskHistory, stage_type: str) -> list[str | None]:
    return [s.date for s in h.stages if s.stage_type == stage_type]


class TestSectionTyping:
    """#87: stages are typed by (section, mark), not by mark alone."""

    def test_senate_steps_of_246_are_senate_stages(self):
        h = _parse(10, 246)
        by_date = {s.date: s.stage_type for s in h.stages}
        assert by_date["29. 9. 2026"] == "senat_postoupeno"
        assert by_date["30. 9. 2026"] == "senat_organizacni"
        assert by_date["14. 10. 2026"] == "senat_vybor"
        assert by_date["21. 10. 2026"] == "senat"

    def test_no_chamber_stage_after_hand_over_unless_returned(self):
        # 82 and 246 went straight through; no Sněmovna stage follows the Senate.
        for ct in (82, 246):
            types = [s.stage_type for s in _parse(10, ct).stages]
            handed_over = types.index("senat_postoupeno")
            assert not _PS_CHAMBER & set(types[handed_over:])

    def test_vl_mark_in_sbirka_section_is_sbirka_not_government(self):
        h = _parse(10, 82)
        assert "vlada" not in {s.stage_type for s in h.stages}
        assert _dates(h, "sbirka") == ["21. 7. 2026"]

    def test_publication_from_p_status_82(self):
        h = _parse(10, 82)
        assert _dates(h, "vyhlaseni") == ["23. 7. 2026"]
        assert h.law_number == "129/2026 Sb."
        assert h.current_status == "vyhlášeno"

    def test_publication_with_sb_mark_167(self):
        h = _parse(10, 167)
        assert _dates(h, "sbirka") == ["8. 6. 2026"]
        assert h.law_number == "91/2026 Sb."
        assert h.current_status == "vyhlášeno"

    def test_no_law_number_without_publication_sentence(self):
        # 231 and 201 are still in the Sněmovna; the footer cites 106/1999 Sb.
        for ct in (231, 201):
            assert _parse(10, ct).law_number is None

    def test_page_boilerplate_gives_no_terminal_status(self):
        # #86: 231 and 201 were cached as "staženo"/"vyhlášeno" + 106/1999 Sb.
        for ct in (231, 201):
            assert _parse(10, ct).current_status == "projednáváno"


class TestSentForPublication:
    """#88: "vyhlášeno" only once the page states the law was published."""

    def test_sent_but_not_published_246(self):
        h = _parse(10, 246)
        assert _dates(h, "sbirka") == ["9. 11. 2026"]
        assert _dates(h, "vyhlaseni") == []
        assert h.current_status == "odesláno k vyhlášení"
        assert h.law_number is None

    def test_sent_for_publication_is_not_approval(self):
        # "Schválený zákon odeslán k publikaci" must not read as "schválen"
        sbirka = next(s for s in _parse(10, 82).stages if s.stage_type == "sbirka")
        assert sbirka.outcome == "odesláno k publikaci"

    def test_published_67(self):
        h = _parse(10, 67)
        assert h.current_status == "vyhlášeno"
        assert h.law_number == "215/2026 Sb."


class TestOutcomeDate:
    """#89: the stage date is the date of the sentence stating the outcome."""

    def test_president_delivery_and_signature_are_two_events_82(self):
        prezident = [s for s in _parse(10, 82).stages if s.stage_type == "prezident"]
        assert [(s.outcome, s.date) for s in prezident] == [
            ("doručeno prezidentovi", "13. 7. 2026"),
            ("podepsal", "16. 7. 2026"),
        ]
        assert "doručen prezidentovi" in prezident[0].details
        assert "podepsal 16. 7. 2026" in prezident[1].details.replace("\xa0", " ")

    def test_signed_law_has_no_president_deadline_82(self):
        tisk = TiskInfo(id_tisk=1, ct=82, nazev="", period=10, history=_parse(10, 82))
        assert president_deadline(tisk) is None

    def test_signed_dates_match_signature_sentence(self):
        # #89 cross-check over all fixtures: "podepsal" stage date = date in that sentence
        for f in _FIXTURES.glob("*.html"):
            period, ct = f.stem[1:].split("_t")
            for s in _parse(int(period), int(ct)).stages:
                if s.outcome == "podepsal":
                    m = _SIGNED_RE.search(s.details.replace("\xa0", " "))
                    assert m and m.group(1) == s.date, f.name

    def test_reading_without_dated_outcome_sentence_keeps_entry_date_82(self):
        third = next(s for s in _parse(10, 82).stages if s.stage_type == "3_cteni")
        assert third.outcome == "schválen"
        assert third.date == "3. 6. 2026"


def _section_page(*sections: tuple[str, str, str]) -> str:
    """Build a minimal history page from (heading, mark, text) sections."""
    body = "".join(
        f'<div class="section"><h2 class="section-title">{heading}</h2>'
        f'<div class="section-content"><ul class="document-log">'
        f'<li class="document-log-item"><span class="mark">{mark}</span><p>{text}</p></li>'
        f"</ul></div></div>"
        for heading, mark, text in sections
    )
    return f"<html><body>{body}</body></html>"


_PASSED = ("Poslanecká sněmovna", "3", "3. Čtení proběhlo 1. 2. 2026. Návrh zákona schválen.")
_SENATE_OK = ("Senát", "S", "Návrh projednán dne 1. 3. 2026. Senát návrh schválil.")


class TestStatusLadder:
    """#90: the status follows the last stage, typed by section."""

    def test_status_progression_167(self):
        stages = _parse(10, 167).stages
        seen = [stage_status for s in stages if (stage_status := _stage_status(s))]
        assert seen == [
            "schváleno sněmovnou",
            "v Senátu",
            "schváleno Senátem",
            "u prezidenta",
            "podepsáno",
            "odesláno k vyhlášení",
            "vyhlášeno",
        ]

    def test_senate_not_deciding_keeps_senate_then_president_246(self):
        h = _parse(10, 246)
        senat = next(s for s in h.stages if s.stage_type == "senat")
        assert senat.outcome == "neprojednal"
        assert _stage_status(senat) is None

    def test_not_passed_after_senate_rejection_85(self):
        h = _parse(10, 85)
        last = h.stages[-1]
        assert (last.stage_type, last.outcome, last.date, last.vote_number) == (
            "ps",
            "nepřijat",
            "24. 3. 2026",
            46,
        )
        assert h.current_status == "nepřijato"
        assert h.current_status in TERMINAL_TISK_STATUSES

    def test_chamber_overrides_senate_rejection_67(self):
        stages = _parse(10, 67).stages
        senat = next(s for s in stages if s.stage_type == "senat")
        assert _stage_status(senat) == "zamítnuto Senátem"
        returned = [s for s in stages if s.stage_type == "ps"][-1]
        assert (returned.outcome, returned.date) == ("setrvala na původním návrhu", "9. 9. 2026")

    def test_returned_by_senate_and_accepted_57(self):
        stages = _parse(10, 57).stages
        senat = next(s for s in stages if s.stage_type == "senat")
        assert _stage_status(senat) == "vráceno Senátem"
        assert _parse(10, 57).current_status == "vyhlášeno"

    def test_withdrawn_65(self):
        h = _parse(10, 65)
        assert h.current_status == "vzato zpět"
        assert h.current_status in TERMINAL_TISK_STATUSES

    def test_in_committee_231(self):
        assert _parse(10, 231).current_status == "projednáváno"
        assert "projednáváno" not in TERMINAL_TISK_STATUSES

    def test_with_president(self):
        page = _section_page(
            _PASSED,
            _SENATE_OK,
            ("Prezident republiky", "P", "Zákon doručen prezidentovi k podepsání 5. 3. 2026."),
        )
        h = parse_tisk_history(page, 10, 1)
        assert h.current_status == "u prezidenta"
        assert h.stages[-1].date == "5. 3. 2026"

    def test_returned_by_president(self):
        page = _section_page(
            _PASSED,
            _SENATE_OK,
            (
                "Prezident republiky",
                "P",
                "Zákon doručen prezidentovi k podepsání 5. 3. 2026. "
                "Prezident zákon vrátil 12. 3. 2026.",
            ),
        )
        h = parse_tisk_history(page, 10, 1)
        assert h.current_status == "vráceno prezidentem"
        assert h.stages[-1].date == "12. 3. 2026"

    def test_rejected_in_first_reading_is_terminal(self):
        page = _section_page(
            ("Poslanecká sněmovna", "1", "Čtení proběhlo 1. 2. 2026. Návrh zákona zamítnut."),
        )
        h = parse_tisk_history(page, 10, 1)
        assert h.current_status == "zamítnuto"
        assert h.current_status in TERMINAL_TISK_STATUSES

    def test_rejection_proposal_is_not_rejection(self):
        page = _section_page(
            ("Poslanecká sněmovna", "1", "Návrh na zamítnutí nebyl přijat 1. 2. 2026."),
        )
        assert parse_tisk_history(page, 10, 1).current_status == "projednáváno"


class TestGovernmentOpinion:
    def test_opinion_read_from_government_section_201(self):
        assert _parse(10, 201).government_opinion == "souhlas"

    def test_government_bill_has_no_opinion_82(self):
        # 82 is a government bill; "souhlas" elsewhere on the page is unrelated
        assert _parse(10, 82).government_opinion is None


# Document type names from druh_tisku.unl
_DRUH_NAMES = {
    "Vládní návrh zákona",
    "Návrh zákona",
    "Státní rozpočet",
    "Mezinárodní smlouva",
    "Zpráva a jiné",
    "Písemná interpelace",
}


class TestSubmitter:
    """#98: the submitter is never a document type."""

    def test_budget_submitted_by_government_324(self):
        # "Návrh zákona o státním rozpočtu vládou předložen 30. 9. 2026."
        h = _parse(10, 324)
        assert (h.submitter, h.submitter_date) == ("Vláda", "30. 9. 2026")

    def test_treaty_has_no_submitter_314(self):
        # "Mezinárodní smlouva předložena sněmovně dne 23. 9. 2026."
        h = _parse(10, 314)
        assert (h.submitter, h.submitter_date) == ("", "23. 9. 2026")

    def test_report_without_submitter_section_142(self):
        # No Předkladatel section; the Senate hand-over must not become "Senát PS"
        assert _parse(10, 142).submitter == ""

    def test_named_submitters(self):
        assert _parse(10, 82).submitter == "Vláda"
        assert _parse(10, 85).submitter == "Ing. Aleš Juchelka"
        assert _parse(10, 201).submitter.startswith("Skupina poslanců")

    def test_no_submitter_is_a_document_type(self):
        for f in _FIXTURES.glob("*.html"):
            period, ct = f.stem[1:].split("_t")
            assert _parse(int(period), int(ct)).submitter not in _DRUH_NAMES, f.name
