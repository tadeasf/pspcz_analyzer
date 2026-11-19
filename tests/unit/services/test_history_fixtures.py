"""Parse saved psp.cz history pages (tests/fixtures/history, fetched 19. 11. 2026).

The snapshots are the original windows-1250 pages of historie.sqw?o=<period>&t=<ct>.
Statuses asserted here are the truth on the snapshot date, which can be later
than the state described in the GitHub issues (e.g. tisk 246 has since been
signed and sent for publication).
"""

from pathlib import Path

from pspcz_analyzer.services.tisk.io.history_scraper import TiskHistory, parse_tisk_history

_FIXTURES = Path(__file__).parents[2] / "fixtures" / "history"

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
