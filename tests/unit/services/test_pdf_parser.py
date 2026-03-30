"""Tests for amendment PDF parser, focusing on multi-submitter parsing."""

from pspcz_analyzer.services.amendments.pdf_parser import (
    _parse_submitter_names,
    parse_amendment_pdf,
)


class TestParseSubmitterNames:
    def test_single_name(self):
        result = _parse_submitter_names("Novák")
        assert result == ["Novák"]

    def test_multi_submitter_poslanec(self):
        """Multiple submitters separated by ', poslanec'."""
        result = _parse_submitter_names("Mračková Vildumetzová, poslanec Novák, poslanec Hora")
        assert result == ["Mračková Vildumetzová", "Novák", "Hora"]

    def test_multi_submitter_mixed_gender(self):
        """Mix of poslanec and poslankyně."""
        result = _parse_submitter_names(
            "Mračková Vildumetzová, poslanec Novák, poslankyně Nová, poslanec Hora"
        )
        assert len(result) == 4
        assert result[0] == "Mračková Vildumetzová"
        assert result[1] == "Novák"
        assert result[2] == "Nová"
        assert result[3] == "Hora"

    def test_conjunction_a_poslanec(self):
        """Conjunction 'a poslanec' between names."""
        result = _parse_submitter_names("Novák a poslanec Hora")
        assert result == ["Novák", "Hora"]

    def test_trailing_punctuation_stripped(self):
        result = _parse_submitter_names("Novák,.")
        assert result == ["Novák"]

    def test_empty_input(self):
        assert _parse_submitter_names("") == []
        assert _parse_submitter_names("  ") == []

    def test_academic_title_stripped(self):
        result = _parse_submitter_names("Mgr. Novák")
        assert result == ["Novák"]

    def test_multi_with_titles(self):
        result = _parse_submitter_names("Ing. Novák, poslanec PhDr. Hora")
        assert result == ["Novák", "Hora"]


class TestParseAmendmentPdfMultiSubmitter:
    def test_multi_submitter_header_parsed(self):
        """Full PDF text with multi-submitter header returns all names."""
        text = (
            "A. Poslankyně Mračková Vildumetzová, poslanec Novák, "
            "poslanec Hora\n"
            "A.1. SD 3327\n"
            "Some amendment text.\n"
        )
        amendments = parse_amendment_pdf(text)
        assert len(amendments) == 1
        amend = amendments[0]
        assert amend.submitter_names == ["Mračková Vildumetzová", "Novák", "Hora"]
        assert amend.submitter_titles == ["Poslankyně"]

    def test_single_submitter_still_works(self):
        """Single submitter header still works correctly."""
        text = "A. Poslanec Jan Novák\nA.1. SD 3327\nSome text.\n"
        amendments = parse_amendment_pdf(text)
        assert len(amendments) == 1
        assert amendments[0].submitter_names == ["Jan Novák"]
