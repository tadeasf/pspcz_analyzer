"""Tests for PDF↔steno amendment merge."""

from pspcz_analyzer.models.amendment_models import AmendmentVote, BillAmendmentData
from pspcz_analyzer.services.amendments.merger import _merge_pdf_and_steno
from pspcz_analyzer.services.amendments.pdf_parser import PdfAmendment


def _bill(amendments: list[AmendmentVote]) -> BillAmendmentData:
    return BillAmendmentData(period=10, schuze=17, bod=2, ct=82, amendments=amendments)


class TestMergePdfAndSteno:
    def test_pdf_only_surfaced_when_bill_has_steno_votes(self):
        bill = _bill([AmendmentVote(letter="A", vote_number=51, result="accepted")])
        pdf = {
            82: [
                PdfAmendment(letter="A", raw_text="text A"),
                PdfAmendment(letter="B", raw_text="text B", submitter_names=["Jan Novák"]),
            ]
        }
        _merge_pdf_and_steno([bill], pdf)

        by_letter = {a.letter: a for a in bill.amendments}
        assert by_letter["A"].vote_number == 51  # matched to steno vote
        # B had no steno vote → surfaced as an unvoted entry, not dropped.
        assert "B" in by_letter
        assert by_letter["B"].vote_number == 0
        assert by_letter["B"].amendment_text == "text B"
        assert by_letter["B"].submitter_names == ["Jan Novák"]

    def test_pdf_only_suppressed_when_no_steno_votes(self):
        # Steno produced no vote-linked amendments → don't dump the whole PDF.
        bill = _bill([])
        pdf = {82: [PdfAmendment(letter="B", raw_text="text B")]}
        _merge_pdf_and_steno([bill], pdf)
        assert bill.amendments == []
        # ...but the count is remembered so the UI can surface the gap.
        assert bill.unlinked_amendment_count == 1

    def test_unlinked_count_zero_when_steno_votes_present(self):
        bill = _bill([AmendmentVote(letter="A", vote_number=51, result="accepted")])
        pdf = {82: [PdfAmendment(letter="A", raw_text="text A")]}
        _merge_pdf_and_steno([bill], pdf)
        assert bill.unlinked_amendment_count == 0
