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

    def test_letterless_entries_all_kept(self):
        # Entries without a letter ("") must not collapse into one — each is
        # a distinct recorded vote.
        bill = _bill(
            [
                AmendmentVote(letter="", vote_number=46, result="rejected"),
                AmendmentVote(letter="", vote_number=47, result="rejected"),
                AmendmentVote(letter="A", vote_number=54, result="accepted"),
            ]
        )
        pdf = {82: [PdfAmendment(letter="A", raw_text="text A")]}
        _merge_pdf_and_steno([bill], pdf)
        assert sorted(a.vote_number for a in bill.amendments) == [46, 47, 54]

    def test_duplicate_letters_first_matched_second_kept(self):
        # Same letter twice (e.g. a revote): the PDF match consumes the first,
        # the second survives as a steno-only leftover.
        bill = _bill(
            [
                AmendmentVote(letter="A", vote_number=51, result="accepted"),
                AmendmentVote(letter="A", vote_number=54, result="accepted"),
            ]
        )
        pdf = {82: [PdfAmendment(letter="A", raw_text="text A")]}
        _merge_pdf_and_steno([bill], pdf)
        by_vote = {a.vote_number: a for a in bill.amendments}
        assert set(by_vote) == {51, 54}
        assert by_vote[51].amendment_text == "text A"
        assert by_vote[54].amendment_text == ""

    def test_grouped_with_entries_merged_not_dropped(self):
        # grouped_with entries were previously popped and silently discarded;
        # they must be merged with the en-bloc PDF text instead.
        bill = _bill(
            [
                AmendmentVote(letter="E1", vote_number=60, result="rejected", grouped_with=["F"]),
                AmendmentVote(letter="F", vote_number=61, result="rejected"),
            ]
        )
        pdf = {82: [PdfAmendment(letter="E", raw_text="text E")]}
        _merge_pdf_and_steno([bill], pdf)
        assert sorted(a.vote_number for a in bill.amendments) == [60, 61]
        assert all(a.amendment_text == "text E" for a in bill.amendments)
