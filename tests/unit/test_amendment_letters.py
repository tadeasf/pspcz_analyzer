"""Tests for the amendment letter unit (#93, #103)."""

from pspcz_analyzer.models.amendment_models import AmendmentVote, BillAmendmentData
from pspcz_analyzer.services.amendment_letters import bill_letters, tally, unassigned_votes

GOV = frozenset({"ANO", "SPD"})


def _bill(*amends: AmendmentVote, final: bool = True) -> BillAmendmentData:
    final_vote = AmendmentVote(letter="", vote_number=99, is_final_vote=True) if final else None
    return BillAmendmentData(
        period=10, schuze=31, bod=12, ct=246, amendments=list(amends), final_vote=final_vote
    )


def test_grouped_vote_gives_two_letters_one_vote() -> None:
    bill = _bill(
        AmendmentVote(
            letter="E1",
            vote_number=5,
            grouped_with=["F2"],
            result="accepted",
            submitter_parties=["STAN"],
        )
    )
    letters = bill_letters(bill, GOV)
    assert [r["letter"] for r in letters] == ["E1", "F2"]
    assert letters[1]["voted_with"] == ["E1"]
    assert {r["fate"] for r in letters} == {"accepted"}


def test_revote_does_not_add_a_letter_and_wins() -> None:
    bill = _bill(
        AmendmentVote(letter="A", vote_number=3, result="rejected"),
        AmendmentVote(
            letter="", vote_number=4, result="accepted", is_revote=True, original_vote_number=3
        ),
    )
    letters = bill_letters(bill, GOV)
    assert len(letters) == 1
    assert letters[0]["fate"] == "accepted"
    assert unassigned_votes(bill) == 0


def test_fates_sum_to_total_and_sides() -> None:
    bill = _bill(
        AmendmentVote(letter="A", vote_number=1, result="accepted", submitter_parties=["ANO"]),
        AmendmentVote(letter="B", vote_number=2, result="rejected", submitter_parties=["ODS"]),
        AmendmentVote(letter="C", vote_number=0, pdf_submitter_names=["Hospodářský výbor"]),
        AmendmentVote(
            letter="D", vote_number=0, is_withdrawn=True, submitter_parties=["ANO", "ODS"]
        ),
        AmendmentVote(letter="L", vote_number=8, is_leg_tech=True, result="accepted"),
    )
    letters = bill_letters(bill, GOV)
    t = tally(letters)
    assert t["total"] == 4 == sum(t["by_fate"].values())
    assert t["by_fate"]["not_voted"] == 1
    assert t["by_fate"]["withdrawn"] == 1
    assert t["by_side"]["government"] == {"submitted": 1, "accepted": 1}
    assert t["by_side"]["opposition"]["submitted"] == 1
    assert t["by_side"]["committee"]["submitted"] == 1
    assert t["by_side"]["mixed"]["submitted"] == 1
    assert unassigned_votes(bill) == 0


def test_unvoted_letter_without_final_vote_is_unknown() -> None:
    bill = _bill(AmendmentVote(letter="A", vote_number=0), final=False)
    assert bill_letters(bill, GOV)[0]["fate"] == "unknown"


def test_unvoted_letter_is_unknown_while_some_vote_is_unassigned() -> None:
    """A letter-less vote may be the vote on that letter: do not claim "not voted"."""
    bill = _bill(
        AmendmentVote(letter="A", vote_number=0, pdf_submitter_names=["Hospodářský výbor"]),
        AmendmentVote(letter="", vote_number=8, result="accepted"),
    )
    assert bill_letters(bill, GOV)[0]["fate"] == "unknown"
