"""Procedure votes, letter-less committee amendments and unvotable letters (#103, #102)."""

from pspcz_analyzer.models.amendment_models import AmendmentVote, BillAmendmentData
from pspcz_analyzer.services.amendment_letters import bill_letters, unassigned_votes
from pspcz_analyzer.services.amendment_listing import law_name, split_core
from pspcz_analyzer.services.amendments.merger import _assign_committee_letters
from pspcz_analyzer.services.amendments.pdf_parser import PdfAmendment
from pspcz_analyzer.services.amendments.steno_parser import parse_steno_amendments
from pspcz_analyzer.services.amendments.summary_carry import carry_summaries

# Shortened 3rd reading of tisk 246 (31st sitting, 23. 9. 2026)
STENO_246 = (
    "<p>Přikročíme k hlasování o pozměňovacích návrzích.</p>"
    "<p>Poslankyně Michaela Šebelová: Přicházím s protinávrhem na hlasovací proceduru, protože "
    "pokud by bylo jako první hlasováno A, stal by se náš pozměňovací návrh nehlasovatelným. "
    "Proto navrhuji proceduru B1, B2 a až následně A. Předseda: Budeme hlasovat o protinávrhu "
    "paní předsedkyně Šebelové. Zahajuji hlasování.</p>"
    "<p>Hlasování číslo 6, přihlášeno 152 poslanců, pro 72, proti 79. Návrh byl zamítnut. "
    "Budeme tedy hlasovat o návrhu procedury, kterou přednesl zpravodaj. Zahajuji hlasování.</p>"
    "<p>Hlasování číslo 7, přihlášeno 154 poslanců, pro 106, proti 34. Návrh byl přijat. "
    "Poslanec Marek Novák: Nyní budeme hlasovat pozměňovací návrh hospodářského výboru, který se "
    "týká změny jednodenního kuponu. Stanovisko výboru je doporučující. Zahajuji hlasování.</p>"
    "<p>Hlasování číslo 8, přihlášeno 154 poslanců, pro 128, proti nikdo. Návrh byl přijat. "
    "Nyní návrh B1 poslance Papajanovského. Zahajuji hlasování.</p>"
    "<p>Hlasování číslo 9, přihlášeno 155 poslanců, pro 33, proti 100. Návrh byl zamítnut. "
    "Jelikož jsme schválili pozměňovací návrh A, stal se B2 nehlasovatelný, takže bychom měli "
    "hlasovat o návrhu zákona jako celku. Zahajuji hlasování.</p>"
    "<p>Hlasování číslo 10, přihlášeno 157 poslanců, pro 84, proti 64. Návrh byl přijat.</p>"
)


def _parsed() -> list[AmendmentVote]:
    amendments, _, _ = parse_steno_amendments(STENO_246, 10, 31, 12)
    return amendments


def test_procedure_votes_carry_no_letter() -> None:
    by_vote = {a.vote_number: a for a in _parsed()}
    assert by_vote[6].is_procedure and by_vote[6].letter == ""
    assert by_vote[7].is_procedure
    assert (
        by_vote[10].is_final_vote and by_vote[10].letter == ""
    )  # "schválili návrh A" is not its subject


def test_committee_amendment_and_unvotable_letter() -> None:
    parsed = _parsed()
    by_vote = {a.vote_number: a for a in parsed}
    assert by_vote[8].letter == "" and by_vote[8].submitter_names == ["hospodářského výbor"]
    assert by_vote[9].letter == "B1"
    unvotable = [a for a in parsed if a.is_unvotable]
    assert [a.letter for a in unvotable] == ["B2"]


def test_committee_letter_assigned_from_pdf_name() -> None:
    parsed = [a for a in _parsed() if not a.is_final_vote]
    by_letter: dict[str, list[AmendmentVote]] = {}
    for a in parsed:
        by_letter.setdefault(a.letter, []).append(a)
    pdf = [
        PdfAmendment(letter="A", submitter_names=["Hospodářský výbor"], raw_text="A. ..."),
        PdfAmendment(letter="B1", submitter_names=["Jan Papajanovský"], raw_text="B1. ..."),
    ]
    _assign_committee_letters(by_letter, pdf)
    assert by_letter["A"][0].vote_number == 8


def test_fates_of_246() -> None:
    a = AmendmentVote(letter="A", vote_number=8, result="accepted")
    b1 = AmendmentVote(letter="B1", vote_number=9, result="rejected")
    b2 = AmendmentVote(letter="B2", vote_number=0, is_unvotable=True)
    proc = AmendmentVote(letter="", vote_number=6, result="rejected", is_procedure=True)
    bill = BillAmendmentData(period=10, schuze=31, bod=12, ct=246, amendments=[proc, a, b1, b2])
    bill.final_vote = AmendmentVote(
        letter="", vote_number=10, result="accepted", is_final_vote=True
    )
    fates = {x["letter"]: x["fate"] for x in bill_letters(bill, frozenset())}
    assert fates == {"A": "accepted", "B1": "rejected", "B2": "not_voted"}
    assert unassigned_votes(bill) == 0  # the procedure vote is not an unassigned amendment vote


def test_carry_summaries_only_for_unchanged_text() -> None:
    old = BillAmendmentData(period=10, schuze=31, bod=12, ct=246)
    old.amendments = [
        AmendmentVote(
            letter="A", vote_number=8, amendment_text="t1", summary="s1", summary_en="e1"
        ),
        AmendmentVote(letter="B1", vote_number=9, amendment_text="t2", summary="s2"),
    ]
    new = BillAmendmentData(period=10, schuze=31, bod=12, ct=246)
    new.amendments = [
        AmendmentVote(letter="A", vote_number=8, amendment_text="t1"),
        AmendmentVote(letter="B1", vote_number=9, amendment_text="changed"),
    ]
    assert carry_summaries([new], {(31, 12): old}) == 1
    assert (new.amendments[0].summary, new.amendments[0].summary_en) == ("s1", "e1")
    assert new.amendments[1].summary == ""


def test_split_core_and_law_name() -> None:
    core, rest = split_core("Kraje by dostaly 6 % z výnosu daně, rozdělené podle délky silnic.")
    assert core == "Kraje by dostaly 6 % z výnosu daně" and rest.startswith(", rozdělené")
    assert law_name("Novela z. o pozemních komunikacích") == "zákona o pozemních komunikacích"
    assert law_name("Státní rozpočet na rok 2027") == ""


def test_certainty_of_letters() -> None:
    a = AmendmentVote(letter="A", vote_number=8, result="accepted", match_source="committee_name")
    b1 = AmendmentVote(letter="B1", vote_number=9, result="rejected", match_source="letter")
    c = AmendmentVote(letter="C", vote_number=0)
    bill = BillAmendmentData(period=10, schuze=31, bod=12, ct=246, amendments=[a, b1, c])
    rows = {x["letter"]: x["certainty"] for x in bill_letters(bill, frozenset())}
    assert rows == {"A": "uncertain", "B1": "certain", "C": "undetermined"}
