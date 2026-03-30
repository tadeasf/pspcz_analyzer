"""Data models for amendment voting analysis.

Dataclasses representing parsed amendment votes from stenographic records
and their aggregation per bill.
"""

from dataclasses import dataclass, field


@dataclass
class AmendmentVote:
    """A single amendment vote parsed from a stenographic record.

    Attributes:
        letter: Amendment letter designation (e.g. "A", "B1", "E1 a F2").
        vote_number: Vote number (cislo) from steno text.
        id_hlasovani: Resolved vote ID from hl_hlasovani table.
        submitter_names: Names of amendment submitters from steno text.
        submitter_ids: Resolved id_osoba values for submitters.
        description: Brief text from steno context.
        committee_stance: Committee recommendation.
        proposer_stance: Proposer's position on the amendment.
        result: Vote outcome.
        is_revote: True if this is a re-vote after a challenge.
        original_vote_number: The challenged vote number (if is_revote).
        is_withdrawn: True if the amendment was withdrawn before voting.
        grouped_with: Other amendment letters voted together.
        is_final_vote: True for "zákon jako celku" final passage vote.
        is_leg_tech: True for §95 legislative-technical corrections.
        amendment_text: Per-amendment text from PDF (or combined blob for legacy).
        summary: LLM-generated Czech summary.
        summary_en: LLM-generated English summary.
        pdf_submitter_names: Nominative-case submitter names from PDF header.
        pdf_letter_raw_text: Raw text of this letter's full PDF section.
    """

    letter: str
    vote_number: int
    id_hlasovani: int | None = None
    submitter_names: list[str] = field(default_factory=list)
    submitter_ids: list[int] = field(default_factory=list)
    submitter_parties: list[str] = field(default_factory=list)
    description: str = ""
    committee_stance: str | None = None
    proposer_stance: str | None = None
    result: str = ""
    is_revote: bool = False
    original_vote_number: int | None = None
    is_withdrawn: bool = False
    grouped_with: list[str] = field(default_factory=list)
    is_final_vote: bool = False
    is_leg_tech: bool = False
    amendment_text: str = ""
    summary: str = ""
    summary_en: str = ""
    pdf_submitter_names: list[str] = field(default_factory=list)
    pdf_letter_raw_text: str = ""


@dataclass
class BillAmendmentData:
    """All amendment data for a single bill (tisk) in one session agenda item.

    Attributes:
        period: Electoral period number.
        schuze: Session number.
        bod: Agenda item number.
        ct: Tisk number (cislo tisku).
        tisk_nazev: Title of the tisk.
        steno_url: URL of the stenographic record page.
        amendments: List of parsed amendment votes.
        final_vote: The final passage vote (zákon jako celku), if found.
        bill_summary: LLM-generated Czech summary of the bill's amendment documents.
        bill_summary_en: LLM-generated English summary of the bill's amendment documents.
        parse_confidence: Parser confidence score (0.0–1.0).
        parse_warnings: List of parser warnings.
        amendment_tisk_ct1: CT1 of the amendment sub-tisk (e.g. 4 for tisk 410/4).
        amendment_tisk_idd: idd for direct PDF download of amendment sub-tisk.
        amendment_pdf_text: Full raw PDF text for bill-level summary.
    """

    period: int
    schuze: int
    bod: int
    ct: int
    tisk_nazev: str = ""
    steno_url: str = ""
    amendments: list[AmendmentVote] = field(default_factory=list)
    final_vote: AmendmentVote | None = None
    parse_confidence: float = 1.0
    bill_summary: str = ""
    bill_summary_en: str = ""
    parse_warnings: list[str] = field(default_factory=list)
    amendment_tisk_ct1: int | None = None
    amendment_tisk_idd: int | None = None
    amendment_pdf_text: str = ""

    @property
    def amendment_count(self) -> int:
        """Number of non-final amendments."""
        return len(self.amendments)

    @property
    def cross_party_count(self) -> int:
        """Number of amendments with submitters from multiple parties.

        Returns 0 if submitter IDs are not resolved.
        """
        return sum(1 for a in self.amendments if len(a.submitter_ids) > 1)
