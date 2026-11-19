"""Bills whose amendments are not voted yet (#112 "Zatím se nehlasovalo").

The consolidated amendment document ("Podané pozměňovací návrhy zpracovány jako
tisk N/X") is published after the 2nd reading. Bills that have it but no
3rd-reading vote get one entry keyed ``(PENDING_SCHUZE, ct)`` with the letters
parsed from that document.
"""

from loguru import logger

from pspcz_analyzer.config import TERMINAL_TISK_STATUSES
from pspcz_analyzer.models.amendment_models import (
    PENDING_SCHUZE,
    AmendmentVote,
    BillAmendmentData,
)
from pspcz_analyzer.models.tisk_models import PeriodData
from pspcz_analyzer.services.amendments.pdf_parser import PdfAmendment

BILL_DRUHY = (1, 2, 3)


def pending_cts(period_data: PeriodData, voted_cts: set[int]) -> dict[int, str]:
    """Bills with a published amendment document and no 3rd reading yet.

    Args:
        period_data: Period data with tisk histories loaded.
        voted_cts: Tisk numbers that already have 3rd-reading votes.

    Returns:
        ``{ct: title}``.
    """
    out: dict[int, str] = {}
    for tisk in period_data.tisk_lookup.values():
        history = tisk.history
        if (
            tisk.ct in voted_cts
            or tisk.id_druh not in BILL_DRUHY
            or history is None
            or history.amendment_tisk_ct1 is None
            or history.current_status in TERMINAL_TISK_STATUSES
            or any(s.stage_type == "3_cteni" for s in history.stages)
        ):
            continue
        out[tisk.ct] = tisk.nazev
    return out


def build_pending_bills(
    period: int,
    pending: dict[int, str],
    pdf_data: dict[int, list[PdfAmendment]],
    period_data: PeriodData,
) -> list[BillAmendmentData]:
    """One unvoted entry per pending bill whose document parsed into letters.

    Args:
        period: Electoral period.
        pending: Output of pending_cts.
        pdf_data: ``ct -> parsed letters`` from the amendment PDF stage.
        period_data: Period data (for the amendment sub-tisk ids).

    Returns:
        Bills keyed ``(PENDING_SCHUZE, ct)``; bills without parsed letters are skipped.
    """
    histories = {t.ct: t.history for t in period_data.tisk_lookup.values() if t.history}
    bills: list[BillAmendmentData] = []
    for ct, title in sorted(pending.items()):
        letters = pdf_data.get(ct) or []
        if not letters:
            continue
        history = histories.get(ct)
        bills.append(
            BillAmendmentData(
                period=period,
                schuze=PENDING_SCHUZE,
                bod=ct,
                ct=ct,
                tisk_nazev=title,
                amendments=[_unvoted(a) for a in letters],
                amendment_tisk_ct1=history.amendment_tisk_ct1 if history else None,
                amendment_tisk_idd=history.amendment_tisk_idd if history else None,
            )
        )
    logger.info(
        "[amendment pipeline] Pending bills: {}/{} with parsed letters",
        len(bills),
        len(pending),
    )
    return bills


def _unvoted(pdf_amend: PdfAmendment) -> AmendmentVote:
    """Letter of the amendment document as an entry without a vote."""
    return AmendmentVote(
        letter=pdf_amend.letter.strip().upper(),
        vote_number=0,
        result="",
        amendment_text=pdf_amend.raw_text,
        submitter_names=list(pdf_amend.submitter_names),
        pdf_submitter_names=list(pdf_amend.submitter_names),
    )
