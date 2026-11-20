"""Reuse of letter summaries across amendment pipeline runs."""

from loguru import logger

from pspcz_analyzer.models.amendment_models import BillAmendmentData


def carry_summaries(
    bills: list[BillAmendmentData], previous: dict[tuple[int, int], BillAmendmentData]
) -> int:
    """Copy summaries from the previous run onto letters whose text did not change.

    A rerun then only sends new or changed letters to the LLM.

    Args:
        bills: Freshly parsed bills (updated in place).
        previous: Bills of the previous run keyed by ``(schuze, bod)``.

    Returns:
        Number of letters that kept their summary.
    """
    kept = 0
    for bill in bills:
        old = previous.get((bill.schuze, bill.bod))
        if not old:
            continue
        by_letter = {(a.letter, a.amendment_text): a for a in old.amendments if a.summary}
        for amend in bill.amendments:
            prev = by_letter.get((amend.letter, amend.amendment_text))
            if prev and amend.letter and amend.amendment_text:
                amend.summary, amend.summary_en = prev.summary, prev.summary_en
                amend.summary_warning = prev.summary_warning
                kept += 1
    logger.info("[amendment pipeline] kept {} letter summaries from the previous run", kept)
    return kept
