"""Hand-kept government clubs per period, with dates and sources (#101).

Used where the confidence vote cannot decide: period 9's investiture vote
(77501) is missing from psp.cz open data, and the Pirates left the government
mid-period. Periods without rows here fall back to ``coalition_detector``.
Club names are the normalised ones from ``mp_builder`` (e.g. "ANO", "TOP09").
"""

from datetime import date
from typing import NamedTuple


class CoalitionSpan(NamedTuple):
    """Clubs in government from ``start`` to ``end`` (inclusive, None = open)."""

    start: date
    end: date | None
    parties: frozenset[str]
    source: str


COALITION_TABLE: dict[int, tuple[CoalitionSpan, ...]] = {
    9: (
        CoalitionSpan(
            date(2021, 10, 12),
            date(2021, 12, 16),
            frozenset({"ANO"}),
            "Babiš's 2nd government (ANO + ČSSD; ČSSD has no MPs in period 9) governed until "
            "17. 12. 2021, psp.cz organ 1494 (27. 6. 2018 - 17. 12. 2021)",
        ),
        CoalitionSpan(
            date(2021, 12, 17),
            date(2024, 9, 30),
            frozenset({"ODS", "KDU-ČSL", "TOP09", "STAN", "Piráti"}),
            "Fiala government appointed 17. 12. 2021 (psp.cz organ 1628); the Pirate forum "
            "voted to leave on 30. 9. 2024: https://www.novinky.cz/clanek/domaci-pirati-si-odhlasovali-konec-ve-vlade-40490984",
        ),
        CoalitionSpan(
            date(2024, 10, 1),
            None,
            frozenset({"ODS", "KDU-ČSL", "TOP09", "STAN"}),
            "Pirate ministers resigned on 1. 10. 2024: "
            "https://www.ceskenoviny.cz/zpravy/-pirati-opousteji-vladu-mmr-ma-ridit-jurecka-fiala-podporil-lipavskeho/2573644",
        ),
    ),
}


def coalition_spans(period: int) -> tuple[CoalitionSpan, ...]:
    """Return the hand-kept government spans of a period (empty if none).

    Args:
        period: Electoral period number.

    Returns:
        Spans ordered by start date.
    """
    return COALITION_TABLE.get(period, ())


def parties_at(spans: tuple[CoalitionSpan, ...], day: date) -> frozenset[str]:
    """Government clubs on a given day (empty set when no span covers it).

    Args:
        spans: Spans from ``coalition_spans``.
        day: The day to look up.

    Returns:
        Club names in government that day.
    """
    for span in spans:
        if span.start <= day and (span.end is None or day <= span.end):
            return span.parties
    return frozenset()
