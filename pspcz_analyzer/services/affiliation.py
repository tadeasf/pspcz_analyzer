"""Government vs opposition classification for parliamentary clubs.

Maps a club abbreviation (as produced by services/mp_builder.py) to a political
side given the period's governing coalition, and derives the "driver" of an
amendment from the set of clubs its submitters belong to. The coalition set is
auto-detected per period (services/coalition_detector.py) and carried on
PeriodData.coalition_parties. An empty coalition set means "unknown" — nothing
is classifiable. Used by the web layer to show who is pushing a legislative change.
"""

from __future__ import annotations

from collections.abc import Iterable

from pspcz_analyzer.config import INDEPENDENT_CLUB_MARKERS

# Affiliation literals
GOVERNMENT = "government"
OPPOSITION = "opposition"
MIXED = "mixed"
UNKNOWN = "unknown"


def _is_independent(party: str) -> bool:
    """Return True if *party* denotes a non-affiliated (independent) MP."""
    return any(marker in party for marker in INDEPENDENT_CLUB_MARKERS)


def classify_party(party: str, coalition: frozenset[str]) -> str:
    """Classify a single club as government, opposition, or unknown.

    Args:
        party: Normalized club abbreviation (e.g. "ANO", "ODS", "SPD").
        coalition: Government-coalition clubs for the period (empty = unknown).

    Returns:
        One of GOVERNMENT, OPPOSITION, or UNKNOWN. An empty coalition (period
        not detected), empty club, or independent club resolves to UNKNOWN.
    """
    if not coalition or not party or _is_independent(party):
        return UNKNOWN
    return GOVERNMENT if party in coalition else OPPOSITION


def amendment_driver(submitter_parties: Iterable[str], coalition: frozenset[str]) -> str:
    """Derive who drives an amendment from its submitters' clubs.

    Args:
        submitter_parties: Club abbreviations of the amendment submitters.
        coalition: Government-coalition clubs for the period (empty = unknown).

    Returns:
        GOVERNMENT if all classifiable submitters are coalition MPs, OPPOSITION
        if all are opposition, MIXED if both sides submitted, UNKNOWN if no
        submitter club could be classified.
    """
    sides = {
        side for party in submitter_parties if (side := classify_party(party, coalition)) != UNKNOWN
    }
    if not sides:
        return UNKNOWN
    if sides == {GOVERNMENT}:
        return GOVERNMENT
    if sides == {OPPOSITION}:
        return OPPOSITION
    return MIXED
