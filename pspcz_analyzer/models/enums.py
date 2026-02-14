"""Domain enumerations for Czech Parliament voting data."""

from enum import StrEnum


class VoteResult(StrEnum):
    """Individual MP vote codes (hl_poslanec.vysledek)."""

    YES = "A"
    NO = "B"
    ABSTAINED = "C"
    DID_NOT_VOTE = "F"
    ABSENT = "@"
    EXCUSED = "M"
    BEFORE_OATH = "W"
    ABSTAIN_ALT = "K"


class VoteOutcome(StrEnum):
    """Aggregate vote outcome (hl_hlasovani.vysledek)."""

    PASSED = "A"
    REJECTED = "R"
    INVALID_X = "X"
    INVALID_Q = "Q"
    INVALID_K = "K"


# Parliamentary club organ type ID
PARLIAMENTARY_CLUB_TYPE = 1
