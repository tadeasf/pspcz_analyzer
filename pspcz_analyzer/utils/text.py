"""Text normalization utilities for Czech diacritics."""

import unicodedata


def strip_diacritics(text: str) -> str:
    """Remove diacritical marks from text (e.g. č→c, ř→r, ž→z)."""
    nfkd = unicodedata.normalize("NFD", text)
    return "".join(c for c in nfkd if unicodedata.category(c) != "Mn")


def normalize_czech(text: str) -> str:
    """Lowercase and strip diacritics — suitable for search matching."""
    return strip_diacritics(text.lower())


# Legislative status keywords (Czech) for pass/fail classification.
# Fail is checked FIRST: "nedoporučuje" contains "doporučuje", so pass-first
# would misclassify a negative committee recommendation as approval.
_STATUS_FAIL = ("zamítnuto", "zamítnut", "staženo", "nedoporučuje")
_STATUS_PASS = (
    "vyhlášeno",
    "podepsáno",
    "schváleno",
    "schválen",
    "přijat",
    "podepsal",
    "doporučuje",
)


def status_category(status: str | None) -> str:
    """Classify a Czech legislative status string as pass, fail, or active.

    Args:
        status: Raw status/outcome text from psp.cz (Czech).

    Returns:
        One of "pass", "fail", or "active".
    """
    sl = (status or "").lower()
    if any(word in sl for word in _STATUS_FAIL):
        return "fail"
    if any(word in sl for word in _STATUS_PASS):
        return "pass"
    return "active"
