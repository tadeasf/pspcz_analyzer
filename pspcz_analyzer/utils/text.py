"""Text normalization utilities for Czech diacritics."""

import re
import unicodedata

_TISK_NUMBER_RE = re.compile(r"^(?:(?:tisk|st|ct)\s*\.?\s*)?(\d{1,5})$")
_WORD_RE = re.compile(r"[a-z0-9]+")


def strip_diacritics(text: str) -> str:
    """Remove diacritical marks from text (e.g. č→c, ř→r, ž→z)."""
    nfkd = unicodedata.normalize("NFD", text)
    return "".join(c for c in nfkd if unicodedata.category(c) != "Mn")


def normalize_czech(text: str) -> str:
    """Lowercase and strip diacritics — suitable for search matching."""
    return strip_diacritics(text.lower())


def tisk_number_query(query: str) -> int | None:
    """Parse "246", "tisk 246" or "ST 246" as a tisk number.

    Args:
        query: Raw search text.

    Returns:
        The tisk number, or None when the query is not a tisk number.
    """
    m = _TISK_NUMBER_RE.match(normalize_czech(query.strip()))
    return int(m.group(1)) if m else None


def search_tokens(query: str) -> list[str]:
    """Split a query into normalised word prefixes for inflection-tolerant matching.

    Each word is cut to max(4, len - 2) characters, so "poplatky" matches
    "poplatcích" and "česká" matches "české" (no full Czech morphology).

    Args:
        query: Raw search text.

    Returns:
        Word prefixes (normalised, no diacritics).
    """
    return [w[: max(4, len(w) - 2)] for w in _WORD_RE.findall(normalize_czech(query))]


def matches_tokens(tokens: list[str], *texts: str | None) -> bool:
    """True when every token is a prefix of some word in the given texts.

    Args:
        tokens: Output of search_tokens.
        *texts: Texts to search (None is skipped).

    Returns:
        Whether all tokens match.
    """
    words = _WORD_RE.findall(normalize_czech(" ".join(t for t in texts if t)))
    return all(any(w.startswith(tok) for w in words) for tok in tokens)


# Legislative status keywords (Czech) for pass/fail classification.
# Fail is checked FIRST: "nedoporučuje" contains "doporučuje", so pass-first
# would misclassify a negative committee recommendation as approval.
# "nepřijat" must be here too: it contains the pass word "přijat".
_STATUS_FAIL = ("zamítnuto", "zamítnut", "zamítl", "nepřijat", "staženo", "vzat", "nedoporučuje")
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
