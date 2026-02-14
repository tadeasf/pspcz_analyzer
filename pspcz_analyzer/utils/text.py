"""Text normalization utilities for Czech diacritics."""

import unicodedata


def strip_diacritics(text: str) -> str:
    """Remove diacritical marks from text (e.g. č→c, ř→r, ž→z)."""
    nfkd = unicodedata.normalize("NFD", text)
    return "".join(c for c in nfkd if unicodedata.category(c) != "Mn")


def normalize_czech(text: str) -> str:
    """Lowercase and strip diacritics — suitable for search matching."""
    return strip_diacritics(text.lower())
