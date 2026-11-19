"""Laws list: tabs by document type, phases, submitter kind, last move (#110)."""

from __future__ import annotations

import re

from pspcz_analyzer.models.tisk_models import TiskInfo

_CZ_DATE_RE = re.compile(r"(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{4})")

# id_druh (druh_tisku.unl) -> tab
_BILL_DRUH = {1, 2, 3}  # vládní návrh zákona, návrh zákona, státní rozpočet
_TREATY_DRUH = {4}
_INTERPELLATION_DRUH = {6}
TABS: tuple[str, ...] = ("navrhy", "smlouvy", "zpravy", "interpelace")

# history status -> phase ("Kde návrh je")
_PHASES: dict[str, str] = {
    "projednáváno": "chamber",
    "vráceno Senátem": "chamber",
    "zamítnuto Senátem": "chamber",
    "vráceno prezidentem": "chamber",
    "schváleno sněmovnou": "senate",
    "v Senátu": "senate",
    "schváleno Senátem": "president",
    "u prezidenta": "president",
    "podepsáno": "president",
    "odesláno k vyhlášení": "president",
    "vyhlášeno": "valid",
    "zamítnuto": "failed",
    "nepřijato": "failed",
    "vzato zpět": "failed",
}
PHASES: tuple[str, ...] = ("chamber", "senate", "president", "valid", "failed")
SUBMITTER_KINDS: tuple[str, ...] = ("vlada", "poslanci", "senat", "kraje")


def tab_of(tisk: TiskInfo) -> str:
    """Tab of a tisk by its document type; unknown types land in 'zpravy'.

    Args:
        tisk: The tisk.

    Returns:
        One of TABS.
    """
    druh = tisk.id_druh
    if druh in _BILL_DRUH:
        return "navrhy"
    if druh in _TREATY_DRUH:
        return "smlouvy"
    if druh in _INTERPELLATION_DRUH:
        return "interpelace"
    return "zpravy"


def phase_of(status: str) -> str:
    """Phase ("Kde návrh je") for a history status.

    Args:
        status: TiskHistory.current_status.

    Returns:
        One of PHASES.
    """
    return _PHASES.get(status, "chamber")


def submitter_kind(tisk: TiskInfo) -> str | None:
    """Who submitted the bill: vlada / poslanci / senat / kraje, or None if unknown.

    Args:
        tisk: The tisk (id_druh and history submitter).

    Returns:
        One of SUBMITTER_KINDS, or None.
    """
    if tisk.id_druh in (1, 3):
        return "vlada"
    text = (tisk.history.submitter if tisk.history else "").lower()
    if "vlád" in text:
        return "vlada"
    if "senát" in text:
        return "senat"
    if "zastupitelstv" in text or "kraj" in text:
        return "kraje"
    if "poslan" in text:
        return "poslanci"
    return None


def _parse_cz_date(value: str | None) -> tuple[int, int, int] | None:
    """Parse 'd. m. yyyy' into a sortable (y, m, d) tuple."""
    m = _CZ_DATE_RE.search(value or "")
    return (int(m.group(3)), int(m.group(2)), int(m.group(1))) if m else None


def last_step(tisk: TiskInfo) -> dict | None:
    """The most recent dated stage: sortable date, raw date and its sentence.

    Args:
        tisk: The tisk.

    Returns:
        {"sort": (y, m, d), "date": "d. m. yyyy", "text": str} or None.
    """
    best: dict | None = None
    for stage in tisk.history.stages if tisk.history else []:
        parsed = _parse_cz_date(stage.date)
        if parsed is not None and (best is None or parsed >= best["sort"]):
            text = stage.outcome or stage.label
            best = {"sort": parsed, "date": stage.date or "", "text": text}
    return best
