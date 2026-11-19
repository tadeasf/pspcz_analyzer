"""Homepage blocks: bills under debate, recently passed, newest submitted (#108)."""

from __future__ import annotations

from pspcz_analyzer.models.tisk_models import PeriodData, TiskInfo
from pspcz_analyzer.services.law_filters import last_step, phase_of, submitter_kind, tab_of
from pspcz_analyzer.services.law_service import STATUS_KEYS
from pspcz_analyzer.services.tisk.brief import brief_view


def _bills(data: PeriodData) -> list[TiskInfo]:
    """Unique bills (document tab 'navrhy'), one TiskInfo per ct."""
    seen: dict[int, TiskInfo] = {}
    for tisk in data.tisk_lookup.values():
        if tisk.ct not in seen and tab_of(tisk) == "navrhy":
            seen[tisk.ct] = tisk
    return list(seen.values())


def _first_sentence(text: str) -> str:
    """First sentence of a summary (plain text), for one-line rows."""
    text = " ".join((text or "").replace("#", "").replace("*", "").split())
    cut = text.find(". ")
    return text if cut < 0 else text[: cut + 1]


def _row(tisk: TiskInfo, lang: str) -> dict:
    """Compact homepage row for one bill."""
    status = tisk.history.current_status if tisk.history else "projednáváno"
    brief = brief_view(tisk.brief, lang)
    return {
        "ct": tisk.ct,
        "nazev": (brief and brief["nazev"]) or tisk.nazev,
        "status": status,
        "status_key": STATUS_KEYS.get(status, ""),
        "phase": phase_of(status),
        "kind": submitter_kind(tisk),
        "last_step": last_step(tisk),
        "submitted": tisk.history.submitter_date if tisk.history else None,
        "summary": _first_sentence(brief["shrnuti"]) if brief else "",
    }


def _by_last_step(rows: list[dict]) -> list[dict]:
    return sorted(rows, key=lambda r: (r["last_step"] or {}).get("sort", (0, 0, 0)), reverse=True)


def homepage_blocks(data: PeriodData, lang: str = "cs") -> dict:
    """Rows for the homepage legislation blocks.

    Args:
        data: Period data with tisk_lookup (and histories).
        lang: Language code for summaries.

    Returns:
        {"in_progress": [...5], "in_progress_total": int, "recently_passed": [...3],
        "newest": [...3]}.
    """
    rows = [_row(t, lang) for t in _bills(data)]
    in_chamber = _by_last_step([r for r in rows if r["phase"] == "chamber"])
    passed = _by_last_step([r for r in rows if r["phase"] in ("president", "valid")])
    newest = sorted(rows, key=lambda r: r["ct"], reverse=True)
    return {
        "in_progress": in_chamber[:5],
        "in_progress_total": len(in_chamber),
        "recently_passed": passed[:3],
        "newest": newest[:3],
    }
