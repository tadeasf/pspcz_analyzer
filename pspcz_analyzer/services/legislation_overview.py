"""Homepage blocks: bills under debate, recently passed, newest submitted (#108)."""

from __future__ import annotations

from collections import Counter
from datetime import date, timedelta

from pspcz_analyzer.models.tisk_models import PeriodData, TiskInfo
from pspcz_analyzer.services.law_filters import last_step, phase_of, submitter_kind, tab_of
from pspcz_analyzer.services.law_service import STATUS_KEYS
from pspcz_analyzer.services.overrides import homepage_pick
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


# Mockup stepper: (label key, history stage types that complete the step)
_STEPS = (
    ("ph.submitted", ()),
    ("ph.reading1", ("1_cteni",)),
    ("ph.committees", ("vybor", "garant")),
    ("ph.reading2", ("2_cteni",)),
    ("ph.reading3", ("3_cteni",)),
    ("ph.senate", ("senat", "senat_postoupeno", "senat_vybor", "senat_organizacni")),
    ("ph.president", ("prezident", "sbirka", "vyhlaseni")),
)


def phase_steps(tisk: TiskInfo) -> list[dict]:
    """Stepper rows for a bill: done / now / next, with the first date of each step.

    Args:
        tisk: Bill with its legislative history.

    Returns:
        One dict per step with ``key``, ``state`` and ``date`` (or None).
    """
    stages = tisk.history.stages if tisk.history else []
    submitted = tisk.history.submitter_date if tisk.history else None
    dates = [submitted] + [
        next((s.date for s in stages if s.stage_type in types), None) for _, types in _STEPS[1:]
    ]
    done = [True] + [any(s.stage_type in types for s in stages) for _, types in _STEPS[1:]]
    last = max(i for i, d in enumerate(done) if d)
    terminal = phase_of(tisk.history.current_status if tisk.history else "") not in (
        "chamber",
        "senate",
        "president",
    )
    return [
        {
            "key": key,
            "date": dates[i] if done[i] else None,
            "state": "done" if i <= last else ("now" if i == last + 1 and not terminal else "next"),
        }
        for i, (key, _) in enumerate(_STEPS)
    ]


def talked_about(data: PeriodData, lang: str, days: int = 14) -> dict | None:
    """Bill with the most speeches in the Chamber over the last ``days`` days (#108).

    Args:
        data: Period data with speeches and tisk_lookup.
        lang: Language code for the brief.
        days: Window length counted back from today.

    Returns:
        Homepage row plus ``speeches``, or None when no bill was debated. A bill
        the admin picked (services/overrides) wins over the speech count.
    """
    since = (date.today() - timedelta(days=days)).isoformat()
    per_ct: dict[int, int] = {}
    for key, by_day in data.speeches.items():
        tisk = data.tisk_lookup.get(key)
        if tisk is not None and tab_of(tisk) == "navrhy":
            n = sum(v for d, v in by_day.items() if d >= since)
            per_ct[tisk.ct] = per_ct.get(tisk.ct, 0) + n
    ct, n = max(per_ct.items(), key=lambda kv: kv[1], default=(0, 0))
    pick = homepage_pick(data.overrides) if data.overrides else None
    if pick:
        ct, n = pick, per_ct.get(pick, 0)
    tisk = next((t for t in _bills(data) if t.ct == ct), None)
    if not tisk or not (n or pick):
        return None
    return {**_row(tisk, lang), "speeches": n, "steps": phase_steps(tisk), "picked": bool(pick)}


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
    # A bill and its accompanying "souvisejici" bill can get the same plain title
    names = Counter(r["nazev"] for r in rows)
    for r in rows:
        r["dup"] = names[r["nazev"]] > 1
    in_chamber = _by_last_step([r for r in rows if r["phase"] == "chamber"])
    passed = _by_last_step([r for r in rows if r["phase"] in ("president", "valid")])
    newest = sorted(rows, key=lambda r: r["ct"], reverse=True)
    return {
        "in_progress": in_chamber[:5],
        "in_progress_total": len(in_chamber),
        "recently_passed": passed[:3],
        "newest": newest[:3],
        "talked_about": talked_about(data, lang),
    }
