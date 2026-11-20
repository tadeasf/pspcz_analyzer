"""Homepage blocks: bills under debate, recently passed, newest submitted (#108)."""

from __future__ import annotations

import re
from collections import Counter
from datetime import date, timedelta

from pspcz_analyzer.models.tisk_models import PeriodData, TiskInfo
from pspcz_analyzer.services.law_filters import last_step, phase_of, submitter_kind, tab_of
from pspcz_analyzer.services.law_service import STATUS_KEYS
from pspcz_analyzer.services.overrides import homepage_hidden, homepage_pick
from pspcz_analyzer.services.step_text import phase_key
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
    m = _SENTENCE_END_RE.search(text)
    return text if not m else text[: m.start() + 1]


MIN_SPEECHES = 10  # #108: below this the block stays hidden

# a sentence ends at ". " before a capital letter, not after "mld.", "č.", "odst." …
_SENTENCE_END_RE = re.compile(
    r"(?<!\bmld)(?<!\bmil)(?<!\btis)(?<!\bč)(?<!\bodst)(?<!\bpísm)(?<!\bSb)(?<!\bzák)(?<!\btzv)"
    r"\.\s+(?=[A-ZÁČĎÉĚÍŇÓŘŠŤÚŮÝŽ])"
)


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
        "phase_key": phase_key(tisk),
        "last_step": last_step(tisk, lang),
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
    hidden = homepage_hidden(data.overrides) if data.overrides else frozenset()
    per_ct: dict[int, int] = {}
    for key, by_day in data.speeches.items():
        tisk = data.tisk_lookup.get(key)
        if tisk is not None and tab_of(tisk) == "navrhy" and tisk.ct not in hidden:
            n = sum(v for d, v in by_day.items() if d >= since)
            per_ct[tisk.ct] = per_ct.get(tisk.ct, 0) + n
    ct, n = max(per_ct.items(), key=lambda kv: kv[1], default=(0, 0))
    pick = homepage_pick(data.overrides) if data.overrides else None
    if pick:
        ct, n = pick, per_ct.get(pick, 0)
    tisk = next((t for t in _bills(data) if t.ct == ct), None)
    if not tisk or not (n >= MIN_SPEECHES or pick):
        return None
    return {**_row(tisk, lang), "speeches": n, "steps": phase_steps(tisk), "picked": bool(pick)}


def _latest_agenda(data: PeriodData, chamber_cts: set[int]) -> tuple[int, set[int]]:
    """Latest sitting with a bill still in the Chamber on its agenda, and those bills (#108).

    Returns:
        ``(sitting, tisk numbers)``; ``(0, set())`` when no agenda has such a bill.
    """
    by_sitting: dict[int, set[int]] = {}
    for (s, _), t in data.tisk_lookup.items():
        if s > 0 and t.ct in chamber_cts:
            by_sitting.setdefault(s, set()).add(t.ct)
    latest = max(by_sitting, default=0)
    return latest, by_sitting.get(latest, set())


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
    sitting, agenda = _latest_agenda(data, {r["ct"] for r in in_chamber})
    on_agenda = [r for r in in_chamber if r["ct"] in agenda] if agenda else in_chamber
    passed = _by_last_step([r for r in rows if r["phase"] in ("president", "valid")])
    newest = sorted(rows, key=lambda r: r["ct"], reverse=True)
    return {
        "in_progress": on_agenda[:5],
        "in_progress_total": len(in_chamber),
        "recently_passed": passed[:3],
        "newest": newest[:3],
        "talked_about": talked_about(data, lang),
        "agenda_sitting": sitting,
        "void_votes": data.void_votes.height if data.void_votes is not None else 0,
        "votes_total": data.votes.height,
    }
