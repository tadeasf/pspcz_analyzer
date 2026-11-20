"""Authorization state of cached steno sub-pages and when to check them again.

psp.cz publishes a steno sub-page first as "Neprošlo jazykovou korekturou, neautorizováno!",
then "Neautorizováno!" or "Autorizováno, neprošlo jazykovou korekturou!", and finally without
any note. The note is a ``<p class="status ...">`` right after the page's ``<a name="_h">``
anchor; the speech itself sits between ``<!-- ex -->`` and ``<!-- sy -->``. Authorization
changes the speech text (punctuation, added or removed sentences), typically 3-6 weeks after
the sitting and sometimes months later.

Each sitting keeps ``state_{schuze}.json`` next to its cached pages in ``steno/{period}/``::

    {"s034169.htm": {"sha": <speech text hash>, "status": "unauthorized", "note": "...",
                     "checked_at": iso, "next_check": iso,
                     "changed_at": iso, "prev_sha": str, "amend_stale": true}}

The hash covers the speech text only, with typography unified (normalize_text), so a new
note, a changed site menu or proofread quotes and dashes are no text change. A page without an entry is checked on its next use and its hash is a baseline:
only a hash that differs from a stored one is a change (``changed_at``, ``amend_stale``).
"""

import hashlib
import html as html_lib
import json
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path

from loguru import logger

from pspcz_analyzer.config import (
    STENO_RECHECK_AUTHORIZED_DAYS,
    STENO_RECHECK_FINAL_DAYS,
    STENO_RECHECK_UNAUTHORIZED_DAYS,
)
from pspcz_analyzer.fileio import write_json_atomic

STATUSES = ("unauthorized", "authorized", "final", "unknown")
AUTHORIZED = frozenset({"authorized", "final"})
_NOTE_RE = re.compile(
    r'<a name="_h"[^>]*>(?:&nbsp;|\s)*</a>\s*(?:<p class="status \w+">([^<]*)</p>)?'
)
_BODY_RE = re.compile(r"<!-- ex -->(.*?)<!-- sy -->", re.S)
_TAG_RE = re.compile(r"<[^>]+>")
# proofreading swaps typography ("" -> „“, - -> –); that is no change of what was said
_TYPO = str.maketrans(
    {"\u201e": '"', "\u201c": '"', "\u201d": '"', "\u201a": "'", "\u2018": "'", "\u2019": "'"}
    | {"\u2013": "-", "\u2014": "-", "\u2012": "-", "\u2212": "-", "\u00a0": " "}
)
# a daily run that starts a little earlier than yesterday's still re-checks
_SLACK = timedelta(hours=3)
_cache: dict[Path, tuple[int, dict[str, dict]]] = {}


def page_status(html: str) -> tuple[str, str]:
    """psp.cz state of a steno sub-page.

    Args:
        html: Page HTML.

    Returns:
        ``(status, note)``; status is one of STATUSES ("unknown" when the page has no
        ``_h`` anchor or an unrecognised note), note the text psp.cz shows.
    """
    m = _NOTE_RE.search(html)
    if not m:
        return "unknown", ""
    note = " ".join((m.group(1) or "").split())
    low = note.lower()
    if not note:
        return "final", ""
    if "neautoriz" in low:
        return "unauthorized", note
    if "autoriz" in low:
        return "authorized", note
    return "unknown", note


def body_html(html: str) -> str:
    """The speech part of a sub-page (no site menus, no state note)."""
    m = _BODY_RE.search(html)
    return m.group(1) if m else _NOTE_RE.sub("", html)


def normalize_text(text: str) -> str:
    """Text for change hashes: one kind of quote, dash and space, collapsed whitespace."""
    return " ".join(text.translate(_TYPO).split())


def body_sha(html: str) -> str:
    """Hash of a sub-page's speech text (normalize_text)."""
    text = normalize_text(html_lib.unescape(_TAG_RE.sub(" ", body_html(html))))
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def _interval(status: str) -> timedelta:
    match status:
        case "unauthorized":
            days = STENO_RECHECK_UNAUTHORIZED_DAYS
        case "authorized":
            days = STENO_RECHECK_AUTHORIZED_DAYS
        case _:  # final, or a page format without the note
            days = STENO_RECHECK_FINAL_DAYS
    return timedelta(days=days) - _SLACK


def _path(sdir: Path, schuze: int) -> Path:
    return sdir / f"state_{schuze}.json"


def load_state(sdir: Path, schuze: int) -> dict[str, dict]:
    """Page states of one sitting (empty when none are stored yet).

    Args:
        sdir: Steno cache directory of the period.
        schuze: Sitting number.

    Returns:
        ``{page name: entry}``; cached in memory until the file changes.
    """
    path = _path(sdir, schuze)
    if not path.exists():
        return {}
    mtime = path.stat().st_mtime_ns
    hit = _cache.get(path)
    if hit and hit[0] == mtime:
        return hit[1]
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (ValueError, OSError):
        logger.opt(exception=True).warning(
            "[steno state] unreadable {}, checking pages again", path
        )
        data = {}
    _cache[path] = (mtime, data)
    return data


def _save(sdir: Path, schuze: int, state: dict[str, dict]) -> None:
    path = _path(sdir, schuze)
    write_json_atomic(state, path)
    _cache[path] = (path.stat().st_mtime_ns, state)


def is_due(sdir: Path, schuze: int, name: str, now: datetime | None = None) -> bool:
    """Whether a cached page should be fetched again (no entry yet, or its time came)."""
    entry = load_state(sdir, schuze).get(name)
    if not entry or not entry.get("next_check"):
        return True
    return (now or datetime.now(UTC)) >= datetime.fromisoformat(entry["next_check"])


def record_check(
    sdir: Path, schuze: int, name: str, html: str, now: datetime | None = None
) -> bool:
    """Store the state of a freshly fetched page and schedule its next check.

    Args:
        sdir: Steno cache directory of the period.
        schuze: Sitting number.
        name: Sub-page name (``s034169.htm``).
        html: The fetched HTML.
        now: Check time (tests).

    Returns:
        True when the speech text differs from the stored hash. A page seen for the
        first time is a baseline and never a change.
    """
    now = now or datetime.now(UTC)
    state = dict(load_state(sdir, schuze))
    prev = state.get(name) or {}
    sha = body_sha(html)
    status, note = page_status(html)
    changed = bool(prev.get("sha")) and prev["sha"] != sha
    entry = {
        **prev,
        "sha": sha,
        "status": status,
        "note": note,
        "checked_at": now.isoformat(timespec="seconds"),
        "next_check": (now + _interval(status)).isoformat(timespec="seconds"),
    }
    if changed:
        entry.update(changed_at=entry["checked_at"], prev_sha=prev["sha"], amend_stale=True)
        logger.info("[steno state] sitting {} {}: speech text changed ({})", schuze, name, note)
    state[name] = entry
    # shortcut: the sitting's file is rewritten per fetched page (fetches are 1 s apart);
    # batch the writes if a first full check of large sittings gets slow
    _save(sdir, schuze, state)
    return changed


def page_states(sdir: Path, schuze: int) -> dict[str, str]:
    """``{page name: status}`` of a sitting's checked pages."""
    return {n: e.get("status", "unknown") for n, e in load_state(sdir, schuze).items()}


def stale_pages(sdir: Path, schuze: int) -> set[str]:
    """Pages whose text changed since the amendment pipeline last read them."""
    return {n for n, e in load_state(sdir, schuze).items() if e.get("amend_stale")}


def clear_stale(sdir: Path, schuze: int, names: set[str]) -> None:
    """Mark changed pages as read by the amendment pipeline."""
    state = dict(load_state(sdir, schuze))
    hit = [n for n in names if state.get(n, {}).get("amend_stale")]
    if not hit:
        return
    for n in hit:
        state[n] = {k: v for k, v in state[n].items() if k != "amend_stale"}
    _save(sdir, schuze, state)


def status_counts(sdir: Path) -> dict[str, int]:
    """Checked sub-pages of a period per status, plus ``unchecked`` cached pages.

    Args:
        sdir: Steno cache directory of the period.

    Returns:
        ``{status: count, "unchecked": count}``.
    """
    counts: dict[str, int] = dict.fromkeys((*STATUSES, "unchecked"), 0)
    checked: set[str] = set()
    for path in sdir.glob("state_*.json"):
        schuze = int(path.stem.split("_", 1)[1])
        for name, status in page_states(sdir, schuze).items():
            counts[status if status in counts else "unknown"] += 1
            checked.add(f"{schuze}_sub_{name.replace('.htm', '.html')}")
    counts["unchecked"] = sum(1 for p in sdir.glob("*_sub_*.html") if p.name not in checked)
    return counts
