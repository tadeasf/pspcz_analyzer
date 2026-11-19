"""Manual corrections from the admin, applied on top of pipeline outputs (TASKS 2X).

One JSON file per period, ``{cache}/overrides/{period}.json``::

    {kind: {key: {...fields, "by": user, "at": iso time}}}

Kinds and keys:

- ``vote_question`` / ``"<id_hlasovani>"``: ``question``, ``question_en``
- ``brief`` / ``"<ct>"``: ``review`` (``ok`` | ``hidden`` | ``edited``) and, when
  edited, ``nazev``, ``shrnuti``
- ``amendment_summary`` / ``"<ct>:<letter>"``: ``summary``, ``summary_en``
- ``homepage_pick`` / ``"talked_about"``: ``ct``

Only the admin writes this file; pipelines never touch it, so they cannot
overwrite a correction. The readers apply it on every ``get_period``.
"""

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from loguru import logger

from pspcz_analyzer.fileio import write_json_atomic
from pspcz_analyzer.models.amendment_models import BillAmendmentData

OVERRIDES_DIR = "overrides"
KINDS = ("vote_question", "brief", "amendment_summary", "homepage_pick")
BRIEF_REVIEWS = ("ok", "hidden", "edited")


def overrides_path(cache_dir: Path, period: int) -> Path:
    """Path of a period's overrides file."""
    return cache_dir / OVERRIDES_DIR / f"{period}.json"


def load_overrides(cache_dir: Path, period: int) -> dict[str, dict[str, dict]]:
    """Read a period's overrides (empty kinds when the file is missing or broken)."""
    path = overrides_path(cache_dir, period)
    data: dict = {}
    if path.exists():
        try:
            data = json.loads(path.read_text())
        except (ValueError, OSError):
            logger.opt(exception=True).warning("Unreadable overrides file {}", path)
    return {kind: dict(data.get(kind) or {}) for kind in KINDS}


def set_override(
    cache_dir: Path, period: int, kind: str, key: str, fields: dict[str, Any] | None, user: str
) -> None:
    """Add, replace or (``fields=None``) remove one correction.

    Args:
        cache_dir: Base cache directory.
        period: Electoral period.
        kind: One of KINDS.
        key: Record key (see module docstring).
        fields: Corrected fields, or None to delete the correction.
        user: Admin user making the change.

    Raises:
        ValueError: Unknown kind.
    """
    if kind not in KINDS:
        raise ValueError(f"unknown override kind {kind!r}")
    data = load_overrides(cache_dir, period)
    if fields is None:
        data[kind].pop(key, None)
    else:
        data[kind][key] = {
            **fields,
            "by": user,
            "at": datetime.now(UTC).isoformat(timespec="seconds"),
        }
    path = overrides_path(cache_dir, period)
    path.parent.mkdir(parents=True, exist_ok=True)
    write_json_atomic(data, path)
    logger.info("[overrides] period {} {} {} by {}", period, kind, key, user)


def _edited(rec: dict) -> dict[str, str]:
    return {"by": rec.get("by", ""), "at": rec.get("at", "")}


def apply_vote_questions(vote_steno: dict[int, dict], ov: dict[str, dict[str, dict]]) -> None:
    """Put corrected vote questions into the steno records (in place)."""
    for key, rec in ov["vote_question"].items():
        target = vote_steno.setdefault(int(key), {"kind": "other", "url": "", "quote": ""})
        target.update(
            question=rec.get("question", ""),
            question_en=rec.get("question_en", ""),
            status="ok",
            edited=_edited(rec),
        )


def apply_briefs(briefs: dict[int, dict], ov: dict[str, dict[str, dict]]) -> None:
    """Apply brief reviews and edits (in place); a hidden brief is removed."""
    for key, rec in ov["brief"].items():
        ct = int(key)
        match rec.get("review"):
            case "hidden":
                briefs.pop(ct, None)
            case "ok" if ct in briefs:
                briefs[ct] = {**briefs[ct], "status": "ok", "edited": _edited(rec)}
            case "edited":
                old = briefs.get(ct) or {}
                cs = {
                    **(old.get("cs") or {}),
                    "nazev": rec.get("nazev", ""),
                    "shrnuti": rec.get("shrnuti", ""),
                }
                # English falls back to the corrected Czech text rather than the old AI one
                briefs[ct] = {**old, "cs": cs, "en": {}, "status": "ok", "edited": _edited(rec)}


def apply_amendment_summaries(
    amendment_data: dict[tuple[int, int], BillAmendmentData], ov: dict[str, dict[str, dict]]
) -> None:
    """Put corrected letter summaries into the amendment data (in place).

    shortcut: removing a correction keeps the edited text in memory until the
    amendment parquet is reloaded (next pipeline run or restart).
    """
    for key, rec in ov["amendment_summary"].items():
        ct_str, _, letter = key.partition(":")
        for bill in amendment_data.values():
            if str(bill.ct) != ct_str:
                continue
            for amend in bill.amendments:
                if amend.letter.strip().upper() == letter:
                    amend.summary = rec.get("summary", "")
                    amend.summary_en = rec.get("summary_en", "")
                    amend.summary_warning = False


def homepage_pick(ov: dict[str, dict[str, dict]]) -> int | None:
    """Tisk number the admin pinned to "Teď se o tom mluví", if any."""
    ct = ov["homepage_pick"].get("talked_about", {}).get("ct")
    return int(ct) if ct else None
