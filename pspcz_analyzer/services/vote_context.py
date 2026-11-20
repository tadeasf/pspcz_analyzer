"""Kind, question and story of a vote (#102, #113, #114).

The kind comes from the amendment data (final / amendment letter), else from the steno
record. The story is what the vote detail tells around the numbers: the consequence,
what "Pro" meant, who proposed it, and whether a challenge voided the result.
"""

import polars as pl

from pspcz_analyzer.models.tisk_models import PeriodData
from pspcz_analyzer.services.vote_steno_pages import is_recent


def vote_kind(data: PeriodData, vote_id: int) -> str | None:
    """Kind from amendment data ('final' / 'amendment'), else from the steno rules (#102)."""
    ctx = data.get_amendment_for_vote(vote_id)
    if ctx is not None and (ctx[3] or ctx[2]):
        return "final" if ctx[3] else "amendment"
    kind = data.vote_steno.get(vote_id, {}).get("kind")
    return None if kind in (None, "other") else kind


_BILL_KINDS = frozenset({"final", "amendment", "reject", "return", "committee"})


def kind_on_item(kind: str | None, tisk_ct: int | None) -> str | None:
    """A bill kind on an item with no bill ("Pořad schůze") is procedural (#113)."""
    return "procedural" if kind in _BILL_KINDS and not tisk_ct else kind


def final_vote_ids(data: PeriodData) -> set[int]:
    """Votes on a bill as a whole ("Jen o celých zákonech", #115)."""
    return {v for v in data.votes["id_hlasovani"].to_list() if vote_kind(data, v) == "final"}


def vote_question(data: PeriodData, vote_id: int, lang: str) -> dict | None:
    """Question, quote and steno link of a vote when the steno check passed (#102)."""
    rec = data.vote_steno.get(vote_id)
    if not rec:
        return None
    q = rec.get("question_en") if lang == "en" else None
    return {
        "question": q or rec.get("question", ""),
        "quote": rec.get("quote", ""),
        "url": rec.get("url", ""),
        "status": rec.get("status", "unknown"),
        "edited": rec.get("edited"),
        "pro": (rec.get("pro_en") if lang == "en" else None) or rec.get("pro", ""),
        "proposer": rec.get("proposer", ""),
        "subkind": rec.get("subkind", ""),
        "invalid_by": rec.get("invalid_by"),
        "repeated_as": rec.get("repeated_as"),
        "repeats": rec.get("repeats"),
        "chair_repeated_as": rec.get("chair_repeated_as"),
    }


# filter value (#113) -> stored kinds it covers
KIND_FILTERS: dict[str, tuple[str, ...]] = {
    "final": ("final",),
    "amendment": ("amendment",),
    "reject_return": ("reject", "return"),
    "committee": ("committee",),
    "procedural": ("procedural",),
}


def filter_kind(votes: pl.DataFrame, data: PeriodData, kind: str) -> pl.DataFrame:
    """Keep votes of the chosen kind (KIND_FILTERS); votes of unknown kind match none."""
    kinds = KIND_FILTERS.get(kind)
    if not kinds:
        return votes
    ids = [v for v in votes.get_column("id_hlasovani").to_list() if vote_kind(data, v) in kinds]
    return votes.filter(pl.col("id_hlasovani").is_in(ids))


# decisive kind of a group -> (key when it passed, key when it failed) for "Výsledek bodu"
_GROUP_RESULTS: tuple[tuple[str, str, str], ...] = (
    ("final", "vl.res_final_pass", "vl.res_final_fail"),
    ("reject", "vl.res_reject_pass", ""),
    ("return", "vl.res_return_pass", ""),
    ("committee", "vl.res_committee_pass", ""),
)


def group_result(rows: list[dict]) -> str:
    """Translation key of the agenda item's outcome in a group ("" when none is decisive)."""
    for kind, passed, failed in _GROUP_RESULTS:
        hits = [r for r in rows if r.get("kind") == kind and not r["is_void"]]
        if not hits:
            continue
        if any(r["vysledek"] == "A" for r in hits):
            return passed
        if failed:
            return failed
    return ""


def reading_at(data: PeriodData, tisk_ct: int | None, schuze: int) -> str:
    """Reading ("1_cteni" …) the bill was in at a sitting, from its history ("" if unknown)."""
    tisk = (
        next((t for t in data.tisk_lookup.values() if t.ct == tisk_ct), None) if tisk_ct else None
    )
    stages = tisk.history.stages if tisk and tisk.history else []
    return next(
        (
            s.stage_type
            for s in reversed(stages)
            if s.session_number == schuze and "cteni" in s.stage_type
        ),
        "",
    )


# kind -> (consequence key when passed, when failed); procedural votes have none
_CONSEQUENCES: dict[str, tuple[str, str]] = {
    "final": ("vd.cons_final_pass", "vd.cons_final_fail"),
    "amendment": ("vd.cons_amend_pass", "vd.cons_amend_fail"),
    "reject": ("vd.cons_reject_pass", "vd.cons_reject_fail"),
    "return": ("vd.cons_return_pass", "vd.cons_return_fail"),
    "committee": ("vd.cons_committee_pass", "vd.cons_committee_fail"),
}


def _verb(women: list[bool | None]) -> str:
    """Translation key of "Navrhl / Navrhla / Navrhli / Navrhl(a)" (None = gender unknown)."""
    if len(women) > 1:
        return "vd.proposed_by_pl"
    match women[0] if women else None:
        case True:
            return "vd.proposed_by_f"
        case False:
            return "vd.proposed_by_m"
        case _:
            return "vd.proposed_by"


def proposer_view(data: PeriodData, names: str, vote_id: int | None) -> dict | None:
    """Proposers with their club on the vote day and the verb form (#114).

    Args:
        data: Period data (mp_info with pohlavi, mp_votes with the club per vote).
        names: Proposer names from the steno record, joined by ", " (may be surnames).
        vote_id: The vote, for the club on that day.

    Returns:
        ``{"verb": key, "text": "Karel Haas (ODS)"}``, or None without names. A name
        that matches no MP, or several, is shown as written, without a club.
    """
    if not names:
        return None
    info = data.mp_info
    has_gender = "pohlavi" in info.columns
    parts: list[str] = []
    women: list[bool | None] = []
    for name in names.split(", "):
        words = name.split()
        hits = info.filter(pl.col("prijmeni") == words[-1])
        if len(words) > 1:
            hits = hits.filter(pl.col("jmeno") == " ".join(words[:-1]))
        if hits.height != 1:
            parts.append(name)
            women.append(True if name.endswith("ová") else None)
            continue
        mp = hits.row(0, named=True)
        club = mp["party"]
        if vote_id and "party" in data.mp_votes.columns:
            row = data.mp_votes.filter(
                (pl.col("id_hlasovani") == vote_id) & (pl.col("id_poslanec") == mp["id_poslanec"])
            )
            club = row["party"][0] if row.height else club
        parts.append(f"{mp['jmeno']} {mp['prijmeni']}" + (f" ({club})" if club else ""))
        women.append(mp["pohlavi"] != "M" if has_gender and mp["pohlavi"] else None)
    return {"verb": _verb(women), "text": ", ".join(parts)}


def _vote_brief(data: PeriodData, vote_id: int | None) -> dict | None:
    """Number, result and tally of another vote of the period (None if unknown)."""
    if not vote_id:
        return None
    row = data.votes.filter(pl.col("id_hlasovani") == vote_id)
    if row.height == 0:
        return None
    r = row.row(0, named=True)
    return {k: r[k] for k in ("id_hlasovani", "cislo", "vysledek", "pro", "kvorum")}


def vote_story(data: PeriodData, info: dict, lang: str = "cs") -> dict:
    """What the vote detail says around the numbers (#114).

    Args:
        data: Period data.
        info: Vote info with ``kind``, ``steno`` (vote_question), ``tisk_ct``, ``schuze``.
        lang: Language.

    Returns:
        Dict with ``consequence`` (translation key or ""), ``pro`` (what Pro meant),
        ``final_meaning`` (True for the bill as a whole), ``proposer``, ``reading``,
        ``tisk_title`` and the linked votes ``invalid`` / ``challenger`` / ``repeat``,
        ``chair_repeat`` and ``repeats``.
    """
    st = info.get("steno") or {}
    kind = info.get("kind") or ""
    passed = info.get("vysledek") == "A"
    cons = _CONSEQUENCES.get(kind, ("", ""))[0 if passed else 1]
    challenge = data.vote_steno.get(st.get("invalid_by") or 0, {})
    tisk = next((t for t in data.tisk_lookup.values() if t.ct == info.get("tisk_ct")), None)
    brief = (tisk.brief or {}).get("en" if lang == "en" else "cs") if tisk and tisk.brief else None
    return {
        "consequence": "" if st.get("invalid_by") else cons,
        "pro": st.get("pro", ""),
        "final_meaning": kind == "final",
        "proposer": proposer_view(data, st.get("proposer", ""), info.get("id_hlasovani")),
        "reading": reading_at(data, info.get("tisk_ct"), info.get("schuze") or 0),
        "tisk_title": (brief or {}).get("nazev") or info.get("tisk_nazev") or "",
        "invalid": bool(st.get("invalid_by")),
        "challenger": (
            proposer_view(data, challenge.get("proposer", ""), st.get("invalid_by")) or {}
        ).get("text", ""),
        "repeat": _vote_brief(data, st.get("repeated_as")),
        "chair_repeat": _vote_brief(data, st.get("chair_repeated_as")),
        "repeats": _vote_brief(data, st.get("repeats")),
        "steno_pending": is_recent(info.get("datum") or ""),
    }
