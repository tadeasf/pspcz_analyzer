"""Law detail page parts (#111): timeline, status sentence, votes per reading, documents.

Everything is derived from the scraped history (services/tisk/io/history_stages) and the
votes linked to the bill; nothing is guessed. Steps without a fixed wording show the first
sentence of psp.cz's own text.
"""

import re
from datetime import date

from pspcz_analyzer.models.tisk_models import PeriodData, TiskInfo
from pspcz_analyzer.services.amendment_listing import law_amendments, law_tally
from pspcz_analyzer.services.law_filters import submitter_kind, tab_of
from pspcz_analyzer.services.step_text import (
    before_chamber,
    phase_key,
    president_deadline,
    step_sentence,
)

SECTIONS: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "ld.sec_chamber",
        ("ps", "vlada", "organizacni", "1_cteni", "vybor", "2_cteni", "garant", "3_cteni"),
    ),
    ("ld.sec_senate", ("senat_postoupeno", "senat_organizacni", "senat_vybor", "senat")),
    ("ld.sec_president", ("prezident", "sbirka", "vyhlaseni")),
)
# phase -> (section that holds the next steps, translation key of the dashed line)
_FUTURE: dict[str, tuple[str, str]] = {
    "phl.before_1st": ("ld.sec_chamber", "ld.next_chamber"),
    "phl.committees": ("ld.sec_chamber", "ld.next_chamber"),
    "phl.before_3rd": ("ld.sec_chamber", "ld.next_third"),
    "phl.chamber_again": ("ld.sec_chamber", "ld.next_again"),
    "phl.senate": ("ld.sec_senate", "ld.next_senate"),
    "phl.president": ("ld.sec_president", "ld.next_president"),
    "phl.publishing": ("ld.sec_president", "ld.next_publishing"),
}
_READINGS = ("1_cteni", "2_cteni", "3_cteni")
_SD_RE = re.compile(r"jako (?:sněmovní )?tisk (\d+)/(\d+)(?:\s*\(([^)]+)\))?", re.I)
_SENATE_TISK_RE = re.compile(r"Senátu jako tisk (\d+)", re.I)
_LAW_REF_RE = re.compile(r"zákon(?:a|u|em)? č\. (\d+)/(\d{4}) Sb\.(?:, ([^,]+))?")
_SENTENCE_RE = re.compile(r"^(.*?[^\d\s][.!?])(?:\s|$)")  # "20. 11. 2025" is no sentence end


def _first_sentence(text: str) -> str:
    text = " ".join(text.split())
    m = _SENTENCE_RE.match(text)
    return m.group(1) if m else text


def _step(tisk: TiskInfo, stage: object, votes: list[dict], lang: str) -> dict:
    """One timeline step: date, sentence and the vote it names."""
    stype = getattr(stage, "stage_type", "")
    text = step_sentence(
        stype,
        getattr(stage, "outcome", "") or "",
        getattr(stage, "details", ""),
        tab_of(tisk),
        lang,
        submitter_kind(tisk),
        before_chamber(tisk, stage),
    ) or _first_sentence(getattr(stage, "details", "") or getattr(stage, "label", ""))
    vote = None
    if getattr(stage, "vote_number", None) and getattr(stage, "session_number", None):
        key = (stage.session_number, stage.vote_number)  # type: ignore[attr-defined]
        vote = next((v for v in votes if (v["schuze"], v["cislo"]) == key), None)
    return {"date": getattr(stage, "date", "") or "", "text": text, "vote": vote}


def timeline(tisk: TiskInfo, votes: list[dict], lang: str = "cs") -> list[dict]:
    """Steps split into Chamber, Senate and President, with the dashed next step.

    Args:
        tisk: The tisk with history.
        votes: Votes linked to the tisk (law_filters.find_votes_for_ct).
        lang: Language.

    Returns:
        ``[{"key", "steps", "future", "silent_senate"}]``; non-bills get one section.
    """
    stages = tisk.history.stages if tisk.history else []
    if tab_of(tisk) != "navrhy":
        return [{"key": "ld.sec_steps", "steps": [_step(tisk, s, votes, lang) for s in stages]}]
    phase = phase_key(tisk)
    future_sec, future_key = _FUTURE.get(phase, ("", ""))
    deadline = president_deadline(tisk)
    out = []
    for key, types in SECTIONS:
        steps = [_step(tisk, s, votes, lang) for s in stages if s.stage_type in types]
        out.append(
            {
                "key": key,
                "steps": steps,
                "future": future_key if key == future_sec else "",
                "deadline": deadline if key == "ld.sec_president" else None,
            }
        )
    return out


def status_sentence(tisk: TiskInfo) -> dict | None:
    """The top sentence of "Kde návrh je" when it needs more than the last step.

    Returns:
        ``{"key", "delivered", "deadline"}`` while the law waits for the President, else None.
    """
    deadline = president_deadline(tisk)
    if not deadline or not tisk.history or tab_of(tisk) != "navrhy" or deadline < date.today():
        return None
    last = tisk.history.stages[-1]
    m = re.search(r"(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{4})", last.details or last.date or "")
    delivered = f"{m.group(1)}. {m.group(2)}. {m.group(3)}" if m else (last.date or "")
    return {"key": "ld.at_president", "delivered": delivered, "deadline": deadline}


def votes_by_reading(tisk: TiskInfo, votes: list[dict], decisive: dict | None) -> list[dict]:
    """Votes on the bill other than the decisive one, grouped by reading.

    Args:
        tisk: The tisk with history (readings carry their sitting number).
        votes: Votes linked to the tisk.
        decisive: The 3rd-reading vote on the whole bill (shown separately).

    Returns:
        ``[{"reading", "date", "votes"}]`` newest reading first; votes of sittings with
        no reading in the history are under reading "other".
    """
    stages = tisk.history.stages if tisk.history else []
    reading_of = {s.session_number: s.stage_type for s in stages if s.stage_type in _READINGS}
    groups: dict[str, list[dict]] = {}
    for v in votes:
        if decisive and v["id_hlasovani"] == decisive["id_hlasovani"]:
            continue
        groups.setdefault(reading_of.get(v["schuze"], "other"), []).append(v)
    order = ("3_cteni", "2_cteni", "1_cteni", "other")
    return [
        {"reading": r, "date": groups[r][0]["datum"], "votes": groups[r]}
        for r in order
        if r in groups
    ]


def documents(tisk: TiskInfo) -> list[dict]:
    """Versions of the tisk named in its history, plus the Senate print.

    Returns:
        ``[{"kind", "number", "date", "url"}]``; kind is a translation key suffix
        (original, amendments, opinion, other, senate).
    """
    out: list[dict] = []
    seen: set[str] = set()
    stages = tisk.history.stages if tisk.history else []
    for s in stages:
        for m in _SD_RE.finditer(s.details or ""):
            number = f"{m.group(1)}/{m.group(2)}"
            if number in seen or int(m.group(1)) != tisk.ct:
                continue
            seen.add(number)
            note = (m.group(3) or "").lower()
            kind = (
                "original"
                if m.group(2) == "0"
                else "amendments"
                if "pozměň" in note
                else "opinion"
                if "stanovisk" in note
                else "other"
            )
            url = f"https://www.psp.cz/sqw/text/tiskt.sqw?o={tisk.period}&ct={tisk.ct}&ct1={m.group(2)}"
            out.append({"kind": kind, "number": number, "date": s.date or "", "url": url})
        if m := _SENATE_TISK_RE.search(s.details or ""):
            out.append(
                {"kind": "senate", "number": m.group(1), "date": s.date or "", "url": tisk.url}
            )
    return out


def amended_laws(nazev_cely: str) -> list[dict]:
    """Laws a bill amends, named in its official title, with e-Sbírka links."""
    out = []
    for m in _LAW_REF_RE.finditer(nazev_cely or ""):
        number, year, name = m.group(1), m.group(2), (m.group(3) or "").strip()
        out.append(
            {
                "label": f"č. {number}/{year} Sb." + (f", {name}" if name else ""),
                "url": f"https://www.e-sbirka.cz/sb/{year}/{number}",
            }
        )
    return out


def published_law_url(law_number: str | None) -> str:
    """e-Sbírka link of a published law ("129/2026 Sb."), "" when not published."""
    m = re.match(r"\s*(\d+)/(\d{4})", law_number or "")
    return f"https://www.e-sbirka.cz/sb/{m.group(2)}/{m.group(1)}" if m else ""


def law_letters(data: PeriodData, ct: int, lang: str) -> list[dict]:
    """Amendment letter cards of a bill (empty when it has no amendment data)."""
    t = law_tally(data, ct)
    detail = law_amendments(data, t["schuze"], t["bod"], lang) if t else None
    return detail["letters"] if detail else []


def second_reading_done(tisk: TiskInfo) -> bool:
    """Whether the bill went through 2nd reading, when amendments are filed (#119)."""
    return bool(tisk.history and any(s.stage_type == "2_cteni" for s in tisk.history.stages))
