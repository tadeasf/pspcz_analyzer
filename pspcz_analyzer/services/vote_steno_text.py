"""Steno text around votes, without the LLM (#102).

Every vote announcement in a steno page is a link
``<a href="/sqw/hlasy.sqw?G=<id>" id="hN">Hlasování číslo N</a>, přihlášeno …``. Speakers
also link earlier votes by the same href ("revokuje usnesení přijaté v hlasování č. 277"),
so a vote is anchored at its announcement (followed by "přihlášen"), falling back to its
first link when no announcement exists. The text between two announcements is the lead-in
to the later vote.
"""

import html
import re
from typing import Any

import polars as pl

WINDOW_CHARS = 2500
HISTORY_CHARS = 300_000  # how far back earlier proposals are searched
SNIPPET_CHARS = 550
MAX_SNIPPETS = 6

_VOTE_ANCHOR_RE = re.compile(r'<a\b[^>]*hlasy\.sqw\?G=(\d+)[^>]*\bid="h(\d+)"[^>]*>', re.I)
_TAG_RE = re.compile(r"<[^>]+>")
_ANNOUNCED_RE = re.compile(r"přihlášen", re.I)

# Kind -> pattern; the kind whose last match is closest to the vote wins (rule fallback).
_KIND_PATTERNS: dict[str, re.Pattern[str]] = {
    "final": re.compile(
        r"celém návrhu zákona|návrh(?:u|em)? zákona jako celku|o zákonu jako celku"
    ),
    "amendment": re.compile(
        r"pozměňovací|\bnávrh(?:u|em)? [A-Z]\d*(?:\.\d+)?\b|legislativně technick|návrh(?:u)? výboru|"
        r"stanovisko (?:výboru|navrhovatele|ministr)"
    ),
    "reject": re.compile(r"zamítnutí|zamítnout|zamítl"),
    "return": re.compile(r"vrácení|vrátit (?:návrh )?(?:navrhovateli|k dopracování)"),
    "committee": re.compile(r"přikáz|lhůt\w* k projednání|zkrácení lhůty"),
    "objection": re.compile(r"námitk"),
    "challenge": re.compile(r"zpochybn"),
    "procedural": re.compile(
        r"pořad|přestávk|procedur|protinávrh|sloučen|jednat (?:a hlasovat )?i po|"
        r"en bloc|po \d+\. hodině|zařazení|vyřazení|přerušení|řečnick|pevn\w* (?:čas|termín)|revokac"
    ),
}
# rule kinds that are procedural sub-kinds in the stored taxonomy
_RULE_SUBKINDS = {"objection": "objection", "challenge": "challenge"}

# lead-ins that only point at an earlier proposal ("hlasujeme o mém návrhu")
_REFERENTIAL_RE = re.compile(
    r"m[éý]m návrhu|můj návrh|o tom(?:to)? návrhu|návrh\w* na revokaci|\bopět\b|\bznovu\b|"
    r"opakova|jak (?:navrhoval|požadoval)|ten(?:to)? návrh",
    re.I,
)
_PROPOSAL_RE = re.compile(r"\bnavrh(?:uj\w*|l[ai]?|nout)\b|procedurální návrh", re.I)
_SPEAKER_RE = re.compile(
    r"((?:Místo)?[Pp]ředsed\w*(?: PSP| vlády)?|Poslan\w*|Ministr\w*(?: [a-zěščřžýáíéůú]+)*|"
    r"Senátor\w*) ((?:[A-ZÁČĎÉĚÍŇÓŘŠŤÚŮÝŽ][\w-]+ ){1,3}):"
)
_RESULT_RE = re.compile(r"přihlášen|návrh byl (?:přijat|zamítnut)", re.I)
_CHAIR_VOID_RE = re.compile(r"zmatečn", re.I)
_VOTE_REF_RE = re.compile(r"hlasování (?:číslo |č\. ?|pořadové číslo )?(\d+)", re.I)
_WORD_RE = re.compile(r"[a-zěščřžýáíéůúňťď]{6,}")
_STOP = {"hlasov", "zahaju", "poslan", "návrhu", "navrhu", "předse", "sněmov", "prosím", "děkuji"}


def _plain(fragment: str) -> str:
    """HTML fragment -> plain text with collapsed whitespace."""
    return " ".join(html.unescape(_TAG_RE.sub(" ", fragment)).split())


def _anchor_events(pages: list[tuple[str, str]]) -> list[tuple[int, re.Match[str], bool]]:
    """All vote links in page order as ``(page index, match, is announcement)``."""
    events = []
    for i, (_, page) in enumerate(pages):
        for m in _VOTE_ANCHOR_RE.finditer(page):
            after = _plain(page[m.end() : m.end() + 400])[:160]
            events.append((i, m, bool(_ANNOUNCED_RE.search(after))))
    return events


def _chosen_anchors(events: list[tuple[int, re.Match[str], bool]]) -> set[tuple[int, int]]:
    """``(page index, offset)`` of the one anchor per vote: its announcement, else first link."""
    chosen: dict[int, tuple[int, int]] = {}
    announced: set[int] = set()
    for i, m, is_ann in events:
        vid = int(m.group(1))
        if is_ann and vid not in announced:
            chosen[vid] = (i, m.start())
            announced.add(vid)
        elif vid not in chosen:
            chosen[vid] = (i, m.start())
    return set(chosen.values())


def sitting_text(pages: list[tuple[str, str]]) -> tuple[str, dict[int, dict[str, Any]]]:
    """Plain text of a sitting and where each vote is announced in it.

    Args:
        pages: ``(page name, html)`` in page order.

    Returns:
        ``(text, {id_hlasovani: {"cislo", "page", "start", "end"}})``; ``text[start:end]``
        is everything since the previous vote's announcement.
    """
    events = _anchor_events(pages)
    chosen = _chosen_anchors(events)
    parts: list[str] = []
    size = 0
    last_end = 0
    out: dict[int, dict[str, Any]] = {}
    by_page: dict[int, list[re.Match[str]]] = {}
    for i, m, _ in events:
        if (i, m.start()) in chosen:
            by_page.setdefault(i, []).append(m)
    for i, (name, page) in enumerate(pages):
        pos = 0
        for m in by_page.get(i, []):
            seg = _plain(page[pos : m.start()])
            parts.append(seg)
            size += len(seg) + 1
            vid = int(m.group(1))
            out[vid] = {"cislo": int(m.group(2)), "page": name, "start": last_end, "end": size}
            last_end, pos = size, m.end()
        seg = _plain(page[pos:])
        parts.append(seg)
        size += len(seg) + 1
    return " ".join(parts), out


def vote_windows(pages: list[tuple[str, str]]) -> dict[int, dict[str, Any]]:
    """Lead-in text of every vote anchored in a sitting's steno pages.

    Args:
        pages: ``(page name, html)`` in page order.

    Returns:
        ``{id_hlasovani: {"cislo", "page", "window"}}``.
    """
    text, votes = sitting_text(pages)
    return {
        vid: {
            "cislo": v["cislo"],
            "page": v["page"],
            "window": text[max(v["start"], v["end"] - WINDOW_CHARS) : v["end"]].strip(),
        }
        for vid, v in votes.items()
    }


def vote_kind(window: str) -> str:
    """Rule-based kind of a vote from its lead-in text (``other`` if none matches)."""
    best, best_pos = "other", -1
    for kind, pattern in _KIND_PATTERNS.items():
        hits = [m.end() for m in pattern.finditer(window)]
        if hits and hits[-1] > best_pos:
            best, best_pos = kind, hits[-1]
    return best


def rule_kind(window: str) -> tuple[str, str]:
    """Rule kind mapped to the stored ``(kind, subkind)`` taxonomy."""
    kind = vote_kind(window)
    if kind in _RULE_SUBKINDS:
        return "procedural", _RULE_SUBKINDS[kind]
    return kind, ""


def is_referential(window: str) -> bool:
    """Whether the lead-in only points at an earlier proposal ("o mém návrhu")."""
    return bool(_REFERENTIAL_RE.search(window[-800:]))


def speaker_before(text: str, pos: int) -> str:
    """Name of the last speaker label before ``pos`` ("" if none in the last 80k chars)."""
    found = list(_SPEAKER_RE.finditer(text, max(0, pos - 80_000), pos))
    return found[-1].group(2).strip() if found else ""


def _keywords(text: str) -> set[str]:
    return {w[:6] for w in _WORD_RE.findall(text.lower())} - _STOP


def _score(snippet: str, speaker: str, tail: str, tail_speaker: str) -> int:
    score = len(_keywords(snippet) & _keywords(tail[-2300:]))
    surname = speaker.split()[-1] if speaker else ""
    if surname and surname[:5] in tail[-800:]:
        score += 3  # "jak navrhoval pan poslanec Haas"
    if speaker and speaker == tail_speaker and re.search(r"m[éý]m návrhu|můj návrh", tail[-800:]):
        score += 5
    return score


def earlier_proposals(text: str, start: int, tail: str, hint: str = "") -> list[str]:
    """Earlier proposals in the sitting a referential lead-in may point at.

    Args:
        text: Plain text of the sitting.
        start: Where the vote's lead-in tail starts (proposals are searched before it).
        tail: The lead-in tail.
        hint: Extra text whose words also rank the proposals (the previous vote's lead-in
            when this one is bare: "druhý návrh … řečnická doba").

    Returns:
        Up to ``MAX_SNIPPETS`` snippets ``"<speaker>: <text>"`` in chronological order.
    """
    lo = max(0, start - HISTORY_CHARS)
    tail_speaker = speaker_before(text, start + len(tail))
    cands: list[tuple[int, int, str]] = []
    last = -SNIPPET_CHARS
    for m in _PROPOSAL_RE.finditer(text, lo, start):
        if m.start() - last < SNIPPET_CHARS:
            continue
        last = m.start()
        snippet = text[max(lo, m.start() - 80) : min(start, m.start() + SNIPPET_CHARS)]
        speaker = speaker_before(text, m.start())
        cands.append(
            (
                _score(snippet, speaker, hint[-1500:] + " " + tail, tail_speaker),
                m.start(),
                f"{speaker}: {snippet}",
            )
        )
    best = sorted(cands, key=lambda c: (c[0], c[1]), reverse=True)[:MAX_SNIPPETS]
    return [s for score, _, s in sorted(best, key=lambda c: c[1]) if score > 0]


def is_result_formula(quote: str) -> bool:
    """Whether a quote is a vote result announcement (it belongs to the previous vote)."""
    return bool(_RESULT_RE.search(quote))


def chair_declared_void(window: str) -> bool:
    """Whether the chair called the vote "zmatečné" right before announcing it."""
    return bool(_CHAIR_VOID_RE.search(window[-200:]))


def referenced_vote(window: str) -> int | None:
    """Last vote number named in a lead-in ("zpochybňuji hlasování 280")."""
    refs = _VOTE_REF_RE.findall(window[-1500:])
    return int(refs[-1]) if refs else None


def link_repeats(records: dict[str, dict], votes: pl.DataFrame, text: str, spans: dict) -> None:
    """Mark challenged votes invalid and link them, and chair-repeated votes, to the repeat.

    Args:
        records: Sitting records keyed by ``str(id_hlasovani)`` (updated in place).
        votes: The sitting's ``hl_hlasovani`` rows.
        text: Plain steno text of the sitting.
        spans: ``sitting_text`` vote spans.
    """
    by_cislo = dict(zip(votes["cislo"].to_list(), votes["id_hlasovani"].to_list(), strict=True))
    result = dict(zip(votes["id_hlasovani"].to_list(), votes["vysledek"].to_list(), strict=True))
    for vid, rec in records.items():
        span = spans.get(int(vid))
        if not span:
            continue
        window = text[max(span["start"], span["end"] - WINDOW_CHARS) : span["end"]]
        cislo = rec["cislo"]
        repeat = by_cislo.get(cislo + 1)
        if rec.get("subkind") == "challenge" or rule_kind(window)[1] == "challenge":
            target = by_cislo.get(referenced_vote(window) or cislo - 1)
            if result.get(int(vid)) == "A" and target and repeat and str(target) in records:
                records[str(target)].update(invalid_by=int(vid), repeated_as=repeat)
                if str(repeat) in records:
                    records[str(repeat)]["repeats"] = target
        elif chair_declared_void(window) and repeat:
            rec["chair_repeated_as"] = repeat
            if str(repeat) in records:
                records[str(repeat)]["repeats"] = int(vid)


def anchor_check(votes: pl.DataFrame, anchored: set[int]) -> dict[str, dict[str, int]]:
    """Agenda items whose anchored vote count differs from ``hl_hlasovani``.

    Args:
        votes: The sitting's ``hl_hlasovani`` rows.
        anchored: Vote ids anchored in the steno.

    Returns:
        ``{str(bod): {"votes": n, "anchored": m}}`` for mismatching items only.
    """
    out: dict[str, dict[str, int]] = {}
    for bod, ids in votes.group_by("bod").agg("id_hlasovani").iter_rows():
        n_anchored = sum(i in anchored for i in ids)
        if n_anchored != len(ids):
            out[str(bod)] = {"votes": len(ids), "anchored": n_anchored}
    return out


def norm_quote(text: str) -> str:
    return " ".join(text.replace("„", '"').replace("“", '"').split()).strip(' "').lower()


def clean_proposer(names: str, context: str) -> str:
    """Proposer names reduced to the words found in the steno text.

    Titles ("pan poslanec") are dropped, and so is a name whose surname is not in the
    text; a first name the model added is dropped on its own.
    """
    low = context.lower()
    kept = []
    for name in re.split(r",| a ", names):
        words = [w for w in name.split() if w[:1].isupper() and len(w) >= 3]
        words = [w for w in words if w[:-1].lower() in low]
        if words and name.split() and words[-1] == name.split()[-1]:
            kept.append(" ".join(words))
    return ", ".join(kept)
