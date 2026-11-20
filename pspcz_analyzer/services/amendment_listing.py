"""Amendments section listing (#112): letters grouped by law, filters, totals.

A law (tisk) can have several (schuze, bod) entries when its 3rd reading
spans sittings; the letters of all entries are shown under one law.
"""

import re
from collections.abc import Callable

import polars as pl

from pspcz_analyzer.models.amendment_models import BillAmendmentData
from pspcz_analyzer.models.tisk_models import PeriodData
from pspcz_analyzer.services.amendment_letters import bill_letters, tally, unassigned_votes
from pspcz_analyzer.services.law_filters import submitter_kind
from pspcz_analyzer.services.tisk.brief import brief_view
from pspcz_analyzer.utils.text import matches_tokens, search_tokens, tisk_number_query

SORTS = ("newest", "most", "oldest")
PREVIEW_LETTERS = 2


def _vote_dates(data: PeriodData) -> dict[int, str]:
    """Vote id -> date as stored ("dd.mm.yyyy")."""
    df = data.votes.select("id_hlasovani", pl.col("datum").cast(pl.Utf8))
    return dict(zip(df["id_hlasovani"].to_list(), df["datum"].to_list(), strict=True))


def pdf_url(bill: BillAmendmentData) -> str:
    """Link to the consolidated amendment document on psp.cz ("" if unknown)."""
    if bill.amendment_tisk_idd:
        return f"https://www.psp.cz/sqw/text/orig2.sqw?idd={bill.amendment_tisk_idd}"
    if bill.amendment_tisk_ct1:
        return (
            f"https://www.psp.cz/sqw/text/tiskt.sqw"
            f"?O={bill.period}&CT={bill.ct}&CT1={bill.amendment_tisk_ct1}"
        )
    return ""


def _sd_numbers(letter: str, text: str) -> list[str]:
    """Sněmovní dokument numbers of a letter ("B1. SD 1731"), else all in its section."""
    head = re.sub(r"^([A-Z])(\d+)$", r"\1\\.?\2", re.escape(letter))  # "G2" also as "G.2"
    own = re.findall(rf"^{head}\.?\s*SD\s*(\d+)", text, re.MULTILINE)
    return own or list(dict.fromkeys(re.findall(r"\bSD\s*(\d+)", text)))


def split_core(summary: str) -> tuple[str, str]:
    """Summary split into the bold core of the change and the rest (#112 "tučně jádro").

    The core is the text up to the first comma or dash when that comes early,
    otherwise the first sentence.
    """
    m = re.search(r",\s|\s[–-]\s", summary)
    if m and 15 <= m.start() <= 110:
        return summary[: m.start()], summary[m.start() :]
    end = summary.find(". ")
    return (summary[: end + 1], summary[end + 1 :]) if 0 < end <= 160 else (summary, "")


def _letter_row(row: dict, bill: BillAmendmentData, lang: str) -> dict:
    """Template-friendly dict for one letter.

    Args:
        row: Output row of bill_letters.
        bill: The bill the letter belongs to.
        lang: Language code for the summary.

    Returns:
        Flat dict for the letter card.
    """
    a = row["amend"]
    summary = a.summary_en if (lang == "en" and a.summary_en) else a.summary
    return {
        "letter": row["letter"],
        "fate": row["fate"],
        "certainty": row["certainty"],
        "steno_url": bill.steno_url,
        "side": row["side"],
        "voted_with": row["voted_with"],
        "names": a.pdf_submitter_names or a.submitter_names,
        "parties": sorted(set(a.submitter_parties)),
        "summary": summary,
        "summary_core": split_core(summary),
        "summary_warning": a.summary_warning,
        "summary_reviewed": a.summary_reviewed,
        "description": a.description,
        "id_hlasovani": a.id_hlasovani,
        "committee_stance": a.committee_stance,
        "proposer_stance": a.proposer_stance,
        "schuze": bill.schuze,
        "bod": bill.bod,
        "ct": bill.ct,
        "pdf_url": pdf_url(bill),
        "sd": _sd_numbers(row["letter"], a.amendment_text),
    }


def _law_titles(data: PeriodData, bill: BillAmendmentData, lang: str) -> tuple[str, str]:
    """``(plain title from the AI brief, official short title)`` of a bill."""
    tisk = next((t for t in data.tisk_lookup.values() if t.ct == bill.ct), None)
    official = (tisk.nazev if tisk else "") or bill.tisk_nazev
    brief = brief_view(tisk.brief, lang) if tisk else None
    return (str(brief["nazev"]) if brief and brief.get("nazev") else official), official


def law_name(official: str) -> str:
    """The law a bill amends, from its short title: "Novela z. o pozemních …" -> "zákona o …".

    Returns "" when the title does not name an amended law.
    """
    m = re.match(r"(?:Novela|Změna)\s+(?:z\.|zákona)\s+(.+)", official.strip(), re.I)
    return f"zákona {m.group(1)}" if m else ""


def _bill_date(bill: BillAmendmentData, dates: dict[int, str]) -> str:
    """Date of the final vote, else of the first voted amendment ("" if none)."""
    ids = [bill.final_vote.id_hlasovani] if bill.final_vote else []
    ids += [a.id_hlasovani for a in bill.amendments]
    return next((dates[i] for i in ids if i in dates), "")


def law_groups(data: PeriodData, lang: str = "cs", include_empty: bool = False) -> list[dict]:
    """One group per law with all its letters, newest 3rd reading first.

    Args:
        data: Period data with amendment_data.
        lang: Language code for summaries.
        include_empty: Keep laws whose 3rd reading has no amendment letter.

    Returns:
        Groups with ct, title, date, final_result, link, letters, unassigned.
    """
    dates = _vote_dates(data)
    by_ct: dict[int, list[BillAmendmentData]] = {}
    for bill in sorted(data.amendment_data.values(), key=lambda b: (b.schuze, b.bod)):
        by_ct.setdefault(bill.ct, []).append(bill)

    groups: list[dict] = []
    for ct, bills in by_ct.items():
        latest = bills[-1]
        letters = [
            _letter_row(row, bill, lang)
            for bill in bills
            for row in bill_letters(bill, data.coalition_for_bill(bill))
        ]
        if not letters and not latest.is_pending and not include_empty:
            continue  # a 3rd reading without any amendment letter
        title, official = _law_titles(data, latest, lang)
        tisk = next((t for t in data.tisk_lookup.values() if t.ct == ct), None)
        gov_bill = bool(tisk and submitter_kind(tisk) == "vlada")
        for x in letters:
            x["gov_bill"] = gov_bill
        groups.append(
            {
                "ct": ct,
                "title": title,
                "official": official,
                "law_name": law_name(official),
                "pending": latest.is_pending,
                "date": _bill_date(latest, dates),
                "final_result": latest.final_vote.result if latest.final_vote else "",
                "schuze": latest.schuze,
                "bod": latest.bod,
                "letters": letters,
                "unassigned": sum(unassigned_votes(b) for b in bills),
            }
        )
    return groups


def _letter_matches(letter: dict, side: str, club: str, fate: str) -> bool:
    """Whether a letter passes the side / club / fate filters."""
    return (
        (not side or letter["side"] == side)
        and (not club or club in letter["parties"])
        and (not fate or letter["fate"] == fate)
    )


def _filter_groups(groups: list[dict], search: str, side: str, club: str, fate: str) -> list[dict]:
    """Groups narrowed to matching letters; groups without any are dropped.

    Search matches the tisk number, the law title (whole law) or a
    submitter name (that letter only).
    """
    number = tisk_number_query(search) if search.strip() else None
    tokens = search_tokens(search) if search.strip() else []
    out: list[dict] = []
    for g in groups:
        if number is not None and g["ct"] != number:
            continue
        title_hit = (
            number is not None or not tokens or matches_tokens(tokens, g["title"], g["official"])
        )
        letters = [
            x
            for x in g["letters"]
            if _letter_matches(x, side, club, fate)
            and (title_hit or matches_tokens(tokens, *x["names"]))
        ]
        if letters or (title_hit and not (side or club or fate)):
            out.append({**g, "shown": letters, "tally": tally(g["letters"])})
    return out


def _sort_key(sort: str) -> Callable[[dict], tuple]:
    """Sort key for law groups (dates are dd.mm.yyyy)."""

    def date_key(g: dict) -> tuple:
        d = g["date"].split(".")
        if g.get("pending"):
            return ("9999", "", "")  # not voted yet: the newest
        return (d[2], d[1], d[0]) if len(d) == 3 else ("", "", "")

    match sort:
        case "most":
            return lambda g: (len(g["letters"]), date_key(g))
        case _:
            return date_key


def list_amendments(
    data: PeriodData,
    search: str = "",
    side: str = "",
    club: str = "",
    fate: str = "",
    view: str = "laws",
    sort: str = "newest",
    page: int = 1,
    per_page: int = 10,
    lang: str = "cs",
) -> dict:
    """Amendments section: period totals, filtered laws or letters, one page.

    Args:
        data: Period data.
        search: Tisk number, law title words or submitter name.
        side: Submitter side filter (opposition/government/mixed/committee).
        club: Club filter (a submitter's club).
        fate: Fate filter (one of amendment_letters.FATES).
        view: "laws" (grouped) or "letters" (one card per letter).
        sort: One of SORTS.
        page: Page number (1-based).
        per_page: Laws (or letters) per page.
        lang: Language code.

    Returns:
        Dict with period_tally, law_count, rows, total, page, total_pages, clubs.
    """
    groups = law_groups(data, lang)
    all_letters = [x for g in groups for x in g["letters"]]
    shown = sorted(
        _filter_groups(groups, search, side, club, fate),
        key=_sort_key(sort),
        reverse=sort != "oldest",
    )
    items: list[dict] = (
        [{**x, "law": g} for g in shown for x in g["shown"]] if view == "letters" else shown
    )
    total = len(items)
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))
    return {
        "period_tally": tally(all_letters),
        "law_count": len(groups),
        "rows": items[(page - 1) * per_page : page * per_page],
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": total_pages,
        "clubs": sorted({p for x in all_letters for p in x["parties"]}),
        "preview": PREVIEW_LETTERS,
    }


def law_amendments(data: PeriodData, schuze: int, bod: int, lang: str = "cs") -> dict | None:
    """All amendment letters of the law voted at (schuze, bod), for its own page.

    The page shows the whole law (every 3rd-reading entry of its tisk), so its
    counts match the listing and the law detail.

    Args:
        data: Period data.
        schuze: Session number.
        bod: Agenda item number.
        lang: Language code.

    Returns:
        The law group plus tally, steno_url and unlinked, or None if not found.
    """
    bill = data.get_amendments(schuze, bod)
    if bill is None:
        return None
    group = next(g for g in law_groups(data, lang, include_empty=True) if g["ct"] == bill.ct)
    entries = [b for b in data.amendment_data.values() if b.ct == bill.ct]
    return {
        **group,
        "tally": tally(group["letters"]),
        "steno_url": bill.steno_url,
        "unlinked": sum(b.unlinked_amendment_count for b in entries),
    }


def law_tally(data: PeriodData, ct: int) -> dict | None:
    """Letter tally of one law and the link to its amendments page (None if none).

    Args:
        data: Period data.
        ct: Tisk number.

    Returns:
        Dict with tally, schuze, bod; None when the law has no amendment data.
    """
    entries = sorted(
        (b for b in data.amendment_data.values() if b.ct == ct), key=lambda b: (b.schuze, b.bod)
    )
    if not entries:
        return None
    letters = [row for b in entries for row in bill_letters(b, data.coalition_for_bill(b))]
    return {"tally": tally(letters), "schuze": entries[-1].schuze, "bod": entries[-1].bod}
