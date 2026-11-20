"""Service functions for the laws/bills (zákony) page."""

from pathlib import Path

from pspcz_analyzer.config import TISKY_PDF_DIR
from pspcz_analyzer.models.amendment_models import BillAmendmentData
from pspcz_analyzer.models.tisk_models import PeriodData, TiskInfo
from pspcz_analyzer.services.affiliation import amendment_driver
from pspcz_analyzer.services.amendment_listing import law_tally
from pspcz_analyzer.services.law_detail import (
    amended_laws,
    documents,
    law_letters,
    published_law_url,
    second_reading_done,
    status_sentence,
    timeline,
    votes_by_reading,
)
from pspcz_analyzer.services.law_filters import (
    TABS,
    decisive_vote,
    find_votes_for_ct,
    last_step,
    phase_of,
    submitter_kind,
    tab_of,
)
from pspcz_analyzer.services.step_text import phase_key
from pspcz_analyzer.services.tisk.brief import brief_view
from pspcz_analyzer.utils.text import matches_tokens, search_tokens, tisk_number_query

# Bill status (as stored, Czech) -> translation key. Templates render
# _(status_key); an unknown status has no key and shows the raw value.
STATUS_KEYS: dict[str, str] = {
    "projednáváno": "laws.status.in_progress",
    "schváleno sněmovnou": "laws.status.passed_chamber",
    "v Senátu": "laws.status.in_senate",
    "schváleno Senátem": "laws.status.passed_senate",
    "vráceno Senátem": "laws.status.returned_by_senate",
    "zamítnuto Senátem": "laws.status.rejected_by_senate",
    "u prezidenta": "laws.status.with_president",
    "vráceno prezidentem": "laws.status.returned_by_president",
    "podepsáno": "laws.status.signed",
    "odesláno k vyhlášení": "laws.status.sent_for_publication",
    "vyhlášeno": "laws.status.published",
    "zamítnuto": "laws.status.rejected",
    "nepřijato": "laws.status.not_passed",
    "vzato zpět": "laws.status.withdrawn",
}


def _brief_texts(tisk: TiskInfo) -> tuple[str, str]:
    """Czech AI title and summary, so search also finds the plain-language words."""
    cs = (tisk.brief or {}).get("cs") or {}
    return cs.get("nazev", ""), cs.get("shrnuti", "")


def _tisk_pdf_exists(cache_dir: Path, period: int, ct: int) -> bool:
    """Check whether a cached bill PDF exists for this tisk.

    Args:
        cache_dir: Base cache directory (the configured one, so the
            Docker bind-mount layout is respected).
        period: Electoral period number.
        ct: Tisk number.

    Returns:
        True if {cache}/tisky_pdf/{period}/{ct}.pdf is present.
    """
    return (cache_dir / TISKY_PDF_DIR / str(period) / f"{ct}.pdf").exists()


def _bill_driver(bill: BillAmendmentData, coalition: frozenset[str]) -> str:
    """Government/opposition driver aggregated across a bill's amendments."""
    parties = [p for a in bill.amendments for p in a.submitter_parties]
    return amendment_driver(parties, coalition)


def _tisk_status(tisk: TiskInfo) -> str:
    """Extract current legislative status from a TiskInfo.

    Args:
        tisk: TiskInfo instance.

    Returns:
        Status string (e.g. 'přijato', 'zamítnuto', 'projednáváno').
    """
    if tisk.history:
        return tisk.history.current_status
    return "projednáváno"


def _tisk_submitter(tisk: TiskInfo) -> str:
    """Extract submitter name from a TiskInfo.

    Args:
        tisk: TiskInfo instance.

    Returns:
        Submitter string or empty.
    """
    # the history names the submitter; else the open data's own short form (#98)
    return (tisk.history.submitter if tisk.history else "") or tisk.predkladatel


def _tisk_law_number(tisk: TiskInfo) -> str:
    """Extract law number from a TiskInfo.

    Args:
        tisk: TiskInfo instance.

    Returns:
        Law number string or empty.
    """
    if tisk.history and tisk.history.law_number:
        return tisk.history.law_number
    return ""


def get_all_status_labels(data: PeriodData) -> list[str]:
    """Collect unique status values from all tisky.

    Args:
        data: Period data containing tisk_lookup.

    Returns:
        Sorted list of unique status strings.
    """
    seen: set[str] = set()
    for tisk in data.tisk_lookup.values():
        seen.add(_tisk_status(tisk))
    return sorted(seen)


def _build_law_row(
    tisk: TiskInfo,
    data: PeriodData,
    lang: str,
) -> dict:
    """Build a single row dict for the laws list.

    Args:
        tisk: TiskInfo instance.
        data: Period data for looking up amendment info.
        lang: Language code for topics.

    Returns:
        Dict with all fields needed for the list template.
    """
    status = _tisk_status(tisk)
    submitter = _tisk_submitter(tisk)
    law_number = _tisk_law_number(tisk)

    topics = tisk.topics_en if (lang == "en" and tisk.topics_en) else tisk.topics

    # Find amendment data for this ct
    amendment_count = 0
    final_result = ""
    amendment_link: dict | None = None
    driver = "unknown"
    for (schuze, bod), bill in data.amendment_data.items():
        if bill.ct == tisk.ct:
            if bill.final_vote:
                final_result = bill.final_vote.result
            amendment_link = {"schuze": schuze, "bod": bod}
            driver = _bill_driver(bill, data.coalition_for_bill(bill))
            break
    amendments = law_tally(data, tisk.ct)
    if amendments is not None:
        amendment_count = amendments["tally"]["total"]
        amendment_link = {"schuze": amendments["schuze"], "bod": amendments["bod"]}

    # Summary – truncated for list view
    brief = brief_view(tisk.brief, lang)
    summary = brief["shrnuti"] if brief else ""
    # cut at a word boundary, never inside a word (#109/#110)
    summary_truncated = summary if len(summary) <= 220 else summary[:220].rsplit(" ", 1)[0] + " …"

    return {
        "ct": tisk.ct,
        "nazev": (brief and brief["nazev"]) or tisk.nazev,
        "id_druh": tisk.id_druh,
        "brief": brief,
        "topics": topics,
        "submitter": submitter,
        "status": status,
        "status_key": STATUS_KEYS.get(status, ""),
        "tab": tab_of(tisk),
        "submitter_kind": submitter_kind(tisk),
        "phase_key": phase_key(tisk) if tab_of(tisk) == "navrhy" else "",
        "last_step": last_step(tisk, lang),
        "law_number": law_number,
        "url": tisk.url,
        "summary_truncated": summary_truncated,
        "has_summary": bool(summary),
        "final_result": final_result,
        "amendment_count": amendment_count,
        "amendment_tally": amendments["tally"] if amendments else None,
        "has_amendments": amendment_link is not None,
        "amendment_link": amendment_link,
        "driver": driver,
    }


def _deduplicate_tisky(data: PeriodData) -> list[TiskInfo]:
    """Get unique TiskInfo entries from tisk_lookup, deduplicated by ct.

    Args:
        data: Period data containing tisk_lookup.

    Returns:
        List of unique TiskInfo, one per ct.
    """
    seen: set[int] = set()
    unique: list[TiskInfo] = []
    for tisk in data.tisk_lookup.values():
        if tisk.ct not in seen:
            seen.add(tisk.ct)
            unique.append(tisk)
    return unique


def list_laws(
    data: PeriodData,
    search: str = "",
    status_filter: str = "",
    topic_filter: str = "",
    page: int = 1,
    per_page: int = 20,
    lang: str = "cs",
    tab: str = "",
    phase: str = "",
    submitter: str = "",
    sort: str = "moved",
) -> dict:
    """List bills with optional search, status, and topic filters, paginated.

    Args:
        data: Period data containing tisk_lookup and amendment_data.
        search: Optional text search filter on tisk name.
        status_filter: Exact status string to match (empty or 'all' = no filter).
        topic_filter: Exact topic label to match (empty = no filter).
        page: Page number (1-based).
        per_page: Results per page.
        lang: Language code for topics/summaries.
        tab: Document-type tab (law_filters.TABS), empty = all types.
        phase: "Kde návrh je" (law_filters.PHASES), empty = all.
        submitter: law_filters.SUBMITTER_KINDS, empty = anyone.
        sort: "moved" (latest move), "newest" or "oldest" tisk number.

    Returns:
        Dict with keys: rows, total, page, per_page, total_pages, tab_counts.
    """
    tisky = _deduplicate_tisky(data)

    # Search: tisk number, or inflection/diacritics-tolerant words (#95)
    if search.strip():
        number = tisk_number_query(search)
        tokens = search_tokens(search)
        tisky = [
            t
            for t in tisky
            if (
                t.ct == number
                if number is not None
                else matches_tokens(tokens, t.nazev, t.nazev_cely, *t.topics, *_brief_texts(t))
            )
        ]

    # Filter by status (exact match)
    if status_filter and status_filter != "all":
        tisky = [t for t in tisky if _tisk_status(t) == status_filter]

    # Filter by topic
    if topic_filter:
        tisky = [
            t
            for t in tisky
            if topic_filter in (t.topics_en if (lang == "en" and t.topics_en) else t.topics)
        ]

    if phase:
        tisky = [t for t in tisky if phase_of(_tisk_status(t)) == phase]
    if submitter:
        tisky = [t for t in tisky if submitter_kind(t) == submitter]
    tab_counts = dict.fromkeys(TABS, 0)
    for t in tisky:
        tab_counts[tab_of(t)] += 1
    if tab:
        tisky = [t for t in tisky if tab_of(t) == tab]

    match sort:
        case "newest":
            tisky.sort(key=lambda t: t.ct, reverse=True)
        case "oldest":
            tisky.sort(key=lambda t: t.ct)
        case _:
            tisky.sort(
                key=lambda t: ((s := last_step(t)) and s["sort"] or (0, 0, 0), t.ct), reverse=True
            )

    total = len(tisky)
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))

    offset = (page - 1) * per_page
    page_tisky = tisky[offset : offset + per_page]

    rows = [_build_law_row(t, data, lang) for t in page_tisky]

    return {
        "rows": rows,
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": total_pages,
        "tab_counts": tab_counts,
    }


def law_detail(
    data: PeriodData,
    ct: int,
    cache_dir: Path,
    lang: str = "cs",
) -> dict | None:
    """Get full detail for a single bill by tisk number.

    Args:
        data: Period data.
        ct: Tisk number (cislo tisku).
        cache_dir: Base cache directory for PDF existence checks.
        lang: Language code for summaries/topics.

    Returns:
        Dict with full bill info, or None if ct not found.
    """
    # Find TiskInfo by ct
    tisk: TiskInfo | None = None
    for t in data.tisk_lookup.values():
        if t.ct == ct:
            tisk = t
            break

    if tisk is None:
        return None

    status = _tisk_status(tisk)
    submitter = _tisk_submitter(tisk)
    law_number = _tisk_law_number(tisk)
    topics = tisk.topics_en if (lang == "en" and tisk.topics_en) else tisk.topics
    brief = brief_view(tisk.brief, lang)

    # Legislative history stages
    history = tisk.history

    # Find related votes, with the question from steno (#102)
    related_votes = find_votes_for_ct(data, ct)
    for v in related_votes:
        rec = data.vote_steno.get(v["id_hlasovani"]) or {}
        v["question"] = rec.get("question_en" if lang == "en" else "question") or ""
        v["kind"] = rec.get("kind") or ""
    decisive = decisive_vote(history, related_votes)

    return {
        "ct": tisk.ct,
        "nazev": (brief and brief["nazev"]) or tisk.nazev,
        "period": tisk.period,
        "url": tisk.url,
        "topics": topics,
        "brief": brief,
        "status": status,
        "status_key": STATUS_KEYS.get(status, ""),
        "submitter": submitter,
        "law_number": law_number,
        "history": history,
        "has_pdf": _tisk_pdf_exists(cache_dir, tisk.period, ct),
        "has_text": tisk.has_text,
        "pdf_url": f"/api/tisk-pdf/{tisk.period}/{ct}",
        "amendments": law_tally(data, ct),
        "related_votes": related_votes,
        "decisive_vote": decisive,
        "nazev_cely": tisk.nazev_cely,
        "kind": submitter_kind(tisk),
        "tab": tab_of(tisk),
        "phase_key": phase_key(tisk) if tab_of(tisk) == "navrhy" else "",
        "last_step": last_step(tisk, lang),
        "status_sentence": status_sentence(tisk),
        "timeline": timeline(tisk, related_votes, lang),
        "votes_by_reading": votes_by_reading(tisk, related_votes, decisive),
        "documents": documents(tisk),
        "amended_laws": amended_laws(tisk.nazev_cely),
        "published_url": published_law_url(law_number),
        "letters": law_letters(data, ct, lang),
        "second_reading_done": second_reading_done(tisk),
    }
