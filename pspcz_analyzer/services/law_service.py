"""Service functions for the laws/bills (zákony) page."""

from pspcz_analyzer.models.tisk_models import PeriodData, TiskInfo


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
    if tisk.history:
        return tisk.history.submitter
    return ""


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
    for (schuze, bod), bill in data.amendment_data.items():
        if bill.ct == tisk.ct:
            amendment_count = bill.amendment_count
            if bill.final_vote:
                final_result = bill.final_vote.result
            amendment_link = {"schuze": schuze, "bod": bod}
            break

    # Summary — truncated for list view
    summary = tisk.summary_en if (lang == "en" and tisk.summary_en) else tisk.summary
    summary_truncated = (summary[:150] + "...") if len(summary) > 150 else summary

    return {
        "ct": tisk.ct,
        "nazev": tisk.nazev,
        "topics": topics,
        "submitter": submitter,
        "status": status,
        "law_number": law_number,
        "url": tisk.url,
        "summary_truncated": summary_truncated,
        "has_summary": bool(summary),
        "final_result": final_result,
        "amendment_count": amendment_count,
        "has_amendments": amendment_link is not None,
        "amendment_link": amendment_link,
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

    Returns:
        Dict with keys: rows, total, page, per_page, total_pages.
    """
    tisky = _deduplicate_tisky(data)

    # Filter by search text
    if search:
        search_lower = search.lower()
        tisky = [t for t in tisky if search_lower in t.nazev.lower()]

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

    # Sort by ct descending (newest first)
    tisky.sort(key=lambda t: t.ct, reverse=True)

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
    }


def _find_votes_for_ct(data: PeriodData, ct: int) -> list[dict]:
    """Find all votes linked to a given tisk ct number.

    Args:
        data: Period data with tisk_lookup and votes DataFrame.
        ct: Tisk number.

    Returns:
        List of vote dicts with id, session, number, date, description, result.
    """
    # Collect (schuze, bod) pairs for this ct
    schuze_bod_pairs: set[tuple[int, int]] = set()
    for (schuze, bod), tisk in data.tisk_lookup.items():
        if tisk.ct == ct:
            schuze_bod_pairs.add((schuze, bod))

    if not schuze_bod_pairs:
        return []

    votes_list: list[dict] = []
    for _, row in enumerate(data.votes.iter_rows(named=True)):
        key = (row.get("schuze"), row.get("bod"))
        if key in schuze_bod_pairs:
            vysledek = row.get("vysledek", "")
            match vysledek:
                case "A":
                    result_label = "passed"
                case "R":
                    result_label = "rejected"
                case "Z":
                    result_label = "void"
                case _:
                    result_label = vysledek
            votes_list.append(
                {
                    "id_hlasovani": row.get("id_hlasovani"),
                    "schuze": row.get("schuze"),
                    "cislo": row.get("cislo"),
                    "datum": row.get("datum"),
                    "nazev_dlouhy": row.get("nazev_dlouhy", ""),
                    "result": result_label,
                }
            )

    # Sort by id_hlasovani descending
    votes_list.sort(key=lambda v: v.get("id_hlasovani", 0) or 0, reverse=True)
    return votes_list


def law_detail(
    data: PeriodData,
    ct: int,
    lang: str = "cs",
) -> dict | None:
    """Get full detail for a single bill by tisk number.

    Args:
        data: Period data.
        ct: Tisk number (cislo tisku).
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
    summary = tisk.summary_en if (lang == "en" and tisk.summary_en) else tisk.summary

    # Legislative history stages
    history = tisk.history

    # Find all amendment data entries for this ct
    amendment_entries: list[dict] = []
    for (schuze, bod), bill in data.amendment_data.items():
        if bill.ct == ct:
            final_result = bill.final_vote.result if bill.final_vote else ""
            amendment_entries.append(
                {
                    "schuze": schuze,
                    "bod": bod,
                    "amendment_count": bill.amendment_count,
                    "final_result": final_result,
                }
            )

    # Find related votes
    related_votes = _find_votes_for_ct(data, ct)

    return {
        "ct": tisk.ct,
        "nazev": tisk.nazev,
        "period": tisk.period,
        "url": tisk.url,
        "topics": topics,
        "summary": summary,
        "status": status,
        "submitter": submitter,
        "law_number": law_number,
        "history": history,
        "amendment_entries": amendment_entries,
        "has_amendments": len(amendment_entries) > 0,
        "related_votes": related_votes,
    }
