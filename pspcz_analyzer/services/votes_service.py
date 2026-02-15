"""Votes / laws browser — search, list, and detail views of parliamentary votes."""

import polars as pl

from pspcz_analyzer.models.enums import VoteResult
from pspcz_analyzer.services.data_service import PeriodData
from pspcz_analyzer.utils.text import normalize_czech

# Outcome labels for display
OUTCOME_LABELS = {
    "A": "Passed",
    "R": "Rejected",
    "Z": "Void",
    "P": "Procedural",
    "N": "Not decided",
}


def list_votes(
    data: PeriodData,
    search: str = "",
    page: int = 1,
    per_page: int = 30,
    outcome_filter: str = "",
    topic_filter: str = "",
) -> dict:
    """List votes with optional text search, topic filter, and pagination.

    Returns dict with keys: rows, total, page, per_page, total_pages.
    """
    void_ids = data.void_votes.get_column("id_hlasovani")
    votes = data.votes.filter(~pl.col("id_hlasovani").is_in(void_ids))

    # Fill nulls in description columns for display and searching
    votes = votes.with_columns(
        pl.col("nazev_dlouhy").fill_null("").alias("nazev_dlouhy"),
        pl.col("nazev_kratky").fill_null("").alias("nazev_kratky"),
    )

    if search.strip():
        q = normalize_czech(search.strip())
        votes = votes.filter(
            pl.col("nazev_dlouhy").map_elements(
                lambda s: q in normalize_czech(s or ""), return_dtype=pl.Boolean,
            )
            | pl.col("nazev_kratky").map_elements(
                lambda s: q in normalize_czech(s or ""), return_dtype=pl.Boolean,
            )
        )

    if outcome_filter:
        votes = votes.filter(pl.col("vysledek") == outcome_filter)

    # Topic filter: only keep votes whose linked tisk has the specified topic
    if topic_filter:
        allowed_keys = set()
        for (schuze, bod), tisk in data.tisk_lookup.items():
            if topic_filter in tisk.topics:
                allowed_keys.add((schuze, bod))
        if allowed_keys:
            allowed_schuze = [k[0] for k in allowed_keys]
            allowed_bod = [k[1] for k in allowed_keys]
            key_df = pl.DataFrame({"schuze": allowed_schuze, "bod": allowed_bod})
            votes = votes.join(key_df, on=["schuze", "bod"], how="inner")
        else:
            votes = votes.head(0)

    total = votes.height
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))

    votes = votes.sort("datum", "cas", descending=[True, True])
    offset = (page - 1) * per_page
    page_rows = votes.slice(offset, per_page)

    rows = page_rows.select(
        "id_hlasovani", "datum", "cas", "schuze", "cislo", "bod",
        "nazev_dlouhy", "nazev_kratky",
        "vysledek", "pro", "proti", "zdrzel", "nehlasoval", "prihlaseno",
    ).to_dicts()

    for r in rows:
        r["outcome_label"] = OUTCOME_LABELS.get(r["vysledek"], r["vysledek"] or "?")
        # Look up linked tisk (parliamentary print)
        schuze = r.get("schuze")
        bod = r.get("bod")
        tisk = data.get_tisk(schuze, bod) if schuze and bod and bod > 0 else None
        r["tisk_url"] = tisk.url if tisk else None
        r["tisk_nazev"] = tisk.nazev if tisk else None
        r["tisk_ct"] = tisk.ct if tisk else None
        r["tisk_topics"] = tisk.topics if tisk else []

    return {
        "rows": rows,
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": total_pages,
    }


def vote_detail(data: PeriodData, vote_id: int) -> dict | None:
    """Get full detail for a single vote: metadata + per-party + per-MP breakdown."""
    vote_row = data.votes.filter(pl.col("id_hlasovani") == vote_id)
    if vote_row.height == 0:
        return None

    info = vote_row.with_columns(
        pl.col("nazev_dlouhy").fill_null(""),
        pl.col("nazev_kratky").fill_null(""),
    ).to_dicts()[0]
    info["outcome_label"] = OUTCOME_LABELS.get(info.get("vysledek", ""), "?")

    # Look up linked parliamentary print (tisk)
    schuze = info.get("schuze")
    bod = info.get("bod")
    tisk = data.get_tisk(schuze, bod) if schuze and bod and bod > 0 else None
    info["tisk_url"] = tisk.url if tisk else None
    info["tisk_nazev"] = tisk.nazev if tisk else None
    info["tisk_ct"] = tisk.ct if tisk else None
    info["tisk_topics"] = tisk.topics if tisk else []
    info["tisk_has_text"] = tisk.has_text if tisk else False
    info["tisk_summary"] = tisk.summary if tisk else ""

    # Individual MP votes for this vote
    mp_rows = data.mp_votes.filter(pl.col("id_hlasovani") == vote_id)
    mp_detail = mp_rows.join(data.mp_info, on="id_poslanec", how="left")

    # Per-party breakdown
    party_stats = (
        mp_detail.group_by("party").agg(
            (pl.col("vysledek") == VoteResult.YES).sum().alias("yes"),
            (pl.col("vysledek") == VoteResult.NO).sum().alias("no"),
            (pl.col("vysledek") == VoteResult.ABSTAINED).sum().alias("abstained"),
            (pl.col("vysledek") == VoteResult.DID_NOT_VOTE).sum().alias("passive"),
            (pl.col("vysledek") == VoteResult.ABSENT).sum().alias("absent"),
            (pl.col("vysledek") == VoteResult.EXCUSED).sum().alias("excused"),
            pl.len().alias("total"),
        )
        .sort("party")
    )

    party_rows = party_stats.to_dicts()

    # Per-MP breakdown sorted by party then name
    mp_list = (
        mp_detail.select("jmeno", "prijmeni", "party", "vysledek")
        .sort("party", "prijmeni", "jmeno")
    )

    vote_labels = {
        VoteResult.YES: "YES",
        VoteResult.NO: "NO",
        VoteResult.ABSTAINED: "ABSTAINED",
        VoteResult.DID_NOT_VOTE: "Passive",
        VoteResult.ABSENT: "Absent",
        VoteResult.EXCUSED: "Excused",
    }

    mp_dicts = mp_list.to_dicts()
    for m in mp_dicts:
        m["vote_label"] = vote_labels.get(m["vysledek"], m["vysledek"] or "?")

    return {
        "info": info,
        "party_breakdown": party_rows,
        "mp_votes": mp_dicts,
    }
