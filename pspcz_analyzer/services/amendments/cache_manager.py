"""Parquet-based cache for amendment voting data.

Follows the same mtime-based invalidation pattern as TiskCacheManager.
"""

import json
from pathlib import Path

import polars as pl
from loguru import logger

from pspcz_analyzer.models.amendment_models import AmendmentVote, BillAmendmentData

AMENDMENTS_SCHEMA = {
    "period": pl.Int64,
    "schuze": pl.Int64,
    "bod": pl.Int64,
    "ct": pl.Int64,
    "tisk_nazev": pl.Utf8,
    "steno_url": pl.Utf8,
    "letter": pl.Utf8,
    "vote_number": pl.Int64,
    "id_hlasovani": pl.Int64,
    "submitter_names": pl.Utf8,
    "submitter_ids": pl.Utf8,
    "description": pl.Utf8,
    "committee_stance": pl.Utf8,
    "proposer_stance": pl.Utf8,
    "result": pl.Utf8,
    "is_revote": pl.Boolean,
    "original_vote_number": pl.Int64,
    "is_withdrawn": pl.Boolean,
    "grouped_with": pl.Utf8,
    "is_final_vote": pl.Boolean,
    "is_leg_tech": pl.Boolean,
    "amendment_text": pl.Utf8,
    "summary": pl.Utf8,
    "summary_en": pl.Utf8,
    "pdf_submitter_names": pl.Utf8,
    "submitter_parties": pl.Utf8,
    "bill_summary": pl.Utf8,
    "bill_summary_en": pl.Utf8,
    "parse_confidence": pl.Float64,
    "parse_warnings": pl.Utf8,
    "amendment_tisk_ct1": pl.Int64,
    "amendment_tisk_idd": pl.Int64,
}


def _cache_path(cache_dir: Path, period: int) -> Path:
    """Get the parquet cache file path for a period.

    Args:
        cache_dir: Base cache directory.
        period: Electoral period number.

    Returns:
        Path to the amendments parquet file.
    """
    d = cache_dir / "amendments" / str(period)
    d.mkdir(parents=True, exist_ok=True)
    return d / "amendments.parquet"


def _serialize_list(lst: list) -> str:
    """Serialize a list to JSON string for parquet storage.

    Args:
        lst: List to serialize.

    Returns:
        JSON string representation.
    """
    return json.dumps(lst, ensure_ascii=False)


def _deserialize_list(raw: str) -> list:
    """Deserialize a JSON string to list.

    Args:
        raw: JSON string.

    Returns:
        Parsed list, or empty list on error.
    """
    if not raw:
        return []
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return []


def save_amendments(
    cache_dir: Path,
    period: int,
    bills: list[BillAmendmentData],
) -> None:
    """Save amendment data to parquet cache.

    Flattens BillAmendmentData into one row per amendment vote.

    Args:
        cache_dir: Base cache directory.
        period: Electoral period number.
        bills: List of bill amendment data to cache.
    """
    rows: list[dict] = []
    for bill in bills:
        all_amendments = list(bill.amendments)
        if bill.final_vote:
            all_amendments.append(bill.final_vote)

        for amend in all_amendments:
            rows.append(
                {
                    "period": bill.period,
                    "schuze": bill.schuze,
                    "bod": bill.bod,
                    "ct": bill.ct,
                    "tisk_nazev": bill.tisk_nazev,
                    "steno_url": bill.steno_url,
                    "letter": amend.letter,
                    "vote_number": amend.vote_number,
                    "id_hlasovani": amend.id_hlasovani,
                    "submitter_names": _serialize_list(amend.submitter_names),
                    "submitter_ids": _serialize_list(amend.submitter_ids),
                    "description": amend.description,
                    "committee_stance": amend.committee_stance or "",
                    "proposer_stance": amend.proposer_stance or "",
                    "result": amend.result,
                    "is_revote": amend.is_revote,
                    "original_vote_number": amend.original_vote_number,
                    "is_withdrawn": amend.is_withdrawn,
                    "grouped_with": _serialize_list(amend.grouped_with),
                    "is_final_vote": amend.is_final_vote,
                    "is_leg_tech": amend.is_leg_tech,
                    "amendment_text": amend.amendment_text,
                    "summary": amend.summary,
                    "summary_en": amend.summary_en,
                    "pdf_submitter_names": _serialize_list(amend.pdf_submitter_names),
                    "submitter_parties": _serialize_list(amend.submitter_parties),
                    "bill_summary": bill.bill_summary,
                    "bill_summary_en": bill.bill_summary_en,
                    "parse_confidence": bill.parse_confidence,
                    "parse_warnings": _serialize_list(bill.parse_warnings),
                    "amendment_tisk_ct1": bill.amendment_tisk_ct1,
                    "amendment_tisk_idd": bill.amendment_tisk_idd,
                }
            )

    if not rows:
        logger.debug("No amendment rows to save for period {}", period)
        return

    df = pl.DataFrame(rows, schema=AMENDMENTS_SCHEMA)
    path = _cache_path(cache_dir, period)
    df.write_parquet(path)
    logger.info(
        "Saved {} amendment rows for {} bills (period {}) to {}",
        len(rows),
        len(bills),
        period,
        path,
    )


def _row_to_amendment(row: dict) -> AmendmentVote:
    """Convert a parquet row dict to an AmendmentVote.

    Args:
        row: Dict from parquet DataFrame.

    Returns:
        AmendmentVote instance.
    """
    return AmendmentVote(
        letter=row["letter"],
        vote_number=row["vote_number"],
        id_hlasovani=row.get("id_hlasovani"),
        submitter_names=_deserialize_list(row.get("submitter_names", "")),
        submitter_ids=_deserialize_list(row.get("submitter_ids", "")),
        description=row.get("description", ""),
        committee_stance=row.get("committee_stance") or None,
        proposer_stance=row.get("proposer_stance") or None,
        result=row.get("result", ""),
        is_revote=row.get("is_revote", False),
        original_vote_number=row.get("original_vote_number"),
        is_withdrawn=row.get("is_withdrawn", False),
        grouped_with=_deserialize_list(row.get("grouped_with", "")),
        is_final_vote=row.get("is_final_vote", False),
        is_leg_tech=row.get("is_leg_tech", False),
        amendment_text=row.get("amendment_text", ""),
        summary=row.get("summary", ""),
        summary_en=row.get("summary_en", ""),
        pdf_submitter_names=_deserialize_list(row.get("pdf_submitter_names", "")),
        submitter_parties=_deserialize_list(row.get("submitter_parties", "")),
    )


def load_amendments(
    cache_dir: Path,
    period: int,
) -> dict[tuple[int, int], BillAmendmentData]:
    """Load amendment data from parquet cache.

    Args:
        cache_dir: Base cache directory.
        period: Electoral period number.

    Returns:
        Dict mapping (schuze, bod) to BillAmendmentData.
    """
    path = _cache_path(cache_dir, period)
    if not path.exists():
        return {}

    df = pl.read_parquet(path)
    result: dict[tuple[int, int], BillAmendmentData] = {}

    # Group by (schuze, bod)
    for (schuze, bod), group in df.group_by(["schuze", "bod"]):
        rows = group.to_dicts()
        if not rows:
            continue

        first = rows[0]
        amendments: list[AmendmentVote] = []
        final_vote: AmendmentVote | None = None

        for row in rows:
            amend = _row_to_amendment(row)
            if amend.is_final_vote:
                final_vote = amend
            else:
                amendments.append(amend)

        bill = BillAmendmentData(
            period=period,
            schuze=schuze,  # type: ignore[arg-type]
            bod=bod,  # type: ignore[arg-type]
            ct=first["ct"],
            tisk_nazev=first.get("tisk_nazev", ""),
            steno_url=first.get("steno_url", ""),
            amendments=amendments,
            final_vote=final_vote,
            bill_summary=first.get("bill_summary", ""),
            bill_summary_en=first.get("bill_summary_en", ""),
            parse_confidence=first.get("parse_confidence", 1.0),
            parse_warnings=_deserialize_list(first.get("parse_warnings", "")),
            amendment_tisk_ct1=first.get("amendment_tisk_ct1"),
            amendment_tisk_idd=first.get("amendment_tisk_idd"),
        )
        result[(schuze, bod)] = bill  # type: ignore[index]

    logger.debug(
        "Loaded {} bill amendment records for period {} from cache",
        len(result),
        period,
    )
    return result
