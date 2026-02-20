"""Build tisk lookup tables mapping (schuze, bod) -> TiskInfo."""

import polars as pl
from loguru import logger

from pspcz_analyzer.config import PERIOD_ORGAN_IDS
from pspcz_analyzer.models.tisk_models import TiskInfo
from pspcz_analyzer.services.tisk_text_service import TiskTextService


def build_tisk_lookup(
    period: int,
    votes: pl.DataFrame,
    schuze: pl.DataFrame,
    bod_schuze: pl.DataFrame,
    tisky: pl.DataFrame,
    tisk_text: TiskTextService,
    topic_cache: dict[int, dict[int, list[str]]],
    summary_cache: dict[int, dict[int, str]],
    summary_en_cache: dict[int, dict[int, str]] | None = None,
    topic_en_cache: dict[int, dict[int, list[str]]] | None = None,
) -> dict[tuple[int, int], TiskInfo]:
    """Build a mapping from (schuze_num, bod_num) -> TiskInfo for a given period.

    Primary path: schuze -> bod_schuze -> tisky (reliable, full coverage).
    Fallback: if schuze data is missing for this period, match vote
    descriptions directly to tisk names (covers new periods where
    schuze.zip hasn't been updated yet).
    """
    organ_id = PERIOD_ORGAN_IDS[period]
    en_cache = summary_en_cache or {}
    en_topic_cache = topic_en_cache or {}

    # Try primary path via schuze -> bod_schuze
    sessions = schuze.filter(pl.col("id_org") == organ_id)
    if sessions.height > 0:
        return build_tisk_lookup_via_schuze(
            period,
            sessions,
            bod_schuze,
            tisky,
            tisk_text,
            topic_cache,
            summary_cache,
            en_cache,
            en_topic_cache,
        )

    # Fallback: text matching for periods without schuze data
    logger.info(
        "No session data for period {} (organ {}), using text-match fallback",
        period,
        organ_id,
    )
    return build_tisk_lookup_via_text(
        period,
        votes,
        tisky,
        tisk_text,
        topic_cache,
        summary_cache,
        en_cache,
        en_topic_cache,
    )


def build_tisk_lookup_via_schuze(
    period: int,
    sessions: pl.DataFrame,
    bod_schuze: pl.DataFrame,
    tisky: pl.DataFrame,
    tisk_text: TiskTextService,
    topic_cache: dict[int, dict[int, list[str]]],
    summary_cache: dict[int, dict[int, str]],
    summary_en_cache: dict[int, dict[int, str]] | None = None,
    topic_en_cache: dict[int, dict[int, list[str]]] | None = None,
) -> dict[tuple[int, int], TiskInfo]:
    """Build lookup using the schuze -> bod_schuze -> tisky chain."""
    session_map = dict(
        zip(
            sessions.get_column("id_schuze").to_list(),
            sessions.get_column("schuze").to_list(),
            strict=False,
        )
    )
    session_ids = set(session_map.keys())

    bods = bod_schuze.filter(
        pl.col("id_schuze").is_in(session_ids)
        & pl.col("id_tisk").is_not_null()
        & (pl.col("id_tisk") != 0)
    )

    if bods.height == 0:
        return {}

    # Load topic classifications, summaries, and text availability
    topic_map = topic_cache.get(period, {})
    topic_en_map = (topic_en_cache or {}).get(period, {})
    summary_map = summary_cache.get(period, {})
    summary_en_map = (summary_en_cache or {}).get(period, {})

    tisk_ids = set(bods.get_column("id_tisk").to_list())
    relevant_tisky = tisky.filter(pl.col("id_tisk").is_in(tisk_ids))
    tisk_map = {}
    for row in relevant_tisky.iter_rows(named=True):
        ct = row.get("ct")
        if ct:
            tisk_map[row["id_tisk"]] = TiskInfo(
                id_tisk=row["id_tisk"],
                ct=ct,
                nazev=row.get("nazev_tisku") or "",
                period=period,
                topics=topic_map.get(ct, []),
                topics_en=topic_en_map.get(ct, []),
                has_text=tisk_text.has_text(period, ct),
                summary=summary_map.get(ct, ""),
                summary_en=summary_en_map.get(ct, ""),
            )

    lookup: dict[tuple[int, int], TiskInfo] = {}
    for row in bods.iter_rows(named=True):
        id_schuze = row["id_schuze"]
        schuze_num = session_map.get(id_schuze)
        bod_num = row.get("bod")
        id_tisk = row["id_tisk"]
        if schuze_num is not None and bod_num is not None and id_tisk in tisk_map:
            lookup[(schuze_num, bod_num)] = tisk_map[id_tisk]

    logger.info(
        "Period {}: built tisk lookup with {} entries (via schuze)",
        period,
        len(lookup),
    )
    return lookup


def build_tisk_lookup_via_text(
    period: int,
    votes: pl.DataFrame,
    tisky: pl.DataFrame,
    tisk_text: TiskTextService,
    topic_cache: dict[int, dict[int, list[str]]],
    summary_cache: dict[int, dict[int, str]],
    summary_en_cache: dict[int, dict[int, str]] | None = None,
    topic_en_cache: dict[int, dict[int, list[str]]] | None = None,
) -> dict[tuple[int, int], TiskInfo]:
    """Fallback: match vote descriptions to tisk names for this period.

    Used when schuze.zip hasn't been updated for a new period yet.
    """
    organ_id = PERIOD_ORGAN_IDS[period]
    period_tisky = tisky.filter(pl.col("id_obdobi") == organ_id)
    if period_tisky.height == 0:
        return {}

    # Load topic classifications, summaries, and text availability
    topic_map = topic_cache.get(period, {})
    topic_en_map = (topic_en_cache or {}).get(period, {})
    summary_map = summary_cache.get(period, {})
    summary_en_map = (summary_en_cache or {}).get(period, {})

    # Build list of tisk names for matching (longest first for greedy match)
    tisk_entries = []
    for row in period_tisky.iter_rows(named=True):
        ct = row.get("ct")
        nazev = (row.get("nazev_tisku") or "").strip()
        if ct and nazev:
            tisk_entries.append(
                TiskInfo(
                    id_tisk=row["id_tisk"],
                    ct=ct,
                    nazev=nazev,
                    period=period,
                    topics=topic_map.get(ct, []),
                    topics_en=topic_en_map.get(ct, []),
                    has_text=tisk_text.has_text(period, ct),
                    summary=summary_map.get(ct, ""),
                    summary_en=summary_en_map.get(ct, ""),
                )
            )
    tisk_entries.sort(key=lambda t: len(t.nazev), reverse=True)

    # Get unique (schuze, bod) combinations with descriptions
    vote_bods = (
        votes.filter(pl.col("nazev_dlouhy").is_not_null() & (pl.col("bod") > 0))
        .select("schuze", "bod", "nazev_dlouhy")
        .unique(subset=["schuze", "bod"])
    )

    lookup: dict[tuple[int, int], TiskInfo] = {}
    for row in vote_bods.iter_rows(named=True):
        desc = (row["nazev_dlouhy"] or "").strip()
        if not desc:
            continue
        for tisk in tisk_entries:
            if desc.startswith(tisk.nazev) or tisk.nazev.startswith(desc):
                lookup[(row["schuze"], row["bod"])] = tisk
                break

    logger.info(
        "Period {}: built tisk lookup with {} entries (via text match, {} tisky available)",
        period,
        len(lookup),
        len(tisk_entries),
    )
    return lookup
