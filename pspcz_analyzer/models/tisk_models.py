"""Data models for parliamentary prints (tisky) and period data."""

from dataclasses import dataclass, field

import polars as pl

from pspcz_analyzer.config import PERIOD_LABELS, PERIOD_YEARS
from pspcz_analyzer.data.history_scraper import TiskHistory


@dataclass
class TiskInfo:
    """Metadata for a parliamentary print (tisk) linked to a vote.

    Attributes:
        id_tisk: Database ID of the tisk.
        ct: Tisk number (cislo tisku).
        nazev: Title of the tisk.
        period: Electoral period number.
        topics: LLM-classified topic labels (1-3 Czech labels).
        has_text: Whether extracted full text is available.
        summary: LLM-generated Czech summary of the tisk.
        history: Scraped legislative history stages.
        law_changes: List of affected existing laws.
        sub_versions: Sub-tisk version dicts (amendments, gov opinions).
    """

    id_tisk: int
    ct: int  # tisk number
    nazev: str
    period: int
    topics: list[str] = field(default_factory=list)
    has_text: bool = False
    summary: str = ""
    summary_en: str = ""
    history: TiskHistory | None = None
    law_changes: list[dict] = field(default_factory=list)
    sub_versions: list[dict] = field(default_factory=list)

    @property
    def url(self) -> str:
        return f"https://www.psp.cz/sqw/historie.sqw?o={self.period}&t={self.ct}"


@dataclass
class PeriodData:
    """All DataFrames and lookups for a single electoral period.

    Attributes:
        period: Electoral period number.
        votes: Polars DataFrame of vote records (hl_hlasovani).
        mp_votes: Polars DataFrame of per-MP vote results (hl_poslanec).
        void_votes: Polars DataFrame of void vote IDs (zmatecne).
        mp_info: Polars DataFrame of MP info (name, party, organ).
        tisk_lookup: Mapping of (session, agenda_item) to TiskInfo.
    """

    period: int
    votes: pl.DataFrame
    mp_votes: pl.DataFrame
    void_votes: pl.DataFrame
    mp_info: pl.DataFrame
    # Lookup: (schuze_num, bod_num) -> TiskInfo
    tisk_lookup: dict[tuple[int, int], TiskInfo] = field(default_factory=dict)

    @property
    def stats(self) -> dict:
        date_col = self.votes.get_column("datum")
        dates = date_col.drop_nulls().str.strip_chars()
        return {
            "period": self.period,
            "label": PERIOD_LABELS.get(self.period, PERIOD_YEARS.get(self.period, "?")),
            "total_votes": self.votes.height,
            "total_mp_records": self.mp_votes.height,
            "total_mps": self.mp_info.height,
            "date_min": dates.sort().head(1).to_list()[0] if dates.len() > 0 else "N/A",
            "date_max": dates.sort().tail(1).to_list()[0] if dates.len() > 0 else "N/A",
            "void_votes": self.void_votes.height,
        }

    def get_tisk(self, schuze: int, bod: int) -> TiskInfo | None:
        """Get tisk info for a vote given its session and agenda item numbers."""
        return self.tisk_lookup.get((schuze, bod))

    def get_all_topic_labels(self) -> list[str]:
        """Collect all unique topic labels across all tisky, sorted."""
        labels: set[str] = set()
        for tisk in self.tisk_lookup.values():
            labels.update(tisk.topics)
        return sorted(labels)
