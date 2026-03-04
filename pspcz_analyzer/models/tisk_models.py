"""Data models for parliamentary prints (tisky) and period data."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import TYPE_CHECKING

import polars as pl

from pspcz_analyzer.config import PERIOD_LABELS, PERIOD_YEARS

if TYPE_CHECKING:
    from pspcz_analyzer.models.amendment_models import BillAmendmentData
    from pspcz_analyzer.services.tisk.io.history_scraper import TiskHistory


@dataclass
class TiskInfo:
    """Metadata for a parliamentary print (tisk) linked to a vote.

    Attributes:
        id_tisk: Database ID of the tisk.
        ct: Tisk number (cislo tisku).
        nazev: Title of the tisk.
        period: Electoral period number.
        topics: LLM-classified topic labels (1-3 Czech labels).
        topics_en: LLM-classified topic labels (1-3 English labels).
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
    topics_en: list[str] = field(default_factory=list)
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
    # Amendment data: (schuze, bod) -> BillAmendmentData
    amendment_data: dict[tuple[int, int], BillAmendmentData] = field(default_factory=dict)
    # Reverse index: id_hlasovani -> (schuze, bod, letter, is_final_vote)
    _amendment_vote_index: dict[int, tuple[int, int, str, bool]] = field(
        default_factory=dict, repr=False
    )

    def get_amendments(self, schuze: int, bod: int) -> BillAmendmentData | None:
        """Get amendment data for a vote given its session and agenda item."""
        return self.amendment_data.get((schuze, bod))

    def build_amendment_vote_index(self) -> None:
        """Build reverse index mapping id_hlasovani to amendment context.

        Should be called whenever amendment_data is loaded or reloaded.
        """
        self._amendment_vote_index.clear()
        for (schuze, bod), bill in self.amendment_data.items():
            for a in bill.amendments:
                if a.id_hlasovani is not None:
                    self._amendment_vote_index[a.id_hlasovani] = (
                        schuze,
                        bod,
                        a.letter,
                        False,
                    )
            if bill.final_vote and bill.final_vote.id_hlasovani is not None:
                self._amendment_vote_index[bill.final_vote.id_hlasovani] = (
                    schuze,
                    bod,
                    "",
                    True,
                )

    def get_amendment_for_vote(self, vote_id: int) -> tuple[int, int, str, bool] | None:
        """Look up amendment context for a vote ID.

        Args:
            vote_id: The id_hlasovani to look up.

        Returns:
            Tuple of (schuze, bod, letter, is_final_vote) or None.
        """
        return self._amendment_vote_index.get(vote_id)

    @property
    def stats(self) -> dict:
        date_col = self.votes.get_column("datum")
        dates = date_col.drop_nulls().str.strip_chars()
        parsed = dates.str.to_date("%d.%m.%Y", strict=False).drop_nulls()
        if parsed.len() > 0:
            min_val: date = parsed.min()  # type: ignore[assignment]
            max_val: date = parsed.max()  # type: ignore[assignment]
            date_min = min_val.strftime("%d.%m.%Y")
            date_max = max_val.strftime("%d.%m.%Y")
        else:
            date_min = "N/A"
            date_max = "N/A"
        return {
            "period": self.period,
            "label": PERIOD_LABELS.get(self.period, PERIOD_YEARS.get(self.period, "?")),
            "total_votes": self.votes.height,
            "total_mp_records": self.mp_votes.height,
            "total_mps": self.mp_info.height,
            "date_min": date_min,
            "date_max": date_max,
            "void_votes": self.void_votes.height,
        }

    def get_tisk(self, schuze: int, bod: int) -> TiskInfo | None:
        """Get tisk info for a vote given its session and agenda item numbers."""
        return self.tisk_lookup.get((schuze, bod))

    def get_all_topic_labels(self, lang: str = "cs") -> list[str]:
        """Collect all unique topic labels across all tisky, sorted.

        Args:
            lang: Language code ('cs' or 'en'). Uses English labels when
                  available and lang == 'en', otherwise Czech.
        """
        labels: set[str] = set()
        for tisk in self.tisk_lookup.values():
            if lang == "en" and tisk.topics_en:
                labels.update(tisk.topics_en)
            else:
                labels.update(tisk.topics)
        return sorted(labels)
