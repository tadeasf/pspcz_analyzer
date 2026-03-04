"""Amendment pipeline progress tracking types.

Contains the stage/status enums and the AmendmentProgress dataclass
used across all pipeline modules.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from enum import StrEnum


class AmendmentStage(StrEnum):
    """Pipeline stage identifiers."""

    SCRAPE_HISTORIES = "scrape_histories"
    IDENTIFY = "identify"
    PDF_DOWNLOAD_PARSE = "pdf_download_parse"
    STENO_DOWNLOAD_PARSE = "steno_download_parse"
    MERGE = "merge"
    RESOLVE_IDS = "resolve_ids"
    RESOLVE_SUBMITTERS = "resolve_submitters"
    LLM_SUMMARIZE = "llm_summarize"
    CACHE = "cache"
    COMPLETED = "completed"
    FAILED = "failed"


class AmendmentStatus(StrEnum):
    """Pipeline status for a period."""

    IDLE = "idle"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class AmendmentProgress:
    """Progress tracking for the amendment pipeline."""

    status: AmendmentStatus = AmendmentStatus.IDLE
    stage: AmendmentStage = AmendmentStage.IDENTIFY
    total_items: int = 0
    done_items: int = 0
    bills_found: int = 0
    bills_parsed: int = 0
    started_at: float = 0.0
    summaries_completed: int = 0
    summaries_failed: int = 0
    amendment_summaries_completed: int = 0

    @property
    def elapsed(self) -> float:
        """Seconds elapsed since pipeline started."""
        if self.started_at <= 0:
            return 0.0
        return time.monotonic() - self.started_at

    @property
    def rate(self) -> float:
        """Items processed per second."""
        elapsed = self.elapsed
        if elapsed <= 0 or self.done_items <= 0:
            return 0.0
        return self.done_items / elapsed

    @property
    def percent(self) -> float | None:
        """Completion percentage, or None if total unknown."""
        if self.total_items <= 0:
            return None
        return (self.done_items / self.total_items) * 100

    @property
    def eta_seconds(self) -> float | None:
        """Estimated seconds remaining, or None if unknown."""
        r = self.rate
        if r <= 0 or self.total_items <= 0:
            return None
        remaining = self.total_items - self.done_items
        if remaining <= 0:
            return 0.0
        return remaining / r
