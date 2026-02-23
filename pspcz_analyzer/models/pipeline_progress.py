"""Progress tracking data model for the tisk AI pipeline."""

import time
from dataclasses import dataclass, field
from enum import StrEnum


class PipelineStage(StrEnum):
    """Stages of the per-period tisk pipeline."""

    IDLE = "idle"
    SCRAPE_HISTORIES = "scrape_histories"
    DOWNLOAD_PDFS = "download_pdfs"
    CLASSIFY = "classify"
    CONSOLIDATE_TOPICS = "consolidate_topics"
    SCRAPE_LAW_CHANGES = "scrape_law_changes"
    DOWNLOAD_VERSIONS = "download_versions"
    ANALYZE_DIFFS = "analyze_diffs"
    COMPLETED = "completed"
    FAILED = "failed"


class PeriodStatus(StrEnum):
    """Status of a single period within the pipeline run."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    SKIPPED = "skipped"
    FAILED = "failed"


@dataclass
class StageProgress:
    """Progress for the currently-running pipeline stage."""

    stage: PipelineStage = PipelineStage.IDLE
    items_done: int = 0
    items_total: int = 0
    started_at: float = 0.0

    @property
    def elapsed(self) -> float:
        """Seconds elapsed since stage started."""
        if self.started_at <= 0:
            return 0.0
        return time.monotonic() - self.started_at

    @property
    def rate(self) -> float:
        """Items processed per second (0 if no items done yet)."""
        elapsed = self.elapsed
        if elapsed <= 0 or self.items_done <= 0:
            return 0.0
        return self.items_done / elapsed

    @property
    def percent(self) -> float | None:
        """Percentage of items completed, or None if total is unknown."""
        if self.items_total <= 0:
            return None
        return min(100.0, (self.items_done / self.items_total) * 100)

    @property
    def eta_seconds(self) -> float | None:
        """Estimated seconds remaining for this stage, or None if unknown."""
        rate = self.rate
        if rate <= 0 or self.items_total <= 0:
            return None
        remaining = self.items_total - self.items_done
        if remaining <= 0:
            return 0.0
        return remaining / rate

    def to_dict(self) -> dict:
        """Serialize to JSON-friendly dict."""
        eta = self.eta_seconds
        pct = self.percent
        return {
            "stage": self.stage.value,
            "items_done": self.items_done,
            "items_total": self.items_total,
            "elapsed_seconds": round(self.elapsed, 1),
            "rate_per_second": round(self.rate, 3),
            "eta_seconds": round(eta, 1) if eta is not None else None,
            "percent": round(pct, 1) if pct is not None else None,
        }


@dataclass
class PeriodProgress:
    """Progress for a single electoral period."""

    period: int
    status: PeriodStatus = PeriodStatus.PENDING
    tisky_count: int = 0
    current_stage: StageProgress | None = None

    def to_dict(self) -> dict:
        """Serialize to JSON-friendly dict."""
        return {
            "period": self.period,
            "status": self.status.value,
            "tisky_count": self.tisky_count,
            "current_stage": self.current_stage.to_dict() if self.current_stage else None,
        }


@dataclass
class PipelineProgress:
    """Overall progress for the multi-period tisk pipeline."""

    running: bool = False
    started_at: float = 0.0
    periods: dict[int, PeriodProgress] = field(default_factory=dict)

    @property
    def current_period(self) -> int | None:
        """Period number currently being processed, or None."""
        for pp in self.periods.values():
            if pp.status == PeriodStatus.IN_PROGRESS:
                return pp.period
        return None

    @property
    def periods_completed(self) -> int:
        """Number of periods that have finished processing."""
        return sum(1 for pp in self.periods.values() if pp.status == PeriodStatus.COMPLETED)

    @property
    def periods_total(self) -> int:
        """Total number of periods in this pipeline run."""
        return len(self.periods)

    @property
    def elapsed(self) -> float:
        """Seconds since pipeline started."""
        if self.started_at <= 0:
            return 0.0
        return time.monotonic() - self.started_at

    @property
    def eta_seconds(self) -> float | None:
        """Estimated seconds remaining for the entire pipeline.

        Uses completed-period rate + current stage ETA for a rough estimate.
        """
        if not self.running or self.periods_total <= 0:
            return None

        completed = self.periods_completed
        remaining_periods = self.periods_total - completed

        # If we have completed periods, estimate from average time per period
        if completed > 0 and remaining_periods > 0:
            elapsed = self.elapsed
            avg_per_period = elapsed / completed
            # Subtract current stage's already-elapsed time from estimate
            current_stage_eta = 0.0
            for pp in self.periods.values():
                if pp.status == PeriodStatus.IN_PROGRESS and pp.current_stage:
                    stage_eta = pp.current_stage.eta_seconds
                    if stage_eta is not None:
                        current_stage_eta = stage_eta
                    break
            return (remaining_periods - 1) * avg_per_period + current_stage_eta

        # No completed periods yet — use current stage ETA only
        for pp in self.periods.values():
            if pp.status == PeriodStatus.IN_PROGRESS and pp.current_stage:
                return pp.current_stage.eta_seconds

        return None

    def to_dict(self) -> dict:
        """Serialize to JSON-friendly dict."""
        eta = self.eta_seconds
        return {
            "running": self.running,
            "elapsed_seconds": round(self.elapsed, 1),
            "current_period": self.current_period,
            "periods_completed": self.periods_completed,
            "periods_total": self.periods_total,
            "eta_seconds": round(eta, 1) if eta is not None else None,
            "periods": {str(period): pp.to_dict() for period, pp in self.periods.items()},
        }
