"""Pipeline run history — JSON-backed record of recent pipeline executions."""

import json
import time
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path

from loguru import logger

from pspcz_analyzer.config import DEFAULT_CACHE_DIR

_HISTORY_FILENAME = "pipeline_history.json"
_MAX_ENTRIES_PER_PERIOD = 20


@dataclass
class PipelineRun:
    """Record of a single pipeline execution."""

    pipeline_type: str
    period: int
    started_at: float
    finished_at: float
    duration_s: float
    status: str  # "success" | "error" | "cancelled"
    error: str = ""


class PipelineHistory:
    """Tracks last N pipeline runs per period, persisted to JSON."""

    def __init__(self, cache_dir: Path = DEFAULT_CACHE_DIR) -> None:
        self._path = cache_dir / _HISTORY_FILENAME
        self._runs: dict[str, list[dict]] = defaultdict(list)
        self._load()

    def _load(self) -> None:
        """Load history from JSON file."""
        if not self._path.exists():
            return
        try:
            data = json.loads(self._path.read_text("utf-8"))
            self._runs = defaultdict(list, data)
        except Exception:
            logger.opt(exception=True).warning("[pipeline-history] Failed to load {}", self._path)

    def _save(self) -> None:
        """Persist history to JSON file."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(json.dumps(dict(self._runs), indent=2) + "\n", encoding="utf-8")

    def record(self, run: PipelineRun) -> None:
        """Record a completed pipeline run."""
        key = f"{run.pipeline_type}:{run.period}"
        entries = self._runs[key]
        entries.insert(0, asdict(run))
        # Trim to max entries
        self._runs[key] = entries[:_MAX_ENTRIES_PER_PERIOD]
        self._save()

    def get_runs(self, pipeline_type: str | None = None, period: int | None = None) -> list[dict]:
        """Get pipeline runs, optionally filtered by type and/or period."""
        result: list[dict] = []
        for _key, entries in self._runs.items():
            for entry in entries:
                if pipeline_type and entry.get("pipeline_type") != pipeline_type:
                    continue
                if period is not None and entry.get("period") != period:
                    continue
                result.append(entry)
        result.sort(key=lambda x: x.get("started_at", 0), reverse=True)
        return result[:_MAX_ENTRIES_PER_PERIOD]

    @staticmethod
    def create_run(pipeline_type: str, period: int) -> dict:
        """Create a run tracking dict (call at pipeline start)."""
        return {
            "pipeline_type": pipeline_type,
            "period": period,
            "started_at": time.time(),
        }

    def finish_run(self, run_data: dict, status: str, error: str = "") -> None:
        """Finalize a run tracking dict and record it."""
        now = time.time()
        run = PipelineRun(
            pipeline_type=run_data["pipeline_type"],
            period=run_data["period"],
            started_at=run_data["started_at"],
            finished_at=now,
            duration_s=round(now - run_data["started_at"], 1),
            status=status,
            error=error,
        )
        self.record(run)
