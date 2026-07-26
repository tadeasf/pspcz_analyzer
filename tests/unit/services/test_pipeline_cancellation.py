"""Cancellation behavior for the amendment + tisk pipeline services.

Covers the cooperative flag model: cancel requests flip a per-period
CancellationFlag, worker threads stop at their next checkpoint, and the
services record CANCELLED status. Regression for the amendment pipeline
cancel being a no-op (task.cancel() cannot interrupt asyncio.to_thread).
"""

import asyncio
import threading
from pathlib import Path

import pytest

from pspcz_analyzer.models.amendment_models import BillAmendmentData
from pspcz_analyzer.models.pipeline_progress import (
    AmendmentMode,
    PeriodProgress,
    PeriodStatus,
)
from pspcz_analyzer.services.amendments import merger, summarizer
from pspcz_analyzer.services.amendments import pipeline as amendment_pipeline
from pspcz_analyzer.services.amendments.progress import (
    AmendmentProgress,
    AmendmentStatus,
)
from pspcz_analyzer.services.cancellation import CancellationFlag, PipelineCancelled
from pspcz_analyzer.services.tisk.pipeline import TiskPipelineService
from tests.fixtures.sample_data import make_period_data

_CANDIDATES = [(1, 1, 100, "t1"), (1, 2, 101, "t2"), (1, 3, 102, "t3")]


def _make_blocking_find_steno(
    calls: list[tuple[int, int]],
    entered: threading.Event,
    release: threading.Event,
):
    """Fake find_steno_for_bod: blocks on the first call until released."""

    def fake_find_steno(
        period: int, schuze: int, bod: int, nazev: str, cache_dir: Path
    ) -> tuple[None, None, None]:
        calls.append((schuze, bod))
        if len(calls) == 1:
            entered.set()
            if not release.wait(10):
                raise RuntimeError("test timed out waiting for cancellation")
        return None, None, None

    return fake_find_steno


class TestAmendmentPipelineCancellation:
    @pytest.mark.asyncio
    async def test_cancel_mid_run_stops_steno_downloads(self, tmp_path, monkeypatch):
        """Cancel while blocked on candidate 1 → CANCELLED, no further downloads."""
        calls: list[tuple[int, int]] = []
        entered = threading.Event()
        release = threading.Event()

        monkeypatch.setattr(amendment_pipeline, "_ensure_tisk_histories", lambda *a, **k: None)
        monkeypatch.setattr(
            amendment_pipeline, "_identify_third_reading_bods", lambda _pd: _CANDIDATES
        )
        monkeypatch.setattr(amendment_pipeline, "_pdf_download_and_parse", lambda *a, **k: ({}, {}))
        monkeypatch.setattr(
            amendment_pipeline,
            "find_steno_for_bod",
            _make_blocking_find_steno(calls, entered, release),
        )
        monkeypatch.setattr(amendment_pipeline, "save_amendments", lambda *a, **k: None)

        svc = amendment_pipeline.AmendmentPipelineService(cache_dir=tmp_path)
        svc.start_period(9, make_period_data(), mode=AmendmentMode.PARSE)

        # Wait until the worker is blocked inside the first steno lookup
        assert await asyncio.to_thread(entered.wait, 10)
        assert svc.cancel_period(9) is True
        release.set()

        await svc.wait_stopped(timeout=10)

        assert svc.progress[9].status == AmendmentStatus.CANCELLED
        assert len(calls) == 1, "steno download continued after cancellation"

    @pytest.mark.asyncio
    async def test_completed_run_prunes_task_and_flag(self, tmp_path, monkeypatch):
        """A normal run clears _tasks/_flags when its task finishes."""
        monkeypatch.setattr(amendment_pipeline, "_ensure_tisk_histories", lambda *a, **k: None)
        monkeypatch.setattr(amendment_pipeline, "_identify_third_reading_bods", lambda _pd: [])
        monkeypatch.setattr(amendment_pipeline, "save_amendments", lambda *a, **k: None)

        svc = amendment_pipeline.AmendmentPipelineService(cache_dir=tmp_path)
        svc.start_period(9, make_period_data(), mode=AmendmentMode.PARSE)

        await svc.wait_stopped(timeout=10)

        assert svc.progress[9].status == AmendmentStatus.COMPLETED
        assert svc._tasks == {}
        assert svc._flags == {}

    def test_cancel_period_not_running_returns_false(self, tmp_path):
        svc = amendment_pipeline.AmendmentPipelineService(cache_dir=tmp_path)
        assert svc.cancel_period(9) is False

    def test_run_pipeline_sync_raises_on_precancelled_flag(self, tmp_path, monkeypatch):
        """A pre-cancelled flag stops the run before any stage executes."""
        ensure_calls: list[int] = []
        monkeypatch.setattr(
            amendment_pipeline,
            "_ensure_tisk_histories",
            lambda *a, **k: ensure_calls.append(1),
        )

        flag = CancellationFlag(9)
        flag.cancel()
        prog = AmendmentProgress(status=AmendmentStatus.RUNNING)

        with pytest.raises(PipelineCancelled):
            amendment_pipeline._run_pipeline_sync(
                9,
                make_period_data(),
                tmp_path,
                prog,
                mode=AmendmentMode.PARSE,
                cancel_check=flag.check,
            )
        assert ensure_calls == []

    def test_pdf_download_loop_respects_cancel(self, tmp_path, monkeypatch):
        downloads: list[int] = []
        monkeypatch.setattr(merger, "_download_amendment_pdf", lambda *a: downloads.append(a[1]))

        flag = CancellationFlag(9)
        flag.cancel()
        bills = [BillAmendmentData(period=9, schuze=1, bod=1, ct=100)]

        with pytest.raises(PipelineCancelled):
            merger._pdf_download_and_parse(bills, 9, tmp_path, make_period_data(), flag.check)
        assert downloads == []

    def test_summarize_loop_respects_cancel(self, tmp_path, monkeypatch):
        class _StubLLM:
            provider = "stub"
            model = "stub-model"

            def is_available(self) -> bool:
                return True

        monkeypatch.setattr(summarizer, "create_llm_client", lambda: _StubLLM())

        flag = CancellationFlag(9)
        flag.cancel()
        bills = [BillAmendmentData(period=9, schuze=1, bod=1, ct=100)]
        prog = AmendmentProgress(status=AmendmentStatus.RUNNING)

        with pytest.raises(PipelineCancelled):
            summarizer._summarize_amendments(bills, tmp_path, 9, prog, cancel_check=flag.check)
        assert prog.done_items == 0


class TestTiskPipelineCancellation:
    def test_in_progress_sets_flag_and_check_raises(self):
        svc = TiskPipelineService()
        pp = PeriodProgress(period=9, tisky_count=3)
        pp.status = PeriodStatus.IN_PROGRESS
        svc.progress.periods[9] = pp

        assert svc.cancel_period(9) is True
        with pytest.raises(PipelineCancelled):
            svc._make_cancel_check(9)()
        with pytest.raises(PipelineCancelled):
            svc._check_period_cancelled(9)

    def test_pending_marks_skipped(self):
        svc = TiskPipelineService()
        pp = PeriodProgress(period=9)  # default PENDING
        svc.progress.periods[9] = pp

        assert svc.cancel_period(9) is True
        assert pp.status == PeriodStatus.SKIPPED
        assert 9 in svc._skip_periods

    def test_completed_period_returns_false(self):
        svc = TiskPipelineService()
        pp = PeriodProgress(period=9)
        pp.status = PeriodStatus.COMPLETED
        svc.progress.periods[9] = pp
        assert svc.cancel_period(9) is False

    def test_unknown_period_returns_false(self):
        assert TiskPipelineService().cancel_period(9) is False

    def test_cancel_all_flags_existing_and_new_periods(self):
        svc = TiskPipelineService()
        running_flag = svc._flag_for(9)

        svc.cancel_all()

        assert running_flag.is_cancelled()
        # Flags created after cancel_all start pre-cancelled, so runs
        # started mid-shutdown die at their first checkpoint.
        assert svc._flag_for(10).is_cancelled()

    @pytest.mark.asyncio
    async def test_wait_stopped_clears_state(self):
        svc = TiskPipelineService()
        svc._flag_for(9)
        svc.cancel_all()

        await svc.wait_stopped(timeout=0.1)

        assert svc._flags == {}
        assert svc._tasks == {}
        assert svc.progress.running is False
        # After the drain, a fresh run is no longer pre-cancelled
        assert not svc._flag_for(9).is_cancelled()
