"""Tests for the cooperative cancellation primitives."""

import threading
from collections.abc import Callable

import pytest

from pspcz_analyzer.services.cancellation import CancellationFlag, PipelineCancelled


class TestCancellationFlag:
    def test_check_passes_when_not_cancelled(self):
        CancellationFlag(9).check()  # must not raise

    def test_cancel_sets_is_cancelled(self):
        flag = CancellationFlag(9)
        assert not flag.is_cancelled()
        flag.cancel()
        assert flag.is_cancelled()

    def test_check_raises_after_cancel(self):
        flag = CancellationFlag(9)
        flag.cancel()
        with pytest.raises(PipelineCancelled, match="period 9") as exc_info:
            flag.check()
        assert exc_info.value.period == 9

    def test_cancel_is_idempotent(self):
        flag = CancellationFlag(9)
        flag.cancel()
        flag.cancel()
        assert flag.is_cancelled()

    def test_cancel_from_another_thread_is_visible(self):
        flag = CancellationFlag(9)
        thread = threading.Thread(target=flag.cancel)
        thread.start()
        thread.join(timeout=5)
        assert flag.is_cancelled()

    def test_check_is_usable_as_plain_callable(self):
        flag = CancellationFlag(9)
        fn: Callable[[], None] = flag.check
        fn()  # uncancelled — passes
        flag.cancel()
        with pytest.raises(PipelineCancelled):
            fn()
