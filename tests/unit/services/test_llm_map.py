"""llm_map: ordered results, errors raised at the caller."""

import pytest

from pspcz_analyzer.services.llm import llm_map


def test_results_keep_the_input_order() -> None:
    assert list(llm_map(lambda x: x * 2, range(20))) == [x * 2 for x in range(20)]


def test_worker_error_is_raised_to_the_caller() -> None:
    def fail_on_3(x: int) -> int:
        if x == 3:
            raise RuntimeError("stop")
        return x

    with pytest.raises(RuntimeError, match="stop"):
        list(llm_map(fail_on_3, range(10)))
