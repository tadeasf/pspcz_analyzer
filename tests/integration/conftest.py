"""Integration test fixtures — shared across all integration tests."""

import pytest

from pspcz_analyzer.config import DEFAULT_CACHE_DIR


@pytest.fixture(scope="session")
def integration_cache_dir():
    """Shared cache directory for integration tests.

    Uses the real cache dir so downloads are reused across runs.
    """
    cache = DEFAULT_CACHE_DIR
    cache.mkdir(parents=True, exist_ok=True)
    return cache


@pytest.fixture(scope="session")
def voting_dir_period1(integration_cache_dir):
    """Download and return the extracted voting data dir for period 1 (1993)."""
    from pspcz_analyzer.data.downloader import download_voting_data

    return download_voting_data(1, cache_dir=integration_cache_dir)


@pytest.fixture(scope="session")
def poslanci_dir(integration_cache_dir):
    """Download and return the extracted MP data dir."""
    from pspcz_analyzer.data.downloader import download_poslanci_data

    return download_poslanci_data(cache_dir=integration_cache_dir)


@pytest.fixture(scope="session")
def schuze_dir(integration_cache_dir):
    """Download and return the extracted session data dir."""
    from pspcz_analyzer.data.downloader import download_schuze_data

    return download_schuze_data(cache_dir=integration_cache_dir)
