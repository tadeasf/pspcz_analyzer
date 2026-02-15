"""Shared test fixtures."""

from contextlib import asynccontextmanager
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from pspcz_analyzer.services.data_service import DataService
from tests.fixtures.sample_data import make_period_data


@pytest.fixture()
def mock_period_data():
    """Synthetic PeriodData with 5 MPs, 5 votes, known patterns."""
    return make_period_data(period=1)


@pytest.fixture()
def mock_data_service(mock_period_data):
    """A DataService-like mock that returns our synthetic data."""
    svc = MagicMock(spec=DataService)
    svc.available_periods = [1]
    svc.loaded_periods = {1}
    svc.get_period.return_value = mock_period_data
    return svc


@pytest.fixture()
def client(mock_data_service):
    """FastAPI TestClient with mocked DataService (no real downloads)."""

    @asynccontextmanager
    async def _test_lifespan(app):
        app.state.data = mock_data_service
        yield

    from pspcz_analyzer.main import app

    app.router.lifespan_context = _test_lifespan
    with TestClient(app) as c:
        yield c


@pytest.fixture()
def test_cache_dir(tmp_path):
    """Isolated cache directory for tests."""
    cache = tmp_path / "cache"
    cache.mkdir()
    return cache
