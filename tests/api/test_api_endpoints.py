"""Tests for HTMX partial endpoints and health check."""

import time

from pspcz_analyzer.middleware import run_with_timeout as real_run_with_timeout
from pspcz_analyzer.routes import voting


class TestHealthEndpoint:
    def test_health_returns_ok(self, client):
        resp = client.get("/api/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert "periods_loaded" in data


class TestHTMXPartials:
    def test_loyalty_api(self, client):
        resp = client.get("/api/loyalty?period=1")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]

    def test_attendance_api(self, client):
        resp = client.get("/api/attendance?period=1")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]

    def test_similarity_api(self, client):
        resp = client.get("/api/similarity?period=1")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]

    def test_votes_api(self, client):
        resp = client.get("/api/votes?period=1")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]

    def test_invalid_period_returns_404(self, client):
        resp = client.get("/api/loyalty?period=999")
        assert resp.status_code == 404


class TestVotesTimeout:
    def test_slow_votes_search_returns_503(self, client, monkeypatch):
        """A votes search exceeding the compute timeout degrades to HTTP 503.

        Regression (audit fix B8): votes_api was the only analysis endpoint
        not wrapped in run_with_timeout, so a slow search blocked the event
        loop instead of returning 503.
        """

        async def _short_timeout(fn, *args, **kwargs):
            # Ignore the endpoint's real timeout; force a tiny one.
            return await real_run_with_timeout(fn, *args, timeout=0.05, label=kwargs["label"])

        monkeypatch.setattr(voting, "run_with_timeout", _short_timeout)

        def _slow_list_votes(*_args, **_kwargs):
            time.sleep(0.3)
            return {}

        monkeypatch.setattr(voting, "list_votes", _slow_list_votes)

        resp = client.get("/api/votes?period=1&search=slowpoke")
        assert resp.status_code == 503
