"""Tests for HTMX partial endpoints and health check."""


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

    def test_active_api(self, client):
        resp = client.get("/api/active?period=1")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]

    def test_votes_api(self, client):
        resp = client.get("/api/votes?period=1")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]

    def test_invalid_period_returns_404(self, client):
        resp = client.get("/api/loyalty?period=999")
        assert resp.status_code == 404
