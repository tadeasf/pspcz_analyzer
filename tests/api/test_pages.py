"""Tests for full HTML page routes."""


class TestPageRoutes:
    def test_index(self, client):
        resp = client.get("/")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]

    def test_loyalty_page(self, client):
        resp = client.get("/loyalty")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]

    def test_attendance_page(self, client):
        resp = client.get("/attendance")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]

    def test_similarity_page(self, client):
        resp = client.get("/similarity")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]

    def test_active_page(self, client):
        resp = client.get("/active")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]

    def test_votes_page(self, client):
        resp = client.get("/votes")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]
