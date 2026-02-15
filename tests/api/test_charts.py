"""Tests for chart PNG endpoints."""


class TestChartEndpoints:
    def test_loyalty_chart(self, client):
        resp = client.get("/charts/loyalty.png?period=1")
        assert resp.status_code == 200
        assert resp.headers["content-type"] == "image/png"
        # PNG magic bytes
        assert resp.content[:4] == b"\x89PNG"

    def test_attendance_chart(self, client):
        resp = client.get("/charts/attendance.png?period=1")
        assert resp.status_code == 200
        assert resp.headers["content-type"] == "image/png"

    def test_similarity_chart(self, client):
        resp = client.get("/charts/similarity.png?period=1")
        assert resp.status_code == 200
        assert resp.headers["content-type"] == "image/png"
