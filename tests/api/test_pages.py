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

    def test_votes_page(self, client):
        resp = client.get("/votes")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]


class TestSiteFrame:
    """Header, menu, AI notice and footer from the redesign (#105)."""

    def test_frame_on_every_page(self, client):
        for path in ("/", "/laws", "/votes", "/loyalty", "/attendance", "/jak-se-v-tom-vyznat"):
            html = client.get(path).text
            assert "Zákony pod lupou" in html, path
            assert "PSP.cz" not in html and "present" not in html, path
            assert 'href="/nahlasit-chybu' in html, path
            assert "Odkud data jsou" in html, path

    def test_similarity_highlights_loyalty_in_menu(self, client):
        html = client.get("/similarity").text
        assert 'href="/loyalty?period=10" class="on" aria-current="page"' in html

    def test_guide_page(self, client):
        resp = client.get("/jak-se-v-tom-vyznat")
        assert resp.status_code == 200
        assert "Jak vzniká zákon" in resp.text

    def test_report_page_keeps_source_page(self, client):
        resp = client.get(
            "/nahlasit-chybu", headers={"referer": "http://testserver/laws/246?period=10"}
        )
        assert resp.status_code == 200
        assert 'name="page" value="/laws/246?period=10"' in resp.text
