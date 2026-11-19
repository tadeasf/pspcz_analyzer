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


class TestVoteDetailRedesign:
    """Vote detail per the mockup (#114)."""

    def test_vote_detail_sections(self, client, mock_period_data):
        vote_id = mock_period_data.votes.get_column("id_hlasovani")[0]
        html = client.get(f"/votes/{vote_id}?period=1").text
        assert "Jak hlasovaly poslanecké kluby" in html
        assert "Jak hlasovali jednotliví poslanci" in html
        assert "zatím neznáme" in html


class TestUrlState:
    """Search and filters live in the URL (#96)."""

    def test_list_request_pushes_page_url(self, client):
        resp = client.get(
            "/api/votes?period=1&search=dalnice&outcome=",
            headers={"HX-Request": "true", "HX-Current-URL": "http://testserver/votes?period=1"},
        )
        assert resp.headers["HX-Push-Url"] == "/votes?period=1&search=dalnice"

    def test_no_push_when_url_unchanged(self, client):
        resp = client.get(
            "/api/votes?period=1",
            headers={"HX-Request": "true", "HX-Current-URL": "http://testserver/votes?period=1"},
        )
        assert "HX-Push-Url" not in resp.headers

    def test_page_prefills_from_url(self, client):
        html = client.get("/votes?period=1&search=dalnice&kind=final").text
        assert 'value="dalnice"' in html
        assert 'value="final" selected' in html
        assert 'hx-get="/api/votes?period=1&amp;search=dalnice&amp;kind=final"' in html
