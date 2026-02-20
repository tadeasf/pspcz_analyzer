"""Endpoint-level security tests."""


class TestSecurityHeaders:
    def test_csp_header_present(self, client) -> None:
        resp = client.get("/")
        assert "Content-Security-Policy" in resp.headers
        csp = resp.headers["Content-Security-Policy"]
        assert "default-src 'self'" in csp
        assert "frame-ancestors 'none'" in csp

    def test_hsts_header_present(self, client) -> None:
        resp = client.get("/")
        assert "Strict-Transport-Security" in resp.headers
        assert "max-age=31536000" in resp.headers["Strict-Transport-Security"]

    def test_permissions_policy_present(self, client) -> None:
        resp = client.get("/")
        assert "Permissions-Policy" in resp.headers
        pp = resp.headers["Permissions-Policy"]
        assert "camera=()" in pp
        assert "microphone=()" in pp

    def test_x_frame_options(self, client) -> None:
        resp = client.get("/")
        assert resp.headers.get("X-Frame-Options") == "DENY"

    def test_x_content_type_options(self, client) -> None:
        resp = client.get("/")
        assert resp.headers.get("X-Content-Type-Options") == "nosniff"


class TestSetLangSecurity:
    def test_external_redirect_rejected(self, client) -> None:
        resp = client.get(
            "/set-lang/en",
            headers={"referer": "https://evil.com/phishing"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        location = resp.headers["location"]
        assert "evil.com" not in location
        assert location.startswith("/")

    def test_cookie_httponly(self, client) -> None:
        resp = client.get("/set-lang/en", follow_redirects=False)
        cookie_header = resp.headers.get("set-cookie", "")
        assert "httponly" in cookie_header.lower()

    def test_normal_referer_works(self, client) -> None:
        resp = client.get(
            "/set-lang/cs",
            headers={"referer": "http://testserver/votes?period=1"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        location = resp.headers["location"]
        assert location == "/votes?period=1"


class TestFeedbackCSRF:
    def test_feedback_rejects_no_origin(self, client) -> None:
        resp = client.post(
            "/api/feedback",
            data={"title": "Test title here", "body": "Test body content here for validation"},
        )
        assert resp.status_code == 200
        # CSRF error shown — red error box, no success/GitHub link
        assert "#ffebee" in resp.text  # error background color
        assert "github.com" not in resp.text

    def test_feedback_rejects_wrong_origin(self, client) -> None:
        resp = client.post(
            "/api/feedback",
            data={"title": "Test title here", "body": "Test body content here for validation"},
            headers={"origin": "https://evil.com"},
        )
        assert resp.status_code == 200
        assert "#ffebee" in resp.text
        assert "github.com" not in resp.text
