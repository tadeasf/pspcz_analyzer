"""Unit tests for admin auth: proxy-aware client IP, whitelist, sessions, CSRF."""

import bcrypt
import pytest
from starlette.requests import Request

from pspcz_analyzer.admin import auth
from pspcz_analyzer.middleware import is_same_origin
from pspcz_analyzer.proxy import extract_client_ip, is_ip_allowed, parse_ip_networks


def _make_request(
    client_host: str | None = "1.2.3.4",
    headers: dict[str, str] | None = None,
    method: str = "GET",
    host: str = "example.com",
) -> Request:
    """Build a minimal Starlette Request with controlled client and headers."""
    raw_headers = [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()]
    scope = {
        "type": "http",
        "method": method,
        "scheme": "http",
        "path": "/admin/",
        "headers": raw_headers,
        "server": (host, 80),
        "client": (client_host, 12345) if client_host else None,
    }
    return Request(scope)


# ── parse_ip_networks / is_ip_allowed ────────────────────────────


class TestParseIpNetworks:
    def test_parses_cidrs_and_addresses(self):
        nets = parse_ip_networks("127.0.0.1, ::1, 10.0.0.0/8")
        assert len(nets) == 3

    def test_skips_invalid_entries_with_warning(self):
        nets = parse_ip_networks("127.0.0.1, not-an-ip, 999.999.1.1")
        assert len(nets) == 1

    def test_empty_and_whitespace(self):
        assert parse_ip_networks("") == []
        assert parse_ip_networks(" , ,") == []


class TestIsIpAllowed:
    def test_in_network(self):
        nets = parse_ip_networks("127.0.0.1, 10.0.0.0/8")
        assert is_ip_allowed("127.0.0.1", nets)
        assert is_ip_allowed("10.1.2.3", nets)

    def test_out_of_network(self):
        nets = parse_ip_networks("127.0.0.1")
        assert not is_ip_allowed("203.0.113.7", nets)

    def test_ipv4_mapped_ipv6_normalized(self):
        nets = parse_ip_networks("192.168.1.0/24")
        assert is_ip_allowed("::ffff:192.168.1.10", nets)

    def test_unparseable_input_rejected(self):
        nets = parse_ip_networks("0.0.0.0/0")
        assert not is_ip_allowed("garbage", nets)
        assert not is_ip_allowed("", nets)


# ── extract_client_ip (the XFF spoofing regression) ──────────────


class TestExtractClientIp:
    def test_no_xff_returns_tcp_peer(self):
        request = _make_request(client_host="203.0.113.7")
        assert extract_client_ip(request, parse_ip_networks("127.0.0.1")) == "203.0.113.7"

    def test_xff_ignored_when_peer_untrusted(self):
        """The core vulnerability: spoofing XFF from the open internet must not help."""
        request = _make_request(client_host="203.0.113.7", headers={"x-forwarded-for": "127.0.0.1"})
        assert extract_client_ip(request, parse_ip_networks("127.0.0.1")) == "203.0.113.7"

    def test_rightmost_untrusted_entry_from_trusted_proxy(self):
        request = _make_request(
            client_host="127.0.0.1",
            headers={"x-forwarded-for": "198.51.100.9, 10.0.0.1"},
        )
        trusted = parse_ip_networks("127.0.0.1, 10.0.0.0/8")
        # 10.0.0.1 is trusted (the proxy); 198.51.100.9 is the real client.
        assert extract_client_ip(request, trusted) == "198.51.100.9"

    def test_all_trusted_chain_returns_leftmost(self):
        request = _make_request(
            client_host="127.0.0.1", headers={"x-forwarded-for": "10.0.0.2, 10.0.0.1"}
        )
        trusted = parse_ip_networks("127.0.0.1, 10.0.0.0/8")
        assert extract_client_ip(request, trusted) == "10.0.0.2"

    def test_trusted_peer_without_xff_returns_peer(self):
        request = _make_request(client_host="127.0.0.1")
        assert extract_client_ip(request, parse_ip_networks("127.0.0.1")) == "127.0.0.1"

    def test_unparseable_xff_entry_treated_as_client(self):
        # A garbage entry from a trusted proxy is returned as-is; the whitelist
        # check then rejects it (fail closed) rather than skipping to a real IP.
        request = _make_request(client_host="127.0.0.1", headers={"x-forwarded-for": "bogus"})
        assert extract_client_ip(request, parse_ip_networks("127.0.0.1")) == "bogus"

    def test_missing_client_falls_back(self):
        request = _make_request(client_host=None)
        assert extract_client_ip(request, parse_ip_networks("127.0.0.1")) == "0.0.0.0"


# ── Session signing / verification ───────────────────────────────


class TestSessions:
    def test_sign_verify_roundtrip(self):
        token = auth.create_session_cookie("admin")
        assert auth._verify_session(token) == "admin"

    def test_tampered_signature_rejected(self):
        token = auth.create_session_cookie("admin")
        username, expires, _sig = token.split(":")
        assert auth._verify_session(f"{username}:{expires}:" + "0" * 64) is None

    def test_expired_token_rejected(self, monkeypatch):
        now = [1_000_000.0]
        monkeypatch.setattr(auth.time, "time", lambda: now[0])
        token = auth.create_session_cookie("admin")  # expires = now + TTL
        now[0] += auth._SESSION_TTL + 1
        assert auth._verify_session(token) is None

    def test_malformed_tokens_rejected(self):
        assert auth._verify_session("onlyonepart") is None
        assert auth._verify_session("a:b") is None
        assert auth._verify_session("a:b:c:d") is None
        assert auth._verify_session("admin:notanumber:abcd") is None


class TestVerifyPassword:
    def test_empty_hash_rejects_everything(self, monkeypatch):
        monkeypatch.setattr(auth, "ADMIN_PASSWORD_HASH", "")
        assert not auth.verify_password("anything")
        assert not auth.verify_password("")

    def test_correct_password_accepted(self, monkeypatch):
        hashed = bcrypt.hashpw(b"s3cret", bcrypt.gensalt()).decode()
        monkeypatch.setattr(auth, "ADMIN_PASSWORD_HASH", hashed)
        assert auth.verify_password("s3cret")

    def test_wrong_password_rejected(self, monkeypatch):
        hashed = bcrypt.hashpw(b"s3cret", bcrypt.gensalt()).decode()
        monkeypatch.setattr(auth, "ADMIN_PASSWORD_HASH", hashed)
        assert not auth.verify_password("wrong")


# ── is_same_origin (CSRF helper) ─────────────────────────────────


class TestIsSameOrigin:
    def test_matching_origin(self):
        request = _make_request(headers={"origin": "http://example.com/admin"})
        assert is_same_origin(request)

    def test_matching_referer(self):
        request = _make_request(headers={"referer": "http://example.com/votes"})
        assert is_same_origin(request)

    def test_foreign_origin_rejected(self):
        request = _make_request(headers={"origin": "http://evil.com"})
        assert not is_same_origin(request)

    def test_missing_headers_fail_closed(self):
        assert not is_same_origin(_make_request())

    def test_origin_checked_before_referer(self):
        request = _make_request(
            headers={"origin": "http://evil.com", "referer": "http://example.com/"}
        )
        assert not is_same_origin(request)


@pytest.mark.parametrize("header_value", [":://bad", "http://"])
def test_is_same_origin_unparseable_values_fail_closed(header_value):
    request = _make_request(headers={"origin": header_value})
    assert not is_same_origin(request)
