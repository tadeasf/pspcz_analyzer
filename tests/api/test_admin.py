"""End-to-end tests for admin auth: IP whitelist, XFF spoofing, login, CSRF, masking."""

import json
from contextlib import asynccontextmanager
from unittest.mock import MagicMock

import bcrypt
import pytest
from fastapi.testclient import TestClient
from starlette.types import Receive, Scope, Send

from pspcz_analyzer.admin import auth, routes
from pspcz_analyzer.main_backend import app as backend_app
from pspcz_analyzer.proxy import parse_ip_networks

LOOPBACK = "127.0.0.1"
ATTACKER = "203.0.113.7"
ORIGIN = {"Origin": "http://testserver"}


@pytest.fixture()
def admin_app(mock_data_service, tmp_path):
    """Backend app with a mocked DataService rooted at a temp cache dir."""
    mock_data_service.cache_dir = tmp_path

    @asynccontextmanager
    async def _test_lifespan(app):
        app.state.data = mock_data_service
        app.state.pipeline_history = MagicMock()
        yield

    backend_app.router.lifespan_context = _test_lifespan
    return backend_app


@pytest.fixture()
def make_client(admin_app):
    """Factory: TestClient whose requests arrive from a chosen client IP.

    Each client enters its context so the (mocked) lifespan runs and
    ``app.state.data`` is populated before any request is handled.
    """
    entered: list[TestClient] = []

    def _factory(client_ip: str) -> TestClient:
        async def _ip_pinning_app(scope: Scope, receive: Receive, send: Send) -> None:
            if scope["type"] == "http":
                scope = dict(scope, client=(client_ip, 0))
            await admin_app(scope, receive, send)

        test_client = TestClient(_ip_pinning_app)
        test_client.__enter__()
        entered.append(test_client)
        return test_client

    yield _factory
    for test_client in entered:
        test_client.__exit__(None, None, None)


@pytest.fixture()
def client(make_client):
    """TestClient arriving from loopback (whitelisted by default)."""
    return make_client(LOOPBACK)


@pytest.fixture()
def session_cookie():
    """A valid signed admin session cookie value."""
    return {auth._SESSION_COOKIE: auth.create_session_cookie("admin")}


@pytest.fixture()
def admin_password(monkeypatch):
    """Configure a known admin password for login tests (side-effect fixture)."""
    hashed = bcrypt.hashpw(b"s3cret", bcrypt.gensalt()).decode()
    monkeypatch.setattr(auth, "ADMIN_PASSWORD_HASH", hashed)
    monkeypatch.setattr(routes, "ADMIN_USERNAME", "admin")
    monkeypatch.setattr(routes, "ADMIN_COOKIE_SECURE", True)
    return "s3cret"


# ── IP whitelist ─────────────────────────────────────────────────


def test_health_allowed_from_loopback(client):
    assert client.get("/admin/api/health").status_code == 200


def test_health_blocked_from_foreign_ip(make_client):
    assert make_client(ATTACKER).get("/admin/api/health").status_code == 403


def test_xff_spoof_from_untrusted_peer_rejected(make_client):
    """The acceptance test for the XFF bypass: spoofed loopback must not help."""
    attacker = make_client(ATTACKER)
    response = attacker.get("/admin/api/health", headers={"X-Forwarded-For": LOOPBACK})
    assert response.status_code == 403


def test_xff_honored_from_trusted_proxy(make_client, monkeypatch):
    monkeypatch.setattr(auth, "_trusted_proxies", parse_ip_networks("203.0.113.0/24"))
    proxy_client = make_client(ATTACKER)  # peer now trusted as a proxy
    # Real client behind the proxy is loopback → allowed.
    ok = proxy_client.get("/admin/api/health", headers={"X-Forwarded-For": LOOPBACK})
    assert ok.status_code == 200
    # Real client behind the proxy is NOT whitelisted → blocked.
    blocked = proxy_client.get("/admin/api/health", headers={"X-Forwarded-For": "198.51.100.9"})
    assert blocked.status_code == 403


# ── Login + session ──────────────────────────────────────────────


def test_login_wrong_password_returns_401(client, request: pytest.FixtureRequest):
    request.getfixturevalue("admin_password")  # applies the password side effect
    response = client.post(
        "/admin/login",
        data={"username": "admin", "password": "wrong"},
        headers=ORIGIN,
    )
    assert response.status_code == 401


def test_login_success_sets_secure_cookie(client, admin_password):
    response = client.post(
        "/admin/login",
        data={"username": "admin", "password": admin_password},
        headers=ORIGIN,
        follow_redirects=False,
    )
    assert response.status_code == 303
    set_cookie = response.headers["set-cookie"]
    assert "HttpOnly" in set_cookie
    assert "samesite=lax" in set_cookie.lower()
    assert "Secure" in set_cookie

    # Cookie grants access to the dashboard.
    dashboard = client.get("/admin/")
    assert dashboard.status_code == 200


def test_dashboard_requires_session(client):
    response = client.get("/admin/", follow_redirects=False)
    assert response.status_code == 303
    assert response.headers["location"] == "/admin/login"


def test_tampered_session_cookie_rejected(client):
    client.cookies.set(auth._SESSION_COOKIE, "admin:9999999999:" + "0" * 64)
    response = client.get("/admin/", follow_redirects=False)
    assert response.status_code == 303
    assert response.headers["location"] == "/admin/login"


# ── CSRF protection ──────────────────────────────────────────────


def test_login_blocked_without_origin(client, request: pytest.FixtureRequest):
    request.getfixturevalue("admin_password")
    response = client.post("/admin/login", data={"username": "admin", "password": "s3cret"})
    assert response.status_code == 403


def test_login_blocked_with_foreign_origin(client, request: pytest.FixtureRequest):
    request.getfixturevalue("admin_password")
    response = client.post(
        "/admin/login",
        data={"username": "admin", "password": "s3cret"},
        headers={"Origin": "http://evil.example"},
    )
    assert response.status_code == 403


def test_logout_blocked_cross_origin(client, session_cookie):
    client.cookies.set(auth._SESSION_COOKIE, session_cookie[auth._SESSION_COOKIE])
    response = client.post("/admin/logout", headers={"Origin": "http://evil.example"})
    assert response.status_code == 403


def test_logout_allowed_same_origin(client, session_cookie):
    client.cookies.set(auth._SESSION_COOKIE, session_cookie[auth._SESSION_COOKIE])
    response = client.post("/admin/logout", headers=ORIGIN, follow_redirects=False)
    assert response.status_code == 303


# ── Config editor secret masking ─────────────────────────────────


def test_config_endpoint_masks_secrets(client, session_cookie, mock_data_service):
    secret = "super-secret-api-key-123"
    config_path = mock_data_service.cache_dir / "runtime_config.json"
    config_path.write_text(
        json.dumps({"openai_api_key": secret, "llm_provider": "openai"}), encoding="utf-8"
    )
    client.cookies.set(auth._SESSION_COOKIE, session_cookie[auth._SESSION_COOKIE])

    response = client.get("/admin/api/config")
    assert response.status_code == 200
    payload = response.json()
    assert payload["openai_api_key"] == "***"
    assert secret not in response.text
