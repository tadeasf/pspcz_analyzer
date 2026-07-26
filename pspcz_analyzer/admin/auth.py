"""Admin authentication: IP whitelist + bcrypt password + HMAC session cookie."""

import hashlib
import hmac
import secrets
import time
from typing import Any

import bcrypt
from loguru import logger
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import RedirectResponse, Response

from pspcz_analyzer.config import (
    ADMIN_ALLOWED_IPS,
    ADMIN_PASSWORD_HASH,
    ADMIN_SESSION_SECRET,
    ADMIN_TRUSTED_PROXIES,
)
from pspcz_analyzer.middleware import is_same_origin
from pspcz_analyzer.proxy import extract_client_ip, is_ip_allowed, parse_ip_networks

_SESSION_COOKIE = "pspcz_admin_session"
_SESSION_TTL = 86400  # 24 hours

# HTTP methods that need no CSRF origin check (safe, side-effect-free).
_SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})

# Auto-generate secret if not provided (ephemeral — sessions lost on restart)
_session_secret = ADMIN_SESSION_SECRET or secrets.token_hex(32)

_allowed_networks = parse_ip_networks(ADMIN_ALLOWED_IPS)
_trusted_proxies = parse_ip_networks(ADMIN_TRUSTED_PROXIES)


def _client_ip(request: Request) -> str:
    """Extract the real client IP, trusting X-Forwarded-For only from known proxies."""
    return extract_client_ip(request, _trusted_proxies)


def _is_ip_allowed(ip_str: str) -> bool:
    """Check if client IP is in the allowed networks."""
    return is_ip_allowed(ip_str, _allowed_networks)


def _sign_session(username: str, expires: int) -> str:
    """Create HMAC-signed session token."""
    payload = f"{username}:{expires}"
    sig = hmac.new(_session_secret.encode(), payload.encode(), hashlib.sha256).hexdigest()
    return f"{payload}:{sig}"


def _verify_session(token: str) -> str | None:
    """Verify session token, return username or None if invalid/expired."""
    parts = token.split(":")
    if len(parts) != 3:
        return None
    username, expires_str, sig = parts
    try:
        expires = int(expires_str)
    except ValueError:
        return None
    if time.time() > expires:
        return None
    expected = hmac.new(
        _session_secret.encode(), f"{username}:{expires_str}".encode(), hashlib.sha256
    ).hexdigest()
    if not hmac.compare_digest(sig, expected):
        return None
    return username


def verify_password(password: str) -> bool:
    """Verify password against stored bcrypt hash."""
    if not ADMIN_PASSWORD_HASH:
        return False
    try:
        return bcrypt.checkpw(password.encode("utf-8"), ADMIN_PASSWORD_HASH.encode("utf-8"))
    except Exception:
        logger.opt(exception=True).warning("[admin-auth] bcrypt verification error")
        return False


def create_session_cookie(username: str) -> str:
    """Create a new signed session cookie value."""
    expires = int(time.time()) + _SESSION_TTL
    return _sign_session(username, expires)


def get_session_username(request: Request) -> str | None:
    """Extract and verify the admin session from request cookies."""
    token = request.cookies.get(_SESSION_COOKIE)
    if not token:
        return None
    return _verify_session(token)


class AdminAuthMiddleware(BaseHTTPMiddleware):
    """IP whitelist + session-based authentication for admin routes."""

    async def dispatch(self, request: Request, call_next: Any) -> Response:
        """Check IP whitelist, then session, redirect to login if needed."""
        path = request.url.path

        # Allow the login page and the health endpoint without a session
        # (still IP-whitelisted) — docker healthchecks need the latter.
        if path in ("/admin/login", "/admin/api/health"):
            return await self._check_ip_then_proceed(request, call_next)

        return await self._check_ip_then_proceed(request, call_next, require_session=True)

    async def _check_ip_then_proceed(
        self,
        request: Request,
        call_next: Any,
        *,
        require_session: bool = False,
    ) -> Response:
        """Verify IP whitelist and CSRF origin, optionally check session."""
        ip = _client_ip(request)
        if not _is_ip_allowed(ip):
            xff = request.headers.get("x-forwarded-for", "(none)")
            client_host = request.client.host if request.client else "(none)"
            logger.warning(
                "[admin-auth] Blocked IP={} (X-Forwarded-For={}, client.host={})",
                ip,
                xff,
                client_host,
            )
            return Response("Forbidden", status_code=403)

        # CSRF: state-changing requests must originate from our own origin.
        # SameSite=Lax cookies remain a backstop; this blocks cross-origin
        # form posts outright (e.g. a malicious site triggering a pipeline).
        if request.method not in _SAFE_METHODS and not is_same_origin(request):
            logger.warning(
                "[admin-auth] Blocked cross-origin {} {} from IP={}",
                request.method,
                request.url.path,
                ip,
            )
            return Response("Forbidden", status_code=403)

        if require_session:
            username = get_session_username(request)
            if not username:
                return RedirectResponse(url="/admin/login", status_code=303)
            request.state.admin_user = username

        return await call_next(request)


def hash_password(password: str) -> str:
    """Generate bcrypt hash for a password. Used by CLI helper."""
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--hash":
        pw = input("Enter password to hash: ")
        print(hash_password(pw))
    else:
        print("Usage: python -m pspcz_analyzer.admin.auth --hash")
