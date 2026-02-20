"""Locale middleware — reads language from cookie and sets ContextVar per request."""

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from pspcz_analyzer.i18n import SUPPORTED_LANGUAGES, set_locale


class LocaleMiddleware(BaseHTTPMiddleware):
    """Set locale ContextVar from the ``lang`` cookie on every request."""

    async def dispatch(self, request: Request, call_next) -> Response:
        lang = request.cookies.get("lang", "cs")
        if lang not in SUPPORTED_LANGUAGES:
            lang = "cs"
        set_locale(lang)
        request.state.lang = lang
        response = await call_next(request)
        return response
