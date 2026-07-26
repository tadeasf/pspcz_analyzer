"""Tests for steno scraper caching: TTL-bounded negative cache (audit fix B4)."""

import time

import httpx
import pytest

from pspcz_analyzer.services.amendments import steno_scraper

_MARKER = steno_scraper._NEGATIVE_CACHE_MARKER


def _status_error(status_code: int) -> httpx.HTTPStatusError:
    """Build an HTTPStatusError carrying the given response status."""
    request = httpx.Request("GET", "https://www.psp.cz/x")
    response = httpx.Response(status_code, request=request)
    return httpx.HTTPStatusError("error", request=request, response=response)


class _FakeResponse:
    """Stand-in for httpx.Response with a successful status."""

    def __init__(self, content: bytes) -> None:
        self.content = content

    def raise_for_status(self) -> None:
        """Successful responses never raise."""


@pytest.fixture(autouse=True)
def _no_request_delay(monkeypatch):
    """Skip the polite-request sleep in every test."""
    monkeypatch.setattr(steno_scraper, "PSP_REQUEST_DELAY", 0)


class TestParseNegativeExpiry:
    def test_real_html_is_not_a_marker(self):
        """Ordinary page content is a positive cache hit."""
        assert steno_scraper._parse_negative_expiry("<html>speech</html>") is None

    def test_bare_legacy_marker_is_expired(self):
        """The old permanent marker must retry once, not poison forever."""
        assert steno_scraper._parse_negative_expiry(_MARKER) == 0.0

    def test_marker_with_expiry(self):
        """A TTL marker reports its expiry epoch."""
        assert steno_scraper._parse_negative_expiry(f"{_MARKER}:12345.5") == 12345.5

    def test_unparseable_expiry_is_expired(self):
        """A corrupted marker is treated as expired (retried and rewritten)."""
        assert steno_scraper._parse_negative_expiry(f"{_MARKER}:garbage") == 0.0


class TestDownloadCached:
    def test_positive_cache_hit_skips_http(self, tmp_path, monkeypatch):
        """Cached HTML is returned without any network call."""
        cache_file = tmp_path / "page.html"
        cache_file.write_text("<html>cached</html>", encoding="utf-8")

        def _fail(*_args, **_kwargs):
            raise AssertionError("HTTP must not be called on cache hit")

        monkeypatch.setattr(steno_scraper.httpx, "get", _fail)
        assert steno_scraper._download_cached("https://x", cache_file) == "<html>cached</html>"

    def test_fresh_negative_marker_skips_http(self, tmp_path, monkeypatch):
        """An unexpired negative marker returns None without a network call."""
        cache_file = tmp_path / "page.html"
        cache_file.write_text(f"{_MARKER}:{time.time() + 3600}", encoding="utf-8")

        def _fail(*_args, **_kwargs):
            raise AssertionError("HTTP must not be called for fresh negative marker")

        monkeypatch.setattr(steno_scraper.httpx, "get", _fail)
        assert steno_scraper._download_cached("https://x", cache_file) is None

    def test_expired_marker_retries_and_caches(self, tmp_path, monkeypatch):
        """An expired marker triggers a real download, and success overwrites it."""
        cache_file = tmp_path / "page.html"
        cache_file.write_text(f"{_MARKER}:1.0", encoding="utf-8")
        monkeypatch.setattr(steno_scraper.httpx, "get", lambda *a, **k: _FakeResponse(b"now here"))

        assert steno_scraper._download_cached("https://x", cache_file) == "now here"
        assert cache_file.read_text(encoding="utf-8") == "now here"

    def test_legacy_marker_retries(self, tmp_path, monkeypatch):
        """The permanent legacy marker is treated as expired and retried."""
        cache_file = tmp_path / "page.html"
        cache_file.write_text(_MARKER, encoding="utf-8")
        monkeypatch.setattr(steno_scraper.httpx, "get", lambda *a, **k: _FakeResponse(b"found"))

        assert steno_scraper._download_cached("https://x", cache_file) == "found"

    def test_404_writes_ttl_marker(self, tmp_path, monkeypatch):
        """A genuine 404 is negative-cached with a future expiry."""
        monkeypatch.setattr(steno_scraper, "STENO_NEGATIVE_CACHE_TTL", 3600)

        def _not_found(*_args, **_kwargs):
            raise _status_error(404)

        monkeypatch.setattr(steno_scraper.httpx, "get", _not_found)
        cache_file = tmp_path / "page.html"

        assert steno_scraper._download_cached("https://x", cache_file) is None

        expiry = steno_scraper._parse_negative_expiry(cache_file.read_text(encoding="utf-8"))
        assert expiry is not None
        assert expiry > time.time()

    def test_timeout_does_not_poison_cache(self, tmp_path, monkeypatch):
        """A timeout returns None but writes no marker, so reruns retry."""

        def _timeout(*_args, **_kwargs):
            raise httpx.ConnectTimeout("timed out")

        monkeypatch.setattr(steno_scraper.httpx, "get", _timeout)
        cache_file = tmp_path / "page.html"

        assert steno_scraper._download_cached("https://x", cache_file) is None
        assert not cache_file.exists()

    def test_server_error_does_not_poison_cache(self, tmp_path, monkeypatch):
        """A 500 returns None but writes no marker, so reruns retry."""

        def _server_error(*_args, **_kwargs):
            raise _status_error(500)

        monkeypatch.setattr(steno_scraper.httpx, "get", _server_error)
        cache_file = tmp_path / "page.html"

        assert steno_scraper._download_cached("https://x", cache_file) is None
        assert not cache_file.exists()
