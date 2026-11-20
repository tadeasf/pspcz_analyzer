"""One failed psp.cz request must not stop the tisk download of a whole period."""

import httpx
import pytest

from pspcz_analyzer.services.tisk.io import scraper


def test_get_best_pdf_returns_none_on_network_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(period: int, ct: int) -> list:
        raise httpx.ConnectError("connection failed")

    monkeypatch.setattr(scraper, "scrape_tisk_documents", boom)
    assert scraper.get_best_pdf(10, 327) is None
