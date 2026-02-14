"""Scrape psp.cz for PDF document links associated with parliamentary prints (tisky)."""

import re
from dataclasses import dataclass

import httpx
from bs4 import BeautifulSoup
from loguru import logger

from pspcz_analyzer.config import PSP_ORIG2_BASE_URL, PSP_TISKT_URL_TEMPLATE


@dataclass
class TiskDocument:
    """A single document (PDF) associated with a parliamentary print."""

    idd: int
    description: str
    format: str  # e.g. "PDF"
    is_complete: bool  # True if this is the full print ("Celý sněmovní tisk")


_IDD_RE = re.compile(r"orig2\.sqw\?idd=(\d+)")


def scrape_tisk_documents(period: int, ct: int) -> list[TiskDocument]:
    """Scrape the document listing page for a given tisk and return all PDF links.

    Fetches ``tiskt.sqw?o={period}&ct={ct}&ct1=0`` and extracts ``orig2.sqw?idd=``
    links along with their descriptions.
    """
    url = PSP_TISKT_URL_TEMPLATE.format(period=period, ct=ct)
    logger.debug("Scraping tisk documents: {}", url)

    with httpx.Client(timeout=30, follow_redirects=True) as client:
        resp = client.get(url)
        resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    documents: list[TiskDocument] = []

    for link in soup.find_all("a", href=_IDD_RE):
        href = link["href"]
        match = _IDD_RE.search(href)
        if not match:
            continue

        idd = int(match.group(1))
        desc = link.get_text(strip=True)
        parent_text = link.parent.get_text(strip=True) if link.parent else desc

        # Detect format from context — psp.cz labels PDFs
        fmt = "PDF" if "PDF" in parent_text.upper() or href.endswith(".pdf") else "unknown"
        is_complete = "cel" in desc.lower() or "úplné znění" in desc.lower()

        documents.append(TiskDocument(
            idd=idd, description=desc, format=fmt, is_complete=is_complete,
        ))

    logger.debug("Tisk {}/{}: found {} documents", period, ct, len(documents))
    return documents


def get_best_pdf(period: int, ct: int) -> TiskDocument | None:
    """Return the best PDF document for a tisk — prefer complete prints."""
    docs = scrape_tisk_documents(period, ct)
    if not docs:
        return None

    # Prefer complete prints, then first available
    complete = [d for d in docs if d.is_complete]
    return complete[0] if complete else docs[0]
