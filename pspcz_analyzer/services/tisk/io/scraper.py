"""Scrape psp.cz for PDF document links associated with parliamentary prints (tisky)."""

import re
from dataclasses import dataclass

import httpx
from bs4 import BeautifulSoup
from loguru import logger

from pspcz_analyzer.config import (
    PSP_SUBTISKT_URL_TEMPLATE,
    PSP_TISKT_URL_TEMPLATE,
)


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
        href = str(link["href"])
        match = _IDD_RE.search(href)
        if not match:
            continue

        idd = int(match.group(1))
        desc = link.get_text(strip=True)
        parent_text = link.parent.get_text(strip=True) if link.parent else desc

        # Detect format from context — psp.cz labels PDFs
        fmt = "PDF" if "PDF" in parent_text.upper() or href.endswith(".pdf") else "unknown"
        is_complete = "cel" in desc.lower() or "úplné znění" in desc.lower()

        documents.append(
            TiskDocument(
                idd=idd,
                description=desc,
                format=fmt,
                is_complete=is_complete,
            )
        )

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


@dataclass
class SubTiskVersion:
    """A sub-tisk version (CT1=0 original, CT1=1 gov opinion, CT1=2+ amendments)."""

    period: int
    ct: int
    ct1: int
    idd: int | None = None  # best PDF idd for this version
    description: str = ""
    has_pdf: bool = False
    has_text: bool = False
    llm_diff_summary: str = ""


def _scrape_subtisk_page(period: int, ct: int, ct1: int) -> list[TiskDocument] | None:
    """Scrape a single sub-tisk page. Returns documents or None if page doesn't exist."""
    url = PSP_SUBTISKT_URL_TEMPLATE.format(period=period, ct=ct, ct1=ct1)
    logger.debug("Scraping sub-tisk page: {}", url)

    try:
        with httpx.Client(timeout=30, follow_redirects=True) as client:
            resp = client.get(url)
            resp.raise_for_status()
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return None
        logger.opt(exception=True).warning(
            "Failed to fetch sub-tisk {}/{}/{}",
            period,
            ct,
            ct1,
        )
        return None
    except Exception:
        logger.opt(exception=True).warning(
            "Failed to fetch sub-tisk {}/{}/{}",
            period,
            ct,
            ct1,
        )
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    # Check for empty/error page — psp.cz returns 200 with minimal content
    body_text = soup.get_text(strip=True)
    if len(body_text) < 50 or "nebyl nalezen" in body_text.lower():
        return None

    documents: list[TiskDocument] = []
    for link in soup.find_all("a", href=_IDD_RE):
        href = str(link["href"])
        match = _IDD_RE.search(href)
        if not match:
            continue
        idd = int(match.group(1))
        desc = link.get_text(strip=True)
        parent_text = link.parent.get_text(strip=True) if link.parent else desc
        fmt = "PDF" if "PDF" in parent_text.upper() or href.endswith(".pdf") else "unknown"
        is_complete = "cel" in desc.lower() or "úplné znění" in desc.lower()
        documents.append(
            TiskDocument(idd=idd, description=desc, format=fmt, is_complete=is_complete)
        )

    return documents


def scrape_all_subtisk_documents(
    period: int,
    ct: int,
    max_ct1: int = 20,
) -> list[SubTiskVersion]:
    """Iterate CT1=0..N for a tisk, collecting sub-tisk versions.

    Stops when a page returns 404/empty. Returns list of SubTiskVersion.
    """
    versions: list[SubTiskVersion] = []

    for ct1 in range(max_ct1 + 1):
        docs = _scrape_subtisk_page(period, ct, ct1)
        if docs is None and ct1 > 0:
            # CT1=0 might legitimately have no PDFs, but once we hit empty
            # for ct1 > 0, we're past the last version
            break

        # Build a description from the page's title/context
        best_doc = None
        desc = ""
        if docs:
            # Prefer complete docs
            complete = [d for d in docs if d.is_complete]
            best_doc = complete[0] if complete else docs[0]
            desc = best_doc.description

        match ct1:
            case 0:
                desc = desc or "Původní znění (original)"
            case 1:
                desc = desc or "Stanovisko vlády (government opinion)"

        version = SubTiskVersion(
            period=period,
            ct=ct,
            ct1=ct1,
            idd=best_doc.idd if best_doc else None,
            description=desc,
            has_pdf=best_doc is not None,
        )
        versions.append(version)

        # If we got no docs for CT1=0, don't bother continuing
        if docs is None and ct1 == 0:
            break

    logger.debug("Tisk {}/{}: found {} sub-tisk versions", period, ct, len(versions))
    return versions
