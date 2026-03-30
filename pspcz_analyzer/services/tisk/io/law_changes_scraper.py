"""Scraper for proposed law changes and related bills from psp.cz.

Parses:
- ``historie.sqw?snzp=1`` — which existing laws a bill proposes to modify
- ``tisky.sqw?idsb=...`` — all other bills modifying the same laws
"""

import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path

import httpx
from bs4 import BeautifulSoup, Tag
from loguru import logger

from pspcz_analyzer.config import (
    PSP_LAW_CHANGES_URL_TEMPLATE,
    PSP_RELATED_BILLS_URL_TEMPLATE,
    TISKY_LAW_CHANGES_DIR,
    TISKY_META_DIR,
    TISKY_RELATED_BILLS_DIR,
)

# Regex to extract idsb parameter from tisky.sqw links
_IDSB_RE = re.compile(r"idsb=(\d+)", re.IGNORECASE)
# Regex to extract period and ct from historie.sqw links
_HISTORIE_LINK_RE = re.compile(r"historie\.sqw\?o=(\d+)&t=(\d+)", re.IGNORECASE)


@dataclass
class ProposedLawChange:
    """A single proposed change to an existing law."""

    citace: str = ""  # law citation (e.g. "zákon č. 89/2012 Sb.")
    zmena: str = ""  # type of change (e.g. "mění", "ruší")
    predpis: str = ""  # law name / description
    od_ct: int | None = None  # sub-tisk number where change is introduced
    idsb: int | None = None  # link to related bills page


@dataclass
class RelatedBill:
    """A bill found via the related-bills page (tisky.sqw?idsb=...)."""

    cislo: str = ""  # tisk number as displayed
    kratky_nazev: str = ""  # short title
    typ_tisku: str = ""  # print type
    stav: str = ""  # current status
    period: int | None = None  # electoral period (parsed from link)
    ct: int | None = None  # tisk number (parsed from link)
    url: str = ""  # full URL to the bill's history page


def _parse_law_changes_table(table: Tag) -> list[ProposedLawChange]:
    """Parse a single table for law change rows."""
    changes: list[ProposedLawChange] = []
    rows = table.find_all("tr")
    if len(rows) < 2:
        return changes

    header = rows[0].get_text(strip=True).lower()
    if "předpis" not in header and "citace" not in header and "zákon" not in header:
        return changes

    for row in rows[1:]:
        cells = row.find_all("td")
        if len(cells) < 2:
            continue

        change = ProposedLawChange()
        texts = [c.get_text(strip=True) for c in cells]
        if len(texts) >= 1:
            change.citace = texts[0]
        if len(texts) >= 2:
            change.zmena = texts[1]
        if len(texts) >= 3:
            change.predpis = texts[2]

        # Look for idsb link in any cell
        for cell in cells:
            link = cell.find("a", href=_IDSB_RE)
            if link:
                m = _IDSB_RE.search(str(link["href"]))
                if m:
                    change.idsb = int(m.group(1))
                break

        if not change.citace and not change.predpis:
            continue

        changes.append(change)

    return changes


def _fallback_extract_law_changes(soup: BeautifulSoup) -> list[ProposedLawChange]:
    """Fallback: extract law changes from any links with idsb parameter."""
    changes: list[ProposedLawChange] = []
    for link in soup.find_all("a", href=_IDSB_RE):
        m = _IDSB_RE.search(str(link["href"]))
        if m:
            text = link.get_text(strip=True)
            parent_text = link.parent.get_text(strip=True) if link.parent else text
            changes.append(
                ProposedLawChange(
                    citace=text or parent_text,
                    idsb=int(m.group(1)),
                )
            )
    return changes


def scrape_proposed_law_changes(
    period: int,
    ct: int,
) -> list[ProposedLawChange]:
    """Parse the law changes table at ``historie.sqw?snzp=1``.

    Returns list of ProposedLawChange or empty list on failure.
    """
    url = PSP_LAW_CHANGES_URL_TEMPLATE.format(period=period, ct=ct)
    logger.debug("Scraping law changes: {}", url)

    try:
        with httpx.Client(timeout=30, follow_redirects=True) as client:
            resp = client.get(url)
            resp.raise_for_status()
    except Exception:
        logger.opt(exception=True).warning(
            "Failed to fetch law changes for tisk {}/{}",
            period,
            ct,
        )
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    changes: list[ProposedLawChange] = []

    for table in soup.find_all("table"):
        changes.extend(_parse_law_changes_table(table))

    if not changes:
        changes = _fallback_extract_law_changes(soup)

    logger.debug("Tisk {}/{}: found {} law changes", period, ct, len(changes))
    return changes


def _parse_related_bills_table(table: Tag) -> list[RelatedBill]:
    """Parse a single table for related bill rows."""
    bills: list[RelatedBill] = []
    rows = table.find_all("tr")
    if len(rows) < 2:
        return bills

    for row in rows[1:]:
        cells = row.find_all("td")
        if not cells:
            continue

        bill = RelatedBill()
        texts = [c.get_text(strip=True) for c in cells]

        if len(texts) >= 1:
            bill.cislo = texts[0]
        if len(texts) >= 2:
            bill.kratky_nazev = texts[1]
        if len(texts) >= 3:
            bill.typ_tisku = texts[2]
        if len(texts) >= 4:
            bill.stav = texts[3]

        # Extract period and ct from any historie.sqw link in the row
        for cell in cells:
            link = cell.find("a", href=_HISTORIE_LINK_RE)
            if link:
                m = _HISTORIE_LINK_RE.search(str(link["href"]))
                if m:
                    bill.period = int(m.group(1))
                    bill.ct = int(m.group(2))
                    bill.url = f"https://www.psp.cz/sqw/{str(link['href'])}"
                break

        if not bill.cislo and not bill.kratky_nazev:
            continue

        bills.append(bill)

    return bills


def scrape_related_bills(idsb: int) -> list[RelatedBill]:
    """Parse the related bills table at ``tisky.sqw?idsb={id}``.

    Returns list of RelatedBill or empty list on failure.
    """
    url = PSP_RELATED_BILLS_URL_TEMPLATE.format(idsb=idsb)
    logger.debug("Scraping related bills: {}", url)

    try:
        with httpx.Client(timeout=30, follow_redirects=True) as client:
            resp = client.get(url)
            resp.raise_for_status()
    except Exception:
        logger.opt(exception=True).warning(
            "Failed to fetch related bills for idsb={}",
            idsb,
        )
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    bills: list[RelatedBill] = []

    for table in soup.find_all("table"):
        bills.extend(_parse_related_bills_table(table))

    logger.debug("idsb={}: found {} related bills", idsb, len(bills))
    return bills


# --- JSON caching ---


def save_law_changes_json(
    changes: list[ProposedLawChange],
    period: int,
    ct: int,
    cache_dir: Path,
) -> Path:
    """Save law changes to ``tisky_meta/{period}/tisky_law_changes/{ct}.json``."""
    dest_dir = cache_dir / TISKY_META_DIR / str(period) / TISKY_LAW_CHANGES_DIR
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / f"{ct}.json"
    data = [asdict(c) for c in changes]
    dest.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return dest


def load_law_changes_json(
    period: int,
    ct: int,
    cache_dir: Path,
) -> list[ProposedLawChange] | None:
    """Load cached law changes. Returns None if not cached."""
    path = cache_dir / TISKY_META_DIR / str(period) / TISKY_LAW_CHANGES_DIR / f"{ct}.json"
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return [ProposedLawChange(**d) for d in data]
    except Exception:
        logger.opt(exception=True).warning("Failed to load law changes from {}", path)
        return None


def save_related_bills_json(
    bills: list[RelatedBill],
    idsb: int,
    cache_dir: Path,
) -> Path:
    """Save related bills to ``tisky_meta/related_bills/{idsb}.json``."""
    dest_dir = cache_dir / TISKY_META_DIR / TISKY_RELATED_BILLS_DIR
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / f"{idsb}.json"
    data = [asdict(b) for b in bills]
    dest.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return dest


def load_related_bills_json(
    idsb: int,
    cache_dir: Path,
) -> list[RelatedBill] | None:
    """Load cached related bills. Returns None if not cached."""
    path = cache_dir / TISKY_META_DIR / TISKY_RELATED_BILLS_DIR / f"{idsb}.json"
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return [RelatedBill(**d) for d in data]
    except Exception:
        logger.opt(exception=True).warning("Failed to load related bills from {}", path)
        return None
