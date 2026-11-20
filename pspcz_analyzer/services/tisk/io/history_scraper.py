"""Scrape psp.cz legislative history pages for parliamentary prints (tisky).

Each tisk has a history page at historie.sqw?o={period}&t={ct} showing stages
of the legislative process: submission, government opinion, readings, committee
work, senate, president, publication.

The page uses div.section elements with headings ("Předkladatel", "Poslanecká
sněmovna", "Senát", etc.) and inside, ul.document-log with li.document-log-item
items. Each item has span.mark (PS, O, 1, V, 2, G, 3, S, P, VL) and <p> content.
Stage parsing and status derivation live in history_stages.
"""

import json
import re
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import httpx
from bs4 import BeautifulSoup
from loguru import logger

from pspcz_analyzer.config import PSP_HISTORIE_URL_TEMPLATE
from pspcz_analyzer.services.tisk.io.history_stages import (
    TiskHistoryStage,
    _determine_status,
    _extract_first_date,
    _law_number_from_stages,
    _parse_stages,
)

# Bump whenever parsing changes what is stored. Cached histories with a lower
# version (0 = written before versioning) are re-scraped, terminal or not, so
# stale statuses and law numbers never stay frozen in the cache.
HISTORY_PARSER_VERSION = (
    3  # 2: President delivery and signature are two stages (#89); 3: submitted to Senate (#87)
)


@dataclass
class TiskHistory:
    """Full legislative history for a parliamentary print (tisk)."""

    ct: int
    period: int
    submitter: str = ""
    submitter_date: str | None = None
    government_opinion: str | None = None
    stages: list[TiskHistoryStage] = field(default_factory=list)
    current_status: str = "projednáváno"
    law_number: str | None = None
    scraped_at: str = ""
    law_changes: list[dict] = field(default_factory=list)
    amendment_tisk_ct1: int | None = None
    amendment_tisk_idd: int | None = None
    parser_version: int = HISTORY_PARSER_VERSION


# Subjects of the Předkladatel sentence that name the document, not its submitter
# ("Mezinárodní smlouva předložena sněmovně ...").
_DOCUMENT_TYPE_SUBJECTS = ("Mezinárodní smlouva", "Návrh", "Zpráva", "Informace")
_SUBMITTER_RE = re.compile(r"(.*?)\s*\bpředlož", re.IGNORECASE | re.DOTALL)


def _submitter_from_text(text: str) -> str:
    """Take the submitter from "<subject> předložil(a) ..." text.

    Args:
        text: Text of the Předkladatel section.

    Returns:
        The submitter; "Vláda" for "... vládou předložen"; "" when the subject is
        a document type or there is no "předlož..." sentence.
    """
    m = _SUBMITTER_RE.match(text)
    if not m:
        return ""
    subject = m.group(1).strip()
    if subject.lower().endswith("vládou"):
        return "Vláda"
    if subject.startswith(_DOCUMENT_TYPE_SUBJECTS):
        return ""
    return subject


def _extract_submitter(soup: BeautifulSoup) -> tuple[str, str | None]:
    """Extract who submitted the tisk and when, from the Předkladatel section.

    Args:
        soup: Parsed history page.

    Returns:
        (submitter, date). ("", None) when the page has no Předkladatel section
        (reports, interpellations); the rest of the page is never searched, as
        it yields fragments like "Senát PS" from "PS předložil Senátu".
    """
    for section in soup.find_all("div", class_="section"):
        heading = section.find(["h2", "h3", "h4", "strong", "b"])
        if heading and "Předkladatel" in heading.get_text():
            content = section.find("div", class_="section-content")
            if content:
                text = content.get_text(" ", strip=True)
                return _submitter_from_text(text), _extract_first_date(text)
    return "", None


_GOV_OPINION_RE = re.compile(r"\(\s*(souhlas|nesouhlas|neutrální)\s*\)")


def _extract_government_opinion(soup: BeautifulSoup) -> str | None:
    """Read the government opinion from the Vláda section only.

    The opinion is the parenthesised word after "Stanovisko vlády rozesláno
    poslancům ... jako tisk N/1 ( souhlas )". The rest of the page contains
    unrelated "souhlas" wording (e.g. "Sněmovna souhlasila s projednáváním"),
    and government bills have no Vláda section at all.

    Args:
        soup: Parsed history page.

    Returns:
        "souhlas", "nesouhlas" or "neutrální", or None when not stated.
    """
    for section in soup.find_all("div", class_="section"):
        heading = section.find(["h2", "h3"])
        if heading and heading.get_text(strip=True) == "Vláda":
            m = _GOV_OPINION_RE.search(section.get_text(" ", strip=True))
            if m:
                return m.group(1)
    return None


_AMENDMENT_TISK_RE = re.compile(
    r"[Pp]odan[ée]\s+pozm[ěe][ňn]ovac[ií]\s+n[áa]vrhy\s+zpracov[áa]ny\s+"
    r"jako\s+tisk\s+(\d+)\s*/\s*(\d+)",
)


def _extract_amendment_tisk_reference(
    page_text: str,
) -> tuple[int | None, int | None]:
    """Extract amendment sub-tisk CT1 reference from history page text.

    Looks for patterns like "Podané pozměňovací návrhy zpracovány jako tisk 410/4".

    Args:
        page_text: Full text content of the history page.

    Returns:
        (ct1, idd) tuple. ct1 is the sub-tisk number, idd is None initially
        (resolved later via scraping).
    """
    m = _AMENDMENT_TISK_RE.search(page_text)
    if not m:
        return None, None
    ct1 = int(m.group(2))
    return ct1, None


def _resolve_amendment_tisk_idd(
    period: int,
    ct: int,
    ct1: int,
) -> int | None:
    """Resolve the idd for a known amendment sub-tisk CT1.

    Scrapes the sub-tisk page to find the downloadable PDF idd.

    Args:
        period: Electoral period number.
        ct: Parent tisk number.
        ct1: Sub-tisk version number.

    Returns:
        The idd for PDF download, or None if not found.
    """
    from pspcz_analyzer.services.tisk.io import scrape_all_subtisk_documents

    try:
        versions = scrape_all_subtisk_documents(period, ct, max_ct1=ct1 + 1)
        for v in versions:
            if v.ct1 == ct1 and v.idd is not None:
                return v.idd
    except Exception:
        logger.warning(
            "Failed to resolve idd for tisk {}/{}/{}",
            period,
            ct,
            ct1,
        )
    return None


def parse_tisk_history(html: str, period: int, ct: int) -> TiskHistory:
    """Parse a fetched history page into a TiskHistory (no network access).

    Args:
        html: Decoded HTML of historie.sqw for the tisk.
        period: Electoral period number.
        ct: Tisk number.

    Returns:
        TiskHistory with stages, status and law number. amendment_tisk_idd is
        left unresolved (None).
    """
    soup = BeautifulSoup(html, "html.parser")
    full_text = soup.get_text(" ", strip=True)

    stages = _parse_stages(soup)
    submitter, submitter_date = _extract_submitter(soup)
    amendment_ct1, amendment_idd = _extract_amendment_tisk_reference(full_text)

    return TiskHistory(
        ct=ct,
        period=period,
        submitter=submitter,
        submitter_date=submitter_date,
        government_opinion=_extract_government_opinion(soup),
        stages=stages,
        current_status=_determine_status(stages),
        law_number=_law_number_from_stages(stages),
        scraped_at=datetime.now(UTC).isoformat(),
        amendment_tisk_ct1=amendment_ct1,
        amendment_tisk_idd=amendment_idd,
    )


def scrape_tisk_history(period: int, ct: int) -> TiskHistory | None:
    """Scrape the legislative history page for a tisk.

    Returns TiskHistory with stages, or None if the page couldn't be fetched.
    """
    url = PSP_HISTORIE_URL_TEMPLATE.format(period=period, ct=ct)
    logger.debug("Scraping tisk history: {}", url)

    try:
        with httpx.Client(timeout=30, follow_redirects=True) as client:
            resp = client.get(url)
            resp.raise_for_status()
    except Exception:
        logger.opt(exception=True).warning(
            "Failed to fetch history for tisk {}/{}",
            period,
            ct,
        )
        return None

    history = parse_tisk_history(resp.text, period, ct)

    # Resolve the amendment sub-tisk PDF idd (network) for "tisk 410/4" references
    if history.amendment_tisk_ct1 is not None and history.amendment_tisk_idd is None:
        history.amendment_tisk_idd = _resolve_amendment_tisk_idd(
            period, ct, history.amendment_tisk_ct1
        )
    return history


def history_to_dict(h: TiskHistory) -> dict:
    """Serialize TiskHistory to a JSON-compatible dict."""
    return asdict(h)


def history_from_dict(d: dict) -> TiskHistory:
    """Deserialize a dict to TiskHistory."""
    stages = [TiskHistoryStage(**s) for s in d.get("stages", [])]
    return TiskHistory(
        ct=d["ct"],
        period=d["period"],
        submitter=d.get("submitter", ""),
        submitter_date=d.get("submitter_date"),
        government_opinion=d.get("government_opinion"),
        stages=stages,
        current_status=d.get("current_status", "projednáváno"),
        law_number=d.get("law_number"),
        scraped_at=d.get("scraped_at", ""),
        law_changes=d.get("law_changes", []),
        amendment_tisk_ct1=d.get("amendment_tisk_ct1"),
        amendment_tisk_idd=d.get("amendment_tisk_idd"),
        parser_version=d.get("parser_version", 0),
    )


def save_history_json(h: TiskHistory, path: Path) -> None:
    """Save a TiskHistory as JSON."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(history_to_dict(h), ensure_ascii=False, indent=2), encoding="utf-8")


def load_history_json(path: Path) -> TiskHistory | None:
    """Load a TiskHistory from JSON file."""
    path = Path(path)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return history_from_dict(data)
    except Exception:
        logger.opt(exception=True).warning("Failed to load history from {}", path)
        return None
