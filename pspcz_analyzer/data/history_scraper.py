"""Scrape psp.cz legislative history pages for parliamentary prints (tisky).

Each tisk has a history page at historie.sqw?o={period}&t={ct} showing stages
of the legislative process: submission, government opinion, readings, committee
work, senate, president, publication.

The page uses div.section elements with headings ("Předkladatel", "Poslanecká
sněmovna", "Senát", etc.) and inside, ul.document-log with li.document-log-item
items. Each item has span.mark (PS, O, 1, V, 2, G, 3, S, P, VL) and <p> content.
"""

import json
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

import httpx
from bs4 import BeautifulSoup
from loguru import logger

from pspcz_analyzer.config import PSP_HISTORIE_URL_TEMPLATE

# Mark text -> (stage_type, label)
_MARK_MAP: dict[str, tuple[str, str]] = {
    "PS": ("ps", "Poslanecká sněmovna"),
    "O": ("organizacni", "Organizační výbor"),
    "1": ("1_cteni", "1. čtení"),
    "V": ("vybor", "Výbor"),
    "2": ("2_cteni", "2. čtení"),
    "G": ("garant", "Garanční výbor"),
    "3": ("3_cteni", "3. čtení"),
    "S": ("senat", "Senát"),
    "P": ("prezident", "Prezident"),
    "VL": ("sbirka", "Sbírka zákonů"),
}

# Regexes for extracting structured data from text
_DATE_RE = re.compile(r"(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{4})")
_SESSION_RE = re.compile(r"(\d+)\.\s*sch[ůu]zi")
_VOTE_RE = re.compile(r"hlasov[áa]n[ií]\s*[čc]\.\s*(\d+)")
_LAW_NUMBER_RE = re.compile(r"(?:pod\s+)?[čc][ií]slem\s+(\d+/\d+\s*Sb\.)")
_LAW_NUMBER_ALT_RE = re.compile(r"(\d+/\d+\s*Sb\.)")

_OUTCOME_PATTERNS = [
    ("schválen", "schválen"),
    ("zamítnut", "zamítnut"),
    ("přikázán", "přikázán výborům"),
    ("doporučuje schválit", "doporučuje schválit"),
    ("nedoporučuje", "nedoporučuje schválit"),
    ("podepsal", "podepsal"),
    ("vrátil", "vrátil"),
    ("přerušuje", "přerušeno"),
    ("stažen", "stažen"),
    ("vzat zpět", "vzat zpět"),
    ("vyhlášen", "vyhlášen"),
]


@dataclass
class TiskHistoryStage:
    """A single stage in the legislative process for a tisk."""

    stage_type: str
    label: str
    date: str | None = None
    session_number: int | None = None
    vote_number: int | None = None
    outcome: str | None = None
    details: str = ""


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


def _extract_first_date(text: str) -> str | None:
    """Extract the first date from text in Czech format."""
    m = _DATE_RE.search(text)
    if m:
        return f"{m.group(1)}. {m.group(2)}. {m.group(3)}"
    return None


def _extract_outcome(text: str) -> str | None:
    """Match known outcome patterns in text."""
    lower = text.lower()
    for pattern, label in _OUTCOME_PATTERNS:
        if pattern in lower:
            return label
    return None


def _build_stage(mark_text: str, content_text: str) -> TiskHistoryStage | None:
    """Build a TiskHistoryStage from a mark label and content text."""
    mapping = _MARK_MAP.get(mark_text)
    if mapping is None:
        return None

    stage_type, label = mapping
    date = _extract_first_date(content_text)
    session_m = _SESSION_RE.search(content_text)
    vote_m = _VOTE_RE.search(content_text)
    outcome = _extract_outcome(content_text)

    return TiskHistoryStage(
        stage_type=stage_type,
        label=label,
        date=date,
        session_number=int(session_m.group(1)) if session_m else None,
        vote_number=int(vote_m.group(1)) if vote_m else None,
        outcome=outcome,
        details=content_text.strip()[:500],
    )


def _parse_stages(soup: BeautifulSoup) -> list[TiskHistoryStage]:
    """Extract legislative stages from the page.

    The page structure is:
      div.section > [h2/h3/strong heading] + div.section-content >
        ul.document-log > li.document-log-item >
          span.mark (PS/O/1/V/2/G/3/S/P/VL) + <p> content

    Some sections (Předkladatel, Prezident, Sbírka) may have simple content
    without document-log items.
    """
    stages: list[TiskHistoryStage] = []

    for section in soup.find_all("div", class_="section"):
        content_div = section.find("div", class_="section-content")
        if not content_div:
            continue

        # Process document-log-items (main stage entries)
        items = content_div.find_all("li", class_="document-log-item")
        if items:
            for item in items:
                try:
                    mark = item.find("span", class_="mark")
                    if not mark:
                        continue
                    mark_text = mark.get_text(strip=True)

                    # Get content from <p> tag, or from sub-list for V/G stages
                    p = item.find("p")
                    if p:
                        text = p.get_text(" ", strip=True)
                    else:
                        # V and G stages may have nested <ul> instead of <p>
                        text = item.get_text(" ", strip=True)

                    stage = _build_stage(mark_text, text)
                    if stage:
                        stages.append(stage)
                except Exception:
                    logger.opt(exception=True).debug(
                        "Failed to parse document-log-item",
                    )
        else:
            # Section without document-log (e.g. simple content)
            # Try to find a mark span in the content
            mark = content_div.find("span", class_="mark")
            if mark:
                mark_text = mark.get_text(strip=True)
                text = content_div.get_text(" ", strip=True)
                try:
                    stage = _build_stage(mark_text, text)
                    if stage:
                        stages.append(stage)
                except Exception:
                    pass

    return stages


def _extract_submitter(soup: BeautifulSoup) -> tuple[str, str | None]:
    """Extract who submitted the tisk and when."""
    # Look specifically in the Předkladatel section
    for section in soup.find_all("div", class_="section"):
        heading = section.find(["h2", "h3", "h4", "strong", "b"])
        if heading and "Předkladatel" in heading.get_text():
            content = section.find("div", class_="section-content")
            if content:
                text = content.get_text(" ", strip=True)
                date = _extract_first_date(text)
                # Extract submitter name — typically "Vláda" or a person name
                submitter = text.split("předlož")[0].strip() if "předlož" in text.lower() else text[:80]
                return submitter, date

    # Fallback: search full text
    text = soup.get_text(" ", strip=True)
    predlozil_re = re.compile(
        r"([\w\s]+?)\s+předlož\w+\s+.*?(\d{1,2}\.\s*\d{1,2}\.\s*\d{4})",
        re.IGNORECASE,
    )
    m = predlozil_re.search(text)
    if m:
        return m.group(1).strip(), _extract_first_date(m.group(2))
    return "", None


def _extract_government_opinion(soup: BeautifulSoup) -> str | None:
    """Look for government opinion (souhlas/nesouhlas/neutrální)."""
    text = soup.get_text(" ", strip=True).lower()
    if "souhlas" in text and "nesouhlas" not in text:
        return "souhlas"
    if "nesouhlas" in text:
        return "nesouhlas"
    if "neutrální" in text:
        return "neutrální"
    return None


def _determine_status(stages: list[TiskHistoryStage], full_text: str) -> str:
    """Determine current overall status from stages and page text."""
    lower = full_text.lower()

    if any(s.stage_type == "sbirka" for s in stages):
        return "vyhlášeno"
    if "zamítnut" in lower:
        return "zamítnuto"
    if "stažen" in lower or "vzat zpět" in lower:
        return "staženo"

    # Check what the last meaningful stage outcome was
    for stage in reversed(stages):
        if stage.outcome:
            if "schválen" in stage.outcome:
                if stage.stage_type == "prezident":
                    return "podepsáno"
                if stage.stage_type == "3_cteni":
                    return "schváleno sněmovnou"
            if "zamítnut" in stage.outcome:
                return "zamítnuto"

    return "projednáváno"


def _extract_law_number(text: str) -> str | None:
    """Extract law number (e.g. '246/2022 Sb.') from page text."""
    m = _LAW_NUMBER_RE.search(text)
    if m:
        return m.group(1)
    m = _LAW_NUMBER_ALT_RE.search(text)
    if m:
        return m.group(1)
    return None


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
            "Failed to fetch history for tisk {}/{}", period, ct,
        )
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    full_text = soup.get_text(" ", strip=True)

    stages = _parse_stages(soup)
    submitter, submitter_date = _extract_submitter(soup)
    gov_opinion = _extract_government_opinion(soup)
    law_number = _extract_law_number(full_text)
    status = _determine_status(stages, full_text)

    return TiskHistory(
        ct=ct,
        period=period,
        submitter=submitter,
        submitter_date=submitter_date,
        government_opinion=gov_opinion,
        stages=stages,
        current_status=status,
        law_number=law_number,
        scraped_at=datetime.now(timezone.utc).isoformat(),
    )


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
    )


def save_history_json(h: TiskHistory, path) -> None:
    """Save a TiskHistory as JSON."""
    from pathlib import Path

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(history_to_dict(h), ensure_ascii=False, indent=2), encoding="utf-8")


def load_history_json(path) -> TiskHistory | None:
    """Load a TiskHistory from JSON file."""
    from pathlib import Path

    path = Path(path)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return history_from_dict(data)
    except Exception:
        logger.opt(exception=True).warning("Failed to load history from {}", path)
        return None
