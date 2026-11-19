"""Parse the legislative stages of a psp.cz tisk history page.

The page has one div.section per institution, each with an h2 heading
("Poslanecká sněmovna", "Vláda", "Senát", "Prezident republiky", "Sbírka
zákonů", ...) and a ul.document-log of li.document-log-item entries. Each entry
has a span.mark and a <p> with the event text. The same mark means different
things in different sections (e.g. "PS" in the Senát section is the hand-over to
the Senate), so a stage is typed by the (section, mark) pair, never the mark
alone. The publication sentence "Zákon vyhlášen ... pod číslem N/RRRR Sb." is
not inside any section: it is a p.status element after the last section.
"""

import re
from dataclasses import dataclass

from bs4 import BeautifulSoup, Tag
from loguru import logger

_SECTION_SBIRKA = "Sbírka zákonů"

# (section heading, mark) -> (stage_type, label)
_SECTION_STAGES: dict[tuple[str, str], tuple[str, str]] = {
    ("Poslanecká sněmovna", "PS"): ("ps", "Poslanecká sněmovna"),
    ("Poslanecká sněmovna", "O"): ("organizacni", "Organizační výbor"),
    ("Poslanecká sněmovna", "1"): ("1_cteni", "1. čtení"),
    ("Poslanecká sněmovna", "V"): ("vybor", "Výbor"),
    ("Poslanecká sněmovna", "2"): ("2_cteni", "2. čtení"),
    ("Poslanecká sněmovna", "G"): ("garant", "Garanční výbor"),
    ("Poslanecká sněmovna", "3"): ("3_cteni", "3. čtení"),
    ("Vláda", "VL"): ("vlada", "Vláda"),
    ("Senát", "PS"): ("senat_postoupeno", "Postoupeno Senátu"),
    ("Senát", "O"): ("senat_organizacni", "Organizační výbor Senátu"),
    ("Senát", "V"): ("senat_vybor", "Výbor Senátu"),
    ("Senát", "S"): ("senat", "Senát"),
    ("Prezident republiky", "P"): ("prezident", "Prezident"),
}

# Regexes for extracting structured data from text
_DATE_RE = re.compile(r"(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{4})")
_SESSION_RE = re.compile(r"(\d+)\.\s*sch[ůu]zi")
_VOTE_RE = re.compile(r"hlasov[áa]n[ií]\s*[čc]\.\s*(\d+)")
_PUBLISHED_RE = re.compile(
    r"Zákon\s+vyhlášen\s+(\d{1,2}\.\s*\d{1,2}\.\s*\d{4}).*?pod\s+číslem\s+(\d+/\d{4})\s*Sb\.",
    re.DOTALL,
)

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


def _format_date(m: re.Match[str]) -> str:
    """Normalise a matched Czech date to 'D. M. RRRR'."""
    return f"{m.group(1)}. {m.group(2)}. {m.group(3)}"


def _extract_first_date(text: str) -> str | None:
    """Extract the first date from text in Czech format."""
    m = _DATE_RE.search(text)
    if m:
        return _format_date(m)
    return None


def _extract_outcome(text: str) -> str | None:
    """Match known outcome patterns in text."""
    lower = text.lower()
    for pattern, label in _OUTCOME_PATTERNS:
        if pattern in lower:
            return label
    return None


def _stage_mapping(section: str, mark: str) -> tuple[str, str] | None:
    """Type a document-log entry by its section heading and mark.

    Args:
        section: Section heading text (e.g. "Senát").
        mark: span.mark text (e.g. "PS").

    Returns:
        (stage_type, label), or None for an unknown (section, mark) pair.
    """
    match section:
        case "Sbírka zákonů":
            # The mark here is "Sb" or "VL" depending on the bill; both mean the
            # Collection of Laws.
            return ("sbirka", _SECTION_SBIRKA)
        case _:
            return _SECTION_STAGES.get((section, mark))


def _build_stage(section: str, mark_text: str, content_text: str) -> TiskHistoryStage | None:
    """Build a TiskHistoryStage from a section heading, mark and content text."""
    mapping = _stage_mapping(section, mark_text)
    if mapping is None:
        logger.debug("Unknown history entry: section={!r} mark={!r}", section, mark_text)
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


def _parse_document_log_items(section: str, items: list[Tag]) -> list[TiskHistoryStage]:
    """Parse document-log-item elements of one section into legislative stages."""
    stages: list[TiskHistoryStage] = []
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

            stage = _build_stage(section, mark_text, text)
            if stage:
                stages.append(stage)
        except Exception:
            logger.opt(exception=True).debug(
                "Failed to parse document-log-item",
            )
    return stages


def _parse_simple_section(section: str, content_div: Tag) -> TiskHistoryStage | None:
    """Try to parse a section without document-log items (e.g. simple content)."""
    mark = content_div.find("span", class_="mark")
    if not mark:
        return None
    mark_text = mark.get_text(strip=True)
    text = content_div.get_text(" ", strip=True)
    try:
        return _build_stage(section, mark_text, text)
    except Exception:
        logger.opt(exception=True).debug("Failed to parse history stage: {!r}", mark_text)
        return None


def _section_heading(section: Tag) -> str:
    """Return the section's heading text ("" when it has none)."""
    heading = section.find(["h2", "h3"])
    return heading.get_text(" ", strip=True) if heading else ""


def _parse_publication(soup: BeautifulSoup) -> TiskHistoryStage | None:
    """Build the publication stage from the p.status "Zákon vyhlášen ..." sentence.

    Args:
        soup: Parsed history page.

    Returns:
        A "vyhlaseni" stage with the publication date and the law number in
        details, or None when the page does not state that the law was published.
    """
    for p in soup.find_all("p", class_="status"):
        text = p.get_text(" ", strip=True)
        m = _PUBLISHED_RE.search(text)
        if m:
            return TiskHistoryStage(
                stage_type="vyhlaseni",
                label="Vyhlášení ve Sbírce zákonů",
                date=_extract_first_date(m.group(1)),
                outcome="vyhlášen",
                details=text[:500],
            )
    return None


def _parse_stages(soup: BeautifulSoup) -> list[TiskHistoryStage]:
    """Extract legislative stages from the page.

    The page structure is:
      div.section > h2 heading + div.section-content >
        ul.document-log > li.document-log-item >
          span.mark (PS/O/1/V/2/G/3/S/P/VL/Sb) + <p> content

    Each stage is typed by (section heading, mark). The publication stage, when
    present, comes from the p.status sentence and is appended last.
    """
    stages: list[TiskHistoryStage] = []

    for section in soup.find_all("div", class_="section"):
        content_div = section.find("div", class_="section-content")
        if not content_div:
            continue
        heading = _section_heading(section)

        items = content_div.find_all("li", class_="document-log-item")
        if items:
            stages.extend(_parse_document_log_items(heading, items))
        else:
            stage = _parse_simple_section(heading, content_div)
            if stage:
                stages.append(stage)

    publication = _parse_publication(soup)
    if publication:
        stages.append(publication)
    return stages


def _determine_status(stages: list[TiskHistoryStage]) -> str:
    """Determine the current overall status from the parsed legislative stages.

    Only the structured stages (the document-log items) are inspected, never the
    full page text, which contains boilerplate like "ke stažení" (download) and a
    reference to zákon 106/1999 Sb. that would otherwise yield false positives.
    """
    if any(s.stage_type in ("sbirka", "vyhlaseni") for s in stages):
        return "vyhlášeno"

    # Walk the legislative log newest-first; the latest decisive outcome wins.
    for stage in reversed(stages):
        outcome = (stage.outcome or "").lower()
        if not outcome:
            continue
        if "zamítnut" in outcome:
            return "zamítnuto"
        if "stažen" in outcome or "vzat zpět" in outcome:
            return "staženo"
        if "schválen" in outcome:
            if stage.stage_type == "prezident":
                return "podepsáno"
            if stage.stage_type == "3_cteni":
                return "schváleno sněmovnou"

    return "projednáváno"


def _law_number_from_stages(stages: list[TiskHistoryStage]) -> str | None:
    """Extract the published law number from the publication stage only.

    Reading from the whole page would pick up the boilerplate "106/1999 Sb."
    (the Freedom of Information Act referenced in every page footer).
    """
    for stage in stages:
        if stage.stage_type == "vyhlaseni":
            m = _PUBLISHED_RE.search(stage.details)
            if m:
                return f"{m.group(2)} Sb."
    return None
