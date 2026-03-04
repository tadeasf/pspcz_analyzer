"""Parse amendment PDF text into structured per-letter amendment data.

Amendment PDFs from psp.cz have a clear structure:
  A. Poslanec Libor Turek
    A.1. SD 3327
    A.2. SD 3328
  B. Poslankyně Jana Nová
    B.1. SD 3330

This parser extracts letter designations, submitter names (in nominative case),
and per-sub-amendment text from extracted PDF text.
"""

import re
from dataclasses import dataclass, field

from loguru import logger

# Top-level letter header: "A. Poslanec Jan Novák" or "B. Poslankyně Jana Nová"
# Also handles "Poslanci" (plural) and "Poslankyně" (plural feminine)
_LETTER_HEADER_RE = re.compile(
    r"^([A-Z])\.?\s+"
    r"(Poslanec|Poslankyně|Poslanci|Poslankyně)\s+"
    r"(.+?)$",
    re.MULTILINE,
)

# Sub-amendment: "A.1. SD 3327" or "A.1." (without SD number for older periods)
_SUB_AMENDMENT_RE = re.compile(
    r"^[A-Z]\.(\d+)\.\s*(?:SD\s+(\d+))?\s*",
    re.MULTILINE,
)

# Alternative header format: "A. Jan Novák" (no title prefix)
_LETTER_HEADER_ALT_RE = re.compile(
    r"^([A-Z])\.?\s+"
    r"([A-ZÁČĎÉĚÍŇÓŘŠŤÚŮÝŽ][a-záčďéěíňóřšťúůýž]+(?:\s+[A-ZÁČĎÉĚÍŇÓŘŠŤÚŮÝŽ][a-záčďéěíňóřšťúůýž]+)+)"
    r"\s*$",
    re.MULTILINE,
)


@dataclass
class PdfSubAmendment:
    """A single sub-amendment within a letter (e.g. A.1, A.2).

    Attributes:
        label: Sub-amendment label (e.g. "A.1", "A.2").
        sd_number: SD reference number (may be empty for older periods).
        text: Amendment text content.
    """

    label: str
    sd_number: str = ""
    text: str = ""


@dataclass
class PdfAmendment:
    """A top-level amendment letter from the PDF.

    Attributes:
        letter: Letter designation (e.g. "A", "B", "C").
        submitter_name: Submitter name in nominative case.
        submitter_title: Title prefix ("Poslanec" / "Poslankyně" / etc.).
        sub_amendments: List of numbered sub-amendments.
        raw_text: Full text block for this letter section.
    """

    letter: str
    submitter_name: str
    submitter_title: str = ""
    sub_amendments: list[PdfSubAmendment] = field(default_factory=list)
    raw_text: str = ""


def _clean_submitter_name(raw_name: str) -> str:
    """Clean and normalize a submitter name from PDF header.

    Handles trailing punctuation, extra whitespace, and multi-submitter
    patterns like "Jan Novák, poslanec Petr Nový".

    Args:
        raw_name: Raw name string from regex capture.

    Returns:
        Cleaned name string (first submitter only for multi-submitter).
    """
    name = raw_name.strip().rstrip(",.:;")
    # Take first submitter if comma-separated with title
    if ", poslan" in name.lower():
        name = name[: name.lower().index(", poslan")].strip()
    return name


def _parse_sub_amendments(
    letter: str,
    section_text: str,
) -> list[PdfSubAmendment]:
    """Parse sub-amendments (A.1, A.2, ...) within a letter section.

    Args:
        letter: The letter designation (e.g. "A").
        section_text: Text of just this letter's section.

    Returns:
        List of parsed sub-amendments.
    """
    matches = list(_SUB_AMENDMENT_RE.finditer(section_text))
    if not matches:
        return []

    subs: list[PdfSubAmendment] = []
    for i, m in enumerate(matches):
        num = m.group(1)
        sd = m.group(2) or ""
        label = f"{letter}.{num}"

        # Text runs from end of this match to start of next match (or end)
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(section_text)
        text = section_text[start:end].strip()

        subs.append(PdfSubAmendment(label=label, sd_number=sd, text=text))

    return subs


def parse_amendment_pdf(text: str) -> list[PdfAmendment]:
    """Parse extracted amendment PDF text into structured amendments.

    Splits by top-level letter headers (A., B., C., ...) and extracts
    submitter names and sub-amendment text for each.

    Args:
        text: Raw text extracted from the amendment PDF.

    Returns:
        List of PdfAmendment, one per letter. Returns empty list if
        the text has no parseable structure.
    """
    if not text or not text.strip():
        return []

    # Find all letter header matches
    matches = list(_LETTER_HEADER_RE.finditer(text))

    # Fallback to alternative format without title prefix
    if not matches:
        matches = list(_LETTER_HEADER_ALT_RE.finditer(text))
        if matches:
            return _parse_with_alt_headers(text, matches)

    if not matches:
        logger.debug("No amendment letter headers found in PDF text ({} chars)", len(text))
        return []

    amendments: list[PdfAmendment] = []

    for i, m in enumerate(matches):
        letter = m.group(1)
        title = m.group(2)
        raw_name = m.group(3)
        name = _clean_submitter_name(raw_name)

        # Section runs from end of header line to start of next header (or end)
        section_start = m.end()
        section_end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        section_text = text[section_start:section_end]
        raw_text = text[m.start() : section_end].strip()

        sub_amendments = _parse_sub_amendments(letter, section_text)

        amendments.append(
            PdfAmendment(
                letter=letter,
                submitter_name=name,
                submitter_title=title,
                sub_amendments=sub_amendments,
                raw_text=raw_text,
            )
        )

    logger.debug(
        "Parsed {} amendment letters from PDF ({} chars)",
        len(amendments),
        len(text),
    )
    return amendments


def _parse_with_alt_headers(
    text: str,
    matches: list[re.Match[str]],
) -> list[PdfAmendment]:
    """Parse amendments using alternative header format (no title prefix).

    Args:
        text: Full PDF text.
        matches: Regex matches from _LETTER_HEADER_ALT_RE.

    Returns:
        List of PdfAmendment.
    """
    amendments: list[PdfAmendment] = []

    for i, m in enumerate(matches):
        letter = m.group(1)
        name = _clean_submitter_name(m.group(2))

        section_start = m.end()
        section_end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        section_text = text[section_start:section_end]
        raw_text = text[m.start() : section_end].strip()

        sub_amendments = _parse_sub_amendments(letter, section_text)

        amendments.append(
            PdfAmendment(
                letter=letter,
                submitter_name=name,
                submitter_title="",
                sub_amendments=sub_amendments,
                raw_text=raw_text,
            )
        )

    return amendments
