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
        submitter_names: Submitter names in nominative case (may be multiple).
        submitter_titles: Title prefixes ("Poslanec" / "Poslankyně" / etc.).
        sub_amendments: List of numbered sub-amendments.
        raw_text: Full text block for this letter section.
    """

    letter: str
    submitter_names: list[str] = field(default_factory=list)
    submitter_titles: list[str] = field(default_factory=list)
    sub_amendments: list[PdfSubAmendment] = field(default_factory=list)
    raw_text: str = ""


_MULTI_SUBMITTER_SPLIT_RE = re.compile(
    r"[,\s]+(?:a\s+)?poslan\w*\s+",
    re.IGNORECASE,
)


def _clean_single_name(name: str) -> str:
    """Clean a single submitter name: strip punctuation, whitespace, titles.

    Args:
        name: Raw single name string.

    Returns:
        Cleaned name string.
    """
    name = name.strip().rstrip(",.:;")
    # Remove academic titles
    name = re.sub(
        r"\b(?:Ing|Mgr|JUDr|MUDr|PhDr|RNDr|doc|prof|Bc|MBA|Ph\.D)\.\s*",
        "",
        name,
    )
    return name.strip()


def _parse_submitter_names(raw_name: str) -> list[str]:
    """Parse one or more submitter names from a PDF header string.

    Splits on patterns like ", poslanec ", ", poslankyně ", " a poslanec "
    to extract all names from multi-submitter headers such as:
      "Mračková Vildumetzová, poslanec Novák, poslankyně Nová, poslanec Hora"

    Args:
        raw_name: Raw name string from regex capture.

    Returns:
        List of cleaned submitter name strings.
    """
    raw_name = raw_name.strip().rstrip(",.:;")
    if not raw_name:
        return []

    parts = _MULTI_SUBMITTER_SPLIT_RE.split(raw_name)
    names: list[str] = []
    for part in parts:
        cleaned = _clean_single_name(part)
        if cleaned:
            names.append(cleaned)
    return names


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
        names = _parse_submitter_names(raw_name)

        # Section runs from end of header line to start of next header (or end)
        section_start = m.end()
        section_end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        section_text = text[section_start:section_end]
        raw_text = text[m.start() : section_end].strip()

        sub_amendments = _parse_sub_amendments(letter, section_text)

        amendments.append(
            PdfAmendment(
                letter=letter,
                submitter_names=names,
                submitter_titles=[title],
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
        names = _parse_submitter_names(m.group(2))

        section_start = m.end()
        section_end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        section_text = text[section_start:section_end]
        raw_text = text[m.start() : section_end].strip()

        sub_amendments = _parse_sub_amendments(letter, section_text)

        amendments.append(
            PdfAmendment(
                letter=letter,
                submitter_names=names,
                sub_amendments=sub_amendments,
                raw_text=raw_text,
            )
        )

    return amendments
