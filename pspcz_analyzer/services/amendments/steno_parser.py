"""Stenographic record parser for amendment vote extraction.

Parses HTML stenographic records from psp.cz to identify which votes
correspond to which amendment letters. Uses validated regex patterns
matching the highly standardized phrases used by the chair/rapporteur.
"""

import re
from dataclasses import dataclass, field
from html import unescape as html_unescape

from loguru import logger
from selectolax.parser import HTMLParser

from pspcz_analyzer.models.amendment_models import AmendmentVote

# ── Compiled regex patterns ────────────────────────────────────────────────

# Start of amendment voting section
_START_RE = re.compile(
    r"přikročíme.*?k\s+hlasování\s+o\s+pozměňovac",
    re.IGNORECASE | re.DOTALL,
)

# Amendment letter designation
_LETTER_RE = re.compile(
    r"(?:označen[éý]m?\s+písmenem|pod\s+označením|"
    r"pozměňovac\w+\s+návrh\w*\s+(?:pod\s+)?písmenem?)\s+"
    r"([A-Z]\d?(?:(?:,\s*|\s+a\s+)[A-Z]\d?)*)",
    re.IGNORECASE,
)

# Committee (výbor) stance
_COMMITTEE_RE = re.compile(
    r"[Ss]tanovisko\s+výboru.*?(?:je\s*)?[-–]?\s*"
    r"(doporučující|nedoporučující|bez\s+stanoviska)",
    re.IGNORECASE,
)

# Proposer (předkladatel) stance — e.g. "Předkladatel? (Souhlas.)"
_PROPOSER_RE = re.compile(
    r"[Pp]ředkladatel\w*\??\s*\(?(\w+)\.\)?",
    re.IGNORECASE,
)

# Vote result with vote number
_VOTE_RESULT_RE = re.compile(
    r"[Hh]lasování\s+(?:číslo|č\.)\s*(\d+)"
    r".*?"
    r"(Přijato|Zamítnuto|Návrh\s+byl\s+přijat|Návrh\s+nebyl\s+přijat)",
    re.DOTALL,
)

# Final passage vote — "zákon jako celku"
_FINAL_VOTE_RE = re.compile(
    r"návrhu?\s+zákona\s+jako\s+celku",
    re.IGNORECASE,
)

# Vote challenge — "zpochybňuji hlasování"
_CHALLENGE_RE = re.compile(
    r"zpochybňuji\s+hlasování",
    re.IGNORECASE,
)

# Amendment withdrawal
_WITHDRAWAL_RE = re.compile(
    r"(?:stah(?:uji|uje)|stažen[ío])\s+pozměňovac",
    re.IGNORECASE,
)

# §95 legislative-technical corrections
_LEG_TECH_RE = re.compile(
    r"(?:§\s*95|legislativně[\s-]+technick)",
    re.IGNORECASE,
)

# Submitter name extraction — Pattern A (dominant): letter + genitive name
# e.g. "pozměňovací návrh B1 pana poslance Exnera"
_SUBMITTER_AFTER_LETTER_RE = re.compile(
    r"pozměňovac\w+\s+návrh\w*\s+"
    r"(?:pod\s+)?(?:písmenem?\s+|označen\w+\s+písmenem?\s+)?"
    r"[A-Z]\d?(?:(?:,\s*|\s+a\s+)[A-Z]\d?)*\s+"
    r"(?:pana\s+|paní\s+)?"
    r"(?:poslanc\w+|poslankyně)\s+"
    r"((?:(?:Ing|Mgr|JUDr|MUDr|PhDr|RNDr|doc|prof|Bc|MBA|Ph\.D)\.\s+)*"
    r"[A-ZÁČĎÉĚÍŇÓŘŠŤÚŮÝŽ][a-záčďéěíňóřšťúůýž]+)",
    re.IGNORECASE,
)

# Submitter name extraction — Pattern B: "předloženy" (singular/plural past tense)
# e.g. "předloženy panem poslancem Bauerem B1 až B6"
_SUBMITTER_PREDLOZENY_RE = re.compile(
    r"předložen[ýáy]\s+"
    r"(?:panem\s+|paní\s+)?"
    r"(?:poslancem|poslankyní)\s+"
    r"((?:(?:Ing|Mgr|JUDr|MUDr|PhDr|RNDr|doc|prof|Bc|MBA|Ph\.D)\.\s+)*"
    r"[A-ZÁČĎÉĚÍŇÓŘŠŤÚŮÝŽ][a-záčďéěíňóřšťúůýž]+)",
    re.IGNORECASE,
)


@dataclass
class _ParseBlock:
    """Intermediate representation of a text block between votes."""

    text: str
    letter: str = ""
    committee_stance: str | None = None
    proposer_stance: str | None = None
    vote_number: int | None = None
    result: str = ""
    is_final: bool = False
    is_challenge: bool = False
    is_withdrawal: bool = False
    is_leg_tech: bool = False
    grouped_letters: list[str] = field(default_factory=list)
    submitter_names: list[str] = field(default_factory=list)


def _clean_html(html: str) -> str:
    """Strip HTML tags, decode entities, normalize whitespace.

    Args:
        html: Raw HTML string from steno page.

    Returns:
        Cleaned plain text.
    """
    text = HTMLParser(html).text(separator=" ", strip=True) or ""
    text = html_unescape(text)
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _extract_section(text: str) -> str:
    """Extract the amendment voting section from cleaned steno text.

    Args:
        text: Full cleaned steno text.

    Returns:
        Substring containing the amendment voting section, or empty string.
    """
    match = _START_RE.search(text)
    if not match:
        return ""
    return text[match.start() :]


def _normalize_result(raw: str) -> str:
    """Map raw Czech result text to a normalized English label.

    Args:
        raw: Raw result string from steno (e.g. "Přijato").

    Returns:
        Normalized result: "accepted", "rejected", or "unknown".
    """
    lower = raw.lower().strip()
    if "přijat" in lower:
        return "accepted"
    if "zamítnut" in lower:
        return "rejected"
    return "unknown"


def _parse_letter_groups(letter_str: str) -> tuple[str, list[str]]:
    """Parse a letter string that may contain grouped amendments.

    Args:
        letter_str: Raw letter string, e.g. "E1 a F2" or "A, B".

    Returns:
        Tuple of (primary_letter, grouped_with_letters).
    """
    # Split on " a " and ", "
    parts = re.split(r"\s+a\s+|,\s*", letter_str.strip())
    parts = [p.strip() for p in parts if p.strip()]
    if not parts:
        return letter_str.strip(), []
    primary = parts[0]
    grouped = parts[1:] if len(parts) > 1 else []
    return primary, grouped


def _split_into_blocks(section: str) -> list[str]:
    """Split the amendment section into blocks at each vote result.

    Each block contains the letter introduction, vote number, and result.
    Splits AFTER each vote result so that text following a result (e.g.
    the next amendment's introduction) belongs to the next block.

    Args:
        section: The amendment voting section text.

    Returns:
        List of text blocks, each ending at a vote result.
    """
    matches = list(_VOTE_RESULT_RE.finditer(section))
    if not matches:
        return [section] if section.strip() else []

    blocks: list[str] = []
    start = 0
    for m in matches:
        end = m.end()
        block = section[start:end].strip()
        if block:
            blocks.append(block)
        start = end

    # Trailing text after last vote (e.g. withdrawal announcements)
    if start < len(section):
        trailing = section[start:].strip()
        if trailing:
            blocks.append(trailing)

    return blocks


def _parse_block(block_text: str) -> _ParseBlock:
    """Parse a single text block into a _ParseBlock.

    Args:
        block_text: Text of one block from the steno section.

    Returns:
        Parsed block with extracted fields.
    """
    pb = _ParseBlock(text=block_text)

    # Extract letter
    letter_match = _LETTER_RE.search(block_text)
    if letter_match:
        raw_letter = letter_match.group(1).strip()
        pb.letter, pb.grouped_letters = _parse_letter_groups(raw_letter)

    # Committee stance
    comm_match = _COMMITTEE_RE.search(block_text)
    if comm_match:
        pb.committee_stance = comm_match.group(1).strip().lower()

    # Proposer stance
    prop_match = _PROPOSER_RE.search(block_text)
    if prop_match:
        raw_stance = prop_match.group(1).strip().lower()
        match raw_stance:
            case "souhlas":
                pb.proposer_stance = "souhlas"
            case "nesouhlas":
                pb.proposer_stance = "nesouhlas"
            case "neutrální":
                pb.proposer_stance = "neutrální"
            case _:
                pb.proposer_stance = raw_stance

    # Vote result
    result_match = _VOTE_RESULT_RE.search(block_text)
    if result_match:
        pb.vote_number = int(result_match.group(1))
        pb.result = _normalize_result(result_match.group(2))

    # Final vote
    pb.is_final = bool(_FINAL_VOTE_RE.search(block_text))

    # Challenge
    pb.is_challenge = bool(_CHALLENGE_RE.search(block_text))

    # Withdrawal
    pb.is_withdrawal = bool(_WITHDRAWAL_RE.search(block_text))

    # Legislative-technical
    pb.is_leg_tech = bool(_LEG_TECH_RE.search(block_text))

    # Submitter name — try letter+genitive pattern first, fall back to předložen*
    submitter_match = _SUBMITTER_AFTER_LETTER_RE.search(block_text)
    if submitter_match:
        pb.submitter_names = [submitter_match.group(1).strip()]
    else:
        submitter_match = _SUBMITTER_PREDLOZENY_RE.search(block_text)
        if submitter_match:
            pb.submitter_names = [submitter_match.group(1).strip()]

    return pb


def _blocks_to_amendments(blocks: list[_ParseBlock]) -> list[AmendmentVote]:
    """Convert parsed blocks into AmendmentVote objects.

    Handles challenges (re-votes) by linking back to the original vote.

    Args:
        blocks: List of parsed blocks.

    Returns:
        List of AmendmentVote objects.
    """
    amendments: list[AmendmentVote] = []
    # Track last vote numbers to detect challenges
    last_vote_number: int | None = None
    last_letter: str = ""

    for block in blocks:
        if block.is_withdrawal:
            if block.letter:
                amendments.append(
                    AmendmentVote(
                        letter=block.letter,
                        vote_number=0,
                        is_withdrawn=True,
                        grouped_with=block.grouped_letters,
                        submitter_names=block.submitter_names,
                    )
                )
            continue

        if block.vote_number is None:
            continue

        is_revote = block.is_challenge and last_vote_number is not None
        letter = block.letter or last_letter

        amendment = AmendmentVote(
            letter=letter,
            vote_number=block.vote_number,
            result=block.result,
            committee_stance=block.committee_stance,
            proposer_stance=block.proposer_stance,
            is_final_vote=block.is_final,
            is_leg_tech=block.is_leg_tech,
            is_revote=is_revote,
            original_vote_number=last_vote_number if is_revote else None,
            grouped_with=block.grouped_letters,
            submitter_names=block.submitter_names,
        )

        amendments.append(amendment)
        last_vote_number = block.vote_number
        if block.letter:
            last_letter = block.letter

    return amendments


def parse_steno_amendments(
    html: str,
    period: int = 0,
    schuze: int = 0,
    bod: int = 0,
) -> tuple[list[AmendmentVote], float, list[str]]:
    """Parse a stenographic record HTML page for amendment votes.

    Args:
        html: Raw HTML of the stenographic record page.
        period: Electoral period (for logging).
        schuze: Session number (for logging).
        bod: Agenda item number (for logging).

    Returns:
        Tuple of (amendments, confidence, warnings).
    """
    warnings: list[str] = []
    confidence = 1.0

    # Clean HTML
    text = _clean_html(html)

    # Find amendment section
    section = _extract_section(text)
    if not section:
        warnings.append("No amendment voting section found in steno text")
        confidence -= 0.3
        logger.debug(
            "No amendment section found in steno for period={} schuze={} bod={}",
            period,
            schuze,
            bod,
        )
        return [], max(0.0, confidence), warnings

    # Split into blocks
    raw_blocks = _split_into_blocks(section)
    if not raw_blocks:
        warnings.append("No vote blocks found in amendment section")
        confidence -= 0.2
        return [], max(0.0, confidence), warnings

    # Parse each block
    parsed_blocks = [_parse_block(b) for b in raw_blocks]

    # Count blocks without vote numbers (unmatched)
    unmatched = sum(1 for b in parsed_blocks if b.vote_number is None and not b.is_withdrawal)
    if unmatched > 0:
        confidence -= 0.1 * min(unmatched, 3)
        warnings.append(f"{unmatched} block(s) without vote numbers")

    # Count blocks without letter designations
    no_letter = sum(
        1 for b in parsed_blocks if not b.letter and b.vote_number is not None and not b.is_final
    )
    if no_letter > 0:
        confidence -= 0.05 * min(no_letter, 3)
        warnings.append(f"{no_letter} vote(s) without amendment letter")

    # Convert to amendments
    amendments = _blocks_to_amendments(parsed_blocks)

    confidence = max(0.0, min(1.0, confidence))

    logger.debug(
        "Parsed {} amendments from steno period={} schuze={} bod={} (confidence={:.2f})",
        len(amendments),
        period,
        schuze,
        bod,
        confidence,
    )

    return amendments, confidence, warnings
