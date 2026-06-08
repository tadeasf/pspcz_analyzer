"""Central configuration: URLs, paths, constants."""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# psp.cz open data base URL
PSP_BASE_URL = "https://www.psp.cz/eknih/cdrom/opendata"

# Download URL templates
VOTING_URL_TEMPLATE = f"{PSP_BASE_URL}/hl-{{year}}ps.zip"
POSLANCI_URL = f"{PSP_BASE_URL}/poslanci.zip"
SCHUZE_URL = f"{PSP_BASE_URL}/schuze.zip"
TISKY_URL = f"{PSP_BASE_URL}/tisky.zip"

# Local cache (overridable via PSPCZ_CACHE_DIR env var for Docker)
DEFAULT_CACHE_DIR = Path(
    os.environ.get("PSPCZ_CACHE_DIR", str(Path.home() / ".cache" / "pspcz-analyzer" / "psp"))
)
RAW_DIR = "raw"
EXTRACTED_DIR = "extracted"
PARQUET_DIR = "parquet"

# Electoral period -> year used in ZIP filenames on psp.cz
PERIOD_YEARS: dict[int, str] = {
    10: "2025",
    9: "2021",
    8: "2017",
    7: "2013",
    6: "2010",
    5: "2006",
    4: "2002",
    3: "1998",
    2: "1996",
    1: "1993",
}

# Electoral period -> human-readable label (start–end)
PERIOD_LABELS: dict[int, str] = {
    10: "2025–present",
    9: "2021–2025",
    8: "2017–2021",
    7: "2013–2017",
    6: "2010–2013",
    5: "2006–2010",
    4: "2002–2006",
    3: "1998–2002",
    2: "1996–1998",
    1: "1993–1996",
}

# Electoral period -> organ ID in psp.cz database
# (id_obdobi in poslanec table uses organ IDs, not period numbers)
PERIOD_ORGAN_IDS: dict[int, int] = {
    10: 174,
    9: 173,
    8: 172,
    7: 171,
    6: 170,
    5: 169,
    4: 168,
    3: 167,
    2: 166,
    1: 165,
}

DEFAULT_PERIOD = 10

# Government-coalition detection (services/coalition_detector.py) reads the
# governing coalition automatically from the chamber's investiture confidence
# vote — no manual per-period list. A coalition is only emitted when a passed
# confidence vote is clearly polarized: at least one club votes YES with a
# fraction >= CONFIDENCE_YES_BLOC and at least one votes YES with a fraction
# <= CONFIDENCE_NO_BLOC, AND the vote is genuinely contested (overall YES share
# <= CONFIDENCE_MAX_YES_SHARE — this rejects near-unanimous procedural votes that
# share the confidence agenda-item name). Otherwise the period is "unknown".
CONFIDENCE_YES_BLOC: float = 0.6
CONFIDENCE_NO_BLOC: float = 0.4
CONFIDENCE_MAX_YES_SHARE: float = 0.75

# Club abbreviation fragments that mark non-affiliated MPs (no gov/opposition side).
INDEPENDENT_CLUB_MARKERS: tuple[str, ...] = ("Nezařaz", "nez.")

# Number of newest electoral periods to process with AI (0 = all)
AI_PERIODS_LIMIT: int = int(os.environ.get("AI_PERIODS_LIMIT", "3"))

# UNL format constants
UNL_ENCODING = "windows-1250"
UNL_SEPARATOR = "|"

# Tisky PDF pipeline
TISKY_PDF_DIR = "tisky_pdf"
TISKY_TEXT_DIR = "tisky_text"
TISKY_META_DIR = "tisky_meta"
PSP_TISKT_URL_TEMPLATE = "https://www.psp.cz/sqw/text/tiskt.sqw?o={period}&ct={ct}&ct1=0"
PSP_HISTORIE_URL_TEMPLATE = "https://www.psp.cz/sqw/historie.sqw?o={period}&t={ct}"
TISKY_HISTORIE_DIR = "tisky_historie"

# Terminal tisk statuses — a bill in one of these states is finished and will
# not change again, so the daily refresh skips re-scraping it. Any other status
# (e.g. "projednáváno", "schváleno sněmovnou", "podepsáno") is still "active":
# it can advance another legislative step, so refresh re-scrapes it.
TERMINAL_TISK_STATUSES: frozenset[str] = frozenset({"vyhlášeno", "zamítnuto", "staženo"})

# Legislative evolution: law changes, related bills, sub-tisk versions
PSP_LAW_CHANGES_URL_TEMPLATE = "https://www.psp.cz/sqw/historie.sqw?o={period}&t={ct}&snzp=1"
PSP_RELATED_BILLS_URL_TEMPLATE = "https://www.psp.cz/sqw/tisky.sqw?idsb={idsb}"
PSP_SUBTISKT_URL_TEMPLATE = "https://www.psp.cz/sqw/text/tiskt.sqw?O={period}&CT={ct}&CT1={ct1}"
TISKY_LAW_CHANGES_DIR = "tisky_law_changes"
TISKY_RELATED_BILLS_DIR = "related_bills"
TISKY_VERSION_DIFFS_DIR = "tisky_version_diffs"
PSP_ORIG2_BASE_URL = "https://www.psp.cz/sqw/text/orig2.sqw"
PSP_REQUEST_DELAY = 1.0  # seconds between requests to psp.cz
# Upper bound on steno sub-pages downloaded per agenda item (bod). Amendment
# voting is at the END of a bod's discussion, so when a bod exceeds this we keep
# the LAST N sub-pages. Prevents pathological 100-220 page bods from flooding I/O.
STENO_MAX_SUBPAGES_PER_BOD = 250
# Day-page TOCs link steno sub-pages by speaker anchor, so voting-continuation
# pages (no speaker) are skipped. After collecting linked sub-pages we fill
# numeric gaps no larger than this between consecutive collected pages, so e.g.
# s017212 between s017211 and s017213 is fetched. Small bound keeps it from
# bridging unrelated segments (e.g. votes split across two sitting days).
STENO_SUBPAGE_GAP_FILL = 3

# LLM provider selection: "ollama" (default) or "openai" (any OpenAI-compatible API)
LLM_PROVIDER = os.environ.get("LLM_PROVIDER", "ollama")

# Ollama (local LLM) integration — optional, falls back to keyword classification
# For remote HTTPS Ollama, set OLLAMA_BASE_URL (e.g. "https://ollama.example.com")
# and OLLAMA_API_KEY (Bearer token for Authorization header).
OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_API_KEY = os.environ.get("OLLAMA_API_KEY", "")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "qwen3:8b")
LLM_STRUCTURED_OUTPUT = (
    os.environ.get("LLM_STRUCTURED_OUTPUT", os.environ.get("OLLAMA_STRUCTURED_OUTPUT", "1")) == "1"
)
TISK_SHORTENER = os.environ.get("TISK_SHORTENER", "0") == "1"
LLM_TIMEOUT = 300.0  # per-request (generous for CPU inference)
LLM_HEALTH_TIMEOUT = 5.0  # connectivity check
LLM_EMPTY_RETRIES = int(os.environ.get("LLM_EMPTY_RETRIES", "2"))
LLM_MAX_TEXT_CHARS = int(
    os.environ.get("LLM_MAX_TEXT_CHARS", "240000")
)  # ~80k tokens @ 3 chars/tok (Czech text)
LLM_VERBATIM_CHARS = int(
    os.environ.get("LLM_VERBATIM_CHARS", "180000")
)  # verbatim portion before structural extraction
LLM_MAX_COMPARISON_CHARS = int(
    os.environ.get("LLM_MAX_COMPARISON_CHARS", "120000")
)  # per-text limit for version comparisons (~40k tok each)

# Version diff cost control — max pairs to compare per tisk (0 = all)
VERSION_DIFF_MAX_PAIRS = int(os.environ.get("VERSION_DIFF_MAX_PAIRS", "2"))

# OpenAI-compatible API integration (OpenAI, Azure OpenAI, Together, Groq, vLLM, etc.)
# Used when LLM_PROVIDER=openai
OPENAI_BASE_URL = os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")

# Daily data refresh — re-downloads psp.cz data and reloads in-memory state
DAILY_REFRESH_ENABLED = os.environ.get("DAILY_REFRESH_ENABLED", "1") == "1"
DAILY_REFRESH_HOUR = int(os.environ.get("DAILY_REFRESH_HOUR", "3"))

# Server port (overridable for Docker and deployment)
PORT = int(os.environ.get("PORT", "8000"))

# Amendment voting analysis — steno record parsing
AMENDMENTS_ENABLED = os.environ.get("AMENDMENTS_ENABLED", "1") == "1"
AMENDMENT_CACHE_SUBDIR = "amendments"

# Dev pipeline skip flags — skip expensive stages during development
DEV_SKIP_CLASSIFY_AND_SUMMARIZE = os.environ.get("DEV_SKIP_CLASSIFY_AND_SUMMARIZE", "0") == "1"
DEV_SKIP_VERSION_DIFFS = os.environ.get("DEV_SKIP_VERSION_DIFFS", "0") == "1"
DEV_SKIP_AMENDMENTS = os.environ.get("DEV_SKIP_AMENDMENTS", "0") == "1"

# GitHub feedback — user feedback creates GitHub issues
GITHUB_FEEDBACK_ENABLED = os.environ.get("GITHUB_FEEDBACK_ENABLED", "0") == "1"
GITHUB_FEEDBACK_TOKEN = os.environ.get("GITHUB_FEEDBACK_TOKEN", "")
GITHUB_FEEDBACK_REPO = os.environ.get("GITHUB_FEEDBACK_REPO", "tadeasf/pspcz_analyzer")
GITHUB_FEEDBACK_LABELS = os.environ.get("GITHUB_FEEDBACK_LABELS", "user-feedback").split(",")

# Admin dashboard — backend-only authentication and access control
ADMIN_USERNAME = os.environ.get("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD_HASH = os.environ.get("ADMIN_PASSWORD_HASH", "")
ADMIN_ALLOWED_IPS = os.environ.get("ADMIN_ALLOWED_IPS", "127.0.0.1,::1,172.16.0.0/12")
ADMIN_SESSION_SECRET = os.environ.get("ADMIN_SESSION_SECRET", "")
ADMIN_PORT = int(os.environ.get("ADMIN_PORT", "8001"))
