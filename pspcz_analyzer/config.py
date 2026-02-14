"""Central configuration: URLs, paths, constants."""

from pathlib import Path

# psp.cz open data base URL
PSP_BASE_URL = "https://www.psp.cz/eknih/cdrom/opendata"

# Download URL templates
VOTING_URL_TEMPLATE = f"{PSP_BASE_URL}/hl-{{year}}ps.zip"
POSLANCI_URL = f"{PSP_BASE_URL}/poslanci.zip"
SCHUZE_URL = f"{PSP_BASE_URL}/schuze.zip"
TISKY_URL = f"{PSP_BASE_URL}/tisky.zip"

# Local cache
DEFAULT_CACHE_DIR = Path.home() / ".cache" / "pspcz-analyzer" / "psp"
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

# UNL format constants
UNL_ENCODING = "windows-1250"
UNL_SEPARATOR = "|"

# Tisky PDF pipeline
TISKY_PDF_DIR = "tisky_pdf"
TISKY_TEXT_DIR = "tisky_text"
TISKY_META_DIR = "tisky_meta"
PSP_TISKT_URL_TEMPLATE = "https://www.psp.cz/sqw/text/tiskt.sqw?o={period}&ct={ct}&ct1=0"
PSP_ORIG2_BASE_URL = "https://www.psp.cz/sqw/text/orig2.sqw"
PSP_REQUEST_DELAY = 1.0  # seconds between requests to psp.cz
