"""Download and extract ZIP files from psp.cz open data."""

import zipfile
from pathlib import Path

import httpx
from loguru import logger

from pspcz_analyzer.config import (
    DEFAULT_CACHE_DIR,
    EXTRACTED_DIR,
    PERIOD_YEARS,
    POSLANCI_URL,
    RAW_DIR,
    SCHUZE_URL,
    TISKY_URL,
    VOTING_URL_TEMPLATE,
)


def _ensure_dirs(cache_dir: Path) -> tuple[Path, Path]:
    raw = cache_dir / RAW_DIR
    extracted = cache_dir / EXTRACTED_DIR
    raw.mkdir(parents=True, exist_ok=True)
    extracted.mkdir(parents=True, exist_ok=True)
    return raw, extracted


def _download_file(url: str, dest: Path, force: bool = False) -> Path:
    """Download a file if it doesn't exist or force is True."""
    if dest.exists() and not force:
        logger.info("Using cached {}", dest.name)
        return dest

    logger.info("Downloading {} ...", url)
    with httpx.Client(timeout=120, follow_redirects=True) as client:
        with client.stream("GET", url) as response:
            response.raise_for_status()
            with open(dest, "wb") as f:
                for chunk in response.iter_bytes(chunk_size=65536):
                    f.write(chunk)

    logger.info("Downloaded {} ({:.1f} MB)", dest.name, dest.stat().st_size / 1e6)
    return dest


def _extract_zip(zip_path: Path, dest_dir: Path) -> Path:
    """Extract a ZIP file into dest_dir/<stem>/."""
    extract_to = dest_dir / zip_path.stem
    if extract_to.exists():
        # Re-extract if ZIP is newer than extracted dir
        if zip_path.stat().st_mtime <= extract_to.stat().st_mtime:
            logger.info("Already extracted {}", extract_to.name)
            return extract_to

    logger.info("Extracting {} ...", zip_path.name)
    extract_to.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_to)

    logger.info("Extracted to {}", extract_to)
    return extract_to


def download_voting_data(
    period: int,
    cache_dir: Path = DEFAULT_CACHE_DIR,
    force: bool = False,
) -> Path:
    """Download and extract voting data for a given electoral period.

    Returns the path to the extracted directory.
    """
    year = PERIOD_YEARS[period]
    raw, extracted = _ensure_dirs(cache_dir)

    zip_name = f"hl-{year}ps.zip"
    url = VOTING_URL_TEMPLATE.format(year=year)
    zip_path = _download_file(url, raw / zip_name, force=force)
    return _extract_zip(zip_path, extracted)


def download_poslanci_data(
    cache_dir: Path = DEFAULT_CACHE_DIR,
    force: bool = False,
) -> Path:
    """Download and extract MP/party data.

    Returns the path to the extracted directory.
    """
    raw, extracted = _ensure_dirs(cache_dir)

    zip_path = _download_file(POSLANCI_URL, raw / "poslanci.zip", force=force)
    return _extract_zip(zip_path, extracted)


def download_schuze_data(
    cache_dir: Path = DEFAULT_CACHE_DIR,
    force: bool = False,
) -> Path:
    """Download and extract session/agenda data."""
    raw, extracted = _ensure_dirs(cache_dir)
    zip_path = _download_file(SCHUZE_URL, raw / "schuze.zip", force=force)
    return _extract_zip(zip_path, extracted)


def download_tisky_data(
    cache_dir: Path = DEFAULT_CACHE_DIR,
    force: bool = False,
) -> Path:
    """Download and extract parliamentary prints data."""
    raw, extracted = _ensure_dirs(cache_dir)
    zip_path = _download_file(TISKY_URL, raw / "tisky.zip", force=force)
    return _extract_zip(zip_path, extracted)
