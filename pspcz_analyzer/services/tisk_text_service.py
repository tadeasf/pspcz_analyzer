"""Service for querying cached tisk text files."""

from pathlib import Path

from pspcz_analyzer.config import DEFAULT_CACHE_DIR, TISKY_TEXT_DIR


class TiskTextService:
    """Lightweight service to query cached extracted text from tisk PDFs."""

    def __init__(self, cache_dir: Path = DEFAULT_CACHE_DIR) -> None:
        self.cache_dir = cache_dir

    def _text_dir(self, period: int) -> Path:
        return self.cache_dir / TISKY_TEXT_DIR / str(period)

    def get_text(self, period: int, ct: int) -> str | None:
        """Read cached text for a tisk, or None if not available."""
        path = self._text_dir(period) / f"{ct}.txt"
        if not path.exists():
            return None
        return path.read_text(encoding="utf-8")

    def has_text(self, period: int, ct: int) -> bool:
        """Check whether extracted text exists for a tisk."""
        return (self._text_dir(period) / f"{ct}.txt").exists()

    def available_tisky(self, period: int) -> list[int]:
        """List all ct numbers that have extracted text for a period."""
        text_dir = self._text_dir(period)
        if not text_dir.exists():
            return []
        return sorted(
            int(p.stem) for p in text_dir.glob("*.txt") if p.stem.isdigit()
        )
