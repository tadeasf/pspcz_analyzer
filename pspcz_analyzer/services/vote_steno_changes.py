"""Whether the steno text under a vote changed when psp.cz replaced a page.

A vote record is reused when its window hash (``sha``, over the whole page text as
before) is unchanged. When it differs, the window is compared again on the speech text
only (``body_sha``, without site menus and the authorization note, typography unified):
a page that only became authorized or proofread keeps its record, a changed speech is
extracted again and the replaced record is kept under ``previous``.

Records written before ``body_sha`` existed get their baseline from the replaced pages,
but only when those pages reproduce the stored ``sha``; otherwise the change cannot be
told apart and the vote is extracted again, as before.
"""

import hashlib
from datetime import UTC, datetime
from typing import Any

from pspcz_analyzer.services.steno_page_state import body_html, normalize_text
from pspcz_analyzer.services.vote_steno_text import WINDOW_CHARS, _plain, norm_quote, sitting_text

# fields of a replaced record kept for the admin review
_KEPT = ("question", "question_en", "pro", "pro_en", "quote", "kind", "subkind", "proposer")
_KEPT += ("status", "model", "sha", "body_sha", "steno_status")
_QUOTE_PAGES_BACK = 3  # a lead-in rarely spans more pages before the vote's own


def _tail(text: str, span: dict[str, Any] | None) -> str:
    return text[span["start"] : span["end"]][-WINDOW_CHARS:] if span else ""


def _raw_sha(window: str) -> str:
    """The record's ``sha`` as vote_steno computes it (no normalization)."""
    return hashlib.sha256(window.encode()).hexdigest()[:16]


def _sha(window: str) -> str:
    return hashlib.sha256(normalize_text(window).encode()).hexdigest()[:16] if window else ""


def _bodies(pages: list[tuple[str, str]]) -> list[tuple[str, str]]:
    return [(name, body_html(page)) for name, page in pages]


class WindowCompare:
    """Old and new vote windows of one sitting (texts are built only when needed).

    Args:
        pages: Current ``(page name, html)`` of the sitting.
        previous: ``{page name: replaced html}`` of pages fetched anew in this run.
    """

    def __init__(self, pages: list[tuple[str, str]], previous: dict[str, str]) -> None:
        self._pages = pages
        self._previous = previous
        self._body: tuple[str, dict[int, dict[str, Any]]] | None = None
        self._old: tuple[tuple[str, dict], tuple[str, dict]] | None = None

    def body_sha(self, vid: int) -> str:
        """Hash of the vote's window on the speech text of the current pages."""
        body = self._body
        if body is None:
            body = self._body = sitting_text(_bodies(self._pages))
        text, spans = body
        return _sha(_tail(text, spans.get(vid)))

    def _old_body(self, vid: int, prev_sha: str) -> tuple[str, str] | None:
        """``(body sha, body window)`` from the replaced pages, if they made ``prev_sha``."""
        if not self._previous:
            return None
        old = self._old
        if old is None:
            old_pages = [(n, self._previous.get(n, h)) for n, h in self._pages]
            old = self._old = (sitting_text(old_pages), sitting_text(_bodies(old_pages)))
        (full, full_spans), (body, body_spans) = old
        if _raw_sha(_tail(full, full_spans.get(vid))) != prev_sha:
            return None
        window = _tail(body, body_spans.get(vid))
        return _sha(window), window

    def unchanged(self, vid: int, prev: dict) -> bool:
        """Whether the speech text under the vote is the one the record was made from."""
        before = prev.get("body_sha") or (self._old_body(vid, prev.get("sha", "")) or ("",))[0]
        return bool(before) and before == self.body_sha(vid)

    def snapshot(self, vid: int, prev: dict) -> dict:
        """The replaced record for the admin review: wording, quote, old window, time."""
        old = self._old_body(vid, prev.get("sha", ""))
        return {k: prev[k] for k in _KEPT if k in prev} | {
            "window": old[1] if old else "",
            "replaced_at": datetime.now(UTC).isoformat(timespec="seconds"),
        }


def quote_status(
    pages: list[tuple[str, str]], anchor_page: str, quote: str, states: dict[str, str]
) -> str:
    """Authorization status of the page the quote comes from.

    Args:
        pages: ``(page name, html)`` of the sitting.
        anchor_page: Page with the vote announcement.
        quote: Verbatim quote of the record ("" when none).
        states: ``{page name: status}`` from steno_page_state.

    Returns:
        Status of the page holding the quote, else of the anchor page; "unknown" when
        the page was never checked.
    """
    names = [n for n, _ in pages]
    nq = norm_quote(quote)
    if nq and anchor_page in names:
        i = names.index(anchor_page)
        for name, page in reversed(pages[max(0, i - _QUOTE_PAGES_BACK) : i + 1]):
            if nq in norm_quote(_plain(body_html(page))):
                return states.get(name, "unknown")
    return states.get(anchor_page, "unknown")
