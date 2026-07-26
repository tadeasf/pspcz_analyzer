"""Tests for steno sub-page discovery and capture (no network).

Covers the fix for the period-10 "0 amendment bills" bug: complete sub-page
collection across a bod's anchor span and bounded full download.
"""

from pathlib import Path

from pspcz_analyzer.services.amendments import steno_scraper
from pspcz_analyzer.services.amendments.steno_scraper import (
    StenoFailure,
    _collect_bod_subpages,
    _fill_subpage_gaps,
    _index_anchors_by_page,
    _subpage_span_for_bod,
    find_steno_for_bod,
)

# Session index: bod 2 spans two day-pages; bod 1/3 give boundary anchors.
INDEX_HTML = """
<html><body>
<a name="b1" id="b1"></a> <a href="17-1.html#q100">Bod 1</a>
<a name="b2" id="b2"></a>
  <a href="17-1.html#q230">Bod 2 (den 1)</a>
  <a href="17-3.html#q300">Bod 2 (den 3)</a>
<a name="b3" id="b3"></a>
  <a href="17-1.html#q260">Bod 3 (den 1)</a>
  <a href="17-3.html#q400">Bod 3 (den 3)</a>
</body></html>
"""

# Day 1: bod 2 (q230) spans a speaker sub-anchor q235 before bod 3 (q260).
DAY1_HTML = """
<html><body>
<a name="q100"></a> <a href="s050.htm">úvod</a>
<a name="q230"></a> <a href="s100.htm">řečník 1</a>
<a name="q235"></a> <a href="s101.htm">řečník 2</a>
<a name="q260"></a> <a href="s200.htm">bod 3</a>
</body></html>
"""

# Day 3: bod 2 (q300) then bod 3 (q400).
DAY3_HTML = """
<html><body>
<a name="q300"></a> <a href="s300.htm">řečník</a> <a href="s301.htm">hlasování</a>
<a name="q400"></a> <a href="s400.htm">bod 3</a>
</body></html>
"""


class TestIndexAnchorsByPage:
    def test_groups_anchors_per_page(self):
        by_page = _index_anchors_by_page(INDEX_HTML)
        assert by_page["17-1.html"] == ["q100", "q230", "q260"]
        assert by_page["17-3.html"] == ["q300", "q400"]


class TestSubpageSpanForBod:
    def test_span_crosses_speaker_subanchor(self):
        # q235 is a speaker anchor, NOT a bod boundary; the span q230->q260 must
        # include both s100 and s101 (the old logic stopped at q235 -> only s100).
        subs = _subpage_span_for_bod(DAY1_HTML, ["q230"], ["q100", "q260"])
        assert subs == ["s100.htm", "s101.htm"]

    def test_span_runs_to_end_when_no_boundary(self):
        subs = _subpage_span_for_bod(DAY3_HTML, ["q300"], [])
        assert subs == ["s300.htm", "s301.htm", "s400.htm"]

    def test_none_anchor_starts_at_page_top(self):
        subs = _subpage_span_for_bod(DAY1_HTML, [None], ["q260"])
        assert subs[0] == "s050.htm"


def _patch_downloads(monkeypatch, *, day_pages: dict[str, str | None]):
    """Patch the module download helpers to serve fixtures (no network)."""
    monkeypatch.setattr(steno_scraper, "_download_index", lambda *a, **k: INDEX_HTML)

    def fake_day(base_url, page_file, period, schuze, cache_dir):
        return day_pages.get(page_file)

    def fake_sub(base_url, subpage_name, period, schuze, cache_dir):
        return f"<p>CONTENT {subpage_name}</p>"

    monkeypatch.setattr(steno_scraper, "_download_day_page", fake_day)
    monkeypatch.setattr(steno_scraper, "_download_subpage", fake_sub)


class TestCollectBodSubpages:
    def test_unions_across_day_pages(self, monkeypatch):
        _patch_downloads(monkeypatch, day_pages={"17-1.html": DAY1_HTML, "17-3.html": DAY3_HTML})
        subs = _collect_bod_subpages(INDEX_HTML, "base/", 10, 17, 2, Path("/tmp"))
        assert subs == ["s100.htm", "s101.htm", "s300.htm", "s301.htm"]

    def test_missing_bod_returns_none(self, monkeypatch):
        _patch_downloads(monkeypatch, day_pages={})
        assert _collect_bod_subpages(INDEX_HTML, "base/", 10, 17, 9, Path("/tmp")) is None


class TestFindStenoForBod:
    def test_concatenates_all_subpages(self, monkeypatch):
        _patch_downloads(monkeypatch, day_pages={"17-1.html": DAY1_HTML, "17-3.html": DAY3_HTML})
        html, first_url, failure = find_steno_for_bod(10, 17, 2, Path("/tmp"))
        assert failure is None
        assert html is not None
        assert first_url.endswith("s100.htm")
        for name in ("s100.htm", "s101.htm", "s300.htm", "s301.htm"):
            assert f"CONTENT {name}" in html

    def test_bod_not_in_index(self, monkeypatch):
        _patch_downloads(monkeypatch, day_pages={"17-1.html": DAY1_HTML, "17-3.html": DAY3_HTML})
        html, _url, failure = find_steno_for_bod(10, 17, 9, Path("/tmp"))
        assert html is None
        assert failure is StenoFailure.BOD_NOT_IN_INDEX

    def test_keeps_last_n_when_over_cap(self, monkeypatch):
        _patch_downloads(monkeypatch, day_pages={"17-1.html": DAY1_HTML, "17-3.html": DAY3_HTML})
        monkeypatch.setattr(steno_scraper, "STENO_MAX_SUBPAGES_PER_BOD", 2)
        html, first_url, failure = find_steno_for_bod(10, 17, 2, Path("/tmp"))
        assert failure is None
        assert html is not None
        # Voting is at the end -> keep the last 2 (s300, s301), drop s100/s101.
        assert first_url.endswith("s300.htm")
        assert "CONTENT s100.htm" not in html
        assert "CONTENT s301.htm" in html


class TestFillSubpageGaps:
    def test_fills_small_gap(self):
        # The real case: s017212 (no speaker anchor) between linked pages.
        out = _fill_subpage_gaps(["s017211.htm", "s017213.htm"], 3)
        assert out == ["s017211.htm", "s017212.htm", "s017213.htm"]

    def test_does_not_bridge_large_gap(self):
        # 29 May (s017158) vs 3 June (s017211) — different sitting days, gap ~50.
        out = _fill_subpage_gaps(["s017158.htm", "s017211.htm"], 3)
        assert out == ["s017158.htm", "s017211.htm"]

    def test_preserves_zero_pad_width(self):
        out = _fill_subpage_gaps(["s017211.htm", "s017214.htm"], 3)
        assert out == ["s017211.htm", "s017212.htm", "s017213.htm", "s017214.htm"]

    def test_consecutive_pages_unchanged(self):
        out = _fill_subpage_gaps(["s100.htm", "s101.htm"], 3)
        assert out == ["s100.htm", "s101.htm"]

    def test_sorts_and_handles_short_names(self):
        out = _fill_subpage_gaps(["s103.htm", "s100.htm"], 3)
        assert out == ["s100.htm", "s101.htm", "s102.htm", "s103.htm"]


# Day 3 variant: TOC links s300 and s302 but NOT the continuation page s301.
DAY3_GAP_HTML = """
<html><body>
<a name="q300"></a> <a href="s300.htm">řečník</a> <a href="s302.htm">pokračování</a>
<a name="q400"></a> <a href="s400.htm">bod 3</a>
</body></html>
"""


class TestCollectBodSubpagesGapFill:
    def test_collects_unlinked_continuation_page(self, monkeypatch):
        _patch_downloads(
            monkeypatch, day_pages={"17-1.html": DAY1_HTML, "17-3.html": DAY3_GAP_HTML}
        )
        subs = _collect_bod_subpages(INDEX_HTML, "base/", 10, 17, 2, Path("/tmp"))
        assert subs is not None
        # s301 was never linked in the TOC but is gap-filled between s300 and s302.
        assert "s301.htm" in subs
        assert subs == ["s100.htm", "s101.htm", "s300.htm", "s301.htm", "s302.htm"]
