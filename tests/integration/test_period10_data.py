"""Checks of the cached period-10 data against the GitHub issue findings (#87, #93, #102).

Reads the local cache only (no network); a test is skipped when its data is missing.
"""

import json

import pytest

from pspcz_analyzer.config import DEFAULT_CACHE_DIR
from pspcz_analyzer.services.amendments.cache_manager import load_amendments
from pspcz_analyzer.services.overrides import apply_vote_questions, load_overrides
from pspcz_analyzer.services.vote_steno import load_vote_steno

PERIOD = 10
_CHAMBER = {"ps", "organizacni", "1_cteni", "vybor", "2_cteni", "garant", "3_cteni"}

# #102: sitting 34, votes 273-302 (cislo -> kind, sub-kind, proposer surnames)
_SITTING_34: dict[int, tuple[str, str, tuple[str, ...]]] = {
    273: ("procedural", "fixed_time", ("Benda",)),
    274: ("procedural", "fixed_time", ("Benda",)),
    275: ("procedural", "fixed_time", ("Hladík",)),
    276: ("procedural", "fixed_time", ("Richterová",)),
    277: ("procedural", "fixed_time", ("Okamura",)),
    278: ("procedural", "speaking_time", ("Benda",)),
    279: ("procedural", "speaking_time", ("Okamura",)),
    280: ("procedural", "revocation", ("Haas",)),
    281: ("procedural", "challenge", ("Gregor",)),
    282: ("procedural", "revocation", ("Haas",)),
    283: ("procedural", "objection", ("Jurečka",)),
    284: ("procedural", "objection", ("Jurečka",)),
    285: ("procedural", "reopen", ("Jurečka",)),
    286: ("return", "", ("Baxa", "Hladík")),
    287: ("reject", "", ("Jakob", "Hladík")),
    **dict.fromkeys(range(288, 303), ("committee", "", ())),
}


@pytest.mark.integration
def test_sitting_34_votes_273_to_302_match_the_issue_table() -> None:
    records = load_vote_steno(DEFAULT_CACHE_DIR, PERIOD)
    if not records:
        pytest.skip("no cached vote steno for period 10")
    apply_vote_questions(records, load_overrides(DEFAULT_CACHE_DIR, PERIOD))
    by_no = {r.get("cislo"): r for v, r in records.items() if 88862 <= v <= 88891}
    for cislo, (kind, subkind, names) in _SITTING_34.items():
        r = by_no[cislo]
        assert (r["kind"], r.get("subkind") or "") == (kind, subkind), cislo
        assert r["status"] == "ok" or r.get("edited"), cislo
        for name in names:
            assert name in (r.get("proposer") or ""), (cislo, r.get("proposer"))
    assert records[88862].get("chair_repeated_as") == 88863  # 273 repeated as 274
    assert records[88869].get("invalid_by") == 88870  # 280 challenged by 281
    assert records[88869].get("repeated_as") == 88871  # and repeated as 282


@pytest.mark.integration
def test_no_chamber_step_after_the_handover_to_the_senate() -> None:
    """#87 for every cached tisk of the period, unless the Senate or President returned it."""
    hist_dir = DEFAULT_CACHE_DIR / "tisky_meta" / str(PERIOD) / "tisky_historie"
    files = sorted(hist_dir.glob("*.json"))
    if not files:
        pytest.skip("no cached histories for period 10")
    bad = []
    for path in files:
        handed_over = False
        for s in json.loads(path.read_text()).get("stages", []):
            if s["stage_type"] == "senat_postoupeno" and s.get("outcome") == "postoupeno Senátu":
                handed_over = True
            elif s.get("outcome") in ("vrátil", "zamítl"):
                handed_over = False  # the Chamber votes again on a returned bill
            elif handed_over and s["stage_type"] in _CHAMBER:
                bad.append((path.stem, s["stage_type"], s.get("date")))
    assert bad == []


@pytest.mark.integration
def test_amendment_list_has_one_row_per_bill() -> None:
    """#93: the list counts bills and letters, never repeated agenda points."""
    bills = load_amendments(DEFAULT_CACHE_DIR, PERIOD)
    if not bills:
        pytest.skip("no cached amendments for period 10")
    voted = [b for b in bills.values() if not b.is_pending]
    cts = [b.ct for b in voted]
    keys = [(b.schuze, b.bod) for b in voted]
    assert len(keys) == len(set(keys))
    assert all(ct > 0 for ct in cts)
