"""Tests for amendment cache round-trips, placeholder rows, and schema compat."""

from pathlib import Path
from types import SimpleNamespace

import polars as pl

from pspcz_analyzer.models.amendment_models import AmendmentVote, BillAmendmentData
from pspcz_analyzer.services.amendment_service import _bill_summary
from pspcz_analyzer.services.amendments.cache_manager import (
    AMENDMENTS_SCHEMA,
    _cache_path,
    load_amendments,
    save_amendments,
)
from pspcz_analyzer.services.amendments.identifier import _resolve_vote_ids
from pspcz_analyzer.services.amendments.submitter_resolver import _match_name_to_mp


def _bill(**overrides) -> BillAmendmentData:
    defaults = {"period": 10, "schuze": 17, "bod": 2, "ct": 82, "tisk_nazev": "novela"}
    return BillAmendmentData(**(defaults | overrides))


class TestAmendmentCacheRoundTrip:
    def test_roundtrip_preserves_unlinked_count(self, tmp_path: Path):
        bill = _bill(
            amendments=[AmendmentVote(letter="A", vote_number=51, result="accepted")],
            unlinked_amendment_count=0,
        )
        save_amendments(tmp_path, 10, [bill])
        loaded = load_amendments(tmp_path, 10)

        assert (17, 2) in loaded
        assert loaded[(17, 2)].amendment_count == 1
        assert loaded[(17, 2)].unlinked_amendment_count == 0

    def test_placeholder_row_keeps_unlinked_bill(self, tmp_path: Path):
        # Bill has PDF amendments but zero recorded votes → no amendment rows,
        # but the bill must survive the cache round-trip with its count.
        bill = _bill(amendments=[], unlinked_amendment_count=3)
        save_amendments(tmp_path, 10, [bill])
        loaded = load_amendments(tmp_path, 10)

        assert (17, 2) in loaded
        reloaded = loaded[(17, 2)]
        assert reloaded.amendments == []
        assert reloaded.unlinked_amendment_count == 3
        assert reloaded.tisk_nazev == "novela"

    def test_old_schema_parquet_loads_with_zero_count(self, tmp_path: Path):
        # Cache files written before the new columns existed must load fine
        # (no migration): .get() defaults fill unlinked count with 0.
        old_schema = {
            k: v
            for k, v in AMENDMENTS_SCHEMA.items()
            if k not in ("unlinked_amendment_count", "is_placeholder")
        }
        row = {name: _default_for(dtype) for name, dtype in old_schema.items()}
        row |= {"schuze": 17, "bod": 2, "ct": 82, "letter": "A", "vote_number": 51}
        path = _cache_path(tmp_path, 10)
        path.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame([row], schema=old_schema).write_parquet(path)

        loaded = load_amendments(tmp_path, 10)

        assert (17, 2) in loaded
        assert loaded[(17, 2)].unlinked_amendment_count == 0
        assert loaded[(17, 2)].amendment_count == 1


def _default_for(dtype: pl.DataType) -> object:
    """Schema-appropriate empty default for building old-schema test rows."""
    match dtype:
        case pl.Int64:
            return 0
        case pl.Float64:
            return 1.0
        case pl.Boolean:
            return False
        case _:
            return ""


class TestBillSummaryUnlinkedKey:
    def test_bill_summary_includes_unlinked_count(self):
        bill = _bill(unlinked_amendment_count=5)
        summary = _bill_summary(bill, frozenset())
        assert summary["unlinked_amendment_count"] == 5


class TestResolveVoteIdsWarnings:
    def test_unresolved_votes_recorded_as_warning(self):
        votes = pl.DataFrame({"schuze": [17], "cislo": [1], "id_hlasovani": [999]})
        period_data = SimpleNamespace(votes=votes)
        bill = _bill(amendments=[AmendmentVote(letter="A", vote_number=99)])

        _resolve_vote_ids([bill], period_data)  # type: ignore[arg-type]

        assert bill.amendments[0].id_hlasovani is None
        assert len(bill.parse_warnings) == 1
        assert "not resolved" in bill.parse_warnings[0]
        assert "99" in bill.parse_warnings[0]

    def test_resolved_votes_no_warning(self):
        votes = pl.DataFrame({"schuze": [17], "cislo": [51], "id_hlasovani": [999]})
        period_data = SimpleNamespace(votes=votes)
        bill = _bill(amendments=[AmendmentVote(letter="A", vote_number=51)])

        _resolve_vote_ids([bill], period_data)  # type: ignore[arg-type]

        assert bill.amendments[0].id_hlasovani == 999
        assert bill.parse_warnings == []

    def test_bod_mismatch_left_unlinked_with_warning(self):
        # Vote officially belongs to bod 89, not the bill's bod 90 — the
        # steno span overshot a bod boundary; refuse the link.
        votes = pl.DataFrame({"schuze": [17], "cislo": [63], "id_hlasovani": [999], "bod": [89]})
        period_data = SimpleNamespace(votes=votes)
        bill = _bill(bod=90, amendments=[AmendmentVote(letter="A", vote_number=63)])

        _resolve_vote_ids([bill], period_data)  # type: ignore[arg-type]

        assert bill.amendments[0].id_hlasovani is None
        assert any("bod 89" in w for w in bill.parse_warnings)

    def test_bod_zero_unassigned_accepted(self):
        # bod=0 means "unassigned" in psp.cz data — accept the link.
        votes = pl.DataFrame({"schuze": [17], "cislo": [65], "id_hlasovani": [998], "bod": [0]})
        period_data = SimpleNamespace(votes=votes)
        bill = _bill(amendments=[AmendmentVote(letter="A", vote_number=65)])

        _resolve_vote_ids([bill], period_data)  # type: ignore[arg-type]

        assert bill.amendments[0].id_hlasovani == 998

    def test_matching_bod_accepted(self):
        votes = pl.DataFrame({"schuze": [17], "cislo": [54], "id_hlasovani": [997], "bod": [2]})
        period_data = SimpleNamespace(votes=votes)
        bill = _bill(amendments=[AmendmentVote(letter="A", vote_number=54)])

        _resolve_vote_ids([bill], period_data)  # type: ignore[arg-type]

        assert bill.amendments[0].id_hlasovani == 997


class TestExactNameMatch:
    def test_exact_match_short_circuits(self):
        mp_rows = [
            {"id_poslanec": 1, "prijmeni": "Novák", "party": "ANO"},
            {"id_poslanec": 2, "prijmeni": "Novák", "party": "ODS"},
        ]
        # Exact normalized match returns the first row immediately, so a
        # later equal-ratio row can never take over.
        assert _match_name_to_mp("Novák", mp_rows) == (1, "ANO")

    def test_fuzzy_match_below_threshold_returns_none(self):
        mp_rows = [{"id_poslanec": 1, "prijmeni": "Bartoš", "party": "ODS"}]
        assert _match_name_to_mp("Svoboda", mp_rows) is None
