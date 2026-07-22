#!/usr/bin/env python
"""Accuracy audit for amendment pipeline output vs official psp.cz votes.

Compares the cached amendment parquet for a period against the official
votes table (hl_hlasovani_<period>.parquet). Pure offline analysis — no
app boot, no network. Run after scripts/test_amendments.py --period N.

Checks:
  1. Per-bill coverage: resolved amendment vote IDs vs official (schuze, bod)
     vote count (void/procedure/final votes excluded from the expectation).
  2. Referential integrity: every resolved id_hlasovani exists in the votes
     table with matching schuze AND bod.
  3. Ground-truth spot-check: tisk 82 (schuze 17, bod 2) cislo 46-70.
  4. Accounting: no silent drops — bills with zero rows must carry an
     unlinked_amendment_count; placeholder rows stay consistent.

Usage: uv run python scripts/audit_amendments.py [--period 10]
"""

import argparse
import os
import re
import sys
from pathlib import Path

import polars as pl

DEFAULT_CACHE_DIR = Path(
    os.environ.get("PSPCZ_CACHE_DIR", str(Path.home() / ".cache" / "pspcz-analyzer" / "psp"))
)

_PROCEDURE_RE = re.compile(r"procedur|postup", re.IGNORECASE)
# Ground truth validated live on psp.cz: tisk 82, schuze 17, bod 2 —
# amendment votes are cislo 46-70 except 50 (officially bod=0/unassigned).
_SPOT_CHECK = {"ct": 82, "schuze": 17, "bod": 2, "cislo_range": set(range(46, 71)) - {50}}


def _load_votes(cache_dir: Path, period: int) -> tuple[pl.DataFrame, set[int]]:
    """Load the official votes table and void-vote IDs for a period."""
    votes = pl.read_parquet(cache_dir / "parquet" / f"hl_hlasovani_{period}.parquet")
    zmatecne_path = cache_dir / "parquet" / f"zmatecne_{period}.parquet"
    void_ids: set[int] = set()
    if zmatecne_path.exists():
        void_ids = set(pl.read_parquet(zmatecne_path).get_column("id_hlasovani").to_list())
        print(f"note: {len(void_ids)} void votes excluded (zmatecne_{period})")
    else:
        print(f"note: no zmatecne_{period}.parquet — treating period {period} as zero-void")
    return votes, void_ids


def _official_counts(votes: pl.DataFrame, void_ids: set[int]) -> pl.DataFrame:
    """Official vote counts per (schuze, bod), split into total/procedure."""
    df = votes
    if void_ids:
        df = df.filter(~pl.col("id_hlasovani").is_in(list(void_ids)))
    title = pl.coalesce(
        [pl.col("nazev_kratky"), pl.col("nazev_dlouhy"), pl.lit("")]
    ).str.to_lowercase()
    return (
        df.drop_nulls(["schuze", "bod"])
        .with_columns(title.alias("_title"))
        .group_by(["schuze", "bod"])
        .agg(
            pl.len().alias("official_votes"),
            pl.col("_title").str.contains(r"procedur|postup").sum().alias("procedure_votes"),
        )
    )


def main() -> int:
    """Run the audit; return 1 if integrity or accounting checks fail."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--period", type=int, default=10)
    args = parser.parse_args()

    cache_dir = DEFAULT_CACHE_DIR
    amend_path = cache_dir / "amendments" / str(args.period) / "amendments.parquet"
    if not amend_path.exists():
        print(f"FAIL: no amendment cache at {amend_path} — run the pipeline first")
        return 1

    am = pl.read_parquet(amend_path)
    votes, void_ids = _load_votes(cache_dir, args.period)
    official = _official_counts(votes, void_ids)

    is_placeholder = am.get_column("is_placeholder") if "is_placeholder" in am.columns else None
    real = am if is_placeholder is None else am.filter(~pl.col("is_placeholder"))

    bills = (
        real.group_by(["schuze", "bod"])
        .agg(
            pl.col("ct").first().alias("ct"),
            pl.len().alias("rows"),
            (pl.col("vote_number") > 0).sum().alias("voted"),
            pl.col("id_hlasovani").drop_nulls().len().alias("resolved"),
            pl.col("is_final_vote").sum().alias("final_rows"),
            pl.col("parse_confidence").first().alias("confidence"),
            pl.col("parse_warnings").first().alias("warnings"),
            pl.col("unlinked_amendment_count").first().alias("unlinked"),
        )
        .join(official, on=["schuze", "bod"], how="left")
        .with_columns(
            pl.col("official_votes").fill_null(0),
            pl.col("procedure_votes").fill_null(0),
        )
        .with_columns(
            (pl.col("official_votes") - pl.col("procedure_votes") - pl.col("final_rows"))
            .clip(lower_bound=0)
            .alias("expected_amend_votes")
        )
        .sort(["schuze", "bod"])
    )

    print(f"\n=== Per-bill coverage (period {args.period}) ===")
    print(
        bills.select(
            [
                "schuze",
                "bod",
                "ct",
                "rows",
                "voted",
                "resolved",
                "expected_amend_votes",
                "confidence",
            ]
        )
    )

    # --- Check 2: referential integrity ---------------------------------
    resolved_rows = real.filter(pl.col("id_hlasovani").is_not_null())
    joined = resolved_rows.join(
        votes.select(["id_hlasovani", "schuze", "bod", "cislo"]),
        on="id_hlasovani",
        how="left",
        suffix="_official",
    )
    missing = joined.filter(pl.col("schuze_official").is_null())
    # bod=0 means "unassigned" in psp.cz data — the resolver accepts those,
    # so they are not integrity violations.
    mismatched = joined.filter(
        pl.col("schuze_official").is_not_null()
        & (
            (pl.col("schuze") != pl.col("schuze_official"))
            | ((pl.col("bod_official") != 0) & (pl.col("bod") != pl.col("bod_official")))
        )
    )
    print("\n=== Referential integrity ===")
    print(f"resolved vote IDs: {joined.height}")
    print(f"  not found in votes table: {missing.height}")
    print(f"  schuze/bod mismatch:      {mismatched.height}")
    integrity_ok = missing.height == 0 and mismatched.height == 0

    # --- Check 3: known ground truth (tisk 82, 17/2, cislo 46-70) -------
    spot = real.filter(
        (pl.col("ct") == _SPOT_CHECK["ct"])
        & (pl.col("schuze") == _SPOT_CHECK["schuze"])
        & (pl.col("bod") == _SPOT_CHECK["bod"])
    )
    spot_ids = spot.get_column("id_hlasovani").drop_nulls().to_list()
    matched_cislo = sorted(
        votes.filter(pl.col("id_hlasovani").is_in(spot_ids)).get_column("cislo").to_list()
    )
    expected = set(_SPOT_CHECK["cislo_range"])
    missing_cislo = sorted(expected - set(matched_cislo))
    print("\n=== Ground truth: tisk 82 (17,2), cislo 46-70 ===")
    if not spot.height:
        print("spot-check bill not present in this period's cache — skipped")
    else:
        print(
            f"matched {len(set(matched_cislo) & expected)}/{len(expected)}: "
            f"{sorted(set(matched_cislo) & expected)}"
        )
        print(f"missing: {missing_cislo}")

    # --- Check 4: accounting — no silent drops --------------------------
    print("\n=== Accounting ===")
    placeholders = 0 if is_placeholder is None else int(is_placeholder.sum())
    unlinked_bills = bills.filter(pl.col("unlinked") > 0)
    # Unlinked bills may legitimately carry a final-vote row and unvoted
    # steno leftovers — only VOTED non-final rows contradict an unlinked
    # count (a vote means steno did find recorded votes for this bill).
    bad_unlinked = unlinked_bills.filter(pl.col("voted") - pl.col("final_rows") > 0)
    print(f"placeholder rows: {placeholders}")
    print(
        f"bills with unlinked counts: {unlinked_bills.height} (must have zero voted non-final rows)"
    )
    print(f"  violations: {bad_unlinked.height}")
    print(f"bills in cache: {bills.height}")
    for row in unlinked_bills.iter_rows(named=True):
        print(f"  ({row['schuze']},{row['bod']}) ct={row['ct']}: {row['unlinked']} unlinked")

    # --- Totals + gap list ----------------------------------------------
    total_resolved = bills.get_column("resolved").sum()
    total_expected = bills.get_column("expected_amend_votes").sum()
    coverage = total_resolved / total_expected if total_expected else 0.0
    print("\n=== Totals ===")
    print(
        f"resolved amendment votes: {total_resolved} / {total_expected} expected ({coverage:.0%})"
    )
    gaps = bills.filter(pl.col("resolved") < pl.col("expected_amend_votes") * 0.9)
    if gaps.height:
        print("bills below 90% coverage:")
        for row in gaps.iter_rows(named=True):
            warnings = row["warnings"] or ""
            print(
                f"  ({row['schuze']},{row['bod']}) ct={row['ct']}: "
                f"{row['resolved']}/{row['expected_amend_votes']} "
                f"conf={row['confidence']:.2f} warnings={warnings[:120]!r}"
            )

    ok = integrity_ok and bad_unlinked.height == 0
    print(
        f"\nAUDIT {'PASSED' if ok else 'FAILED'} "
        f"(integrity={'ok' if integrity_ok else 'VIOLATIONS'}, coverage={coverage:.0%})"
    )
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
