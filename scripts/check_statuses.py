#!/usr/bin/env python
"""Check cached tisk statuses and law numbers against live psp.cz (#86, #90).

For every tisk of the period with a cached history JSON
(tisky_meta/<period>/tisky_historie/<ct>.json), re-fetches the psp.cz history
page, re-parses it with the current parser and reports any difference in
current_status or law_number. Exit code 1 when differences exist.

Usage:
    uv run python -m scripts.check_statuses             # period 10, all tisky
    uv run python -m scripts.check_statuses --limit 20
"""

import argparse
import sys
import time
from pathlib import Path

import httpx

from pspcz_analyzer.config import (
    DEFAULT_CACHE_DIR,
    PSP_HISTORIE_URL_TEMPLATE,
    PSP_REQUEST_DELAY,
    TISKY_HISTORIE_DIR,
    TISKY_META_DIR,
)
from pspcz_analyzer.services.tisk.io.history_scraper import (
    TiskHistory,
    load_history_json,
    parse_tisk_history,
)

_FIELDS = ("current_status", "law_number")


def compare_history(cached: TiskHistory, fresh: TiskHistory) -> list[tuple[str, object, object]]:
    """List the checked fields whose cached value differs from the fresh parse.

    Args:
        cached: History loaded from the cache.
        fresh: History parsed from the live page.

    Returns:
        (field, cached value, fresh value) per differing field; empty when equal.
    """
    return [
        (name, getattr(cached, name), getattr(fresh, name))
        for name in _FIELDS
        if getattr(cached, name) != getattr(fresh, name)
    ]


def _cached_paths(cache_dir: Path, period: int, limit: int) -> list[Path]:
    """Cached history JSONs of the period, by tisk number, at most *limit* (0 = all)."""
    folder = cache_dir / TISKY_META_DIR / str(period) / TISKY_HISTORIE_DIR
    paths = sorted(folder.glob("*.json"), key=lambda p: int(p.stem) if p.stem.isdigit() else 0)
    return paths[:limit] if limit else paths


def _fetch(client: httpx.Client, period: int, ct: int) -> TiskHistory | None:
    """Fetch and parse the live history page; None when it cannot be fetched."""
    url = PSP_HISTORIE_URL_TEMPLATE.format(period=period, ct=ct)
    try:
        resp = client.get(url)
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        print(f"  {ct}: fetch failed ({exc})", file=sys.stderr)
        return None
    return parse_tisk_history(resp.text, period, ct)


def main() -> int:
    """Run the check and print a report.

    Returns:
        0 when every checked tisk matches, 1 when any differs.
    """
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--period", type=int, default=10)
    parser.add_argument("--limit", type=int, default=0, help="check at most N tisky (0 = all)")
    parser.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    args = parser.parse_args()

    paths = _cached_paths(args.cache_dir, args.period, args.limit)
    checked = differing = failed = 0
    with httpx.Client(timeout=30, follow_redirects=True) as client:
        for i, path in enumerate(paths):
            cached = load_history_json(path)
            if cached is None:
                continue
            if i:
                time.sleep(PSP_REQUEST_DELAY)
            fresh = _fetch(client, args.period, cached.ct)
            if fresh is None:
                failed += 1
                continue
            checked += 1
            diffs = compare_history(cached, fresh)
            if diffs:
                differing += 1
            for name, old, new in diffs:
                print(f"tisk {cached.ct}: {name} cached={old!r} psp.cz={new!r}")

    print(f"checked {checked}, differing {differing}, fetch failed {failed}")
    return 1 if differing else 0


if __name__ == "__main__":
    sys.exit(main())
