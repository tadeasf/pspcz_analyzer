#!/usr/bin/env python
"""Smoke-test frontend pages, HTMX partials, charts, and admin against a live instance.

Crawls the main pages in both languages, discovers detail pages (law, vote,
amendment) from the links in HTMX partials, checks chart PNGs, and reports a
PASS/FAIL table. Non-zero exit on any failure.

Usage:
    uv run python scripts/check_pages.py                          # localhost:8000
    uv run python scripts/check_pages.py --base-url http://localhost:8091 --period 10
    uv run python scripts/check_pages.py --admin-url http://localhost:8009
"""

import argparse
import re
import sys

import httpx

PAGES = ["/", "/votes", "/laws", "/amendments", "/loyalty", "/attendance", "/similarity"]
PARTIALS = [
    "/api/votes?period={p}",
    "/api/laws?period={p}",
    "/api/amendments?period={p}",
    "/api/loyalty?period={p}&top=30",
    "/api/attendance?period={p}&top=30&sort=worst",
]
CHARTS = ["/charts/loyalty.png", "/charts/attendance.png", "/charts/similarity.png"]

# Language-specific markers proving the locale actually switched
LANG_MARKERS = {"cs": "Přeskočit na obsah", "en": "Skip to content"}


def check(
    client: httpx.Client,
    path: str,
    *,
    lang: str | None = None,
    expect_png: bool = False,
    markers: tuple[str, ...] = (),
) -> tuple[bool, str]:
    """GET a path and validate status + content markers."""
    headers = {"Cookie": f"lang={lang}"} if lang else {}
    try:
        resp = client.get(path, headers=headers, follow_redirects=False)
    except httpx.HTTPError as exc:
        return False, f"request error: {exc}"
    if resp.status_code >= 400:
        return False, f"HTTP {resp.status_code}"
    if expect_png:
        ctype = resp.headers.get("content-type", "")
        if "image/png" not in ctype or len(resp.content) < 1000:
            return False, f"not a real PNG ({ctype}, {len(resp.content)} B)"
        return True, f"PNG {len(resp.content) // 1024} kB"
    body = resp.text
    if "Internal Server Error" in body:
        return False, "server error in body"
    if lang and LANG_MARKERS[lang] not in body:
        return False, f"locale marker missing ({lang})"
    for marker in markers:
        if marker not in body:
            return False, f"marker missing: {marker!r}"
    return True, f"HTTP {resp.status_code}, {len(body) // 1024} kB"


def discover_details(client: httpx.Client, period: int) -> dict[str, str]:
    """Pull one law/vote/amendment detail path each from the list partials."""
    found: dict[str, str] = {}
    laws = client.get(f"/api/laws?period={period}").text
    m = re.search(r'href="(/laws/\d+[^"]*)"', laws)
    if m:
        found["law"] = m.group(1)
    votes = client.get(f"/api/votes?period={period}").text
    m = re.search(r'href="(/votes/\d+[^"]*)"', votes)
    if m:
        found["vote"] = m.group(1)
    amendments = client.get(f"/api/amendments?period={period}").text
    m = re.search(r'href="(/amendments/\d+/\d+[^"]*)"', amendments)
    if m:
        found["amendment"] = m.group(1)
    return found


def main() -> int:
    """Run the sweep; return 1 if anything fails."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--admin-url", default=None, help="also sweep the admin backend")
    parser.add_argument("--period", type=int, default=10)
    parser.add_argument("--timeout", type=float, default=120.0)
    args = parser.parse_args()

    results: list[tuple[str, bool, str]] = []
    with httpx.Client(base_url=args.base_url, timeout=args.timeout) as client:
        health_ok, detail = check(client, "/api/health")
        results.append(("GET /api/health", health_ok, detail))
        if not health_ok:
            print(f"Frontend not reachable at {args.base_url} — start it first.")
            return 1

        llm_ok, detail = check(client, "/api/llm/health")
        results.append(
            ("GET /api/llm/health (info)", True, detail if llm_ok else f"LLM down: {detail}")
        )

        for page in PAGES:
            for lang in ("cs", "en"):
                path = f"{page}?period={args.period}"
                ok, detail = check(client, path, lang=lang, markers=('<main id="main-content">',))
                results.append((f"{page} [{lang}]", ok, detail))

        for partial in PARTIALS:
            path = partial.format(p=args.period)
            ok, detail = check(client, path)
            results.append((f"partial {path.split('?')[0]}", ok, detail))

        for chart in CHARTS:
            path = f"{chart}?period={args.period}"
            ok, detail = check(client, path, expect_png=True)
            results.append((f"chart {chart}", ok, detail))

        details = discover_details(client, args.period)
        detail_markers = {
            "law": ("status-pill", "timeline"),
            "vote": ("vote-detail",),
            "amendment": ("driver-badge",),
        }
        for kind, path in details.items():
            # driver badges may legitimately be absent for unknown coalitions
            markers = () if kind == "amendment" else detail_markers[kind]
            ok, detail = check(client, path, markers=markers)
            results.append((f"detail {kind} {path.split('?')[0]}", ok, detail))

    if args.admin_url:
        with httpx.Client(base_url=args.admin_url, timeout=args.timeout) as client:
            ok, detail = check(client, "/api/health")
            results.append(("admin /api/health", ok, detail))
            ok, detail = check(client, "/login", markers=("password",))
            results.append(("admin /login", ok, detail))

    failures = 0
    for name, ok, detail in results:
        status = "PASS" if ok else "FAIL"
        if not ok:
            failures += 1
        print(f"[{status}] {name} — {detail}")

    total = len(results)
    print(f"\n{total - failures}/{total} checks passed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
