"""Validate vLLM structured output compatibility for pspcz_analyzer.

Tests connection, plain generation, and structured JSON output using the exact
same schemas the app sends.  Designed to run against any OpenAI-compatible
endpoint (vLLM, LiteLLM proxy, Ollama, etc.).

Usage:
    uv run python scripts/test_vllm.py
    uv run python scripts/test_vllm.py --model gpt-oss-120b
    uv run python scripts/test_vllm.py --base-url http://localhost:8000/v1 --model gpt-oss-20b-vllm
    uv run python scripts/test_vllm.py --verbose
    uv run python scripts/test_vllm.py --no-strict   # strip additionalProperties for vLLM xgrammar
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Any

import httpx
from dotenv import load_dotenv
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

# ── Schemas (copied from pspcz_analyzer.services.llm.prompts) ────────────
# Kept inline so the script works without installing the package.

CLASSIFY_AND_SUMMARIZE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "topics": {
            "type": "array",
            "items": {"type": "string"},
            "description": "1-3 short topic labels",
        },
        "changes": {"type": "string", "description": "What specifically changes"},
        "impact": {"type": "string", "description": "Who benefits and who is harmed"},
        "risks": {"type": "string", "description": "Risk of abuse or unintended consequences"},
    },
    "required": ["topics", "changes", "impact", "risks"],
    "additionalProperties": False,
}

COMPARISON_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "changed_paragraphs": {
            "type": "string",
            "description": "Which paragraphs/articles changed and how",
        },
        "additions_removals": {
            "type": "string",
            "description": "What was added or removed",
        },
        "overall_character": {
            "type": "string",
            "description": "Overall character: tightening/loosening/technical",
        },
    },
    "required": ["changed_paragraphs", "additions_removals", "overall_character"],
    "additionalProperties": False,
}

# ── Prompts (simplified versions of the structured prompts from prompts.py) ──

CLASSIFY_SYSTEM = (
    "Jsi kriticko-analytický komentátor českého parlamentu. Analyzuješ parlamentní tisky, "
    "přiřazuješ jim tematické štítky a píšeš ostré, věcné komentáře bez přikrašlování. "
    "VŽDY odpovídej POUZE v češtině."
)

CLASSIFY_PROMPT = (
    "Analyzuj následující parlamentní tisk.\n\n"
    "ÚKOL 1 — TÉMATA: Urči 1–3 hlavní témata. Použij krátké české názvy (2–4 slova). "
    "Buď konkrétní — např. 'Trestní právo', ne 'Právo'.\n\n"
    "ÚKOL 2 — KRITICKÁ ANALÝZA:\n"
    "Pro pole 'changes': Co KONKRÉTNĚ se mění (žádné vágní formulace)\n"
    "Pro pole 'impact': Komu to prospívá a komu škodí\n"
    "Pro pole 'risks': Jaké je RIZIKO zneužití nebo nezamýšlený důsledek\n"
    "Buď přímý a kriticko-analytický. 1–2 věty na pole.\n\n"
    "Název:\n---BEGIN USER TEXT---\n{title}\n---END USER TEXT---\n\n"
    "Text:\n---BEGIN USER TEXT---\n{text}\n---END USER TEXT---"
)

COMPARISON_SYSTEM = (
    "Jsi analyticko-právní expert na českou legislativu. Srovnáváš verze parlamentních tisků "
    "a identifikuješ KONKRÉTNÍ změny mezi nimi. VŽDY odpovídej POUZE v češtině."
)

COMPARISON_PROMPT = (
    "Porovnej následující dvě verze parlamentního tisku a popiš KONKRÉTNÍ rozdíly.\n"
    "Pro pole 'changed_paragraphs': Které paragrafy/články se změnily a jak\n"
    "Pro pole 'additions_removals': Co bylo přidáno nebo odebráno\n"
    "Pro pole 'overall_character': Celkový charakter změn (zpřísnění/zmírnění/technická úprava)\n"
    "Buď konkrétní — cituj čísla paragrafů. 1–2 věty na pole.\n\n"
    "VERZE 0 (původní znění):\n---BEGIN USER TEXT---\n{text_old}\n---END USER TEXT---\n\n"
    "VERZE 1 (pozměňovací návrh):\n---BEGIN USER TEXT---\n{text_new}\n---END USER TEXT---"
)

# ── Sample data ──────────────────────────────────────────────────────────

SAMPLE_TISK_TITLE = "Novela zákona o dani z přidané hodnoty"

SAMPLE_TISK_TEXT = (
    "Tento zákon mění zákon č. 235/2004 Sb., o dani z přidané hodnoty, "
    "ve znění pozdějších předpisů. Hlavní změny se týkají snížení sazby DPH "
    "na vybrané potraviny z 15 % na 10 %. Dále se zavádí nová kategorie "
    "osvobození od daně pro neziskové organizace poskytující sociální služby. "
    "Zákon nabývá účinnosti dnem 1. ledna 2026."
)

SAMPLE_TEXT_OLD = (
    "§ 47 Sazby daně\n"
    "(1) U zdanitelného plnění se uplatňuje základní sazba daně ve výši 21 %.\n"
    "(2) U zdanitelného plnění uvedeného v příloze č. 2 se uplatňuje "
    "první snížená sazba daně ve výši 15 %.\n"
    "(3) Potraviny uvedené v příloze č. 3a podléhají sazbě 15 %."
)

SAMPLE_TEXT_NEW = (
    "§ 47 Sazby daně\n"
    "(1) U zdanitelného plnění se uplatňuje základní sazba daně ve výši 21 %.\n"
    "(2) U zdanitelného plnění uvedeného v příloze č. 2 se uplatňuje "
    "první snížená sazba daně ve výši 12 %.\n"
    "(3) Potraviny uvedené v příloze č. 3a podléhají sazbě 10 %.\n"
    "(4) Neziskové organizace poskytující sociální služby jsou osvobozeny od daně."
)

console = Console()


# ── Helpers ──────────────────────────────────────────────────────────────


def _strip_additional_properties(schema: dict[str, Any]) -> dict[str, Any]:
    """Recursively remove 'additionalProperties' from a JSON schema.

    vLLM's xgrammar backend does not support additionalProperties.
    """
    result = {}
    for k, v in schema.items():
        if k == "additionalProperties":
            continue
        if isinstance(v, dict):
            result[k] = _strip_additional_properties(v)
        elif isinstance(v, list):
            result[k] = [
                _strip_additional_properties(item) if isinstance(item, dict) else item for item in v
            ]
        else:
            result[k] = v
    return result


def _build_headers(api_key: str) -> dict[str, str]:
    """Build HTTP headers for the OpenAI-compatible API."""
    headers: dict[str, str] = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    return headers


def _wrap_schema(schema: dict[str, Any], *, strict: bool = True) -> dict[str, Any]:
    """Wrap a JSON schema in the OpenAI response_format envelope."""
    clean = _strip_additional_properties(schema) if not strict else schema
    envelope: dict[str, Any] = {
        "type": "json_schema",
        "json_schema": {
            "name": "response",
            "schema": clean,
        },
    }
    if strict:
        envelope["json_schema"]["strict"] = True
    return envelope


def _chat_completion(
    base_url: str,
    model: str,
    headers: dict[str, str],
    system: str,
    user: str,
    *,
    response_format: dict[str, Any] | None = None,
    timeout: float = 120.0,
) -> tuple[str | None, float]:
    """Send a chat completion request. Returns (content, elapsed_seconds)."""
    payload: dict[str, Any] = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "stream": False,
    }
    if response_format is not None:
        payload["response_format"] = response_format

    t0 = time.perf_counter()
    resp = httpx.post(
        f"{base_url.rstrip('/')}/chat/completions",
        headers=headers,
        json=payload,
        timeout=timeout,
    )
    elapsed = time.perf_counter() - t0

    if resp.status_code >= 400:
        try:
            err_body = resp.json()
        except Exception:
            err_body = resp.text[:500]
        raise RuntimeError(f"HTTP {resp.status_code}: {err_body}")

    data = resp.json()
    choices = data.get("choices", [])
    if not choices:
        return None, elapsed
    return choices[0].get("message", {}).get("content"), elapsed


# ── Test functions ───────────────────────────────────────────────────────


def test_connection(
    base_url: str, model: str, headers: dict[str, str], verbose: bool, *, strict: bool = True
) -> tuple[bool, str, float]:
    """Test 1: GET /models — verify endpoint reachable and model listed."""
    t0 = time.perf_counter()
    resp = httpx.get(
        f"{base_url.rstrip('/')}/models",
        headers=headers,
        timeout=10.0,
    )
    elapsed = time.perf_counter() - t0
    resp.raise_for_status()

    data = resp.json()
    if verbose:
        console.print(
            Panel(json.dumps(data, indent=2, ensure_ascii=False), title="/models response")
        )

    model_ids = [m.get("id", "") for m in data.get("data", [])]
    if model in model_ids:
        return True, f"Model '{model}' found among {len(model_ids)} models", elapsed
    return False, f"Model '{model}' NOT found. Available: {model_ids}", elapsed


def test_plain_generation(
    base_url: str, model: str, headers: dict[str, str], verbose: bool, *, strict: bool = True
) -> tuple[bool, str, float]:
    """Test 2: Plain text generation — verify basic chat completion works."""
    content, elapsed = _chat_completion(
        base_url,
        model,
        headers,
        system="You are a helpful assistant. Reply in one sentence.",
        user="What is the capital of the Czech Republic?",
    )
    if verbose and content:
        console.print(Panel(content, title="Plain generation response"))

    if content and len(content.strip()) > 5:
        return True, f"Got {len(content)} chars", elapsed
    return False, f"Empty or too short response: {content!r}", elapsed


def test_structured_classify_summarize(
    base_url: str, model: str, headers: dict[str, str], verbose: bool, *, strict: bool = True
) -> tuple[bool, str, float]:
    """Test 3: Structured output — classify + summarize with app schema."""
    prompt = CLASSIFY_PROMPT.format(
        title=SAMPLE_TISK_TITLE,
        text=SAMPLE_TISK_TEXT,
    )
    content, elapsed = _chat_completion(
        base_url,
        model,
        headers,
        system=CLASSIFY_SYSTEM,
        user=prompt,
        response_format=_wrap_schema(CLASSIFY_AND_SUMMARIZE_SCHEMA, strict=strict),
    )

    if verbose and content:
        console.print(Panel(content, title="Classify+Summarize response"))

    if not content:
        return False, "Empty response", elapsed

    try:
        parsed = json.loads(content)
    except json.JSONDecodeError as exc:
        return False, f"Invalid JSON: {exc}", elapsed

    required_keys = {"topics", "changes", "impact", "risks"}
    missing = required_keys - set(parsed.keys())
    if missing:
        return False, f"Missing keys: {missing}", elapsed

    if not isinstance(parsed["topics"], list) or len(parsed["topics"]) == 0:
        return False, f"'topics' should be non-empty list, got: {parsed['topics']!r}", elapsed

    for key in ("changes", "impact", "risks"):
        if not isinstance(parsed[key], str) or len(parsed[key].strip()) < 5:
            return False, f"'{key}' too short or wrong type: {parsed[key]!r}", elapsed

    return True, f"OK — {len(parsed['topics'])} topics, all fields valid", elapsed


def test_structured_comparison(
    base_url: str, model: str, headers: dict[str, str], verbose: bool, *, strict: bool = True
) -> tuple[bool, str, float]:
    """Test 4: Structured output — version comparison with app schema."""
    prompt = COMPARISON_PROMPT.format(
        text_old=SAMPLE_TEXT_OLD,
        text_new=SAMPLE_TEXT_NEW,
    )
    content, elapsed = _chat_completion(
        base_url,
        model,
        headers,
        system=COMPARISON_SYSTEM,
        user=prompt,
        response_format=_wrap_schema(COMPARISON_SCHEMA, strict=strict),
    )

    if verbose and content:
        console.print(Panel(content, title="Comparison response"))

    if not content:
        return False, "Empty response", elapsed

    try:
        parsed = json.loads(content)
    except json.JSONDecodeError as exc:
        return False, f"Invalid JSON: {exc}", elapsed

    required_keys = {"changed_paragraphs", "additions_removals", "overall_character"}
    missing = required_keys - set(parsed.keys())
    if missing:
        return False, f"Missing keys: {missing}", elapsed

    for key in required_keys:
        if not isinstance(parsed[key], str) or len(parsed[key].strip()) < 5:
            return False, f"'{key}' too short or wrong type: {parsed[key]!r}", elapsed

    return True, "OK — all fields valid", elapsed


# ── Main ─────────────────────────────────────────────────────────────────


def _parse_args() -> argparse.Namespace:
    """Parse CLI arguments with env-var fallbacks."""
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Validate vLLM structured output for pspcz_analyzer",
    )
    parser.add_argument(
        "--base-url",
        default=os.environ.get("OPENAI_BASE_URL", "http://localhost:8000/v1"),
        help="OpenAI-compatible base URL (default: $OPENAI_BASE_URL)",
    )
    parser.add_argument(
        "--model",
        default=os.environ.get("OPENAI_MODEL", "gpt-oss-20b-vllm"),
        help="Model name (default: $OPENAI_MODEL)",
    )
    parser.add_argument(
        "--api-key",
        default=os.environ.get("OPENAI_API_KEY", ""),
        help="API key (default: $OPENAI_API_KEY)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Print full request/response payloads",
    )
    parser.add_argument(
        "--no-strict",
        action="store_true",
        help="Drop 'strict' and 'additionalProperties' from schemas (vLLM xgrammar compat)",
    )
    return parser.parse_args()


def main() -> None:
    """Run all validation tests."""
    args = _parse_args()
    headers = _build_headers(args.api_key)
    strict = not args.no_strict

    console.print(
        Panel(
            f"[bold]Endpoint:[/] {args.base_url}\n"
            f"[bold]Model:[/]    {args.model}\n"
            f"[bold]Auth:[/]     {'Bearer ***' + args.api_key[-4:] if args.api_key else 'none'}\n"
            f"[bold]Strict:[/]   {strict}",
            title="vLLM Structured Output Validation",
        )
    )

    tests = [
        ("Connection (GET /models)", test_connection),
        ("Plain generation", test_plain_generation),
        ("Structured: classify+summarize", test_structured_classify_summarize),
        ("Structured: version comparison", test_structured_comparison),
    ]

    table = Table(title="Test Results")
    table.add_column("Test", style="bold")
    table.add_column("Status")
    table.add_column("Detail")
    table.add_column("Latency", justify="right")

    all_passed = True
    for name, test_fn in tests:
        try:
            passed, detail, elapsed = test_fn(
                args.base_url, args.model, headers, args.verbose, strict=strict
            )
        except Exception as exc:
            passed, detail, elapsed = False, f"Exception: {exc}", 0.0

        status = "[green]PASS[/]" if passed else "[red]FAIL[/]"
        if not passed:
            all_passed = False
        table.add_row(name, status, detail, f"{elapsed:.2f}s")

    console.print()
    console.print(table)
    console.print()

    if all_passed:
        console.print("[bold green]All tests passed.[/]")
    else:
        console.print("[bold red]Some tests failed.[/]")
        if strict:
            console.print(
                "[yellow]Tip: try --no-strict to strip additionalProperties "
                "(required for vLLM xgrammar backend)[/]"
            )

    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
