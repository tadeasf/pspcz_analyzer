"""Health check and LLM diagnostic endpoints."""

import asyncio
import time

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from pspcz_analyzer.config import LLM_PROVIDER
from pspcz_analyzer.rate_limit import limiter
from pspcz_analyzer.services.llm import create_llm_client

router = APIRouter(tags=["Health"])


@router.get("/health", response_class=JSONResponse)
@limiter.limit("120/minute")
async def health(request: Request):
    """Health check endpoint."""
    data_svc = request.app.state.data
    return {"status": "ok", "periods_loaded": list(data_svc.loaded_periods)}


# ── LLM diagnostic constants ─────────────────────────────────────────────

_SMOKE_TEST_TITLE = "Novela zákona o státní službě"
_SMOKE_TEST_TEXT = (
    "Navrhovaná novela zákona č. 234/2014 Sb., o státní službě, zavádí nový systém "
    "hodnocení výkonu státních zaměstnanců založený na klíčových ukazatelích výkonnosti. "
    "Služební orgán bude oprávněn stanovit individuální výkonnostní cíle pro každého "
    "zaměstnance a jejich plnění bude podmínkou pro přiznání osobního příplatku.\n\n"
    "Dále se mění ustanovení § 72 odst. 1 tak, že se zkracuje doba potřebná pro vznik "
    "nároku na služební volno z pěti na tři roky nepřetržité služby. Současně se ruší "
    "povinnost služebního úřadu zajistit zastupitelnost zaměstnanců v době jejich "
    "nepřítomnosti delší než 30 dnů.\n\n"
    "Přechodná ustanovení stanoví, že stávající zaměstnanci budou hodnoceni podle nových "
    "pravidel nejpozději od 1. ledna následujícího kalendářního roku po nabytí účinnosti "
    "této novely."
)


@router.get("/llm/health", response_class=JSONResponse)
@limiter.limit("10/minute")
async def llm_health(request: Request) -> dict:
    """Check LLM connectivity and model availability."""
    try:
        client = create_llm_client()
    except ValueError as exc:
        return {"available": False, "provider": LLM_PROVIDER, "error": str(exc)}
    available = await asyncio.to_thread(client.is_available)
    return {
        "available": available,
        "provider": LLM_PROVIDER,
        "base_url": client.base_url,
        "model": client.model,
    }


def _build_smoke_error(error: str, duration: float, model: str) -> dict:
    """Build a failure response dict for the smoke-test endpoint."""
    return {
        "success": False,
        "provider": LLM_PROVIDER,
        "error": error,
        "duration_seconds": round(duration, 2),
        "model": model,
    }


@router.get("/llm/smoke-test", response_class=JSONResponse)
@limiter.limit("2/minute")
async def llm_smoke_test(request: Request) -> dict:
    """Run concurrent bilingual generation to verify LLM end-to-end."""
    try:
        client = create_llm_client()
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=_build_smoke_error(str(exc), 0.0, "unknown"),
        ) from exc
    start = time.monotonic()

    available = await asyncio.to_thread(client.is_available)
    if not available:
        duration = time.monotonic() - start
        raise HTTPException(
            status_code=503,
            detail=_build_smoke_error("LLM is not available", duration, client.model),
        )

    try:
        cs_result = await asyncio.to_thread(client.summarize, _SMOKE_TEST_TEXT, _SMOKE_TEST_TITLE)
        en_result = await asyncio.to_thread(
            client.summarize_en, _SMOKE_TEST_TEXT, _SMOKE_TEST_TITLE
        )
    except Exception as exc:
        duration = time.monotonic() - start
        raise HTTPException(
            status_code=502,
            detail=_build_smoke_error(str(exc), duration, client.model),
        ) from exc

    duration = time.monotonic() - start
    return {
        "success": True,
        "provider": LLM_PROVIDER,
        "model": client.model,
        "duration_seconds": round(duration, 2),
        "summary_cs": cs_result,
        "summary_en": en_result,
        "summary_cs_length": len(cs_result),
        "summary_en_length": len(en_result),
    }
