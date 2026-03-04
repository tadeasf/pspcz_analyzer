"""LLM client implementation — LLMClient class."""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from typing import Any

import httpx
from loguru import logger

from pspcz_analyzer.config import (
    LLM_EMPTY_RETRIES,
    LLM_HEALTH_TIMEOUT,
    LLM_MAX_COMPARISON_CHARS,
    LLM_TIMEOUT,
)
from pspcz_analyzer.services.llm.helpers import (
    _THINK_RE,
    _sanitize_llm_input,
    truncate_legislative_text,
)
from pspcz_analyzer.services.llm.parsers import (
    _format_amendments_list,
    _parse_amendment_summaries_json,
    _parse_amendment_summaries_text,
    _parse_consolidation_json,
    _render_comparison_markdown_cs,
    _render_comparison_markdown_en,
    _render_summary_markdown_cs,
    _render_summary_markdown_en,
)
from pspcz_analyzer.services.llm.prompts import (
    _AMENDMENT_SUMMARIES_PROMPT_CS,
    _AMENDMENT_SUMMARIES_PROMPT_EN,
    _AMENDMENT_SUMMARIES_SCHEMA,
    _AMENDMENT_SUMMARIES_SYSTEM_CS,
    _AMENDMENT_SUMMARIES_SYSTEM_EN,
    _CLASSIFICATION_PROMPT_TEMPLATE,
    _CLASSIFICATION_PROMPT_TEMPLATE_EN,
    _CLASSIFICATION_SCHEMA,
    _CLASSIFICATION_SYSTEM,
    _CLASSIFICATION_SYSTEM_EN,
    _CLASSIFY_AND_SUMMARIZE_SCHEMA,
    _COMBINED_PROMPT_TEMPLATE_CS,
    _COMBINED_PROMPT_TEMPLATE_EN,
    _COMBINED_SYSTEM_CS,
    _COMBINED_SYSTEM_EN,
    _COMPARISON_PROMPT_TEMPLATE,
    _COMPARISON_PROMPT_TEMPLATE_EN,
    _COMPARISON_SCHEMA,
    _COMPARISON_SYSTEM,
    _COMPARISON_SYSTEM_EN,
    _CONSOLIDATION_PROMPT_TEMPLATE,
    _CONSOLIDATION_PROMPT_TEMPLATE_EN,
    _CONSOLIDATION_SCHEMA,
    _CONSOLIDATION_SYSTEM,
    _CONSOLIDATION_SYSTEM_EN,
    _STRUCTURED_AMENDMENT_SUMMARIES_PROMPT_CS,
    _STRUCTURED_AMENDMENT_SUMMARIES_PROMPT_EN,
    _STRUCTURED_AMENDMENT_SUMMARIES_SYSTEM_CS,
    _STRUCTURED_AMENDMENT_SUMMARIES_SYSTEM_EN,
    _STRUCTURED_CLASSIFICATION_PROMPT_CS,
    _STRUCTURED_CLASSIFICATION_PROMPT_EN,
    _STRUCTURED_CLASSIFICATION_SYSTEM_CS,
    _STRUCTURED_CLASSIFICATION_SYSTEM_EN,
    _STRUCTURED_CLASSIFY_AND_SUMMARIZE_PROMPT_CS,
    _STRUCTURED_CLASSIFY_AND_SUMMARIZE_PROMPT_EN,
    _STRUCTURED_CLASSIFY_AND_SUMMARIZE_SYSTEM_CS,
    _STRUCTURED_CLASSIFY_AND_SUMMARIZE_SYSTEM_EN,
    _STRUCTURED_COMPARISON_PROMPT_CS,
    _STRUCTURED_COMPARISON_PROMPT_EN,
    _STRUCTURED_COMPARISON_SYSTEM_CS,
    _STRUCTURED_COMPARISON_SYSTEM_EN,
    _STRUCTURED_CONSOLIDATION_PROMPT_CS,
    _STRUCTURED_CONSOLIDATION_PROMPT_EN,
    _STRUCTURED_CONSOLIDATION_SYSTEM_CS,
    _STRUCTURED_CONSOLIDATION_SYSTEM_EN,
    _STRUCTURED_SUMMARY_PROMPT_CS,
    _STRUCTURED_SUMMARY_PROMPT_EN,
    _STRUCTURED_SUMMARY_SYSTEM_CS,
    _STRUCTURED_SUMMARY_SYSTEM_EN,
    _SUMMARY_PROMPT_TEMPLATE,
    _SUMMARY_PROMPT_TEMPLATE_EN,
    _SUMMARY_SCHEMA,
    _SUMMARY_SYSTEM,
    _SUMMARY_SYSTEM_EN,
)


class LLMClient:
    """Unified LLM client supporting ollama and openai providers.

    Args:
        provider: ``"ollama"`` or ``"openai"`` — controls which protocol is used.
        base_url: LLM API base URL.
        model: Model name/identifier.
        timeout: Per-request timeout in seconds.
        api_key: Bearer token for API authentication (empty = no auth).
        structured_output: Whether to use JSON schema–constrained output.
    """

    def __init__(
        self,
        *,
        provider: str,
        base_url: str,
        model: str,
        timeout: float = LLM_TIMEOUT,
        api_key: str = "",
        structured_output: bool = True,
    ) -> None:
        self.provider = provider
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.timeout = timeout
        self._structured_output = structured_output
        self._available: bool | None = None
        self._openai_compat: bool = False
        self._headers: dict[str, str] = {}
        self._log_prefix = f"[{provider}]"
        if api_key:
            self._headers["Authorization"] = f"Bearer {api_key}"
        if provider == "openai":
            self._headers.setdefault("Content-Type", "application/json")

    @property
    def supports_structured_output(self) -> bool:
        """Whether this client uses JSON schema–constrained output."""
        return self._structured_output

    # ── Availability detection ────────────────────────────────────────

    def is_available(self) -> bool:
        """Check if the LLM backend is reachable. Caches after first call."""
        if self._available is not None:
            return self._available

        match self.provider:
            case "ollama":
                return self._check_ollama_availability()
            case "openai":
                return self._check_openai_availability()
            case _:
                self._available = False
                return False

    def _check_ollama_availability(self) -> bool:
        """Try native Ollama, then fall back to OpenAI-compatible endpoint."""
        if self._check_native_ollama():
            self._openai_compat = False
            self._available = True
            return True

        if self._check_openai_compat():
            self._openai_compat = True
            self._log_prefix = "[ollama/openai-compat]"
            self._available = True
            logger.info(
                "[ollama/openai-compat] Available at {} with model {}",
                self.base_url,
                self.model,
            )
            return True

        self._available = False
        logger.info("[ollama] Not available (connection failed)")
        return False

    def _check_openai_availability(self) -> bool:
        """Check if the OpenAI-compatible API is reachable."""
        try:
            resp = httpx.get(
                f"{self.base_url}/models",
                headers=self._headers,
                timeout=LLM_HEALTH_TIMEOUT,
            )
            resp.raise_for_status()
            self._available = True
            logger.info("[openai] Available at {} with model {}", self.base_url, self.model)
        except Exception:
            self._available = False
            logger.info("[openai] Not available (connection to {} failed)", self.base_url)
        return self._available

    def _check_native_ollama(self) -> bool:
        """Try native Ollama ``GET /api/tags``. Returns True if reachable."""
        try:
            resp = httpx.get(
                f"{self.base_url}/api/tags",
                headers=self._headers,
                timeout=LLM_HEALTH_TIMEOUT,
            )
            resp.raise_for_status()
            models = [m.get("name", "") for m in resp.json().get("models", [])]
            found = any(
                m == self.model
                or m.startswith(f"{self.model}:")
                or self.model.startswith(f"{m}:")
                or m == self.model.split(":")[0]
                for m in models
            )
            if found:
                logger.info("[ollama] Available (native) with model {}", self.model)
            else:
                logger.info(
                    "[ollama] Running (native) but model {} not found (available: {})",
                    self.model,
                    ", ".join(models),
                )
            return found
        except Exception:
            return False

    def _check_openai_compat(self) -> bool:
        """Try OpenAI-compatible ``GET /models``. Returns True if reachable."""
        try:
            resp = httpx.get(
                f"{self.base_url}/models",
                headers=self._headers,
                timeout=LLM_HEALTH_TIMEOUT,
            )
            resp.raise_for_status()
            return True
        except Exception:
            return False

    # ── Generation dispatch ───────────────────────────────────────────

    def _generate(
        self,
        prompt: str,
        system: str,
        *,
        response_format: dict[str, Any] | None = None,
    ) -> str | None:
        """Dispatch generation to the appropriate backend."""
        match self.provider:
            case "ollama":
                if self._openai_compat:
                    return self._generate_openai_compat(
                        prompt, system, response_format=response_format
                    )
                return self._generate_native_ollama(prompt, system, response_format=response_format)
            case "openai":
                return self._generate_openai_compat(prompt, system, response_format=response_format)
            case _:
                return None

    def _generate_with_retry(
        self,
        prompt: str,
        system: str,
        *,
        validator: Callable[[str], bool],
    ) -> str | None:
        """Generate with retries when validator rejects the response.

        Used for the free-text (non-structured) path where regex parsing
        may fail on weaker models. Retries up to LLM_EMPTY_RETRIES times.
        """
        for attempt in range(1 + LLM_EMPTY_RETRIES):
            response = self._generate(prompt, system)
            if response is not None and validator(response):
                return response
            if attempt < LLM_EMPTY_RETRIES:
                logger.debug(
                    "{} Attempt {}/{} returned empty/invalid, retrying",
                    self._log_prefix,
                    attempt + 1,
                    1 + LLM_EMPTY_RETRIES,
                )
        logger.warning(
            "{} All {} attempts returned empty/invalid",
            self._log_prefix,
            1 + LLM_EMPTY_RETRIES,
        )
        return None

    def _generate_native_ollama(
        self,
        prompt: str,
        system: str,
        *,
        response_format: dict[str, Any] | None = None,
    ) -> str | None:
        """Send a generation request via native Ollama ``/api/generate``."""
        payload: dict[str, Any] = {
            "model": self.model,
            "prompt": prompt,
            "system": system,
            "stream": False,
        }
        if response_format is not None:
            raw_schema = response_format.get("json_schema", {}).get("schema")
            if raw_schema is not None:
                payload["format"] = raw_schema
        try:
            resp = httpx.post(
                f"{self.base_url}/api/generate",
                headers=self._headers,
                json=payload,
                timeout=self.timeout,
            )
            resp.raise_for_status()
            return resp.json().get("response")
        except Exception:
            logger.opt(exception=True).debug("[ollama] Native generation request failed")
            return None

    def _generate_openai_compat(
        self,
        prompt: str,
        system: str,
        *,
        response_format: dict[str, Any] | None = None,
    ) -> str | None:
        """Send a generation request via OpenAI-compatible ``/chat/completions``."""
        payload: dict[str, Any] = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": prompt},
            ],
            "stream": False,
        }
        if response_format is not None:
            payload["response_format"] = response_format
        try:
            resp = httpx.post(
                f"{self.base_url}/chat/completions",
                headers=self._headers,
                json=payload,
                timeout=self.timeout,
            )
            resp.raise_for_status()
            data = resp.json()
            choices = data.get("choices", [])
            if choices:
                return choices[0].get("message", {}).get("content")
            return None
        except Exception:
            logger.opt(exception=True).debug("{} Chat completion request failed", self._log_prefix)
            return None

    # ── Structured JSON generation with fallback ─────────────────────

    def _generate_json(
        self,
        prompt: str,
        system: str,
        schema: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Generate structured JSON with schema-constrained output + prompt fallback.

        1. Try schema-constrained generation via response_format.
        2. On failure, retry with prompt-based JSON instructions (no schema constraint).
        """
        result = self._generate_json_schema_constrained(prompt, system, schema)
        if result is not None:
            return result
        logger.debug(
            "{} Structured output failed, trying prompt-based JSON fallback",
            self._log_prefix,
        )
        return self._generate_json_via_prompt(prompt, system, schema)

    def _generate_json_schema_constrained(
        self,
        prompt: str,
        system: str,
        schema: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Try schema-constrained JSON generation via response_format."""
        response_format = {
            "type": "json_schema",
            "json_schema": {"name": "response", "strict": True, "schema": schema},
        }
        raw = self._generate(prompt, system, response_format=response_format)
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            logger.debug("{} Failed to parse JSON response: {}", self._log_prefix, raw[:200])
            return None

    @staticmethod
    def _extract_json_from_text(text: str) -> dict[str, Any] | None:
        """Extract a JSON object from free-form text.

        Tries ``json.loads`` on the full text first.  Falls back to extracting
        the substring between the first ``{`` and the last ``}``.
        """
        text = _THINK_RE.sub("", text).strip()
        try:
            return json.loads(text)
        except (json.JSONDecodeError, TypeError):
            pass
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end > start:
            try:
                return json.loads(text[start : end + 1])
            except (json.JSONDecodeError, TypeError):
                pass
        return None

    def _generate_json_via_prompt(
        self,
        prompt: str,
        system: str,
        schema: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Fallback: append JSON instructions to the prompt and parse the result.

        Does NOT pass ``response_format`` so the model generates freely.
        """
        required_keys = list(schema.get("properties", {}).keys())
        keys_hint = ", ".join(f'"{k}"' for k in required_keys)
        json_instruction = (
            "\n\nIMPORTANT: Respond with valid JSON only.  "
            "Do NOT include any text before or after the JSON object.  "
            f"The JSON object MUST contain these keys: {keys_hint}"
        )
        raw = self._generate(prompt + json_instruction, system)
        if raw is None:
            return None
        result = self._extract_json_from_text(raw)
        if result is None:
            logger.debug(
                "%s JSON extraction failed from prompt fallback. Raw[:300]: %s",
                self._log_prefix,
                raw[:300],
            )
        return result

    # ── Business methods ────────────────────────────────────────────

    def classify_topics(self, text: str, title: str, *, _truncated: bool = False) -> list[str]:
        """Classify a tisk into 1-3 free-form Czech topic labels using the LLM."""
        truncated = text if _truncated else truncate_legislative_text(text)
        sanitized_title = _sanitize_llm_input(title or "(bez názvu)")
        sanitized_text = _sanitize_llm_input(truncated)

        if self.supports_structured_output:
            prompt = _STRUCTURED_CLASSIFICATION_PROMPT_CS.format(
                title=sanitized_title, text=sanitized_text
            )
            data = self._generate_json(
                prompt,
                _STRUCTURED_CLASSIFICATION_SYSTEM_CS,
                _CLASSIFICATION_SCHEMA,
            )
            if data is None:
                return []
            topics = [t.strip() for t in data.get("topics", []) if isinstance(t, str) and t.strip()]
            return topics[:3]

        prompt = _CLASSIFICATION_PROMPT_TEMPLATE.format(title=sanitized_title, text=sanitized_text)
        response = self._generate_with_retry(
            prompt,
            _CLASSIFICATION_SYSTEM,
            validator=lambda r: bool(self._parse_topics_response(r)),
        )
        if response is None:
            return []
        return self._parse_topics_response(response)

    def summarize(self, text: str, title: str, *, _truncated: bool = False) -> str:
        """Generate a Czech-language summary of what a proposed law changes."""
        truncated = text if _truncated else truncate_legislative_text(text)
        sanitized_title = _sanitize_llm_input(title or "(bez názvu)")
        sanitized_text = _sanitize_llm_input(truncated)

        if self.supports_structured_output:
            prompt = _STRUCTURED_SUMMARY_PROMPT_CS.format(
                title=sanitized_title, text=sanitized_text
            )
            data = self._generate_json(
                prompt,
                _STRUCTURED_SUMMARY_SYSTEM_CS,
                _SUMMARY_SCHEMA,
            )
            if data is None:
                return ""
            return _render_summary_markdown_cs(data)

        prompt = _SUMMARY_PROMPT_TEMPLATE.format(title=sanitized_title, text=sanitized_text)
        response = self._generate_with_retry(
            prompt,
            _SUMMARY_SYSTEM,
            validator=lambda r: bool(self._strip_think(r).strip()),
        )
        if not response:
            return ""
        return self._strip_think(response)

    def summarize_en(self, text: str, title: str, *, _truncated: bool = False) -> str:
        """Generate an English-language critical summary of a proposed law."""
        truncated = text if _truncated else truncate_legislative_text(text)
        sanitized_title = _sanitize_llm_input(title or "(no title)")
        sanitized_text = _sanitize_llm_input(truncated)

        if self.supports_structured_output:
            prompt = _STRUCTURED_SUMMARY_PROMPT_EN.format(
                title=sanitized_title, text=sanitized_text
            )
            data = self._generate_json(
                prompt,
                _STRUCTURED_SUMMARY_SYSTEM_EN,
                _SUMMARY_SCHEMA,
            )
            if data is None:
                return ""
            return _render_summary_markdown_en(data)

        prompt = _SUMMARY_PROMPT_TEMPLATE_EN.format(title=sanitized_title, text=sanitized_text)
        response = self._generate_with_retry(
            prompt,
            _SUMMARY_SYSTEM_EN,
            validator=lambda r: bool(self._strip_think(r).strip()),
        )
        if not response:
            return ""
        return self._strip_think(response)

    def consolidate_topics(self, all_topics: list[str]) -> dict[str, str]:
        """Consolidate/deduplicate topic labels via the LLM."""
        topics_list = "\n".join(f"- {t}" for t in all_topics)

        if self.supports_structured_output:
            prompt = _STRUCTURED_CONSOLIDATION_PROMPT_CS.format(
                n=len(all_topics), topics_list=topics_list
            )
            data = self._generate_json(
                prompt,
                _STRUCTURED_CONSOLIDATION_SYSTEM_CS,
                _CONSOLIDATION_SCHEMA,
            )
            if data is None:
                return {t: t for t in all_topics}
            return _parse_consolidation_json(data, all_topics)

        prompt = _CONSOLIDATION_PROMPT_TEMPLATE.format(n=len(all_topics), topics_list=topics_list)
        response = self._generate(prompt, _CONSOLIDATION_SYSTEM)
        if not response:
            return {t: t for t in all_topics}
        return self._parse_consolidation_response(response, all_topics)

    def classify_topics_en(self, text: str, title: str, *, _truncated: bool = False) -> list[str]:
        """Classify a tisk into 1-3 free-form English topic labels using the LLM."""
        truncated = text if _truncated else truncate_legislative_text(text)
        sanitized_title = _sanitize_llm_input(title or "(no title)")
        sanitized_text = _sanitize_llm_input(truncated)

        if self.supports_structured_output:
            prompt = _STRUCTURED_CLASSIFICATION_PROMPT_EN.format(
                title=sanitized_title, text=sanitized_text
            )
            data = self._generate_json(
                prompt,
                _STRUCTURED_CLASSIFICATION_SYSTEM_EN,
                _CLASSIFICATION_SCHEMA,
            )
            if data is None:
                return []
            topics = [t.strip() for t in data.get("topics", []) if isinstance(t, str) and t.strip()]
            return topics[:3]

        prompt = _CLASSIFICATION_PROMPT_TEMPLATE_EN.format(
            title=sanitized_title, text=sanitized_text
        )
        response = self._generate_with_retry(
            prompt,
            _CLASSIFICATION_SYSTEM_EN,
            validator=lambda r: bool(self._parse_topics_response(r)),
        )
        if response is None:
            return []
        return self._parse_topics_response(response)

    def classify_topics_bilingual(self, text: str, title: str) -> tuple[list[str], list[str]]:
        """Classify a tisk into topic labels in both Czech and English."""
        truncated = truncate_legislative_text(text)
        topics_cs = self.classify_topics(truncated, title, _truncated=True)
        topics_en = self.classify_topics_en(truncated, title, _truncated=True)
        return topics_cs, topics_en

    def consolidate_topics_en(self, all_topics: list[str]) -> dict[str, str]:
        """Consolidate/deduplicate English topic labels via the LLM."""
        topics_list = "\n".join(f"- {t}" for t in all_topics)

        if self.supports_structured_output:
            prompt = _STRUCTURED_CONSOLIDATION_PROMPT_EN.format(
                n=len(all_topics), topics_list=topics_list
            )
            data = self._generate_json(
                prompt,
                _STRUCTURED_CONSOLIDATION_SYSTEM_EN,
                _CONSOLIDATION_SCHEMA,
            )
            if data is None:
                return {t: t for t in all_topics}
            return _parse_consolidation_json(data, all_topics)

        prompt = _CONSOLIDATION_PROMPT_TEMPLATE_EN.format(
            n=len(all_topics), topics_list=topics_list
        )
        response = self._generate(prompt, _CONSOLIDATION_SYSTEM_EN)
        if not response:
            return {t: t for t in all_topics}
        return self._parse_consolidation_response(response, all_topics)

    def consolidate_topics_bilingual(
        self,
        all_topics_cs: list[str],
        all_topics_en: list[str],
    ) -> tuple[dict[str, str], dict[str, str]]:
        """Consolidate topic labels in both Czech and English."""
        mapping_cs = self.consolidate_topics(all_topics_cs)
        mapping_en = self.consolidate_topics_en(all_topics_en)
        return mapping_cs, mapping_en

    def compare_versions(
        self,
        text_old: str,
        text_new: str,
        ct1_old: int,
        ct1_new: int,
        label_old: str = "",
        label_new: str = "",
        *,
        _truncated: bool = False,
    ) -> str:
        """Compare two versions of a tisk and return a Czech-language diff summary."""
        trunc_old = (
            text_old
            if _truncated
            else truncate_legislative_text(text_old, max_chars=LLM_MAX_COMPARISON_CHARS)
        )
        trunc_new = (
            text_new
            if _truncated
            else truncate_legislative_text(text_new, max_chars=LLM_MAX_COMPARISON_CHARS)
        )
        fmt_kwargs = {
            "ct1_old": ct1_old,
            "ct1_new": ct1_new,
            "label_old": label_old or f"CT1={ct1_old}",
            "label_new": label_new or f"CT1={ct1_new}",
            "text_old": _sanitize_llm_input(trunc_old),
            "text_new": _sanitize_llm_input(trunc_new),
        }

        if self.supports_structured_output:
            prompt = _STRUCTURED_COMPARISON_PROMPT_CS.format(**fmt_kwargs)
            data = self._generate_json(
                prompt,
                _STRUCTURED_COMPARISON_SYSTEM_CS,
                _COMPARISON_SCHEMA,
            )
            if data is None:
                return ""
            return _render_comparison_markdown_cs(data)

        prompt = _COMPARISON_PROMPT_TEMPLATE.format(**fmt_kwargs)
        response = self._generate_with_retry(
            prompt,
            _COMPARISON_SYSTEM,
            validator=lambda r: bool(self._strip_think(r).strip()),
        )
        if not response:
            return ""
        return self._strip_think(response)

    def summarize_bilingual(self, text: str, title: str) -> dict[str, str]:
        """Generate both Czech and English summaries."""
        truncated = truncate_legislative_text(text)
        cs = self.summarize(truncated, title, _truncated=True)
        en = self.summarize_en(truncated, title, _truncated=True)
        return {"cs": cs, "en": en}

    def _parse_combined_response(self, response: str) -> tuple[list[str], dict[str, str]]:
        """Parse combined classify+summarize free-text response.

        Expects format:
            TOPICS: topic1, topic2, topic3
            CHANGES: ...
            IMPACT: ...
            RISKS: ...

        Returns:
            (topics, summary_data_dict) — partial success is acceptable.
        """
        response = self._strip_think(response)

        # Extract topics
        topics: list[str] = []
        topics_match = re.search(r"TOPICS?:\s*(.+)", response, re.IGNORECASE)
        if topics_match:
            raw = topics_match.group(1).strip()
            topics = [t.strip().strip(".,;:-–*#") for t in raw.split(",")]
            topics = [t for t in topics if t and t.lower() != "none" and re.search(r"\w", t)]
            topics = topics[:3]

        # Extract summary fields
        changes = ""
        impact = ""
        risks = ""

        changes_match = re.search(
            r"CHANGES?:\s*(.+?)(?=\n(?:IMPACT|RISKS?)\s*:|$)", response, re.IGNORECASE | re.DOTALL
        )
        if changes_match:
            changes = changes_match.group(1).strip()

        impact_match = re.search(
            r"IMPACT:\s*(.+?)(?=\n(?:RISKS?)\s*:|$)", response, re.IGNORECASE | re.DOTALL
        )
        if impact_match:
            impact = impact_match.group(1).strip()

        risks_match = re.search(r"RISKS?:\s*(.+)", response, re.IGNORECASE | re.DOTALL)
        if risks_match:
            risks = risks_match.group(1).strip()

        return topics, {"changes": changes, "impact": impact, "risks": risks}

    def classify_and_summarize(
        self, text: str, title: str, *, _truncated: bool = False
    ) -> tuple[list[str], str]:
        """Classify topics + summarize in a single LLM call (Czech).

        Returns:
            (topics, summary_markdown) — ([], "") on failure.
        """
        truncated = text if _truncated else truncate_legislative_text(text)
        sanitized_title = _sanitize_llm_input(title or "(bez názvu)")
        sanitized_text = _sanitize_llm_input(truncated)

        if self.supports_structured_output:
            prompt = _STRUCTURED_CLASSIFY_AND_SUMMARIZE_PROMPT_CS.format(
                title=sanitized_title, text=sanitized_text
            )
            data = self._generate_json(
                prompt,
                _STRUCTURED_CLASSIFY_AND_SUMMARIZE_SYSTEM_CS,
                _CLASSIFY_AND_SUMMARIZE_SCHEMA,
            )
            if data is None:
                return [], ""
            topics = [t.strip() for t in data.get("topics", []) if isinstance(t, str) and t.strip()]
            return topics[:3], _render_summary_markdown_cs(data)

        prompt = _COMBINED_PROMPT_TEMPLATE_CS.format(title=sanitized_title, text=sanitized_text)

        def _validate_combined(r: str) -> bool:
            topics, data = self._parse_combined_response(r)
            return bool(topics) or bool(data.get("changes"))

        response = self._generate_with_retry(
            prompt, _COMBINED_SYSTEM_CS, validator=_validate_combined
        )
        if response is None:
            return [], ""
        topics, summary_data = self._parse_combined_response(response)
        return topics, _render_summary_markdown_cs(summary_data)

    def classify_and_summarize_en(
        self, text: str, title: str, *, _truncated: bool = False
    ) -> tuple[list[str], str]:
        """Classify topics + summarize in a single LLM call (English).

        Returns:
            (topics, summary_markdown) — ([], "") on failure.
        """
        truncated = text if _truncated else truncate_legislative_text(text)
        sanitized_title = _sanitize_llm_input(title or "(no title)")
        sanitized_text = _sanitize_llm_input(truncated)

        if self.supports_structured_output:
            prompt = _STRUCTURED_CLASSIFY_AND_SUMMARIZE_PROMPT_EN.format(
                title=sanitized_title, text=sanitized_text
            )
            data = self._generate_json(
                prompt,
                _STRUCTURED_CLASSIFY_AND_SUMMARIZE_SYSTEM_EN,
                _CLASSIFY_AND_SUMMARIZE_SCHEMA,
            )
            if data is None:
                return [], ""
            topics = [t.strip() for t in data.get("topics", []) if isinstance(t, str) and t.strip()]
            return topics[:3], _render_summary_markdown_en(data)

        prompt = _COMBINED_PROMPT_TEMPLATE_EN.format(title=sanitized_title, text=sanitized_text)

        def _validate_combined(r: str) -> bool:
            topics, data = self._parse_combined_response(r)
            return bool(topics) or bool(data.get("changes"))

        response = self._generate_with_retry(
            prompt, _COMBINED_SYSTEM_EN, validator=_validate_combined
        )
        if response is None:
            return [], ""
        topics, summary_data = self._parse_combined_response(response)
        return topics, _render_summary_markdown_en(summary_data)

    def classify_and_summarize_bilingual(
        self, text: str, title: str
    ) -> tuple[list[str], list[str], str, str]:
        """Classify + summarize in both languages (2 LLM calls instead of 4).

        Returns:
            (topics_cs, topics_en, summary_cs, summary_en)
        """
        truncated = truncate_legislative_text(text)
        topics_cs, summary_cs = self.classify_and_summarize(truncated, title, _truncated=True)
        topics_en, summary_en = self.classify_and_summarize_en(truncated, title, _truncated=True)
        return topics_cs, topics_en, summary_cs, summary_en

    def compare_versions_bilingual(
        self,
        text_old: str,
        text_new: str,
        ct1_old: int,
        ct1_new: int,
        label_old: str = "",
        label_new: str = "",
    ) -> dict[str, str]:
        """Compare two versions and return bilingual diff summaries."""
        trunc_old = truncate_legislative_text(text_old, max_chars=LLM_MAX_COMPARISON_CHARS)
        trunc_new = truncate_legislative_text(text_new, max_chars=LLM_MAX_COMPARISON_CHARS)
        cs = self.compare_versions(
            trunc_old, trunc_new, ct1_old, ct1_new, label_old, label_new, _truncated=True
        )
        fmt_kwargs = {
            "ct1_old": ct1_old,
            "ct1_new": ct1_new,
            "label_old": label_old or f"CT1={ct1_old}",
            "label_new": label_new or f"CT1={ct1_new}",
            "text_old": _sanitize_llm_input(trunc_old),
            "text_new": _sanitize_llm_input(trunc_new),
        }

        if self.supports_structured_output:
            prompt = _STRUCTURED_COMPARISON_PROMPT_EN.format(**fmt_kwargs)
            data = self._generate_json(
                prompt,
                _STRUCTURED_COMPARISON_SYSTEM_EN,
                _COMPARISON_SCHEMA,
            )
            en = _render_comparison_markdown_en(data) if data else ""
        else:
            prompt = _COMPARISON_PROMPT_TEMPLATE_EN.format(**fmt_kwargs)
            response = self._generate(prompt, _COMPARISON_SYSTEM_EN)
            en = self._strip_think(response) if response else ""

        return {"cs": cs, "en": en}

    def summarize_amendments(
        self,
        text: str,
        title: str,
        amendments: list[dict[str, str]],
        *,
        lang: str = "cs",
        bill_context: str = "",
        _truncated: bool = False,
    ) -> dict[str, str]:
        """Generate per-amendment summaries for a bill in a single LLM call.

        Args:
            text: Combined amendment PDF text.
            title: Bill title.
            amendments: List of dicts with 'letter', 'submitter', 'description'.
            lang: Language code ('cs' or 'en').
            bill_context: Optional AI-generated summary of the original bill
                for additional context in prompts.
            _truncated: If True, skip truncation.

        Returns:
            Dict mapping amendment letter to summary text.
        """
        truncated = text if _truncated else truncate_legislative_text(text)
        sanitized_title = _sanitize_llm_input(
            title or ("(bez názvu)" if lang == "cs" else "(no title)")
        )
        sanitized_text = _sanitize_llm_input(truncated)
        amendments_list = _format_amendments_list(amendments)

        if bill_context:
            if lang == "cs":
                bill_context_section = (
                    f"Kontext návrhu zákona (AI shrnutí původního tisku):\n{bill_context}\n\n"
                )
            else:
                bill_context_section = (
                    f"Bill context (AI-generated summary of the original bill):\n{bill_context}\n\n"
                )
        else:
            bill_context_section = ""

        fmt_kwargs = {
            "title": sanitized_title,
            "text": sanitized_text,
            "amendments_list": amendments_list,
            "bill_context_section": bill_context_section,
        }

        if lang == "cs":
            structured_system = _STRUCTURED_AMENDMENT_SUMMARIES_SYSTEM_CS
            structured_prompt_tpl = _STRUCTURED_AMENDMENT_SUMMARIES_PROMPT_CS
            freetext_system = _AMENDMENT_SUMMARIES_SYSTEM_CS
            freetext_prompt_tpl = _AMENDMENT_SUMMARIES_PROMPT_CS
        else:
            structured_system = _STRUCTURED_AMENDMENT_SUMMARIES_SYSTEM_EN
            structured_prompt_tpl = _STRUCTURED_AMENDMENT_SUMMARIES_PROMPT_EN
            freetext_system = _AMENDMENT_SUMMARIES_SYSTEM_EN
            freetext_prompt_tpl = _AMENDMENT_SUMMARIES_PROMPT_EN

        if self.supports_structured_output:
            prompt = structured_prompt_tpl.format(**fmt_kwargs)
            data = self._generate_json(prompt, structured_system, _AMENDMENT_SUMMARIES_SCHEMA)
            if data is None:
                logger.debug(
                    "%s Amendment summaries: structured output returned None",
                    self._log_prefix,
                )
                return {}
            result = _parse_amendment_summaries_json(data)
            logger.debug(
                "%s Amendment summaries (%s): %d keys from %d items",
                self._log_prefix,
                lang,
                len(result),
                len(data.get("amendments", [])),
            )
            return result

        prompt = freetext_prompt_tpl.format(**fmt_kwargs)
        response = self._generate_with_retry(
            prompt,
            freetext_system,
            validator=lambda r: bool(_parse_amendment_summaries_text(r)),
        )
        if response is None:
            logger.debug(
                "%s Amendment summaries (%s): free-text returned None",
                self._log_prefix,
                lang,
            )
            return {}
        result = _parse_amendment_summaries_text(response)
        logger.debug(
            "%s Amendment summaries (%s): %d keys from free-text",
            self._log_prefix,
            lang,
            len(result),
        )
        return result

    def summarize_amendments_bilingual(
        self,
        text: str,
        title: str,
        amendments: list[dict[str, str]],
        *,
        bill_context: str = "",
    ) -> tuple[dict[str, str], dict[str, str]]:
        """Generate per-amendment summaries in both Czech and English.

        Truncates text once and calls summarize_amendments for each language.

        Args:
            text: Combined amendment PDF text.
            title: Bill title.
            amendments: List of dicts with 'letter', 'submitter', 'description'.
            bill_context: Optional AI-generated summary of the original bill
                for additional context in prompts.

        Returns:
            (cs_map, en_map) — each mapping amendment letter to summary.
        """
        truncated = truncate_legislative_text(text)
        cs_map = self.summarize_amendments(
            truncated,
            title,
            amendments,
            lang="cs",
            bill_context=bill_context,
            _truncated=True,
        )
        en_map = self.summarize_amendments(
            truncated,
            title,
            amendments,
            lang="en",
            bill_context=bill_context,
            _truncated=True,
        )
        return cs_map, en_map

    @staticmethod
    def _strip_think(text: str) -> str:
        """Remove <think>...</think> blocks from responses."""
        return _THINK_RE.sub("", text).strip()

    def _parse_topics_response(self, response: str) -> list[str]:
        """Extract topic labels from LLM response.

        Expects format: TOPICS: topic1, topic2, topic3
        """
        response = self._strip_think(response)
        match = re.search(r"TOPICS?:\s*(.+)", response, re.IGNORECASE)
        if not match:
            logger.debug(
                "{} Could not parse topics from response: {}",
                self._log_prefix,
                response[:200],
            )
            return []

        raw = match.group(1).strip()
        # Split on comma, clean up each topic (strip markdown bold markers too)
        topics = [t.strip().strip(".,;:-–*#") for t in raw.split(",")]
        # Filter empty strings, "none", and topics with no alphanumeric content
        topics = [t for t in topics if t and t.lower() != "none" and re.search(r"\w", t)]

        if not topics:
            logger.debug(
                "{} No valid topics parsed from: {}",
                self._log_prefix,
                raw[:200],
            )
            return []

        # Cap at 3 topics
        return topics[:3]

    @staticmethod
    def _parse_consolidation_response(response: str, all_topics: list[str]) -> dict[str, str]:
        """Parse consolidation mapping from LLM response."""
        response = _THINK_RE.sub("", response).strip()
        mapping: dict[str, str] = {}
        for line in response.splitlines():
            line = line.strip()
            if " -> " not in line:
                continue
            parts = line.split(" -> ", 1)
            old = parts[0].strip().strip("- ")
            new = parts[1].strip()
            if old and new:
                mapping[old] = new
        for t in all_topics:
            if t not in mapping:
                mapping[t] = t
        return mapping
