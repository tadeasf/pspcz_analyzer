"""LLM integration package — re-exports public API for backward compatibility."""

from pspcz_analyzer.services.llm.client import (
    LLMClient,
    _sanitize_llm_input,
    create_llm_client,
    deserialize_topics,
    serialize_topics,
    truncate_legislative_text,
)

__all__ = [
    "LLMClient",
    "_sanitize_llm_input",
    "create_llm_client",
    "deserialize_topics",
    "serialize_topics",
    "truncate_legislative_text",
]
