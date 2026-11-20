"""LLM integration package – re-exports public API for backward compatibility."""

from pspcz_analyzer.services.llm.client import LLMClient
from pspcz_analyzer.services.llm.health import LLMUnavailable
from pspcz_analyzer.services.llm.helpers import (
    _sanitize_llm_input,
    create_llm_client,
    llm_map,
    truncate_legislative_text,
)
from pspcz_analyzer.services.llm.parsers import (
    deserialize_topics,
    serialize_topics,
)

__all__ = [
    "LLMClient",
    "LLMUnavailable",
    "_sanitize_llm_input",
    "create_llm_client",
    "deserialize_topics",
    "llm_map",
    "serialize_topics",
    "truncate_legislative_text",
]
