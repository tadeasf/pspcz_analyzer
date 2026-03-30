"""LLM integration package — re-exports public API for backward compatibility."""

from pspcz_analyzer.services.llm.client import LLMClient
from pspcz_analyzer.services.llm.helpers import (
    _sanitize_llm_input,
    create_llm_client,
    truncate_legislative_text,
)
from pspcz_analyzer.services.llm.parsers import (
    deserialize_topics,
    serialize_topics,
)

__all__ = [
    "LLMClient",
    "_sanitize_llm_input",
    "create_llm_client",
    "deserialize_topics",
    "serialize_topics",
    "truncate_legislative_text",
]
