"""Tests for OpenAIClient, OllamaClient, BaseLLMClient, and create_llm_client factory."""

import json
from unittest.mock import patch

import httpx
import pytest

from pspcz_analyzer.services.llm_service import (
    BaseLLMClient,
    OllamaClient,
    OpenAIClient,
    _parse_consolidation_json,
    _render_comparison_markdown_cs,
    _render_comparison_markdown_en,
    _render_summary_markdown_cs,
    _render_summary_markdown_en,
    create_llm_client,
)


class TestCreateLLMClientFactory:
    """Tests for the create_llm_client() factory function."""

    def test_default_returns_ollama(self):
        with patch("pspcz_analyzer.services.llm_service.LLM_PROVIDER", "ollama"):
            client = create_llm_client()
        assert isinstance(client, OllamaClient)

    def test_openai_provider_returns_openai_client(self):
        with (
            patch("pspcz_analyzer.services.llm_service.LLM_PROVIDER", "openai"),
            patch("pspcz_analyzer.services.llm_service.OPENAI_API_KEY", "sk-test-key"),
        ):
            client = create_llm_client()
        assert isinstance(client, OpenAIClient)

    def test_openai_provider_without_key_raises(self):
        with (
            patch("pspcz_analyzer.services.llm_service.LLM_PROVIDER", "openai"),
            patch("pspcz_analyzer.services.llm_service.OPENAI_API_KEY", ""),
        ):
            with pytest.raises(ValueError, match="OPENAI_API_KEY is not set"):
                create_llm_client()

    def test_unknown_provider_raises(self):
        with patch("pspcz_analyzer.services.llm_service.LLM_PROVIDER", "bogus"):
            with pytest.raises(ValueError, match="Unknown LLM_PROVIDER"):
                create_llm_client()

    def test_case_insensitive_provider(self):
        with patch("pspcz_analyzer.services.llm_service.LLM_PROVIDER", "OLLAMA"):
            client = create_llm_client()
        assert isinstance(client, OllamaClient)

    def test_factory_returns_base_llm_client(self):
        with patch("pspcz_analyzer.services.llm_service.LLM_PROVIDER", "ollama"):
            client = create_llm_client()
        assert isinstance(client, BaseLLMClient)


class TestSupportsStructuredOutput:
    """Tests for the supports_structured_output property."""

    def test_openai_supports_structured_output(self):
        client = OpenAIClient(
            base_url="https://api.example.com/v1",
            model="gpt-4o-mini",
            timeout=30.0,
            api_key="sk-test",
        )
        assert client.supports_structured_output is True

    @patch("pspcz_analyzer.services.llm_service.OLLAMA_STRUCTURED_OUTPUT", True)
    def test_ollama_supports_structured_output_when_enabled(self):
        client = OllamaClient(
            base_url="http://localhost:11434",
            model="llama3",
            timeout=30.0,
        )
        assert client.supports_structured_output is True

    @patch("pspcz_analyzer.services.llm_service.OLLAMA_STRUCTURED_OUTPUT", False)
    def test_ollama_no_structured_output_when_disabled(self):
        client = OllamaClient(
            base_url="http://localhost:11434",
            model="llama3",
            timeout=30.0,
        )
        assert client.supports_structured_output is False


class TestOpenAIClientGenerate:
    """Tests for OpenAIClient._generate() with mocked httpx."""

    _DUMMY_REQUEST = httpx.Request("POST", "https://api.example.com/v1/chat/completions")

    def _make_client(self) -> OpenAIClient:
        return OpenAIClient(
            base_url="https://api.example.com/v1",
            model="gpt-4o-mini",
            timeout=30.0,
            api_key="sk-test",
        )

    def _ok_response(self, json_data: dict) -> httpx.Response:
        resp = httpx.Response(200, json=json_data)
        resp.request = self._DUMMY_REQUEST
        return resp

    def test_generate_success(self):
        client = self._make_client()
        mock_response = self._ok_response(
            {"choices": [{"message": {"role": "assistant", "content": "TOPICS: Dane, Pravo"}}]}
        )
        with patch("httpx.post", return_value=mock_response) as mock_post:
            result = client._generate("classify this", "system prompt")

        assert result == "TOPICS: Dane, Pravo"
        call_kwargs = mock_post.call_args
        assert "chat/completions" in call_kwargs.args[0]
        payload = call_kwargs.kwargs["json"]
        assert payload["model"] == "gpt-4o-mini"
        assert len(payload["messages"]) == 2
        assert payload["messages"][0]["role"] == "system"
        assert payload["messages"][1]["role"] == "user"

    def test_generate_passes_response_format(self):
        client = self._make_client()
        mock_response = self._ok_response({"choices": [{"message": {"content": "{}"}}]})
        rf = {"type": "json_schema", "json_schema": {"name": "test", "schema": {}}}
        with patch("httpx.post", return_value=mock_response) as mock_post:
            client._generate("prompt", "system", response_format=rf)

        payload = mock_post.call_args.kwargs["json"]
        assert payload["response_format"] == rf

    def test_generate_omits_response_format_when_none(self):
        client = self._make_client()
        mock_response = self._ok_response({"choices": [{"message": {"content": "result"}}]})
        with patch("httpx.post", return_value=mock_response) as mock_post:
            client._generate("prompt", "system")

        payload = mock_post.call_args.kwargs["json"]
        assert "response_format" not in payload

    def test_generate_returns_none_on_http_error(self):
        client = self._make_client()
        mock_response = httpx.Response(500, text="Internal Server Error")
        mock_response.request = self._DUMMY_REQUEST
        with patch("httpx.post", return_value=mock_response):
            result = client._generate("test", "system")
        assert result is None

    def test_generate_returns_none_on_connection_error(self):
        client = self._make_client()
        with patch("httpx.post", side_effect=httpx.ConnectError("Connection refused")):
            result = client._generate("test", "system")
        assert result is None

    def test_generate_returns_none_on_empty_choices(self):
        client = self._make_client()
        mock_response = self._ok_response({"choices": []})
        with patch("httpx.post", return_value=mock_response):
            result = client._generate("test", "system")
        assert result is None

    def test_authorization_header_set(self):
        client = self._make_client()
        assert client._headers["Authorization"] == "Bearer sk-test"

    def test_no_authorization_header_when_no_key(self):
        client = OpenAIClient(
            base_url="https://api.example.com/v1",
            model="gpt-4o-mini",
            timeout=30.0,
            api_key="",
        )
        assert "Authorization" not in client._headers


class TestOpenAIClientIsAvailable:
    """Tests for OpenAIClient.is_available()."""

    _DUMMY_GET_REQUEST = httpx.Request("GET", "https://api.example.com/v1/models")

    def _make_client(self) -> OpenAIClient:
        return OpenAIClient(
            base_url="https://api.example.com/v1",
            model="gpt-4o-mini",
            timeout=30.0,
            api_key="sk-test",
        )

    def _ok_response(self, json_data: dict) -> httpx.Response:
        resp = httpx.Response(200, json=json_data)
        resp.request = self._DUMMY_GET_REQUEST
        return resp

    def test_available_on_success(self):
        client = self._make_client()
        mock_response = self._ok_response({"data": [{"id": "gpt-4o-mini"}]})
        with patch("httpx.get", return_value=mock_response):
            assert client.is_available() is True

    def test_not_available_on_error(self):
        client = self._make_client()
        with patch("httpx.get", side_effect=httpx.ConnectError("Connection refused")):
            assert client.is_available() is False

    def test_caches_result(self):
        client = self._make_client()
        mock_response = self._ok_response({"data": []})
        with patch("httpx.get", return_value=mock_response) as mock_get:
            client.is_available()
            client.is_available()
        assert mock_get.call_count == 1


# ── Structured output tests (OpenAI path) ────────────────────────────────


class TestOpenAIStructuredClassification:
    """Tests for OpenAI classify_topics with structured output."""

    _DUMMY_REQUEST = httpx.Request("POST", "https://api.example.com/v1/chat/completions")

    def _make_client(self) -> OpenAIClient:
        return OpenAIClient(
            base_url="https://api.example.com/v1",
            model="gpt-4o-mini",
            timeout=30.0,
            api_key="sk-test",
        )

    def _ok_response(self, content: str) -> httpx.Response:
        resp = httpx.Response(200, json={"choices": [{"message": {"content": content}}]})
        resp.request = self._DUMMY_REQUEST
        return resp

    def test_classify_topics_structured_parses_json(self):
        client = self._make_client()
        json_content = json.dumps({"topics": ["Dane a poplatky", "Socialni pojisteni"]})
        mock_response = self._ok_response(json_content)
        with patch("httpx.post", return_value=mock_response):
            topics = client.classify_topics("some law text", "Novela zakona")
        assert topics == ["Dane a poplatky", "Socialni pojisteni"]

    def test_classify_topics_structured_caps_at_3(self):
        client = self._make_client()
        json_content = json.dumps({"topics": ["A", "B", "C", "D"]})
        mock_response = self._ok_response(json_content)
        with patch("httpx.post", return_value=mock_response):
            topics = client.classify_topics("text", "title")
        assert len(topics) == 3

    def test_classify_topics_structured_filters_empty(self):
        client = self._make_client()
        json_content = json.dumps({"topics": ["Dane", "", "  ", "Pravo"]})
        mock_response = self._ok_response(json_content)
        with patch("httpx.post", return_value=mock_response):
            topics = client.classify_topics("text", "title")
        assert topics == ["Dane", "Pravo"]

    def test_classify_topics_structured_returns_empty_on_failure(self):
        client = self._make_client()
        with patch("httpx.post", side_effect=httpx.ConnectError("fail")):
            topics = client.classify_topics("text", "title")
        assert topics == []

    def test_classify_topics_en_structured(self):
        client = self._make_client()
        json_content = json.dumps({"topics": ["Taxes & Fees", "Social Insurance"]})
        mock_response = self._ok_response(json_content)
        with patch("httpx.post", return_value=mock_response):
            topics = client.classify_topics_en("text", "title")
        assert topics == ["Taxes & Fees", "Social Insurance"]

    def test_classify_sends_response_format(self):
        """Verify that response_format is included in the API request."""
        client = self._make_client()
        json_content = json.dumps({"topics": ["Dane"]})
        mock_response = self._ok_response(json_content)
        with patch("httpx.post", return_value=mock_response) as mock_post:
            client.classify_topics("text", "title")

        payload = mock_post.call_args.kwargs["json"]
        assert "response_format" in payload
        assert payload["response_format"]["type"] == "json_schema"


class TestOpenAIStructuredSummary:
    """Tests for OpenAI summarize with structured output."""

    _DUMMY_REQUEST = httpx.Request("POST", "https://api.example.com/v1/chat/completions")

    def _make_client(self) -> OpenAIClient:
        return OpenAIClient(
            base_url="https://api.example.com/v1",
            model="gpt-4o-mini",
            timeout=30.0,
            api_key="sk-test",
        )

    def _ok_response(self, content: str) -> httpx.Response:
        resp = httpx.Response(200, json={"choices": [{"message": {"content": content}}]})
        resp.request = self._DUMMY_REQUEST
        return resp

    def test_summarize_structured_renders_markdown_cs(self):
        client = self._make_client()
        json_content = json.dumps(
            {
                "changes": "Mění sazby DPH.",
                "impact": "Prospívá podnikatelům.",
                "risks": "Může vést ke snížení příjmů.",
            }
        )
        mock_response = self._ok_response(json_content)
        with patch("httpx.post", return_value=mock_response):
            result = client.summarize("text", "title")
        assert "**Co se mění:**" in result
        assert "**Dopady:**" in result
        assert "**Rizika:**" in result

    def test_summarize_en_structured_renders_markdown_en(self):
        client = self._make_client()
        json_content = json.dumps(
            {
                "changes": "Changes VAT rates.",
                "impact": "Benefits businesses.",
                "risks": "May reduce revenue.",
            }
        )
        mock_response = self._ok_response(json_content)
        with patch("httpx.post", return_value=mock_response):
            result = client.summarize_en("text", "title")
        assert "**Changes:**" in result
        assert "**Impact:**" in result
        assert "**Risks:**" in result

    def test_summarize_returns_empty_on_failure(self):
        client = self._make_client()
        with patch("httpx.post", side_effect=httpx.ConnectError("fail")):
            result = client.summarize("text", "title")
        assert result == ""


class TestOpenAIStructuredConsolidation:
    """Tests for OpenAI consolidate_topics with structured output."""

    _DUMMY_REQUEST = httpx.Request("POST", "https://api.example.com/v1/chat/completions")

    def _make_client(self) -> OpenAIClient:
        return OpenAIClient(
            base_url="https://api.example.com/v1",
            model="gpt-4o-mini",
            timeout=30.0,
            api_key="sk-test",
        )

    def _ok_response(self, content: str) -> httpx.Response:
        resp = httpx.Response(200, json={"choices": [{"message": {"content": content}}]})
        resp.request = self._DUMMY_REQUEST
        return resp

    def test_consolidate_structured_parses_mappings(self):
        client = self._make_client()
        json_content = json.dumps(
            {
                "mappings": [
                    {"old": "Dane", "canonical": "Dane a poplatky"},
                    {"old": "Poplatky", "canonical": "Dane a poplatky"},
                ]
            }
        )
        mock_response = self._ok_response(json_content)
        with patch("httpx.post", return_value=mock_response):
            mapping = client.consolidate_topics(["Dane", "Poplatky", "Pravo"])
        assert mapping["Dane"] == "Dane a poplatky"
        assert mapping["Poplatky"] == "Dane a poplatky"
        assert mapping["Pravo"] == "Pravo"  # fallback identity

    def test_consolidate_returns_identity_on_failure(self):
        client = self._make_client()
        with patch("httpx.post", side_effect=httpx.ConnectError("fail")):
            mapping = client.consolidate_topics(["A", "B"])
        assert mapping == {"A": "A", "B": "B"}


class TestOpenAIStructuredComparison:
    """Tests for OpenAI compare_versions with structured output."""

    _DUMMY_REQUEST = httpx.Request("POST", "https://api.example.com/v1/chat/completions")

    def _make_client(self) -> OpenAIClient:
        return OpenAIClient(
            base_url="https://api.example.com/v1",
            model="gpt-4o-mini",
            timeout=30.0,
            api_key="sk-test",
        )

    def _ok_response(self, content: str) -> httpx.Response:
        resp = httpx.Response(200, json={"choices": [{"message": {"content": content}}]})
        resp.request = self._DUMMY_REQUEST
        return resp

    def test_compare_structured_renders_markdown(self):
        client = self._make_client()
        json_content = json.dumps(
            {
                "changed_paragraphs": "§ 5 upraven.",
                "additions_removals": "Přidán § 6a.",
                "overall_character": "Zpřísnění.",
            }
        )
        mock_response = self._ok_response(json_content)
        with patch("httpx.post", return_value=mock_response):
            result = client.compare_versions("old text", "new text", 100, 200)
        assert "**Změněné paragrafy:**" in result
        assert "**Přidáno/odebráno:**" in result
        assert "**Charakter změn:**" in result

    def test_compare_returns_empty_on_failure(self):
        client = self._make_client()
        with patch("httpx.post", side_effect=httpx.ConnectError("fail")):
            result = client.compare_versions("old", "new", 1, 2)
        assert result == ""


# ── Ollama fallback tests (free-text regex parsing) ──────────────────────


@patch("pspcz_analyzer.services.llm_service.OLLAMA_STRUCTURED_OUTPUT", False)
class TestOllamaFallbackClassification:
    """Tests that OllamaClient uses the free-text regex parsing path."""

    _DUMMY_REQUEST = httpx.Request("POST", "http://localhost:11434/api/generate")

    def _make_client(self) -> OllamaClient:
        return OllamaClient(
            base_url="http://localhost:11434",
            model="llama3",
            timeout=30.0,
        )

    def _ok_response(self, text: str) -> httpx.Response:
        resp = httpx.Response(200, json={"response": text})
        resp.request = self._DUMMY_REQUEST
        return resp

    def test_classify_topics_uses_regex_parsing(self):
        client = self._make_client()
        mock_response = self._ok_response("TOPICS: Dane a poplatky, Socialni pojisteni")
        with patch("httpx.post", return_value=mock_response):
            topics = client.classify_topics("some law text", "Novela zakona")
        assert topics == ["Dane a poplatky", "Socialni pojisteni"]

    def test_classify_topics_handles_think_blocks(self):
        client = self._make_client()
        mock_response = self._ok_response("<think>hmm...</think>TOPICS: Dane, Pravo")
        with patch("httpx.post", return_value=mock_response):
            topics = client.classify_topics("text", "title")
        assert topics == ["Dane", "Pravo"]

    def test_classify_returns_empty_on_unparseable(self):
        client = self._make_client()
        mock_response = self._ok_response("I don't understand the question")
        with patch("httpx.post", return_value=mock_response):
            topics = client.classify_topics("text", "title")
        assert topics == []


@patch("pspcz_analyzer.services.llm_service.OLLAMA_STRUCTURED_OUTPUT", False)
class TestOllamaFallbackSummary:
    """Tests that OllamaClient uses free-text summary path."""

    _DUMMY_REQUEST = httpx.Request("POST", "http://localhost:11434/api/generate")

    def _make_client(self) -> OllamaClient:
        return OllamaClient(
            base_url="http://localhost:11434",
            model="llama3",
            timeout=30.0,
        )

    def _ok_response(self, text: str) -> httpx.Response:
        resp = httpx.Response(200, json={"response": text})
        resp.request = self._DUMMY_REQUEST
        return resp

    def test_summarize_strips_think_blocks(self):
        client = self._make_client()
        mock_response = self._ok_response("<think>let me think</think>Novela mění sazby DPH.")
        with patch("httpx.post", return_value=mock_response):
            result = client.summarize("text", "title")
        assert result == "Novela mění sazby DPH."
        assert "<think>" not in result


# ── Ollama structured output tests ────────────────────────────────────────


@patch("pspcz_analyzer.services.llm_service.OLLAMA_STRUCTURED_OUTPUT", True)
class TestOllamaStructuredClassification:
    """Tests that OllamaClient uses JSON structured output when enabled."""

    _DUMMY_REQUEST = httpx.Request("POST", "http://localhost:11434/api/generate")

    def _make_client(self) -> OllamaClient:
        return OllamaClient(
            base_url="http://localhost:11434",
            model="llama3",
            timeout=30.0,
        )

    def _ok_response(self, text: str) -> httpx.Response:
        resp = httpx.Response(200, json={"response": text})
        resp.request = self._DUMMY_REQUEST
        return resp

    def test_classify_topics_structured_parses_json(self):
        client = self._make_client()
        json_content = json.dumps({"topics": ["Dane a poplatky", "Socialni pojisteni"]})
        mock_response = self._ok_response(json_content)
        with patch("httpx.post", return_value=mock_response):
            topics = client.classify_topics("some law text", "Novela zakona")
        assert topics == ["Dane a poplatky", "Socialni pojisteni"]

    def test_classify_sends_format_in_payload(self):
        """Verify that format (raw JSON schema) is included in the Ollama payload."""
        client = self._make_client()
        json_content = json.dumps({"topics": ["Dane"]})
        mock_response = self._ok_response(json_content)
        with patch("httpx.post", return_value=mock_response) as mock_post:
            client.classify_topics("text", "title")

        payload = mock_post.call_args.kwargs["json"]
        assert "format" in payload
        assert "properties" in payload["format"]
        assert "topics" in payload["format"]["properties"]

    def test_classify_topics_structured_caps_at_3(self):
        client = self._make_client()
        json_content = json.dumps({"topics": ["A", "B", "C", "D"]})
        mock_response = self._ok_response(json_content)
        with patch("httpx.post", return_value=mock_response):
            topics = client.classify_topics("text", "title")
        assert len(topics) == 3

    def test_classify_topics_structured_filters_empty(self):
        client = self._make_client()
        json_content = json.dumps({"topics": ["Dane", "", "  ", "Pravo"]})
        mock_response = self._ok_response(json_content)
        with patch("httpx.post", return_value=mock_response):
            topics = client.classify_topics("text", "title")
        assert topics == ["Dane", "Pravo"]


@patch("pspcz_analyzer.services.llm_service.OLLAMA_STRUCTURED_OUTPUT", True)
class TestOllamaStructuredCombined:
    """Tests that OllamaClient uses JSON structured classify-and-summarize when enabled."""

    _DUMMY_REQUEST = httpx.Request("POST", "http://localhost:11434/api/generate")

    def _make_client(self) -> OllamaClient:
        return OllamaClient(
            base_url="http://localhost:11434",
            model="llama3",
            timeout=30.0,
        )

    def _ok_response(self, text: str) -> httpx.Response:
        resp = httpx.Response(200, json={"response": text})
        resp.request = self._DUMMY_REQUEST
        return resp

    def test_combined_parses_json_cs(self):
        client = self._make_client()
        json_content = json.dumps(
            {
                "topics": ["Dane a poplatky", "Rozpočet"],
                "changes": "Mění sazby.",
                "impact": "Dopad na firmy.",
                "risks": "Riziko poklesu.",
            }
        )
        mock_response = self._ok_response(json_content)
        with patch("httpx.post", return_value=mock_response):
            topics, summary = client.classify_and_summarize("text", "title")
        assert topics == ["Dane a poplatky", "Rozpočet"]
        assert "**Co se mění:** Mění sazby." in summary
        assert "**Dopady:** Dopad na firmy." in summary
        assert "**Rizika:** Riziko poklesu." in summary

    def test_combined_sends_format_in_payload(self):
        """Verify that format includes the combined schema in the Ollama payload."""
        client = self._make_client()
        json_content = json.dumps(
            {
                "topics": ["Dane"],
                "changes": "x",
                "impact": "y",
                "risks": "z",
            }
        )
        mock_response = self._ok_response(json_content)
        with patch("httpx.post", return_value=mock_response) as mock_post:
            client.classify_and_summarize("text", "title")

        payload = mock_post.call_args.kwargs["json"]
        assert "format" in payload
        schema = payload["format"]
        assert "topics" in schema["properties"]
        assert "changes" in schema["properties"]
        assert "impact" in schema["properties"]
        assert "risks" in schema["properties"]

    def test_combined_returns_empty_on_failure(self):
        client = self._make_client()
        with patch("httpx.post", side_effect=httpx.ConnectError("fail")):
            topics, summary = client.classify_and_summarize("text", "title")
        assert topics == []
        assert summary == ""


# ── Markdown rendering helper tests ──────────────────────────────────────


class TestRenderHelpers:
    """Tests for markdown rendering helper functions."""

    def test_render_summary_cs(self):
        data = {
            "changes": "Mění sazby.",
            "impact": "Dopad na firmy.",
            "risks": "Žádné riziko.",
        }
        result = _render_summary_markdown_cs(data)
        assert "**Co se mění:** Mění sazby." in result
        assert "**Dopady:** Dopad na firmy." in result
        assert "**Rizika:** Žádné riziko." in result

    def test_render_summary_en(self):
        data = {
            "changes": "Changes rates.",
            "impact": "Impacts firms.",
            "risks": "No risks.",
        }
        result = _render_summary_markdown_en(data)
        assert "**Changes:** Changes rates." in result
        assert "**Impact:** Impacts firms." in result
        assert "**Risks:** No risks." in result

    def test_render_summary_skips_empty_fields(self):
        data = {"changes": "Something changes.", "impact": "", "risks": ""}
        result = _render_summary_markdown_cs(data)
        assert "**Co se mění:**" in result
        assert "**Dopady:**" not in result
        assert "**Rizika:**" not in result

    def test_render_comparison_cs(self):
        data = {
            "changed_paragraphs": "§ 5",
            "additions_removals": "Přidán § 6",
            "overall_character": "Zpřísnění",
        }
        result = _render_comparison_markdown_cs(data)
        assert "**Změněné paragrafy:**" in result
        assert "**Přidáno/odebráno:**" in result
        assert "**Charakter změn:**" in result

    def test_render_comparison_en(self):
        data = {
            "changed_paragraphs": "§ 5",
            "additions_removals": "Added § 6",
            "overall_character": "Tightening",
        }
        result = _render_comparison_markdown_en(data)
        assert "**Changed paragraphs:**" in result
        assert "**Additions/removals:**" in result
        assert "**Overall character:**" in result


class TestParseConsolidationJson:
    """Tests for _parse_consolidation_json helper."""

    def test_parses_valid_mappings(self):
        data = {
            "mappings": [
                {"old": "Dane", "canonical": "Dane a poplatky"},
                {"old": "Pravo", "canonical": "Pravo"},
            ]
        }
        result = _parse_consolidation_json(data, ["Dane", "Pravo"])
        assert result == {"Dane": "Dane a poplatky", "Pravo": "Pravo"}

    def test_fills_missing_topics_with_identity(self):
        data = {"mappings": [{"old": "A", "canonical": "B"}]}
        result = _parse_consolidation_json(data, ["A", "C"])
        assert result["A"] == "B"
        assert result["C"] == "C"

    def test_handles_empty_mappings(self):
        data = {"mappings": []}
        result = _parse_consolidation_json(data, ["X", "Y"])
        assert result == {"X": "X", "Y": "Y"}


# ── Combined classify + summarize tests ──────────────────────────────────


class TestParseCombinedResponse:
    """Tests for BaseLLMClient._parse_combined_response."""

    def _make_client(self) -> OllamaClient:
        return OllamaClient(
            base_url="http://localhost:11434",
            model="llama3",
            timeout=30.0,
        )

    def test_parses_all_fields(self):
        client = self._make_client()
        response = (
            "TOPICS: Dane a poplatky, Socialni pojisteni\n"
            "CHANGES: Mění sazby DPH z 21% na 19%.\n"
            "IMPACT: Prospívá firmám, škodí rozpočtu.\n"
            "RISKS: Riziko poklesu příjmů státu."
        )
        topics, summary_data = client._parse_combined_response(response)
        assert topics == ["Dane a poplatky", "Socialni pojisteni"]
        assert summary_data["changes"] == "Mění sazby DPH z 21% na 19%."
        assert summary_data["impact"] == "Prospívá firmám, škodí rozpočtu."
        assert summary_data["risks"] == "Riziko poklesu příjmů státu."

    def test_handles_think_blocks(self):
        client = self._make_client()
        response = (
            "<think>Let me analyze this...</think>"
            "TOPICS: Trestní právo\n"
            "CHANGES: Zpřísňuje tresty.\n"
            "IMPACT: Dopad na odsouzené.\n"
            "RISKS: Přeplnění věznic."
        )
        topics, summary_data = client._parse_combined_response(response)
        assert topics == ["Trestní právo"]
        assert "Zpřísňuje tresty" in summary_data["changes"]

    def test_partial_success_topics_only(self):
        client = self._make_client()
        response = "TOPICS: Zdravotnictví, Pojištění\nSome random text without fields"
        topics, summary_data = client._parse_combined_response(response)
        assert topics == ["Zdravotnictví", "Pojištění"]
        # Summary fields should be empty strings
        assert summary_data["changes"] == ""
        assert summary_data["impact"] == ""

    def test_empty_on_garbage(self):
        client = self._make_client()
        response = "I don't understand the question."
        topics, summary_data = client._parse_combined_response(response)
        assert topics == []
        assert summary_data["changes"] == ""

    def test_caps_topics_at_3(self):
        client = self._make_client()
        response = "TOPICS: A, B, C, D, E\nCHANGES: x\nIMPACT: y\nRISKS: z"
        topics, _ = client._parse_combined_response(response)
        assert len(topics) == 3

    def test_multiline_field_values(self):
        client = self._make_client()
        response = (
            "TOPICS: Dane\n"
            "CHANGES: Mění sazby.\nPřidává nové kategorie.\n"
            "IMPACT: Dopad na firmy.\n"
            "RISKS: Riziko."
        )
        topics, summary_data = client._parse_combined_response(response)
        assert topics == ["Dane"]
        assert "Mění sazby" in summary_data["changes"]
        assert "Přidává nové kategorie" in summary_data["changes"]


class TestOpenAIStructuredClassifyAndSummarize:
    """Tests for OpenAI combined classify_and_summarize with structured output."""

    _DUMMY_REQUEST = httpx.Request("POST", "https://api.example.com/v1/chat/completions")

    def _make_client(self) -> OpenAIClient:
        return OpenAIClient(
            base_url="https://api.example.com/v1",
            model="gpt-4o-mini",
            timeout=30.0,
            api_key="sk-test",
        )

    def _ok_response(self, content: str) -> httpx.Response:
        resp = httpx.Response(200, json={"choices": [{"message": {"content": content}}]})
        resp.request = self._DUMMY_REQUEST
        return resp

    def test_combined_parses_json_cs(self):
        client = self._make_client()
        json_content = json.dumps(
            {
                "topics": ["Dane a poplatky", "Rozpočet"],
                "changes": "Mění sazby.",
                "impact": "Dopad na firmy.",
                "risks": "Riziko poklesu.",
            }
        )
        mock_response = self._ok_response(json_content)
        with patch("httpx.post", return_value=mock_response):
            topics, summary = client.classify_and_summarize("text", "title")
        assert topics == ["Dane a poplatky", "Rozpočet"]
        assert "**Co se mění:** Mění sazby." in summary
        assert "**Dopady:** Dopad na firmy." in summary
        assert "**Rizika:** Riziko poklesu." in summary

    def test_combined_parses_json_en(self):
        client = self._make_client()
        json_content = json.dumps(
            {
                "topics": ["Taxes", "Budget"],
                "changes": "Changes rates.",
                "impact": "Impacts firms.",
                "risks": "Revenue risk.",
            }
        )
        mock_response = self._ok_response(json_content)
        with patch("httpx.post", return_value=mock_response):
            topics, summary = client.classify_and_summarize_en("text", "title")
        assert topics == ["Taxes", "Budget"]
        assert "**Changes:** Changes rates." in summary
        assert "**Impact:** Impacts firms." in summary
        assert "**Risks:** Revenue risk." in summary

    def test_combined_caps_topics_at_3(self):
        client = self._make_client()
        json_content = json.dumps(
            {
                "topics": ["A", "B", "C", "D"],
                "changes": "x",
                "impact": "y",
                "risks": "z",
            }
        )
        mock_response = self._ok_response(json_content)
        with patch("httpx.post", return_value=mock_response):
            topics, _ = client.classify_and_summarize("text", "title")
        assert len(topics) == 3

    def test_combined_returns_empty_on_failure(self):
        client = self._make_client()
        with patch("httpx.post", side_effect=httpx.ConnectError("fail")):
            topics, summary = client.classify_and_summarize("text", "title")
        assert topics == []
        assert summary == ""

    def test_combined_sends_response_format(self):
        """Verify that response_format includes the combined schema."""
        client = self._make_client()
        json_content = json.dumps(
            {
                "topics": ["Dane"],
                "changes": "x",
                "impact": "y",
                "risks": "z",
            }
        )
        mock_response = self._ok_response(json_content)
        with patch("httpx.post", return_value=mock_response) as mock_post:
            client.classify_and_summarize("text", "title")

        payload = mock_post.call_args.kwargs["json"]
        assert "response_format" in payload
        schema = payload["response_format"]["json_schema"]["schema"]
        assert "topics" in schema["properties"]
        assert "changes" in schema["properties"]
        assert "impact" in schema["properties"]
        assert "risks" in schema["properties"]

    def test_bilingual_returns_four_values(self):
        client = self._make_client()
        json_content_cs = json.dumps(
            {
                "topics": ["Dane"],
                "changes": "Mění.",
                "impact": "Dopad.",
                "risks": "Riziko.",
            }
        )
        json_content_en = json.dumps(
            {
                "topics": ["Taxes"],
                "changes": "Changes.",
                "impact": "Impact.",
                "risks": "Risk.",
            }
        )
        responses = [
            self._ok_response(json_content_cs),
            self._ok_response(json_content_en),
        ]
        with patch("httpx.post", side_effect=responses):
            topics_cs, topics_en, summary_cs, summary_en = client.classify_and_summarize_bilingual(
                "text", "title"
            )
        assert topics_cs == ["Dane"]
        assert topics_en == ["Taxes"]
        assert "**Co se mění:**" in summary_cs
        assert "**Changes:**" in summary_en


@patch("pspcz_analyzer.services.llm_service.OLLAMA_STRUCTURED_OUTPUT", False)
class TestOllamaFallbackCombined:
    """Tests that OllamaClient uses the free-text combined parsing path."""

    _DUMMY_REQUEST = httpx.Request("POST", "http://localhost:11434/api/generate")

    def _make_client(self) -> OllamaClient:
        return OllamaClient(
            base_url="http://localhost:11434",
            model="llama3",
            timeout=30.0,
        )

    def _ok_response(self, text: str) -> httpx.Response:
        resp = httpx.Response(200, json={"response": text})
        resp.request = self._DUMMY_REQUEST
        return resp

    def test_combined_cs_parses_response(self):
        client = self._make_client()
        mock_response = self._ok_response(
            "TOPICS: Dane a poplatky\n"
            "CHANGES: Mění sazby DPH.\n"
            "IMPACT: Dopad na firmy.\n"
            "RISKS: Riziko poklesu."
        )
        with patch("httpx.post", return_value=mock_response):
            topics, summary = client.classify_and_summarize("text", "title")
        assert topics == ["Dane a poplatky"]
        assert "**Co se mění:** Mění sazby DPH." in summary

    def test_combined_en_parses_response(self):
        client = self._make_client()
        mock_response = self._ok_response(
            "TOPICS: Taxes\n"
            "CHANGES: Changes VAT rates.\n"
            "IMPACT: Impacts businesses.\n"
            "RISKS: Revenue decline risk."
        )
        with patch("httpx.post", return_value=mock_response):
            topics, summary = client.classify_and_summarize_en("text", "title")
        assert topics == ["Taxes"]
        assert "**Changes:** Changes VAT rates." in summary

    def test_combined_handles_think_blocks(self):
        client = self._make_client()
        mock_response = self._ok_response(
            "<think>hmm...</think>"
            "TOPICS: Pravo\n"
            "CHANGES: Zpřísňuje tresty.\n"
            "IMPACT: Dopad.\n"
            "RISKS: Riziko."
        )
        with patch("httpx.post", return_value=mock_response):
            topics, summary = client.classify_and_summarize("text", "title")
        assert topics == ["Pravo"]
        assert "Zpřísňuje tresty" in summary

    def test_combined_returns_empty_on_failure(self):
        client = self._make_client()
        with patch("httpx.post", side_effect=httpx.ConnectError("fail")):
            topics, summary = client.classify_and_summarize("text", "title")
        assert topics == []
        assert summary == ""


# ── Ollama OpenAI-compat auto-detection tests ─────────────────────────────


class TestOllamaCompatDetection:
    """Tests for OllamaClient auto-detection of native vs OpenAI-compatible mode."""

    _DUMMY_TAGS_REQUEST = httpx.Request("GET", "http://localhost:11434/api/tags")
    _DUMMY_MODELS_REQUEST = httpx.Request("GET", "http://localhost:11434/models")

    def _make_client(self) -> OllamaClient:
        return OllamaClient(
            base_url="http://localhost:11434",
            model="llama3",
            timeout=30.0,
        )

    def test_native_mode_when_api_tags_succeeds(self):
        """Native Ollama detected when /api/tags returns the model."""
        client = self._make_client()
        tags_resp = httpx.Response(200, json={"models": [{"name": "llama3:latest"}]})
        tags_resp.request = self._DUMMY_TAGS_REQUEST
        with patch("httpx.get", return_value=tags_resp):
            assert client.is_available() is True
        assert client._openai_compat is False
        assert client._log_prefix == "[ollama]"

    def test_compat_mode_when_api_tags_fails_but_models_succeeds(self):
        """OpenAI-compat detected when /api/tags fails but /models succeeds."""
        client = self._make_client()
        tags_resp = httpx.Response(404, text="Not Found")
        tags_resp.request = self._DUMMY_TAGS_REQUEST
        models_resp = httpx.Response(200, json={"data": [{"id": "llama3"}]})
        models_resp.request = self._DUMMY_MODELS_REQUEST

        def mock_get(url: str, **kwargs):
            if "/api/tags" in url:
                return tags_resp
            return models_resp

        with patch("httpx.get", side_effect=mock_get):
            assert client.is_available() is True
        assert client._openai_compat is True
        assert client._log_prefix == "[ollama/openai-compat]"

    def test_unavailable_when_both_fail(self):
        """Not available when both /api/tags and /models fail."""
        client = self._make_client()
        with patch("httpx.get", side_effect=httpx.ConnectError("Connection refused")):
            assert client.is_available() is False
        assert client._openai_compat is False

    def test_caches_result(self):
        """is_available() caches result after first call."""
        client = self._make_client()
        tags_resp = httpx.Response(200, json={"models": [{"name": "llama3:latest"}]})
        tags_resp.request = self._DUMMY_TAGS_REQUEST
        with patch("httpx.get", return_value=tags_resp) as mock_get:
            client.is_available()
            client.is_available()
        assert mock_get.call_count == 1

    def test_native_mode_model_not_found_falls_to_compat(self):
        """When /api/tags works but model not found, tries /models."""
        client = self._make_client()
        tags_resp = httpx.Response(200, json={"models": [{"name": "qwen3:8b"}]})
        tags_resp.request = self._DUMMY_TAGS_REQUEST
        models_resp = httpx.Response(200, json={"data": [{"id": "llama3"}]})
        models_resp.request = self._DUMMY_MODELS_REQUEST

        def mock_get(url: str, **kwargs):
            if "/api/tags" in url:
                return tags_resp
            return models_resp

        with patch("httpx.get", side_effect=mock_get):
            assert client.is_available() is True
        assert client._openai_compat is True


# ── Ollama OpenAI-compat generation tests ─────────────────────────────────


class TestOllamaCompatGeneration:
    """Tests for OllamaClient._generate() in OpenAI-compat mode."""

    _DUMMY_REQUEST = httpx.Request("POST", "http://localhost:11434/chat/completions")

    def _make_compat_client(self) -> OllamaClient:
        client = OllamaClient(
            base_url="http://localhost:11434",
            model="gpt-oss-lite",
            timeout=30.0,
        )
        client._openai_compat = True
        client._log_prefix = "[ollama/openai-compat]"
        return client

    def _ok_response(self, content: str) -> httpx.Response:
        resp = httpx.Response(200, json={"choices": [{"message": {"content": content}}]})
        resp.request = self._DUMMY_REQUEST
        return resp

    def test_compat_generate_success(self):
        client = self._make_compat_client()
        mock_response = self._ok_response("TOPICS: Dane, Pravo")
        with patch("httpx.post", return_value=mock_response) as mock_post:
            result = client._generate("classify this", "system prompt")

        assert result == "TOPICS: Dane, Pravo"
        call_kwargs = mock_post.call_args
        assert "chat/completions" in call_kwargs.args[0]
        payload = call_kwargs.kwargs["json"]
        assert payload["model"] == "gpt-oss-lite"
        assert len(payload["messages"]) == 2

    def test_compat_generate_passes_response_format(self):
        client = self._make_compat_client()
        mock_response = self._ok_response("{}")
        rf = {"type": "json_schema", "json_schema": {"name": "test", "schema": {}}}
        with patch("httpx.post", return_value=mock_response) as mock_post:
            client._generate("prompt", "system", response_format=rf)

        payload = mock_post.call_args.kwargs["json"]
        assert payload["response_format"] == rf

    def test_compat_generate_returns_none_on_error(self):
        client = self._make_compat_client()
        with patch("httpx.post", side_effect=httpx.ConnectError("fail")):
            result = client._generate("test", "system")
        assert result is None

    def test_native_mode_uses_api_generate(self):
        """Verify that native mode still uses /api/generate."""
        client = OllamaClient(
            base_url="http://localhost:11434",
            model="llama3",
            timeout=30.0,
        )
        # _openai_compat defaults to False
        mock_response = httpx.Response(200, json={"response": "TOPICS: Dane"})
        mock_response.request = httpx.Request("POST", "http://localhost:11434/api/generate")
        with patch("httpx.post", return_value=mock_response) as mock_post:
            result = client._generate("prompt", "system")

        assert result == "TOPICS: Dane"
        assert "/api/generate" in mock_post.call_args.args[0]


# ── Ollama structured output fallback tests ───────────────────────────────


class TestOllamaStructuredOutputFallback:
    """Tests for OllamaClient._generate_json() with prompt-based fallback."""

    _DUMMY_REQUEST = httpx.Request("POST", "http://localhost:11434/api/generate")

    def _make_client(self) -> OllamaClient:
        return OllamaClient(
            base_url="http://localhost:11434",
            model="llama3",
            timeout=30.0,
        )

    def _ok_response(self, text: str) -> httpx.Response:
        resp = httpx.Response(200, json={"response": text})
        resp.request = self._DUMMY_REQUEST
        return resp

    @patch("pspcz_analyzer.services.llm_service.OLLAMA_STRUCTURED_OUTPUT", True)
    def test_returns_structured_when_first_try_succeeds(self):
        """When schema-constrained generation works, return it directly."""
        client = self._make_client()
        json_content = json.dumps({"topics": ["Dane"]})
        mock_response = self._ok_response(json_content)
        with patch("httpx.post", return_value=mock_response):
            result = client._generate_json("classify", "system", {"properties": {"topics": {}}})
        assert result == {"topics": ["Dane"]}

    @patch("pspcz_analyzer.services.llm_service.OLLAMA_STRUCTURED_OUTPUT", True)
    def test_fallback_on_first_failure(self):
        """When schema-constrained generation fails, fallback succeeds."""
        client = self._make_client()
        # First call (structured) fails, second call (prompt-based) succeeds
        responses = [
            None,  # _generate returns None for structured attempt
            '{"topics": ["Dane a poplatky"]}',  # prompt-based succeeds
        ]
        with patch.object(client, "_generate", side_effect=responses):
            result = client._generate_json(
                "classify",
                "system",
                {"properties": {"topics": {"type": "array"}}},
            )
        assert result == {"topics": ["Dane a poplatky"]}

    @patch("pspcz_analyzer.services.llm_service.OLLAMA_STRUCTURED_OUTPUT", True)
    def test_returns_none_when_both_fail(self):
        """When both structured and fallback fail, return None."""
        client = self._make_client()
        with patch.object(client, "_generate", return_value=None):
            result = client._generate_json(
                "classify",
                "system",
                {"properties": {"topics": {}}},
            )
        assert result is None


# ── JSON extraction helper tests ──────────────────────────────────────────


class TestExtractJsonFromText:
    """Tests for OllamaClient._extract_json_from_text() static method."""

    def test_parses_clean_json(self):
        result = OllamaClient._extract_json_from_text('{"topics": ["Dane"]}')
        assert result == {"topics": ["Dane"]}

    def test_parses_json_with_think_blocks(self):
        text = '<think>Let me think...</think>{"topics": ["Dane"]}'
        result = OllamaClient._extract_json_from_text(text)
        assert result == {"topics": ["Dane"]}

    def test_extracts_json_from_surrounding_text(self):
        text = 'Here is the answer: {"topics": ["Dane"]} hope this helps!'
        result = OllamaClient._extract_json_from_text(text)
        assert result == {"topics": ["Dane"]}

    def test_returns_none_on_no_json(self):
        result = OllamaClient._extract_json_from_text("no json here")
        assert result is None

    def test_returns_none_on_invalid_json(self):
        result = OllamaClient._extract_json_from_text("{invalid json}")
        assert result is None

    def test_handles_nested_json(self):
        text = '{"mappings": [{"old": "A", "canonical": "B"}]}'
        result = OllamaClient._extract_json_from_text(text)
        assert result == {"mappings": [{"old": "A", "canonical": "B"}]}

    def test_handles_empty_string(self):
        result = OllamaClient._extract_json_from_text("")
        assert result is None
