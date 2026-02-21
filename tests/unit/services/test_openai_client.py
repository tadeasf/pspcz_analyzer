"""Tests for OpenAIClient and create_llm_client factory."""

from unittest.mock import patch

import httpx
import pytest

from pspcz_analyzer.services.ollama_service import (
    OllamaClient,
    OpenAIClient,
    create_llm_client,
)


class TestCreateLLMClientFactory:
    """Tests for the create_llm_client() factory function."""

    def test_default_returns_ollama(self):
        with patch("pspcz_analyzer.services.ollama_service.LLM_PROVIDER", "ollama"):
            client = create_llm_client()
        assert isinstance(client, OllamaClient)

    def test_openai_provider_returns_openai_client(self):
        with (
            patch("pspcz_analyzer.services.ollama_service.LLM_PROVIDER", "openai"),
            patch("pspcz_analyzer.services.ollama_service.OPENAI_API_KEY", "sk-test-key"),
        ):
            client = create_llm_client()
        assert isinstance(client, OpenAIClient)

    def test_openai_provider_without_key_raises(self):
        with (
            patch("pspcz_analyzer.services.ollama_service.LLM_PROVIDER", "openai"),
            patch("pspcz_analyzer.services.ollama_service.OPENAI_API_KEY", ""),
        ):
            with pytest.raises(ValueError, match="OPENAI_API_KEY is not set"):
                create_llm_client()

    def test_unknown_provider_raises(self):
        with patch("pspcz_analyzer.services.ollama_service.LLM_PROVIDER", "bogus"):
            with pytest.raises(ValueError, match="Unknown LLM_PROVIDER"):
                create_llm_client()

    def test_case_insensitive_provider(self):
        with patch("pspcz_analyzer.services.ollama_service.LLM_PROVIDER", "OLLAMA"):
            client = create_llm_client()
        assert isinstance(client, OllamaClient)


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
        mock_response = self._ok_response({
            "choices": [
                {"message": {"role": "assistant", "content": "TOPICS: Dane, Pravo"}}
            ]
        })
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

    def test_generate_strips_no_think_from_prompt(self):
        client = self._make_client()
        mock_response = self._ok_response({"choices": [{"message": {"content": "result"}}]})
        with patch("httpx.post", return_value=mock_response) as mock_post:
            client._generate("classify this /no_think", "system prompt /no_think")

        payload = mock_post.call_args.kwargs["json"]
        assert "/no_think" not in payload["messages"][1]["content"]
        assert "/no_think" not in payload["messages"][0]["content"]

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


class TestOpenAIClientClassifyTopics:
    """Tests for OpenAIClient topic classification."""

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

    def test_classify_topics_parses_response(self):
        client = self._make_client()
        mock_response = self._ok_response({
            "choices": [
                {"message": {"content": "TOPICS: Dane a poplatky, Socialni pojisteni"}}
            ]
        })
        with patch("httpx.post", return_value=mock_response):
            topics = client.classify_topics("some law text", "Novela zakona")
        assert topics == ["Dane a poplatky", "Socialni pojisteni"]

    def test_classify_returns_empty_on_failure(self):
        client = self._make_client()
        with patch("httpx.post", side_effect=httpx.ConnectError("fail")):
            topics = client.classify_topics("text", "title")
        assert topics == []
