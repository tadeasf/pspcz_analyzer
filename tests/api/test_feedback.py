"""Tests for POST /api/feedback endpoint."""

from unittest.mock import MagicMock, patch

import pytest

from pspcz_analyzer.rate_limit import limiter

_CSRF_HEADERS = {"origin": "http://testserver"}


class TestFeedbackEndpoint:
    @pytest.fixture(autouse=True)
    def _reset_rate_limiter(self):
        """Reset rate limiter storage between tests to avoid 429s."""
        limiter.reset()

    @patch("pspcz_analyzer.routes.api.GITHUB_FEEDBACK_ENABLED", True)
    @patch("pspcz_analyzer.routes.api.GitHubFeedbackClient")
    def test_valid_feedback_returns_success(self, mock_client_cls, client):
        mock_instance = MagicMock()
        mock_instance.create_issue.return_value = {
            "number": 1,
            "html_url": "https://github.com/test/issues/1",
        }
        mock_client_cls.return_value = mock_instance

        resp = client.post(
            "/api/feedback",
            data={
                "vote_id": "100",
                "period": "1",
                "title": "Test issue title",
                "body": "This is a detailed description of the problem",
            },
            headers=_CSRF_HEADERS,
        )
        assert resp.status_code == 200
        assert "github.com" in resp.text

    @patch("pspcz_analyzer.routes.api.GITHUB_FEEDBACK_ENABLED", True)
    def test_short_title_returns_validation_error(self, client):
        resp = client.post(
            "/api/feedback",
            data={
                "vote_id": "100",
                "period": "1",
                "title": "Hi",
                "body": "This is a detailed description of the problem",
            },
            headers=_CSRF_HEADERS,
        )
        assert resp.status_code == 200
        assert "github.com" not in resp.text

    @patch("pspcz_analyzer.routes.api.GITHUB_FEEDBACK_ENABLED", True)
    def test_short_body_returns_validation_error(self, client):
        resp = client.post(
            "/api/feedback",
            data={"vote_id": "100", "period": "1", "title": "Valid title here", "body": "Short"},
            headers=_CSRF_HEADERS,
        )
        assert resp.status_code == 200
        assert "github.com" not in resp.text

    @patch("pspcz_analyzer.routes.api.GITHUB_FEEDBACK_ENABLED", False)
    def test_disabled_returns_unavailable(self, client):
        resp = client.post(
            "/api/feedback",
            data={
                "vote_id": "100",
                "period": "1",
                "title": "Test issue title",
                "body": "This is a detailed description of the problem",
            },
            headers=_CSRF_HEADERS,
        )
        assert resp.status_code == 200
        assert "github.com" not in resp.text

    @patch("pspcz_analyzer.routes.api.GITHUB_FEEDBACK_ENABLED", True)
    @patch("pspcz_analyzer.routes.api.GitHubFeedbackClient")
    def test_github_api_failure_returns_error(self, mock_client_cls, client):
        mock_instance = MagicMock()
        mock_instance.create_issue.return_value = None
        mock_client_cls.return_value = mock_instance

        resp = client.post(
            "/api/feedback",
            data={
                "vote_id": "100",
                "period": "1",
                "title": "Test issue title",
                "body": "This is a detailed description of the problem",
            },
            headers=_CSRF_HEADERS,
        )
        assert resp.status_code == 200
        assert "github.com" not in resp.text
