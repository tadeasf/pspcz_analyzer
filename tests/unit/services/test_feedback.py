"""Unit tests for GitHubFeedbackClient."""

from unittest.mock import MagicMock, patch

import httpx

from pspcz_analyzer.services.feedback_service import GitHubFeedbackClient, _build_issue_body


class TestBuildIssueBody:
    def test_includes_metadata(self):
        result = _build_issue_body(
            "User text", vote_id=123, period=9, page_url="/votes/123?period=9", lang="cs"
        )
        assert "**Vote ID:** 123" in result
        assert "**Period:** 9" in result
        assert "**Page URL:** /votes/123?period=9" in result
        assert "**Language:** cs" in result
        assert "User text" in result

    def test_escapes_html_in_body(self):
        result = _build_issue_body(
            "<script>alert('xss')</script>", vote_id=1, period=1, page_url="/", lang="en"
        )
        assert "<script>" not in result
        assert "&lt;script&gt;" in result


class TestIsConfigured:
    @patch("pspcz_analyzer.services.feedback_service.GITHUB_FEEDBACK_ENABLED", False)
    @patch("pspcz_analyzer.services.feedback_service.GITHUB_FEEDBACK_TOKEN", "tok123")
    def test_returns_false_when_disabled(self):
        client = GitHubFeedbackClient()
        assert client.is_configured() is False

    @patch("pspcz_analyzer.services.feedback_service.GITHUB_FEEDBACK_ENABLED", True)
    @patch("pspcz_analyzer.services.feedback_service.GITHUB_FEEDBACK_TOKEN", "")
    def test_returns_false_without_token(self):
        client = GitHubFeedbackClient()
        assert client.is_configured() is False

    @patch("pspcz_analyzer.services.feedback_service.GITHUB_FEEDBACK_ENABLED", True)
    @patch("pspcz_analyzer.services.feedback_service.GITHUB_FEEDBACK_TOKEN", "ghp_abc123")
    def test_returns_true_when_configured(self):
        client = GitHubFeedbackClient()
        assert client.is_configured() is True


class TestCreateIssue:
    @patch("pspcz_analyzer.services.feedback_service.GITHUB_FEEDBACK_ENABLED", True)
    @patch("pspcz_analyzer.services.feedback_service.GITHUB_FEEDBACK_TOKEN", "ghp_test")
    @patch("pspcz_analyzer.services.feedback_service.httpx.post")
    def test_success_returns_issue_info(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 201
        mock_resp.json.return_value = {
            "number": 42,
            "html_url": "https://github.com/test/issues/42",
        }
        mock_post.return_value = mock_resp

        client = GitHubFeedbackClient()
        result = client.create_issue(
            "Bug", "Details", vote_id=100, period=9, page_url="/votes/100", lang="cs"
        )

        assert result is not None
        assert result["number"] == 42
        assert "github.com" in result["html_url"]
        mock_post.assert_called_once()

    @patch("pspcz_analyzer.services.feedback_service.GITHUB_FEEDBACK_ENABLED", True)
    @patch("pspcz_analyzer.services.feedback_service.GITHUB_FEEDBACK_TOKEN", "ghp_test")
    @patch("pspcz_analyzer.services.feedback_service.httpx.post")
    def test_api_error_returns_none(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 403
        mock_resp.text = "Forbidden"
        mock_post.return_value = mock_resp

        client = GitHubFeedbackClient()
        result = client.create_issue(
            "Bug", "Details", vote_id=100, period=9, page_url="/", lang="cs"
        )

        assert result is None

    @patch("pspcz_analyzer.services.feedback_service.GITHUB_FEEDBACK_ENABLED", True)
    @patch("pspcz_analyzer.services.feedback_service.GITHUB_FEEDBACK_TOKEN", "ghp_test")
    @patch(
        "pspcz_analyzer.services.feedback_service.httpx.post",
        side_effect=httpx.ConnectError("Network error"),
    )
    def test_network_error_returns_none(self, mock_post):
        client = GitHubFeedbackClient()
        result = client.create_issue(
            "Bug", "Details", vote_id=100, period=9, page_url="/", lang="cs"
        )

        assert result is None

    @patch("pspcz_analyzer.services.feedback_service.GITHUB_FEEDBACK_ENABLED", False)
    def test_not_configured_returns_none(self):
        client = GitHubFeedbackClient()
        result = client.create_issue(
            "Bug", "Details", vote_id=100, period=9, page_url="/", lang="cs"
        )

        assert result is None
