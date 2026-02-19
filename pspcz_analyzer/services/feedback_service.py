"""GitHub Issues integration for user feedback on vote data and AI summaries."""

import html as html_mod

import httpx
from loguru import logger

from pspcz_analyzer.config import (
    GITHUB_FEEDBACK_ENABLED,
    GITHUB_FEEDBACK_LABELS,
    GITHUB_FEEDBACK_REPO,
    GITHUB_FEEDBACK_TOKEN,
)

_GITHUB_API_BASE = "https://api.github.com"
_GITHUB_API_VERSION = "2022-11-28"
_REQUEST_TIMEOUT = 15.0


def _build_issue_body(body: str, vote_id: int, period: int, page_url: str, lang: str) -> str:
    """Assemble the issue body with vote metadata header and user text."""
    escaped_body = html_mod.escape(body)
    return (
        f"**Vote ID:** {vote_id}\n"
        f"**Period:** {period}\n"
        f"**Page URL:** {page_url}\n"
        f"**Language:** {lang}\n\n"
        f"---\n\n"
        f"{escaped_body}"
    )


class GitHubFeedbackClient:
    """Creates GitHub issues from user feedback, modeled after OllamaClient."""

    def __init__(self) -> None:
        self.enabled = GITHUB_FEEDBACK_ENABLED
        self.token = GITHUB_FEEDBACK_TOKEN
        self.repo = GITHUB_FEEDBACK_REPO
        self.labels = [label.strip() for label in GITHUB_FEEDBACK_LABELS if label.strip()]

    def is_configured(self) -> bool:
        """Check if the feedback feature is enabled and has a valid token."""
        return self.enabled and bool(self.token)

    def create_issue(
        self,
        title: str,
        body: str,
        vote_id: int,
        period: int,
        page_url: str,
        lang: str,
    ) -> dict | None:
        """Create a GitHub issue with vote metadata.

        Returns:
            {"number": N, "html_url": "..."} on success, None on failure.
        """
        if not self.is_configured():
            logger.warning("GitHub feedback not configured, skipping issue creation")
            return None

        issue_title = f"[Feedback] Vote #{vote_id}: {title}"
        issue_body = _build_issue_body(body, vote_id, period, page_url, lang)

        url = f"{_GITHUB_API_BASE}/repos/{self.repo}/issues"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": _GITHUB_API_VERSION,
        }
        payload: dict = {
            "title": issue_title,
            "body": issue_body,
        }
        if self.labels:
            payload["labels"] = self.labels

        try:
            resp = httpx.post(url, json=payload, headers=headers, timeout=_REQUEST_TIMEOUT)
            if resp.status_code == 201:
                data = resp.json()
                logger.info("Created GitHub issue #{} for vote {}", data["number"], vote_id)
                return {"number": data["number"], "html_url": data["html_url"]}
            logger.error("GitHub API returned {}: {}", resp.status_code, resp.text[:200])
            return None
        except httpx.HTTPError as exc:
            logger.error("GitHub API request failed: {}", exc)
            return None
