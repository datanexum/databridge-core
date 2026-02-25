"""Tests for the integrations module."""

import json
from unittest.mock import MagicMock, patch

import pytest


class TestBaseClient:
    """Test the BaseClient HTTP wrapper."""

    def test_init_with_api_key(self):
        from databridge_core.integrations import BaseClient

        client = BaseClient(base_url="https://api.example.com", api_key="test-key")
        assert client.base_url == "https://api.example.com"
        assert client.api_key == "test-key"

    def test_init_strips_trailing_slash(self):
        from databridge_core.integrations import BaseClient

        client = BaseClient(base_url="https://api.example.com/")
        assert client.base_url == "https://api.example.com"

    def test_init_from_env(self, monkeypatch):
        from databridge_core.integrations import BaseClient

        monkeypatch.setenv("MY_API_KEY", "env-key-123")
        client = BaseClient(base_url="https://api.example.com", token_env="MY_API_KEY")
        assert client.api_key == "env-key-123"

    def test_check_configured_raises_without_key(self):
        from databridge_core.integrations import BaseClient

        client = BaseClient(base_url="https://api.example.com")
        with pytest.raises(ValueError, match="Slack API key not configured"):
            client.check_configured("Slack")

    def test_check_configured_passes_with_key(self):
        from databridge_core.integrations import BaseClient

        client = BaseClient(base_url="https://api.example.com", api_key="key")
        client.check_configured("Slack")  # Should not raise


class TestSlackClient:
    """Test the SlackClient."""

    def test_init_defaults(self):
        from databridge_core.integrations import SlackClient

        client = SlackClient()
        assert client.base_url == "https://slack.com/api"
        assert client.api_key == ""

    def test_init_with_bot_token(self):
        from databridge_core.integrations import SlackClient

        client = SlackClient(bot_token="xoxb-test-token")
        assert client.api_key == "xoxb-test-token"

    def test_init_with_webhook(self):
        from databridge_core.integrations import SlackClient

        client = SlackClient(webhook_url="https://hooks.slack.com/services/T/B/X")
        assert client.webhook_url == "https://hooks.slack.com/services/T/B/X"

    def test_init_from_env(self, monkeypatch):
        from databridge_core.integrations import SlackClient

        monkeypatch.setenv("DATABRIDGE_SLACK_BOT_TOKEN", "xoxb-env-token")
        client = SlackClient()
        assert client.api_key == "xoxb-env-token"

    def test_send_message_requires_config(self):
        from databridge_core.integrations import SlackClient

        client = SlackClient()
        with pytest.raises(ValueError, match="Slack API key not configured"):
            client.send_message("#general", "test message")

    def test_post_reconciliation_report_format(self):
        from databridge_core.integrations import SlackClient

        client = SlackClient(bot_token="xoxb-test")

        # Mock the post method to capture the call
        with patch.object(client, "post", return_value={"ok": True}) as mock_post:
            report = {
                "matches": 950,
                "orphans_left": 10,
                "orphans_right": 5,
                "conflicts": 35,
                "source_a": "ERP",
                "source_b": "GL",
            }
            client.post_reconciliation_report("#data-ops", report)

            # Verify the post was called with correct structure
            mock_post.assert_called_once()
            call_args = mock_post.call_args
            body = call_args[1]["body"] if "body" in call_args[1] else call_args[0][1]
            assert body["channel"] == "#data-ops"
            assert "blocks" in body
            assert "950 matches" in body["text"]

    def test_notify_workflow_complete(self):
        from databridge_core.integrations import SlackClient

        client = SlackClient(bot_token="xoxb-test")

        with patch.object(client, "post", return_value={"ok": True}):
            result = client.notify_workflow_complete(
                "#data-ops", "Daily ETL", "completed", "Processed 1M rows"
            )
            assert result == {"ok": True}

    def test_webhook_fallback(self):
        from databridge_core.integrations import SlackClient

        # Client with webhook but no bot token
        client = SlackClient(webhook_url="https://hooks.slack.com/services/T/B/X")

        with patch.object(client, "_post_webhook", return_value={"ok": True}) as mock_wh:
            client.send_message("#general", "test via webhook")
            mock_wh.assert_called_once_with("test via webhook", None)
