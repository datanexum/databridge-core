"""Slack integration — post messages, reconciliation reports, and workflow alerts."""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional

from ._base import BaseClient


class SlackClient(BaseClient):
    """Slack Bot API client for posting structured messages."""

    def __init__(self, bot_token: str = "", webhook_url: str = ""):
        self.webhook_url = webhook_url or os.getenv("DATABRIDGE_SLACK_WEBHOOK", "")
        token = bot_token or os.getenv("DATABRIDGE_SLACK_BOT_TOKEN", "")
        super().__init__(
            base_url="https://slack.com/api",
            api_key=token,
            auth_prefix="Bearer",
        )

    def send_message(
        self,
        channel: str,
        text: str,
        blocks: Optional[List[dict]] = None,
    ) -> Dict[str, Any]:
        """Post a message to a Slack channel.

        Args:
            channel: Channel name or ID (e.g. #data-ops or C01234).
            text: Plain-text fallback message.
            blocks: Optional Block Kit blocks for rich formatting.

        Returns:
            Slack API response dict with ok, ts, channel keys.
        """
        if self.webhook_url and not self.api_key:
            return self._post_webhook(text, blocks)

        self.check_configured("Slack")
        body: Dict[str, Any] = {"channel": channel, "text": text}
        if blocks:
            body["blocks"] = blocks
        return self.post("chat.postMessage", body=body)

    def post_reconciliation_report(
        self,
        channel: str,
        report: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Format and post a reconciliation report as Slack blocks.

        Args:
            channel: Target channel.
            report: Reconciliation result dict with keys like
                matches, orphans_left, orphans_right, conflicts.

        Returns:
            Slack API response.
        """
        matches = report.get("matches", 0)
        orphans_l = report.get("orphans_left", 0)
        orphans_r = report.get("orphans_right", 0)
        conflicts = report.get("conflicts", 0)
        source_a = report.get("source_a", "Source A")
        source_b = report.get("source_b", "Source B")

        status = "All matched" if (orphans_l + orphans_r + conflicts) == 0 else "Issues found"
        emoji = ":white_check_mark:" if status == "All matched" else ":warning:"

        blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"{emoji} Reconciliation Report"},
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Sources:*\n{source_a} vs {source_b}"},
                    {"type": "mrkdwn", "text": f"*Status:*\n{status}"},
                ],
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Matches:* {matches}"},
                    {"type": "mrkdwn", "text": f"*Conflicts:* {conflicts}"},
                    {"type": "mrkdwn", "text": f"*Orphans ({source_a}):* {orphans_l}"},
                    {"type": "mrkdwn", "text": f"*Orphans ({source_b}):* {orphans_r}"},
                ],
            },
        ]
        text = f"Reconciliation: {matches} matches, {conflicts} conflicts, {orphans_l + orphans_r} orphans"
        return self.send_message(channel, text, blocks)

    def notify_workflow_complete(
        self,
        channel: str,
        workflow_name: str,
        status: str = "completed",
        details: str = "",
    ) -> Dict[str, Any]:
        """Send a workflow completion notification.

        Args:
            channel: Target channel.
            workflow_name: Name of the completed workflow.
            status: "completed", "failed", or "warning".
            details: Optional detail text.

        Returns:
            Slack API response.
        """
        emoji_map = {"completed": ":white_check_mark:", "failed": ":x:", "warning": ":warning:"}
        emoji = emoji_map.get(status, ":information_source:")
        text = f"{emoji} Workflow *{workflow_name}* — {status}"
        if details:
            text += f"\n{details}"
        return self.send_message(channel, text)

    def _post_webhook(self, text: str, blocks: Optional[List[dict]] = None) -> Dict[str, Any]:
        """Post via incoming webhook (no bot token needed)."""
        from urllib.request import Request, urlopen
        import json as _json

        body: Dict[str, Any] = {"text": text}
        if blocks:
            body["blocks"] = blocks
        data = _json.dumps(body).encode("utf-8")
        req = Request(self.webhook_url, data=data, headers={"Content-Type": "application/json"})
        with urlopen(req, timeout=15) as resp:
            return {"ok": True, "status": resp.status}
