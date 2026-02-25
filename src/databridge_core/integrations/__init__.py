"""DataBridge Integrations — reusable clients for third-party tools.

Core package includes BaseClient and SlackClient.
Full platform (databridge-ai) includes all 8 integration clients.
"""

from ._base import BaseClient
from .slack import SlackClient

__all__ = [
    "BaseClient",
    "SlackClient",
]
