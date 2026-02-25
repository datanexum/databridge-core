"""Base HTTP client for DataBridge integrations."""

from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


class BaseClient:
    """Lightweight HTTP client using stdlib only (no requests dependency)."""

    def __init__(
        self,
        base_url: str,
        api_key: str = "",
        token_env: str = "",
        auth_header: str = "Authorization",
        auth_prefix: str = "Bearer",
    ):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key or os.getenv(token_env, "")
        self._auth_header = auth_header
        self._auth_prefix = auth_prefix

    def _request(
        self,
        method: str,
        path: str,
        body: Optional[dict] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: int = 30,
    ) -> Dict[str, Any]:
        """Execute an HTTP request and return parsed JSON."""
        url = f"{self.base_url}/{path.lstrip('/')}"
        data = json.dumps(body).encode("utf-8") if body else None
        req_headers = {"Content-Type": "application/json"}
        if self.api_key:
            req_headers[self._auth_header] = f"{self._auth_prefix} {self.api_key}"
        if headers:
            req_headers.update(headers)

        req = Request(url, data=data, headers=req_headers, method=method.upper())
        try:
            with urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw) if raw.strip() else {}
        except HTTPError as e:
            body_text = e.read().decode("utf-8", errors="replace")[:500]
            return {"error": str(e), "status": e.code, "detail": body_text}
        except URLError as e:
            return {"error": f"Connection failed: {e.reason}"}

    def get(self, path: str, **kw) -> Dict[str, Any]:
        return self._request("GET", path, **kw)

    def post(self, path: str, body: dict = None, **kw) -> Dict[str, Any]:
        return self._request("POST", path, body=body, **kw)

    def put(self, path: str, body: dict = None, **kw) -> Dict[str, Any]:
        return self._request("PUT", path, body=body, **kw)

    def check_configured(self, service_name: str) -> None:
        """Raise if API key is not set."""
        if not self.api_key:
            raise ValueError(
                f"{service_name} API key not configured. "
                f"Set it in .env or pass via settings."
            )
