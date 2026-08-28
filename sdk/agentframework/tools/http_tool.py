"""Built-in HTTP call tool. Uses stdlib urllib — no `requests` dependency, consistent with the
rest of core/io/tools (see the pydantic note in core/flow.py).
"""
from __future__ import annotations

import asyncio
import json
import urllib.error
import urllib.request
from typing import Any

from agentframework.core.errors import GuardrailViolation, ToolError
from agentframework.tools.base import Tool


class HttpTool(Tool):
    """Input: {"url": str, "method": "GET"|"POST"|..., "headers": dict, "body": Any}
    Output: {"status": int, "body": Any}  (body is parsed JSON if the response is JSON, else text)
    """

    name = "http_call"

    def __init__(self, timeout_seconds: float = 10.0):
        self.timeout_seconds = timeout_seconds

    def validate_input(self, input: dict[str, Any]) -> None:
        if "url" not in input:
            raise GuardrailViolation("http_call requires an 'url' field")

    async def run(self, input: dict[str, Any]) -> dict[str, Any]:
        method = input.get("method", "GET").upper()
        headers = dict(input.get("headers", {}))
        body = input.get("body")
        data = None
        if body is not None:
            data = json.dumps(body).encode()
            headers.setdefault("Content-Type", "application/json")

        req = urllib.request.Request(input["url"], data=data, headers=headers, method=method)

        def _do_request() -> dict[str, Any]:
            try:
                with urllib.request.urlopen(req, timeout=self.timeout_seconds) as resp:
                    raw = resp.read()
                    try:
                        parsed: Any = json.loads(raw) if raw else None
                    except json.JSONDecodeError:
                        parsed = raw.decode(errors="replace")
                    return {"status": resp.status, "body": parsed}
            except urllib.error.HTTPError as exc:
                return {"status": exc.code, "body": exc.read().decode(errors="replace")}
            except urllib.error.URLError as exc:
                raise ToolError(f"http_call failed: {exc}", retryable=True) from exc

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, _do_request)
