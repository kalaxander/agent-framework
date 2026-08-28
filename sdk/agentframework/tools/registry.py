"""Tool registry (docs/PRD.md > Tools/Actions: "adding one never touches orchestrator code").
Tasks reference tools by name (`Task(tool="http_call")`); the registry resolves the name and
runs the tool's input/output validation hooks around the call.
"""
from __future__ import annotations

from typing import Any

from agentframework.tools.base import Tool


class ToolRegistry:
    def __init__(self) -> None:
        self._tools: dict[str, Tool] = {}

    def register(self, tool: Tool) -> None:
        self._tools[tool.name] = tool

    def get(self, name: str) -> Tool:
        if name not in self._tools:
            raise KeyError(f"No tool registered under name '{name}'")
        return self._tools[name]

    def names(self) -> list[str]:
        return sorted(self._tools)

    async def invoke(self, name: str, input: dict[str, Any]) -> Any:
        tool = self.get(name)
        tool.validate_input(input)
        result = await tool.run(input)
        tool.validate_output(result)
        return result
