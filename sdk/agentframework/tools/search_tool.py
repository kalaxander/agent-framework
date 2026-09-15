"""Built-in simple search tool. Reference implementation is an in-memory keyword search over a
provided document set (no network/API key needed — this sandbox has neither). A real deployment
would swap in a web search API or vector search backend behind the same `Tool` interface; the
`documents` dict here plays the role that a real backend's index would play.
"""
from __future__ import annotations

from typing import Any

from agentframework.core.errors import GuardrailViolation
from agentframework.tools.base import Tool


class SimpleSearchTool(Tool):
    """Input: {"query": str, "top_k": int (default 3)}
    Output: {"results": [{"doc_id": str, "score": int, "snippet": str}, ...]}, best score first.
    """

    name = "search"

    def __init__(self, documents: dict[str, str]):
        self.documents = documents  # doc_id -> text

    def validate_input(self, input: dict[str, Any]) -> None:
        if "query" not in input or not isinstance(input["query"], str):
            raise GuardrailViolation("search requires a string 'query' field")

    async def run(self, input: dict[str, Any]) -> dict[str, Any]:
        terms = input["query"].lower().split()
        top_k = input.get("top_k", 3)
        scored = []
        for doc_id, text in self.documents.items():
            text_lower = text.lower()
            score = sum(text_lower.count(term) for term in terms)
            if score > 0:
                scored.append((score, doc_id, text))
        scored.sort(key=lambda t: t[0], reverse=True)
        return {
            "results": [
                {"doc_id": doc_id, "score": score, "snippet": text[:120]}
                for score, doc_id, text in scored[:top_k]
            ]
        }
