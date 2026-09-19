from __future__ import annotations

import json
import sys
from typing import Any

from .engine import ScoutEngine


ENGINE = ScoutEngine()


TOOLS = [
    {
        "name": "arsenal_analyze_weaknesses",
        "description": "Analyze Arsenal match features, return evidence-backed weaknesses and ranked recruitment candidates.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "budget_m": {"type": "number", "minimum": 0},
                "top_k": {"type": "integer", "minimum": 1, "maximum": 20}
            }
        }
    },
    {
        "name": "arsenal_search_evidence",
        "description": "Run ontology-expanded BM25 plus vector hybrid search across matches, weaknesses and player profiles.",
        "inputSchema": {
            "type": "object",
            "required": ["query"],
            "properties": {
                "query": {"type": "string"},
                "top_k": {"type": "integer", "minimum": 1, "maximum": 25},
                "kinds": {"type": "array", "items": {"enum": ["match", "weakness", "player"]}}
            }
        }
    },
    {
        "name": "arsenal_graphrag_context",
        "description": "Return explainable Weakness→Role→Player graph paths for selected entities.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "weakness_ids": {"type": "array", "items": {"type": "string"}},
                "candidate_ids": {"type": "array", "items": {"type": "string"}}
            }
        }
    },
    {
        "name": "arsenal_scout_health",
        "description": "Return health, index size, ontology and workflow metadata.",
        "inputSchema": {"type": "object", "properties": {}}
    }
]


def call_tool(name: str, arguments: dict[str, Any]) -> Any:
    if name == "arsenal_analyze_weaknesses":
        return ENGINE.analyze(
            query=str(arguments.get("query", "")),
            budget_m=float(arguments.get("budget_m", 80)),
            top_k=int(arguments.get("top_k", 6)),
        )
    if name == "arsenal_search_evidence":
        kinds = set(arguments.get("kinds", [])) or None
        return ENGINE.search(str(arguments["query"]), top_k=int(arguments.get("top_k", 8)), kinds=kinds)
    if name == "arsenal_graphrag_context":
        return ENGINE.graph_context(arguments.get("weakness_ids", []), arguments.get("candidate_ids", []))
    if name == "arsenal_scout_health":
        return ENGINE.health()
    raise ValueError(f"Unknown tool: {name}")


def response(request_id: Any, result: Any = None, error: dict[str, Any] | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {"jsonrpc": "2.0", "id": request_id}
    if error is not None:
        payload["error"] = error
    else:
        payload["result"] = result
    return payload


def handle(message: dict[str, Any]) -> dict[str, Any] | None:
    method = message.get("method")
    request_id = message.get("id")
    if request_id is None:
        return None
    if method == "initialize":
        return response(
            request_id,
            {
                "protocolVersion": "2025-03-26",
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": "arsenal-scout", "version": "0.1.0"},
            },
        )
    if method == "ping":
        return response(request_id, {})
    if method == "tools/list":
        return response(request_id, {"tools": TOOLS})
    if method == "tools/call":
        params = message.get("params", {})
        try:
            result = call_tool(params.get("name", ""), params.get("arguments", {}))
            return response(
                request_id,
                {
                    "content": [{"type": "text", "text": json.dumps(result, ensure_ascii=False)}],
                    "structuredContent": result,
                    "isError": False,
                },
            )
        except Exception as exc:
            return response(
                request_id,
                {
                    "content": [{"type": "text", "text": f"{type(exc).__name__}: {exc}"}],
                    "isError": True,
                },
            )
    return response(request_id, error={"code": -32601, "message": f"Method not found: {method}"})


def main() -> None:
    for line in sys.stdin:
        if not line.strip():
            continue
        try:
            result = handle(json.loads(line))
            if result is not None:
                sys.stdout.write(json.dumps(result, ensure_ascii=False) + "\n")
                sys.stdout.flush()
        except Exception as exc:
            sys.stdout.write(json.dumps(response(None, error={"code": -32603, "message": str(exc)})) + "\n")
            sys.stdout.flush()


if __name__ == "__main__":
    main()
