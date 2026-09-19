from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable

from .observability import TraceStore


NodeFunction = Callable[[dict[str, Any]], Any]


@dataclass
class WorkflowNode:
    id: str
    dependencies: tuple[str, ...]
    function: NodeFunction
    description: str


class WorkflowGraph:
    """Deterministic DAG so every recommendation has a replayable lineage."""

    def __init__(self, trace_store: TraceStore) -> None:
        self.trace_store = trace_store
        self.nodes: dict[str, WorkflowNode] = {}

    def add(self, node_id: str, dependencies: tuple[str, ...], function: NodeFunction, description: str) -> None:
        self.nodes[node_id] = WorkflowNode(node_id, dependencies, function, description)

    def execute(self, initial_state: dict[str, Any], trace_id: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        state = dict(initial_state)
        remaining = set(self.nodes)
        completed: set[str] = set()
        run_log: list[dict[str, Any]] = []
        while remaining:
            ready = sorted(
                node_id
                for node_id in remaining
                if set(self.nodes[node_id].dependencies).issubset(completed)
            )
            if not ready:
                raise RuntimeError("Workflow graph has a cycle or missing dependency")
            for node_id in ready:
                node = self.nodes[node_id]
                started = time.perf_counter()
                with self.trace_store.span(
                    f"workflow.{node_id}",
                    trace_id=trace_id,
                    attributes={"dependencies": list(node.dependencies)},
                ) as span:
                    value = node.function(state)
                    state[node_id] = value
                    span.attributes["output_type"] = type(value).__name__
                    if hasattr(value, "__len__"):
                        span.attributes["output_size"] = len(value)
                duration = round((time.perf_counter() - started) * 1000, 3)
                run_log.append({"node": node_id, "status": "ok", "duration_ms": duration, "description": node.description})
                completed.add(node_id)
                remaining.remove(node_id)
        return state, run_log

    def describe(self) -> dict[str, Any]:
        return {
            "nodes": [
                {
                    "id": node.id,
                    "dependencies": list(node.dependencies),
                    "description": node.description,
                }
                for node in self.nodes.values()
            ],
            "edges": [
                {"source": dependency, "target": node.id}
                for node in self.nodes.values()
                for dependency in node.dependencies
            ],
        }
