from __future__ import annotations

from collections import defaultdict, deque
from typing import Any


class KnowledgeGraph:
    """Small property graph used for local GraphRAG and explainable paths."""

    def __init__(self) -> None:
        self.nodes: dict[str, dict[str, Any]] = {}
        self.edges: list[dict[str, Any]] = []
        self.adjacency: dict[str, list[dict[str, Any]]] = defaultdict(list)

    def add_node(self, node_id: str, kind: str, label: str, **properties: Any) -> None:
        self.nodes[node_id] = {"id": node_id, "kind": kind, "label": label, **properties}

    def add_edge(self, source: str, target: str, relation: str, **properties: Any) -> None:
        edge = {"source": source, "target": target, "relation": relation, **properties}
        self.edges.append(edge)
        self.adjacency[source].append(edge)
        self.adjacency[target].append({**edge, "source": target, "target": source, "reverse": True})

    def shortest_path(self, source: str, target: str, max_depth: int = 5) -> list[dict[str, Any]]:
        if source not in self.nodes or target not in self.nodes:
            return []
        queue = deque([(source, [])])
        visited = {source}
        while queue:
            current, path = queue.popleft()
            if len(path) >= max_depth:
                continue
            for edge in self.adjacency[current]:
                nxt = edge["target"]
                next_path = [*path, edge]
                if nxt == target:
                    return next_path
                if nxt not in visited:
                    visited.add(nxt)
                    queue.append((nxt, next_path))
        return []

    def subgraph(self, node_ids: set[str], depth: int = 1) -> dict[str, Any]:
        selected = set(node_ids)
        frontier = set(node_ids)
        for _ in range(depth):
            following = set()
            for node_id in frontier:
                following.update(edge["target"] for edge in self.adjacency.get(node_id, []))
            following -= selected
            selected |= following
            frontier = following
        edges = [edge for edge in self.edges if edge["source"] in selected and edge["target"] in selected]
        return {
            "nodes": [self.nodes[node_id] for node_id in selected if node_id in self.nodes],
            "edges": edges,
        }

    def graphrag_context(
        self,
        weakness_ids: list[str],
        candidate_ids: list[str],
        retrieval_hits: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        seed_nodes = {"team:arsenal"}
        seed_nodes.update(f"weakness:{item}" for item in weakness_ids)
        seed_nodes.update(f"player:{item}" for item in candidate_ids)
        for hit in retrieval_hits or []:
            if hit["id"] in self.nodes:
                seed_nodes.add(hit["id"])
        graph = self.subgraph(seed_nodes, depth=1)
        paths = []
        for candidate_id in candidate_ids:
            target = f"player:{candidate_id}"
            for weakness_id in weakness_ids:
                source = f"weakness:{weakness_id}"
                path = self.shortest_path(source, target, max_depth=3)
                if path:
                    paths.append(
                        {
                            "from": source,
                            "to": target,
                            "relations": [edge["relation"] for edge in path],
                            "nodes": [source, *[edge["target"] for edge in path]],
                        }
                    )
        return {**graph, "paths": paths}

    def export(self) -> dict[str, Any]:
        return {"nodes": list(self.nodes.values()), "edges": self.edges}


def build_graph(
    match_data: dict[str, Any],
    players_data: dict[str, Any],
    ontology: dict[str, Any],
    weaknesses: list[dict[str, Any]],
) -> KnowledgeGraph:
    graph = KnowledgeGraph()
    graph.add_node("team:arsenal", "Team", "Arsenal")
    graph.add_node("source:demo", "Source", "Bundled synthetic demo", quality="synthetic_demo")

    for weakness in weaknesses:
        weakness_id = f"weakness:{weakness['id']}"
        graph.add_node(
            weakness_id,
            "Weakness",
            weakness["label"],
            severity=weakness["severity"],
            confidence=weakness["confidence"],
            status=weakness["status"],
        )
        graph.add_edge("team:arsenal", weakness_id, "HAS_WEAKNESS", weight=weakness["severity"])
        for role in weakness["target_roles"]:
            role_id = f"role:{role}"
            role_data = ontology["roles"][role]
            graph.add_node(role_id, "TacticalRole", role_data["label_ja"])
            graph.add_edge(weakness_id, role_id, "NEEDS_ROLE")
        for signal in weakness["signals"]:
            metric_id = f"metric:{signal['metric_id']}"
            graph.add_node(metric_id, "Metric", signal["metric_label"], value=signal["value"], unit=signal["unit"])
            graph.add_edge(metric_id, weakness_id, "INDICATES", z=signal["deficiency_z"])
            for evidence in signal["evidence"]:
                match_id = f"match:{evidence['match_id']}"
                graph.add_edge(match_id, metric_id, "HAS_OBSERVATION", value=evidence["value"])

    for match in match_data["matches"]:
        node_id = f"match:{match['id']}"
        graph.add_node(
            node_id,
            "Match",
            f"Arsenal vs {match['opponent']}",
            date=match["date"],
            result=match["result"],
            opponent_shape=match["opponent_shape"],
        )
        graph.add_edge("team:arsenal", node_id, "PLAYED_IN")
        graph.add_edge(node_id, "source:demo", "FROM_SOURCE")

    target_role_to_weaknesses: dict[str, list[str]] = defaultdict(list)
    for weakness_id, definition in ontology["weaknesses"].items():
        for role in definition["target_roles"]:
            target_role_to_weaknesses[role].append(weakness_id)

    for player in players_data["players"]:
        player_id = f"player:{player['id']}"
        club_id = f"club:{player['club'].lower().replace(' ', '-')}"
        graph.add_node(player_id, "Player", player["name"], age=player["age"], fee_m=player["estimated_fee_m"])
        graph.add_node(club_id, "Club", player["club"], league=player["league"])
        graph.add_edge(player_id, club_id, "PLAYS_FOR")
        for role in player["roles"]:
            role_id = f"role:{role}"
            role_data = ontology["roles"][role]
            graph.add_node(role_id, "TacticalRole", role_data["label_ja"])
            graph.add_edge(player_id, role_id, "CAN_PLAY")
            for weakness_id in target_role_to_weaknesses.get(role, []):
                graph.add_edge(player_id, f"weakness:{weakness_id}", "ADDRESSES", via_role=role)
    return graph
