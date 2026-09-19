from __future__ import annotations

import json
import uuid
from copy import deepcopy
from pathlib import Path
from typing import Any

from .hybrid_search import HybridIndex, build_documents
from .knowledge_graph import KnowledgeGraph, build_graph
from .observability import TraceStore, utc_now
from .ranking import CandidateRanker
from .semantic_layer import SemanticLayer
from .workflow import WorkflowGraph


PACKAGE_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = PACKAGE_ROOT / "data"


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


class ScoutEngine:
    """Facade joining the semantic layer, hybrid retrieval, graph and ranker."""

    def __init__(self, data_root: Path | str | None = None) -> None:
        root = Path(data_root) if data_root else DATA_ROOT
        self.data_root = root
        self.ontology = load_json(root / "ontology.json")
        self.metric_catalog = load_json(root / "semantic_metrics.json")
        self.match_data = load_json(root / "matches.demo.json")
        self.players_data = load_json(root / "players.demo.json")
        self.traces = TraceStore()
        self.semantic = SemanticLayer(self.metric_catalog, self.ontology)
        self.ranker = CandidateRanker(self.ontology)
        documents = build_documents(self.match_data, self.players_data, self.ontology, self.metric_catalog)
        self.index = HybridIndex(documents, self.ontology)
        self._last_graph: KnowledgeGraph | None = None
        self._last_report: dict[str, Any] | None = None
        self.workflow = self._build_workflow()

    def _validate(self) -> dict[str, Any]:
        metric_ids = set(self.metric_catalog["metrics"])
        for match in self.match_data["matches"]:
            unknown = set(match["metrics"]) - metric_ids
            if unknown:
                raise ValueError(f"Unknown metrics in {match['id']}: {sorted(unknown)}")
        role_ids = set(self.ontology["roles"])
        for player in self.players_data["players"]:
            unknown_roles = set(player["roles"]) - role_ids
            if unknown_roles:
                raise ValueError(f"Unknown roles for {player['id']}: {sorted(unknown_roles)}")
        return {
            "matches": len(self.match_data["matches"]),
            "players": len(self.players_data["players"]),
            "metrics": len(metric_ids),
            "data_quality": self.match_data["meta"]["data_quality"],
        }

    def _build_workflow(self) -> WorkflowGraph:
        workflow = WorkflowGraph(self.traces)
        workflow.add("validate", (), lambda state: self._validate(), "Validate schema, metric IDs and tactical roles")
        workflow.add(
            "diagnose",
            ("validate",),
            lambda state: self.semantic.diagnose(self.match_data),
            "Compare Arsenal match features with semantic peer baselines",
        )

        def retrieve(state: dict[str, Any]) -> list[dict[str, Any]]:
            labels = " ".join(item["label"] for item in state["diagnose"][:3])
            query = state.get("query") or f"Arsenal weaknesses {labels} recruitment profile"
            return self.index.search(query, top_k=state.get("retrieval_k", 10))

        workflow.add("retrieve", ("diagnose",), retrieve, "Hybrid BM25 and vector retrieval with ontology query expansion")
        workflow.add(
            "rank",
            ("diagnose",),
            lambda state: self.ranker.rank(
                self.players_data["players"],
                state["diagnose"],
                budget_m=state.get("budget_m", 80.0),
                top_k=state.get("top_k", 6),
            ),
            "Rank market candidates against severity-weighted role requirements",
        )

        def graph_expand(state: dict[str, Any]) -> dict[str, Any]:
            graph = build_graph(self.match_data, self.players_data, self.ontology, state["diagnose"])
            self._last_graph = graph
            weakness_ids = [item["id"] for item in state["diagnose"] if item["status"] != "strength"][:4]
            candidate_ids = [item["player_id"] for item in state["rank"]]
            return graph.graphrag_context(weakness_ids, candidate_ids, state["retrieve"])

        workflow.add(
            "graph_expand",
            ("diagnose", "retrieve", "rank"),
            graph_expand,
            "Expand Team→Evidence→Weakness→Role→Player paths for GraphRAG",
        )

        def explain(state: dict[str, Any]) -> dict[str, Any]:
            priorities = [item for item in state["diagnose"] if item["status"] == "priority"][:3]
            leaders = state["rank"][:3]
            return {
                "summary": (
                    f"{len(priorities)}件を優先課題として検出。"
                    f"最上位候補は {leaders[0]['name']}（適合度 {leaders[0]['score']}）です。"
                    if leaders
                    else "候補を生成できませんでした。"
                ),
                "priority_weaknesses": [item["label"] for item in priorities],
                "candidate_headline": [f"{item['rank']}. {item['name']} — {item['why']}" for item in leaders],
                "evidence_rule": "Every claim must resolve to a match metric, semantic definition or player profile node.",
            }

        workflow.add(
            "explain",
            ("diagnose", "retrieve", "rank", "graph_expand"),
            explain,
            "Create a grounded report from retrieved documents and graph paths",
        )
        return workflow

    def analyze(
        self,
        *,
        query: str = "",
        budget_m: float = 80.0,
        top_k: int = 6,
        retrieval_k: int = 10,
    ) -> dict[str, Any]:
        trace_id = uuid.uuid4().hex
        with self.traces.span(
            "scout.analyze",
            trace_id=trace_id,
            attributes={"budget_m": budget_m, "top_k": top_k, "query": query},
        ) as root_span:
            state, workflow_run = self.workflow.execute(
                {"query": query, "budget_m": budget_m, "top_k": top_k, "retrieval_k": retrieval_k},
                trace_id,
            )
            priorities = [item for item in state["diagnose"] if item["status"] != "strength"]
            report = {
                "meta": {
                    "generated_at": utc_now(),
                    "trace_id": trace_id,
                    "team": self.match_data["meta"]["team"],
                    "season": self.match_data["meta"]["season"],
                    "data_quality": self.match_data["meta"]["data_quality"],
                    "disclaimer": self.match_data["meta"]["disclaimer"],
                    "candidate_disclaimer": self.players_data["meta"]["disclaimer"],
                },
                "summary": state["explain"],
                "weaknesses": state["diagnose"],
                "priority_count": len(priorities),
                "candidates": state["rank"],
                "retrieval": state["retrieve"],
                "graphrag": state["graph_expand"],
                "workflow_run": workflow_run,
                "validation": state["validate"],
            }
            root_span.attributes.update(
                {
                    "priority_count": len(priorities),
                    "candidate_count": len(state["rank"]),
                    "retrieval_hits": len(state["retrieve"]),
                    "graph_nodes": len(state["graph_expand"]["nodes"]),
                }
            )
            self.traces.increment("analyses_total")
            self.traces.gauge("last_priority_count", len(priorities))
            self.traces.gauge("last_top_candidate_score", state["rank"][0]["score"] if state["rank"] else 0)
            self._last_report = deepcopy(report)
            return report

    def search(self, query: str, *, top_k: int = 8, kinds: set[str] | None = None) -> list[dict[str, Any]]:
        trace_id = uuid.uuid4().hex
        with self.traces.span(
            "retrieval.hybrid_search",
            trace_id=trace_id,
            attributes={"query": query, "top_k": top_k, "kinds": sorted(kinds or [])},
        ) as span:
            results = self.index.search(query, top_k=top_k, kinds=kinds)
            span.attributes["hit_count"] = len(results)
            self.traces.increment("searches_total")
            return results

    def graph_context(self, weakness_ids: list[str], candidate_ids: list[str]) -> dict[str, Any]:
        if self._last_graph is None:
            self.analyze()
        assert self._last_graph is not None
        return self._last_graph.graphrag_context(weakness_ids, candidate_ids)

    def graph(self) -> dict[str, Any]:
        if self._last_graph is None:
            self.analyze()
        assert self._last_graph is not None
        return self._last_graph.export()

    def health(self) -> dict[str, Any]:
        return {
            "status": "ok",
            "version": "0.1.0",
            "data_quality": self.match_data["meta"]["data_quality"],
            "documents": len(self.index.documents),
            "semantic_metrics": len(self.metric_catalog["metrics"]),
            "ontology_entities": len(self.ontology["entities"]),
            "workflow_nodes": len(self.workflow.nodes),
        }

    def semantic_catalog(self) -> dict[str, Any]:
        return self.semantic.describe()

    def workflow_graph(self) -> dict[str, Any]:
        return self.workflow.describe()

    @property
    def last_report(self) -> dict[str, Any]:
        if self._last_report is None:
            return self.analyze()
        return deepcopy(self._last_report)
