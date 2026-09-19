from __future__ import annotations

import json
import unittest

from arsenal_scout import ScoutEngine
from arsenal_scout.mcp_server import handle


class ScoutEngineTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.engine = ScoutEngine()
        cls.report = cls.engine.analyze(budget_m=80, top_k=6)

    def test_diagnosis_has_traceable_signals(self) -> None:
        self.assertGreaterEqual(len(self.report["weaknesses"]), 3)
        first = self.report["weaknesses"][0]
        self.assertIn("signals", first)
        self.assertTrue(first["signals"][0]["evidence"])
        self.assertGreater(first["confidence"], 0)

    def test_candidate_ranking_is_bounded_and_explained(self) -> None:
        scores = [candidate["score"] for candidate in self.report["candidates"]]
        self.assertEqual(scores, sorted(scores, reverse=True))
        self.assertTrue(all(0 <= score <= 100 for score in scores))
        self.assertTrue(all(candidate["why"] and candidate["risk"] for candidate in self.report["candidates"]))

    def test_hybrid_search_returns_score_components(self) -> None:
        results = self.engine.search("ローブロックを崩す右サイドの前進役", top_k=5)
        self.assertEqual(len(results), 5)
        self.assertTrue(any(result["kind"] == "player" for result in results))
        self.assertTrue(all("lexical_score" in result and "vector_score" in result for result in results))

    def test_graphrag_paths_connect_weaknesses_to_players(self) -> None:
        paths = self.report["graphrag"]["paths"]
        self.assertTrue(paths)
        self.assertTrue(any("ADDRESSES" in path["relations"] or "NEEDS_ROLE" in path["relations"] for path in paths))

    def test_workflow_completed_every_node(self) -> None:
        expected = set(self.engine.workflow.nodes)
        actual = {row["node"] for row in self.report["workflow_run"]}
        self.assertEqual(expected, actual)
        self.assertTrue(all(row["status"] == "ok" for row in self.report["workflow_run"]))

    def test_mcp_tools_list_and_call(self) -> None:
        listing = handle({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
        names = {tool["name"] for tool in listing["result"]["tools"]}
        self.assertIn("arsenal_analyze_weaknesses", names)
        result = handle(
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {"name": "arsenal_scout_health", "arguments": {}},
            }
        )
        payload = json.loads(result["result"]["content"][0]["text"])
        self.assertEqual(payload["status"], "ok")


if __name__ == "__main__":
    unittest.main()
