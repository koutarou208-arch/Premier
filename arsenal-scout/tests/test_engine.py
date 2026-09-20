from __future__ import annotations

import json
import unittest

from arsenal_scout import ScoutEngine
from arsenal_scout.instruction_parser import parse_instruction
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
        self.assertTrue(all("financial_assessment" in candidate for candidate in self.report["candidates"]))
        self.assertTrue(all(0 <= candidate["financial_fit"] <= 100 for candidate in self.report["candidates"]))

    def test_club_finance_is_included_in_ranking(self) -> None:
        finance = self.report["club_finance"]
        self.assertEqual(finance["usable_transfer_budget_m"], 71)
        self.assertEqual(
            finance["usable_transfer_budget_m"],
            finance["transfer_budget_m"]
            - finance["committed_transfer_spend_m"]
            + finance["expected_sales_m"]
            - finance["protected_cash_reserve_m"],
        )
        self.assertTrue(self.report["graphrag"]["financial_paths"])

    def test_financial_priority_changes_score_blend(self) -> None:
        report = self.engine.analyze(query="財政負担を抑え、費用対効果を重視してランキング", top_k=8)
        self.assertTrue(report["instruction"]["financial_priority"])
        ranks = {candidate["player_id"]: candidate["rank"] for candidate in report["candidates"]}
        self.assertGreater(ranks["p-okafor"], 3)

    def test_hybrid_search_returns_score_components(self) -> None:
        results = self.engine.search("ローブロックを崩す右サイドの前進役", top_k=5)
        self.assertEqual(len(results), 5)
        self.assertTrue(any(result["kind"] == "player" for result in results))
        self.assertTrue(all("lexical_score" in result and "vector_score" in result for result in results))

    def test_hybrid_search_can_retrieve_financial_context(self) -> None:
        results = self.engine.search("クラブの移籍予算と賃金余力", top_k=5)
        self.assertTrue(any(result["kind"] == "finance" for result in results))

    def test_graphrag_paths_connect_weaknesses_to_players(self) -> None:
        paths = self.report["graphrag"]["paths"]
        self.assertTrue(paths)
        self.assertTrue(any("ADDRESSES" in path["relations"] or "NEEDS_ROLE" in path["relations"] for path in paths))

    def test_workflow_completed_every_node(self) -> None:
        expected = set(self.engine.workflow.nodes)
        actual = {row["node"] for row in self.report["workflow_run"]}
        self.assertEqual(expected, actual)
        self.assertTrue(all(row["status"] == "ok" for row in self.report["workflow_run"]))

    def test_free_japanese_instruction_parser(self) -> None:
        parsed = parse_instruction(
            "23歳以下、移籍金6,000万ユーロ以内で、ローブロック攻略を最優先した右WGを上位3人"
        )
        self.assertEqual(parsed["max_age"], 23)
        self.assertEqual(parsed["max_fee_m"], 60)
        self.assertEqual(parsed["top_n"], 3)
        self.assertIn("one_v_one_winger", parsed["required_roles"])
        self.assertEqual(parsed["priority_weights"]["low_block_creation"], 2.4)
        self.assertNotIn("right_progression", parsed["priority_weights"])

    def test_instruction_filters_and_ranks_candidates(self) -> None:
        report = self.engine.analyze(
            query="23歳以下、移籍金6000万ユーロ以内で、ローブロック攻略を最優先した右WGをランキング",
            top_k=6,
        )
        self.assertTrue(report["candidates"])
        self.assertEqual(report["instruction"]["max_age"], 23)
        for candidate in report["candidates"]:
            self.assertLessEqual(candidate["age"], 23)
            self.assertLessEqual(candidate["estimated_fee_m"], 60)
            self.assertTrue(
                {"one_v_one_winger", "right_progressor"}.intersection(candidate["roles"])
            )

    def test_ready_six_excludes_low_availability(self) -> None:
        parsed = parse_instruction("25歳以下、怪我の多い選手を除外。即戦力の6番をランキング")
        self.assertEqual(parsed["mode"], "ready")
        self.assertEqual(parsed["min_availability"], 82)
        self.assertIn("transition_controller", parsed["required_roles"])
        report = self.engine.analyze(query=parsed["original"])
        self.assertTrue(all(candidate["availability_score"] >= 82 for candidate in report["candidates"]))

    def test_impossible_instruction_returns_empty_ranking(self) -> None:
        report = self.engine.analyze(query="18歳以下、予算1000万ユーロ以内の右WG")
        self.assertEqual(report["candidates"], [])
        self.assertIn("候補は見つかりませんでした", report["summary"]["summary"])

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
