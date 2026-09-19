from __future__ import annotations

from typing import Any


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


class CandidateRanker:
    def __init__(self, ontology: dict[str, Any]) -> None:
        self.ontology = ontology

    def _role_fit(self, player: dict[str, Any], role: str) -> tuple[float, list[dict[str, Any]]]:
        weights = self.ontology["roles"][role]["required_metrics"]
        contributions = []
        weighted = 0.0
        covered = 0.0
        for metric, weight in weights.items():
            if metric not in player["metrics"]:
                continue
            value = float(player["metrics"][metric])
            weighted += value * weight
            covered += weight
            contributions.append({"metric": metric, "value": value, "weight": weight, "points": round(value * weight, 2)})
        fit = weighted / max(covered, 1e-9)
        if role in player["roles"]:
            fit = min(100.0, fit + 3.0)
        contributions.sort(key=lambda row: row["points"], reverse=True)
        return fit, contributions

    def rank(
        self,
        players: list[dict[str, Any]],
        weaknesses: list[dict[str, Any]],
        *,
        budget_m: float = 80.0,
        top_k: int = 6,
        instruction: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        instruction = instruction or {}
        priority_weights = instruction.get("priority_weights", {})
        if priority_weights:
            priorities = [item for item in weaknesses if item["id"] in priority_weights]
        else:
            priorities = [item for item in weaknesses if item["status"] in {"priority", "watch"}]
            priorities = priorities[:4] or weaknesses[:2]

        weighted_priorities = [
            {
                **item,
                "instruction_weight": float(priority_weights.get(item["id"], 1.0)),
                "effective_severity": float(item["severity"]) * float(priority_weights.get(item["id"], 1.0)),
            }
            for item in priorities
        ]
        severity_sum = sum(item["effective_severity"] for item in weighted_priorities) or 1.0
        required_roles = set(instruction.get("required_roles", []))
        max_age = instruction.get("max_age")
        max_fee = instruction.get("max_fee_m")
        min_availability = instruction.get("min_availability")
        scoring_budget = min(float(budget_m), float(max_fee)) if max_fee is not None else float(budget_m)
        mode = instruction.get("mode", "balanced")
        requested_top_n = instruction.get("top_n")
        if requested_top_n is not None:
            top_k = int(requested_top_n)

        rankings = []
        for player in players:
            availability = float(player.get("availability", player["metrics"].get("availability", 70)))
            if max_age is not None and int(player["age"]) > int(max_age):
                continue
            if max_fee is not None and float(player["estimated_fee_m"]) > float(max_fee):
                continue
            if min_availability is not None and availability < float(min_availability):
                continue
            if required_roles and required_roles.isdisjoint(player["roles"]):
                continue

            weakness_fits = []
            for weakness in weighted_priorities:
                role_rows = []
                for role in weakness["target_roles"]:
                    fit, contributions = self._role_fit(player, role)
                    role_rows.append({"role": role, "fit": fit, "contributions": contributions})
                best = max(role_rows, key=lambda row: row["fit"])
                role_compatibility = 1.0 if best["role"] in player["roles"] else 0.88
                adjusted_fit = best["fit"] * role_compatibility
                weakness_fits.append(
                    {
                        "weakness_id": weakness["id"],
                        "weakness_label": weakness["label"],
                        "severity": weakness["severity"],
                        "instruction_weight": weakness["instruction_weight"],
                        "effective_severity": round(weakness["effective_severity"], 2),
                        "best_role": best["role"],
                        "role_label": self.ontology["roles"][best["role"]]["label_ja"],
                        "fit": round(adjusted_fit, 1),
                        "top_metrics": best["contributions"][:3],
                    }
                )
            tactical_fit = sum(row["fit"] * row["effective_severity"] for row in weakness_fits) / severity_sum
            if mode == "potential":
                age_fit = clamp(100 - max(0.0, float(player["age"]) - 21.0) * 10.0, 50, 100)
                score_weights = (0.68, 0.06, 0.18, 0.08)
            elif mode == "ready":
                age_fit = clamp(100 - abs(float(player["age"]) - 25.0) * 4.0, 65, 100)
                score_weights = (0.78, 0.14, 0.03, 0.05)
            else:
                age_fit = clamp(100 - abs(float(player["age"]) - 24.0) * 6.0, 55, 100)
                score_weights = (0.74, 0.10, 0.08, 0.08)
            affordability = clamp(
                100 - max(0.0, float(player["estimated_fee_m"]) - scoring_budget) * 2.5,
                15,
                100,
            )
            multi_role_bonus = min(4.0, max(0, len(set(player["roles"])) - 1) * 2.0)
            final_score = (
                tactical_fit * score_weights[0]
                + availability * score_weights[1]
                + age_fit * score_weights[2]
                + affordability * score_weights[3]
                + multi_role_bonus
            )
            confidence = 0.45  # demo data: deliberately capped
            strongest = max(weakness_fits, key=lambda row: row["fit"])
            weakest = min(weakness_fits, key=lambda row: row["fit"])
            rankings.append(
                {
                    "player_id": player["id"],
                    "name": player["name"],
                    "club": player["club"],
                    "league": player["league"],
                    "age": player["age"],
                    "estimated_fee_m": player["estimated_fee_m"],
                    "roles": player["roles"],
                    "score": round(clamp(final_score, 0, 100), 1),
                    "tactical_fit": round(tactical_fit, 1),
                    "availability_score": round(availability, 1),
                    "age_fit": round(age_fit, 1),
                    "affordability": round(affordability, 1),
                    "confidence": confidence,
                    "instruction_mode": mode,
                    "matched_constraints": list(instruction.get("labels", [])),
                    "weakness_fits": sorted(
                        weakness_fits,
                        key=lambda row: row["effective_severity"],
                        reverse=True,
                    ),
                    "why": f"{strongest['weakness_label']}に対する{strongest['role_label']}適合が最も強い。",
                    "risk": f"相対的な弱点は{weakest['weakness_label']}への適合。デモ指標のため映像・負傷歴・契約条件で要検証。",
                }
            )
        rankings.sort(key=lambda row: row["score"], reverse=True)
        for index, row in enumerate(rankings[:top_k], start=1):
            row["rank"] = index
        return rankings[:top_k]
