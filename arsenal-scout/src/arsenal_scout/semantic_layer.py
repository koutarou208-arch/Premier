from __future__ import annotations

import math
import statistics
from collections import defaultdict
from typing import Any


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


class SemanticLayer:
    """One source of truth for metric meaning, grain and comparison logic."""

    def __init__(self, catalog: dict[str, Any], ontology: dict[str, Any]) -> None:
        self.catalog = catalog
        self.ontology = ontology
        self.metrics: dict[str, dict[str, Any]] = catalog["metrics"]

    def _eligible(self, match: dict[str, Any], spec: dict[str, Any]) -> bool:
        filters = spec.get("context_filter", {})
        return all(match.get(key) == value for key, value in filters.items())

    def _deficiency(self, value: float, spec: dict[str, Any]) -> float:
        std = max(float(spec.get("peer_std", 1.0)), 1e-6)
        mean = float(spec["peer_mean"])
        if spec["direction"] == "lower_is_better":
            return (value - mean) / std
        return (mean - value) / std

    def diagnose(self, match_data: dict[str, Any]) -> list[dict[str, Any]]:
        matches = match_data["matches"]
        signals: list[dict[str, Any]] = []
        for metric_id, spec in self.metrics.items():
            rows = [m for m in matches if self._eligible(m, spec) and metric_id in m.get("metrics", {})]
            if not rows:
                continue
            values = [float(row["metrics"][metric_id]) for row in rows]
            value = statistics.fmean(values)
            deficiency_z = self._deficiency(value, spec)
            reliability = clamp(len(rows) / 4.0, 0.25, 1.0)
            adjusted_z = deficiency_z * reliability
            match_evidence = sorted(
                (
                    {
                        "match_id": row["id"],
                        "date": row["date"],
                        "opponent": row["opponent"],
                        "opponent_shape": row["opponent_shape"],
                        "value": row["metrics"][metric_id],
                        "deficiency_z": round(self._deficiency(float(row["metrics"][metric_id]), spec), 3),
                    }
                    for row in rows
                ),
                key=lambda row: row["deficiency_z"],
                reverse=True,
            )[:3]
            signals.append(
                {
                    "metric_id": metric_id,
                    "metric_label": spec["label_ja"],
                    "weakness_id": spec["weakness"],
                    "value": round(value, 3),
                    "unit": spec["unit"],
                    "peer_mean": spec["peer_mean"],
                    "peer_std": spec["peer_std"],
                    "direction": spec["direction"],
                    "deficiency_z": round(deficiency_z, 3),
                    "adjusted_z": round(adjusted_z, 3),
                    "importance": float(spec.get("importance", 1.0)),
                    "sample_size": len(rows),
                    "reliability": round(reliability, 3),
                    "evidence": match_evidence,
                }
            )

        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for signal in signals:
            grouped[signal["weakness_id"]].append(signal)

        weaknesses = []
        for weakness_id, items in grouped.items():
            total_weight = sum(item["importance"] for item in items)
            combined_z = sum(item["adjusted_z"] * item["importance"] for item in items) / total_weight
            # A sigmoid avoids pretending that tiny sample differences are exact.
            severity = 100 / (1 + math.exp(-1.45 * (combined_z - 0.10)))
            evidence_count = sum(item["sample_size"] for item in items)
            confidence = clamp(0.30 + 0.065 * evidence_count + 0.05 * len(items), 0.35, 0.93)
            definition = self.ontology["weaknesses"][weakness_id]
            weaknesses.append(
                {
                    "id": weakness_id,
                    "label": definition["label_ja"],
                    "description": definition["description"],
                    "severity": round(severity, 1),
                    "confidence": round(confidence, 2),
                    "combined_z": round(combined_z, 3),
                    "target_roles": definition["target_roles"],
                    "signals": sorted(items, key=lambda item: item["adjusted_z"], reverse=True),
                    "status": "priority" if combined_z >= 0.55 else "watch" if combined_z >= 0.10 else "strength",
                }
            )
        return sorted(weaknesses, key=lambda row: row["severity"], reverse=True)

    def describe(self) -> dict[str, Any]:
        return {
            "version": self.catalog["version"],
            "grain": self.catalog["grain"],
            "dimensions": self.catalog["dimensions"],
            "metric_count": len(self.metrics),
            "metrics": self.metrics,
        }
