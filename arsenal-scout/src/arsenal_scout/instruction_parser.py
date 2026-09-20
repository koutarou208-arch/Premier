from __future__ import annotations

import re
import unicodedata
from typing import Any


ROLE_GROUPS = {
    "rw": ["one_v_one_winger", "right_progressor"],
    "cf": ["box_forward"],
    "eight": ["creative_eight", "transition_controller"],
    "six": ["transition_controller", "duel_midfielder"],
    "cb": ["recovery_defender"],
}


WEAKNESS_TERMS = {
    "low_block_creation": ("ローブロック攻略", ["ローブロック", "中央攻略", "チャンスメイク"]),
    "right_progression": ("右サイド前進", ["右サイド", "前進"]),
    "box_presence": ("ボックス内得点力", ["ボックス", "得点力", "決定力", "ストライカー", "9番"]),
    "duel_control": ("デュエル", ["空中戦", "デュエル", "セカンドボール", "フィジカル"]),
    "rest_defence": ("トランジション守備", ["トランジション", "移行守備", "被カウンター", "守備貢献", "カウンター対策"]),
}


def _normalized(text: str) -> str:
    return (
        unicodedata.normalize("NFKC", text)
        .lower()
        .replace(",", "")
        .replace("、", " ")
        .replace("。", " ")
    )


def parse_instruction(text: str) -> dict[str, Any]:
    """Parse common Japanese scouting commands without an LLM or paid API.

    Unrecognized language is preserved in ``unparsed`` and never silently
    converted into a hard constraint.
    """

    original = text.strip()
    value = _normalized(original)
    result: dict[str, Any] = {
        "original": original,
        "max_age": None,
        "max_fee_m": None,
        "min_availability": None,
        "role_groups": [],
        "required_roles": [],
        "priority_weights": {},
        "mode": "balanced",
        "financial_priority": False,
        "top_n": None,
        "labels": [],
        "unparsed": [],
    }

    def label(item: str) -> None:
        if item not in result["labels"]:
            result["labels"].append(item)

    def role(group: str, display: str) -> None:
        if group not in result["role_groups"]:
            result["role_groups"].append(group)
            result["required_roles"].extend(ROLE_GROUPS[group])
        label(f"役割：{display}")

    age = re.search(r"(\d{1,2})\s*歳\s*(?:以下|以内|まで)", value) or re.search(r"u[- ]?(\d{1,2})", value)
    if age:
        result["max_age"] = int(age.group(1))
        label(f"年齢上限：{result['max_age']}歳")

    ten_thousand = re.search(r"(\d{2,5})\s*万\s*(?:ユーロ|€)", value)
    million = re.search(r"(?:€\s*)?(\d+(?:\.\d+)?)\s*(?:m|百万ユーロ)", value)
    hundred_million = re.search(r"(\d+(?:\.\d+)?)\s*億\s*(?:ユーロ|€)", value)
    if ten_thousand:
        result["max_fee_m"] = float(ten_thousand.group(1)) / 100
    elif million:
        result["max_fee_m"] = float(million.group(1))
    elif hundred_million:
        result["max_fee_m"] = float(hundred_million.group(1)) * 100
    if result["max_fee_m"] is not None:
        fee_ten_thousand = int(result["max_fee_m"] * 100)
        label(f"移籍金上限：{fee_ten_thousand:,}万ユーロ")

    if re.search(r"右\s*(?:wg|ウイング)|\brw\b", value):
        role("rw", "右WG")
    if re.search(r"(?:\bcf\b|センターフォワード|ストライカー|9番)", value):
        role("cf", "CF／9番")
    if re.search(r"(?:8番|インサイドハーフ)", value):
        role("eight", "8番")
    if re.search(r"(?:6番|アンカー)", value):
        role("six", "6番")
    if re.search(r"(?:\bcb\b|センターバック)", value):
        role("cb", "CB")

    for weakness_id, (display, terms) in WEAKNESS_TERMS.items():
        term = next((item for item in terms if item in value), None)
        if term is None:
            continue
        index = value.index(term)
        nearby = value[max(0, index - 10) : index + len(term) + 12]
        weight = 2.4 if "最優先" in nearby else 1.7 if re.search(r"重視|重要|優先", nearby) else 1.25
        result["priority_weights"][weakness_id] = max(result["priority_weights"].get(weakness_id, 0), weight)
        suffix = "（最優先）" if weight > 2 else "（重視）" if weight > 1.5 else ""
        label(f"優先課題：{display}{suffix}")

    if re.search(r"ライス.*共存|共存.*ライス", value):
        role("eight", "ライスと共存する8番")
        result["priority_weights"]["low_block_creation"] = max(result["priority_weights"].get("low_block_creation", 0), 1.6)
        result["priority_weights"]["rest_defence"] = max(result["priority_weights"].get("rest_defence", 0), 1.6)
        label("戦術条件：ライスとの共存")

    if re.search(r"将来性|若手|伸びしろ|ポテンシャル", value):
        result["mode"] = "potential"
        label("評価方針：将来性を重視")
    if re.search(r"即戦力|完成度|今すぐ", value):
        result["mode"] = "ready"
        label("評価方針：即戦力を重視")
    if re.search(r"財政|費用対効果|コスパ|割安|予算.*抑|負担.*抑", value):
        result["financial_priority"] = True
        label("財政方針：費用対効果を重視")
    if re.search(r"怪我.*(?:除外|少な)|負傷.*(?:除外|少な)|稼働率.*重視", value):
        result["min_availability"] = 82.0
        label("稼働率スコア：82以上")

    top = re.search(r"(?:上位|トップ|top)\s*(\d{1,2})", value)
    if top:
        result["top_n"] = max(1, min(20, int(top.group(1))))
        label(f"表示人数：上位{result['top_n']}人")

    result["required_roles"] = sorted(set(result["required_roles"]))
    if original and not result["labels"]:
        result["unparsed"].append(original)
    return result
