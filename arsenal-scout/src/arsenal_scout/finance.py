from __future__ import annotations

from typing import Any


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def summarize_finance(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Return the auditable recruitment capacity derived from a club snapshot."""

    usable_budget = (
        float(snapshot["transfer_budget_m"])
        - float(snapshot["committed_transfer_spend_m"])
        + float(snapshot["expected_sales_m"])
        - float(snapshot["protected_cash_reserve_m"])
    )
    return {
        **snapshot,
        "usable_transfer_budget_m": round(max(0.0, usable_budget), 1),
        "calculation": (
            "移籍予算 − 確定済み支出 ＋ 売却見込み − 確保する予備費"
        ),
    }


def assess_deal(
    player: dict[str, Any],
    finance: dict[str, Any],
    *,
    user_budget_m: float,
) -> dict[str, Any]:
    """Score a transfer against both transfer and wage capacity.

    The score is deliberately transparent rather than predictive. It measures
    how much of the remaining transfer and wage capacity one deal consumes.
    """

    usable_budget = float(finance["usable_transfer_budget_m"])
    spending_limit = min(usable_budget, float(user_budget_m))
    wage_headroom = float(finance["annual_wage_headroom_m"])
    guideline = float(finance["max_single_fee_guideline_m"])
    fee = float(player["estimated_fee_m"])
    wage = float(player.get("estimated_annual_wage_m", 0.0))
    years = max(1, int(player.get("contract_years", finance["default_contract_years"])))
    annual_amortization = fee / years
    first_year_cost = annual_amortization + wage

    transfer_pressure = fee / max(spending_limit, 1.0)
    wage_pressure = wage / max(wage_headroom, 1.0)
    annual_capacity = spending_limit / years + wage_headroom
    annual_pressure = first_year_cost / max(annual_capacity, 1.0)
    guideline_overrun = max(0.0, fee - guideline) / max(guideline, 1.0)
    score = clamp(
        100
        - transfer_pressure * 32
        - wage_pressure * 25
        - annual_pressure * 18
        - guideline_overrun * 35,
        0,
        100,
    )

    affordable = fee <= spending_limit and wage <= wage_headroom
    if not affordable:
        status = "予算超過"
        rationale = "移籍金または年俸が、現在の補強余力を上回ります。"
    elif score >= 70:
        status = "余力あり"
        rationale = "獲得後も移籍予算と賃金枠を比較的多く残せます。"
    elif score >= 50:
        status = "許容範囲"
        rationale = "獲得可能ですが、後続の補強に使える余力は小さくなります。"
    else:
        status = "要交渉"
        rationale = "分割払い、売却収入、年俸条件の調整を前提に再検討が必要です。"

    return {
        "score": round(score, 1),
        "status": status,
        "affordable": affordable,
        "spending_limit_m": round(spending_limit, 1),
        "budget_after_deal_m": round(spending_limit - fee, 1),
        "annual_wage_m": round(wage, 1),
        "contract_years": years,
        "annual_amortization_m": round(annual_amortization, 1),
        "first_year_cost_m": round(first_year_cost, 1),
        "budget_share_pct": round(transfer_pressure * 100, 1),
        "wage_headroom_share_pct": round(wage_pressure * 100, 1),
        "rationale": rationale,
    }
