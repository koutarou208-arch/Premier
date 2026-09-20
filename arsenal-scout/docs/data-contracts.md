# Data contracts

## Match features

The engine expects one row per team-match. Every metric must exist in `semantic_metrics.json`.

```json
{
  "id": "provider-match-id",
  "date": "2026-08-15",
  "opponent": "Opponent canonical name",
  "venue": "H",
  "opponent_shape": "low_block",
  "result": "2-1",
  "metrics": {
    "transition_xga": 0.24,
    "right_final_third_entries": 21.0
  }
}
```

`opponent_shape` should be produced by a documented classifier, not manually changed to make a narrative fit. Store the classifier version and probability in production.

## Player market profiles

The prototype consumes 0–100 positional percentiles. Production ingestion should preserve raw values, minutes, competition strength, age curve, source timestamp and provider license alongside the normalized value.

```json
{
  "id": "provider-player-id",
  "name": "Player name",
  "club": "Current club",
  "league": "Competition",
  "age": 23,
  "estimated_fee_m": 55,
  "estimated_annual_wage_m": 6.5,
  "contract_years": 5,
  "availability": 88,
  "roles": ["right_progressor"],
  "metrics": {
    "right_progression": 92,
    "progressive_passes": 86,
    "retention": 81
  }
}
```

## Club finance snapshot

The local finance model expects a season-level planning snapshot. Values are in millions of the declared currency.

```json
{
  "club": "Arsenal",
  "transfer_budget_m": 95,
  "committed_transfer_spend_m": 26,
  "expected_sales_m": 14,
  "protected_cash_reserve_m": 12,
  "annual_wage_headroom_m": 17,
  "max_single_fee_guideline_m": 60,
  "default_contract_years": 5
}
```

`usable_transfer_budget_m` is derived rather than entered directly. Production finance adapters should preserve the reporting period, currency, source document, accounting policy and approval status. PSR/FFP headroom must come from a separately reviewed rules engine; the prototype does not infer regulatory compliance.

## Provider boundary

Implement provider adapters outside the domain layer. A provider adapter may map StatsBomb/Wyscout/SkillCorner/Opta fields into canonical IDs, but provider-specific field names must not leak into ranking code. Respect feed licenses; scraping websites that prohibit it is not a data strategy.
