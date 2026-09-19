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
  "availability": 88,
  "roles": ["right_progressor"],
  "metrics": {
    "right_progression": 92,
    "progressive_passes": 86,
    "retention": 81
  }
}
```

## Provider boundary

Implement provider adapters outside the domain layer. A provider adapter may map StatsBomb/Wyscout/SkillCorner/Opta fields into canonical IDs, but provider-specific field names must not leak into ranking code. Respect feed licenses; scraping websites that prohibit it is not a data strategy.
