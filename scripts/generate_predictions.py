#!/usr/bin/env python3
"""
Premier League forecast generator.

This script generates data/predictions.json.
It reads:
- data/historical_10y.json
- data/team_features_2025_26.json
- optional openfootball current results

Rain is integrated into the overall prediction, not treated as a separate-only view.
"""

from __future__ import annotations

import json
import math
import random
import re
import statistics
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
HISTORICAL_PATH = ROOT / "data" / "historical_10y.json"
FEATURES_PATH = ROOT / "data" / "team_features_2025_26.json"
OUTPUT_PATH = ROOT / "data" / "predictions.json"
OPENFOOTBALL_URL = "https://raw.githubusercontent.com/openfootball/england/master/2025-26/1-premierleague.txt"
SIMULATIONS = 20000

FALLBACK_TABLE = [
    ("Arsenal", 56, 27, 16, 8, 3, 58, 24),
    ("Liverpool", 55, 27, 16, 7, 4, 59, 29),
    ("Manchester City", 53, 27, 15, 8, 4, 56, 27),
    ("Chelsea", 49, 27, 14, 7, 6, 51, 34),
    ("Newcastle United", 47, 27, 13, 8, 6, 49, 35),
    ("Tottenham Hotspur", 45, 27, 13, 6, 8, 50, 39),
    ("Aston Villa", 44, 27, 12, 8, 7, 45, 36),
    ("Manchester United", 42, 27, 12, 6, 9, 43, 38),
    ("Brighton", 39, 27, 10, 9, 8, 44, 41),
    ("Bournemouth", 38, 27, 10, 8, 9, 41, 40),
    ("Crystal Palace", 36, 27, 9, 9, 9, 35, 36),
    ("Fulham", 35, 27, 9, 8, 10, 37, 39),
    ("Brentford", 33, 27, 9, 6, 12, 38, 45),
    ("Everton", 32, 27, 8, 8, 11, 30, 37),
    ("West Ham United", 31, 27, 8, 7, 12, 34, 46),
    ("Nottingham Forest", 30, 27, 8, 6, 13, 32, 44),
    ("Wolves", 27, 27, 7, 6, 14, 29, 47),
    ("Leeds United", 25, 27, 6, 7, 14, 31, 51),
    ("Burnley", 22, 27, 5, 7, 15, 25, 53),
    ("Sunderland", 21, 27, 5, 6, 16, 24, 55),
]


def load_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def canonical(name: str) -> str:
    name = re.sub(r"\s+", " ", name.strip())
    name = name.replace("AFC Bournemouth", "Bournemouth")
    name = name.replace("Brighton & Hove Albion FC", "Brighton")
    name = name.replace("Wolverhampton Wanderers FC", "Wolves")
    name = re.sub(r" FC$", "", name)
    return name


def sort_table(row):
    return (row["points"], row["goals_for"] - row["goals_against"], row["goals_for"], row["team"])


def blank_team(name, features):
    f = features.get(name, {})
    return {
        "team": name,
        "short": f.get("short", name[:3].upper()),
        "points": 0,
        "played": 0,
        "wins": 0,
        "draws": 0,
        "losses": 0,
        "goals_for": 0,
        "goals_against": 0,
        **f,
    }


def parse_openfootball(text, features):
    table = {}
    fixtures = []
    matches = 0
    last_date = ""

    for line in text.splitlines():
        date_match = re.match(r"^\s*(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+([A-Z][a-z]{2}/\d{1,2}(?:\s+\d{4})?)", line)
        if date_match:
            last_date = date_match.group(1)

        result = re.match(r"^\s*(?:\d{1,2}\.\d{2}\s+)?(.+?)\s+v\s+(.+?)\s+(\d+)-(\d+)(?:\s|$)", line)
        if result:
            home = canonical(result.group(1))
            away = canonical(result.group(2))
            hg = int(result.group(3))
            ag = int(result.group(4))
            table.setdefault(home, blank_team(home, features))
            table.setdefault(away, blank_team(away, features))
            h = table[home]
            a = table[away]
            h["played"] += 1
            a["played"] += 1
            h["goals_for"] += hg
            h["goals_against"] += ag
            a["goals_for"] += ag
            a["goals_against"] += hg
            if hg > ag:
                h["wins"] += 1
                a["losses"] += 1
                h["points"] += 3
            elif hg < ag:
                a["wins"] += 1
                h["losses"] += 1
                a["points"] += 3
            else:
                h["draws"] += 1
                a["draws"] += 1
                h["points"] += 1
                a["points"] += 1
            matches += 1
            continue

        fixture = re.match(r"^\s*(?:\d{1,2}\.\d{2}\s+)?(.+?)\s+v\s+(.+?)\s*$", line)
        if fixture and "Matchday" not in line:
            home = canonical(fixture.group(1))
            away = canonical(fixture.group(2))
            if home and away and home != away:
                fixtures.append({"home": home, "away": away})

    return sorted(table.values(), key=sort_table, reverse=True), fixtures, matches, last_date


def fallback_table(features):
    rows = []
    for name, pts, played, wins, draws, losses, gf, ga in FALLBACK_TABLE:
        row = blank_team(name, features)
        row.update({"points": pts, "played": played, "wins": wins, "draws": draws, "losses": losses, "goals_for": gf, "goals_against": ga})
        rows.append(row)
    return sorted(rows, key=sort_table, reverse=True)


def get_current_table(features):
    try:
        with urllib.request.urlopen(OPENFOOTBALL_URL, timeout=15) as res:
            text = res.read().decode("utf-8")
        rows, fixtures, matches, last_date = parse_openfootball(text, features)
        if len(rows) < 20:
            raise RuntimeError("Could not parse 20 teams")
        return rows, fixtures, {"source": "openfootball", "matches": matches, "last_date": last_date}
    except Exception as exc:
        return fallback_table(features), [], {"source": "fallback", "error": str(exc), "matches": 0, "last_date": ""}


def avg_coefficients(historical):
    seasons = historical["seasons"]
    keys = seasons[0]["observed_coefficients"].keys()
    return {k: statistics.mean(s["observed_coefficients"][k] for s in seasons) for k in keys}


def rain_share(historical):
    recent = historical["seasons"][-3:]
    return statistics.mean(s["rain_match_share"] for s in recent)


def strength(team, coeff):
    return (
        team["elo"] * coeff["elo_weight"]
        + team["xg"] * coeff["xg_weight"]
        + team["defense"] * coeff["defense_weight"]
        + team["squad"] * coeff["squad_weight"]
        + team["form"] * coeff["form_weight"]
        + team["rain_skill"] * coeff["rain_skill_weight"]
        + team["injury_penalty"] * coeff["injury_penalty_weight"]
    )


def rain_adjustment(team):
    return (
        (team["rain_skill"] - 75) * 0.018
        + (team["defense"] - 75) * 0.006
        + (team["squad"] - 75) * 0.004
        - team["injury_penalty"] * 0.004
    )


def match_probability(home, away, coeff, wet):
    diff = strength(home, coeff) + 2.7 - strength(away, coeff)
    if wet:
        diff += (rain_adjustment(home) - rain_adjustment(away)) * 7
        draw = max(0.20, min(0.32, 0.29 - abs(diff) * 0.004))
    else:
        draw = max(0.18, min(0.29, 0.27 - abs(diff) * 0.004))
    home_non_draw = 1 / (1 + math.exp(-diff / 7.5))
    return (1 - draw) * home_non_draw, draw, (1 - draw) * (1 - home_non_draw)


def make_fallback_fixtures(rows):
    names = [r["team"] for r in rows]
    fixtures = []
    for r in rows:
        for _ in range(max(0, 38 - r["played"])):
            opponent = random.choice([n for n in names if n != r["team"]])
            fixtures.append({"home": r["team"], "away": opponent})
    return fixtures


def simulate(rows, fixtures, coeff, wet_share):
    random.seed(42)
    by_name = {r["team"]: r for r in rows}
    if not fixtures:
        fixtures = make_fallback_fixtures(rows)

    result = {r["team"]: {"rank_counts": [0] * 20, "points": [], "rain_points_added": []} for r in rows}

    for _ in range(SIMULATIONS):
        table = {r["team"]: {"points": r["points"], "gf": r["goals_for"], "ga": r["goals_against"], "rain_added": 0.0} for r in rows}
        for f in fixtures:
            home_name = f["home"]
            away_name = f["away"]
            if home_name not in by_name or away_name not in by_name:
                continue
            wet = random.random() < wet_share
            ph, pd, pa = match_probability(by_name[home_name], by_name[away_name], coeff, wet)
            x = random.random()
            if x < ph:
                table[home_name]["points"] += 3
                if wet:
                    table[home_name]["rain_added"] += 3
            elif x < ph + pd:
                table[home_name]["points"] += 1
                table[away_name]["points"] += 1
                if wet:
                    table[home_name]["rain_added"] += 1
                    table[away_name]["rain_added"] += 1
            else:
                table[away_name]["points"] += 3
                if wet:
                    table[away_name]["rain_added"] += 3

        ranked = sorted(table.items(), key=lambda kv: (kv[1]["points"], kv[1]["gf"] - kv[1]["ga"], kv[1]["gf"]), reverse=True)
        for idx, (team, row) in enumerate(ranked):
            result[team]["rank_counts"][idx] += 1
            result[team]["points"].append(row["points"])
            result[team]["rain_points_added"].append(row["rain_added"])

    standings = []
    for r in rows:
        team = r["team"]
        record = result[team]
        rank_probs = [c / SIMULATIONS * 100 for c in record["rank_counts"]]
        expected_rank = sum((i + 1) * p / 100 for i, p in enumerate(rank_probs))
        standings.append({
            **r,
            "strength": round(strength(r, coeff), 2),
            "rain_ppg_adjustment": round(rain_adjustment(r), 3),
            "expected_points": round(statistics.mean(record["points"]), 1),
            "expected_rank": round(expected_rank, 1),
            "title_probability": round(rank_probs[0], 1),
            "top4_probability": round(sum(rank_probs[:4]), 1),
            "top5_probability": round(sum(rank_probs[:5]), 1),
            "relegation_probability": round(sum(rank_probs[-3:]), 1),
            "rain_points_added": round(statistics.mean(record["rain_points_added"]), 2),
        })
    standings.sort(key=lambda x: (x["expected_rank"], -x["expected_points"]))
    return standings


def main():
    historical = load_json(HISTORICAL_PATH)
    features_data = load_json(FEATURES_PATH)
    features = {t["team"]: t for t in features_data["teams"]}
    coeff = avg_coefficients(historical)
    wet_share = rain_share(historical)
    current_rows, fixtures, source = get_current_table(features)
    standings = simulate(current_rows, fixtures, coeff, wet_share)
    output = {
        "meta": {
            "generated_by": "scripts/generate_predictions.py",
            "simulations": SIMULATIONS,
            "rain_match_share_used": round(wet_share, 3),
            "data_source": source,
            "historical_seasons": historical["meta"]["seasons_covered"],
            "model_note": "Rain is integrated into the overall forecast, not only shown as a separate tab.",
        },
        "standings": standings,
    }
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"created: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
