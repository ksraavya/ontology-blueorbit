from __future__ import annotations
import sys
sys.path.insert(0, '.')

import logging
import re
from collections import defaultdict
from itertools import combinations

from common.intelligence.aggregation import average, max_value
from common.intelligence.composite import weighted_score
from common.intelligence.normalization import clamp, min_max_normalize, normalize_by_max
from common.ontology import (
    AFFECTED_BY,
    CAUSED_DAMAGE,
    DEPENDS_ON_RESOURCE,
    DISRUPTS_SUPPLY_CHAIN,
    EMITS,
    EXPERIENCED,
    HAS_RESOURCE_STRESS,
    INCREASES_CONFLICT_RISK,
    IS_HIGH_RISK_FOR,
    RESULTED_IN_FATALITIES,
    get_relation_type,
)
from modules.climate.constants import (
    CLIMATE_EVENT_LABEL,
    COUNTRY_LABEL,
    EARTHQUAKE_RISK_WEIGHTS,
    EMISSIONS_PROFILE_LABEL,
    LIVE_WEATHER_LABEL,
    RISK_HIGH_THRESHOLD,
    RISK_MEDIUM_THRESHOLD,
    RISK_SCORE_MAP,
)

logger = logging.getLogger(__name__)

# Synthetic placeholder nodes that must never appear as real countries
SYNTHETIC_NODES: frozenset[str] = frozenset({
    "Global Forest Commons",
    "Global Supply Network",
})


# ── Helpers ───────────────────────────────────────────────────────────────────

def _event_name(disaster_type: str, country: str, year: int) -> str:
    raw = f"{disaster_type}_{country}_{year}"
    return re.sub(r"[^A-Za-z0-9_]", "_", raw).strip("_")


def _classify_risk(normalized_score: float) -> str:
    if normalized_score >= RISK_HIGH_THRESHOLD:
        return "High"
    if normalized_score >= RISK_MEDIUM_THRESHOLD:
        return "Medium"
    return "Low"


# ── Disaster relationships ────────────────────────────────────────────────────

def compute_disaster_relationships(rows: list[dict]) -> list[dict]:
    """
    From cleaned EM-DAT rows produce four relationship types per event:
        Country      -[EXPERIENCED]-----------> ClimateEvent
        ClimateEvent -[RESULTED_IN_FATALITIES]-> Country
        ClimateEvent -[CAUSED_DAMAGE]----------> Country
        Country      -[AFFECTED_BY]-----------> ClimateEvent

    All normalized_weight values are clamped to [0, 1].
    Rows where country, disaster_type, or year are missing are skipped.
    """
    if not rows:
        return []

    max_deaths   = max_value([float(r.get("deaths",     0.0)) for r in rows])
    max_damage   = max_value([float(r.get("damage_usd", 0.0)) for r in rows])
    max_affected = max_value([float(r.get("affected",   0.0)) for r in rows])

    out: list[dict] = []

    for row in rows:
        country      = row.get("country")
        year         = row.get("year")
        disaster_type = row.get("disaster_type")

        if not country or not disaster_type or year is None:
            continue
        if country in SYNTHETIC_NODES:
            continue

        event_name = _event_name(str(disaster_type), str(country), int(year))
        if not event_name:
            continue

        deaths     = float(row.get("deaths",     0.0))
        damage_usd = float(row.get("damage_usd", 0.0))
        affected   = float(row.get("affected",   0.0))

        norm_deaths   = clamp(normalize_by_max(deaths,     max_deaths))
        norm_damage   = clamp(normalize_by_max(damage_usd, max_damage))
        norm_affected = clamp(normalize_by_max(affected,   max_affected))

        event_risk = clamp(
            weighted_score(
                {"deaths": norm_deaths, "damage": norm_damage},
                {"deaths": 0.5,         "damage": 0.5},
            )
        )

        out.extend([
            # 1. Country -[EXPERIENCED]-> ClimateEvent
            {
                "source":       country,
                "target":       event_name,
                "rel":          EXPERIENCED,
                "source_label": COUNTRY_LABEL,
                "target_label": CLIMATE_EVENT_LABEL,
                "properties": {
                    "value":             1.0,
                    "normalized_weight": event_risk,
                    "year":              int(year),
                    "risk":              event_risk,
                    "disaster_type":     disaster_type,
                    "deaths":            deaths,
                    "relation_type":     get_relation_type(EXPERIENCED),
                },
            },
            # 2. ClimateEvent -[RESULTED_IN_FATALITIES]-> Country
            {
                "source":       event_name,
                "target":       country,
                "rel":          RESULTED_IN_FATALITIES,
                "source_label": CLIMATE_EVENT_LABEL,
                "target_label": COUNTRY_LABEL,
                "properties": {
                    "value":             deaths,
                    "normalized_weight": norm_deaths,
                    "year":              int(year),
                    "relation_type":     get_relation_type(RESULTED_IN_FATALITIES),
                },
            },
            # 3. ClimateEvent -[CAUSED_DAMAGE]-> Country
            {
                "source":       event_name,
                "target":       country,
                "rel":          CAUSED_DAMAGE,
                "source_label": CLIMATE_EVENT_LABEL,
                "target_label": COUNTRY_LABEL,
                "properties": {
                    "value":             damage_usd,
                    "normalized_weight": norm_damage,
                    "year":              int(year),
                    "relation_type":     get_relation_type(CAUSED_DAMAGE),
                },
            },
            # 4. Country -[AFFECTED_BY]-> ClimateEvent
            {
                "source":       country,
                "target":       event_name,
                "rel":          AFFECTED_BY,
                "source_label": COUNTRY_LABEL,
                "target_label": CLIMATE_EVENT_LABEL,
                "properties": {
                    "value":             affected,
                    "normalized_weight": norm_affected,
                    "year":              int(year),
                    "relation_type":     get_relation_type(AFFECTED_BY),
                },
            },
        ])

    logger.info(
        "compute_disaster_relationships: input=%d output=%d edges",
        len(rows), len(out),
    )
    return out


# ── Emissions relationships ───────────────────────────────────────────────────

def compute_emissions_relationships(emissions_rows: list[dict]) -> list[dict]:
    """
    From combined CO2 + forest rows produce:
        Country -[EMITS]-> EmissionsProfile

    And for heavily deforested countries (norm_forest < 0.1):
        Country -[DEPENDS_ON_RESOURCE]-> "Global Forest Commons"

    EmissionsProfile node name: f"EmissionsProfile_{country}_{year}"
    CO2 is normalized by the per-year maximum across all countries.
    Forest % (0–100) is normalized to [0, 1] by dividing by 100.
    normalized_weight = average of whichever normalized values are available.
    """
    if not emissions_rows:
        return []

    # Group by (country, year) and collect indicator values
    grouped: dict[tuple[str, int], dict[str, float]] = defaultdict(dict)
    for row in emissions_rows:
        country        = row.get("country")
        indicator_type = row.get("indicator_type")
        year           = row.get("year")
        value          = row.get("value")
        if not country or not indicator_type or year is None or value is None:
            continue
        if country in SYNTHETIC_NODES:
            continue
        grouped[(country, int(year))][indicator_type] = float(value)

    # Compute per-year CO2 maximum for normalization
    co2_max_by_year: dict[int, float] = {}
    for (_, year), vals in grouped.items():
        co2 = vals.get("co2")
        if co2 is not None:
            co2_max_by_year[year] = max(co2_max_by_year.get(year, 0.0), co2)

    out: list[dict] = []

    for (country, year), vals in grouped.items():
        node_name  = f"EmissionsProfile_{re.sub(r'[^A-Za-z0-9_]', '_', country)}_{year}"
        co2_pc     = vals.get("co2")
        forest_pct = vals.get("forest")

        norm_values: list[float] = []

        norm_co2 = None
        if co2_pc is not None:
            norm_co2 = clamp(normalize_by_max(co2_pc, co2_max_by_year.get(year, 0.0)))
            norm_values.append(norm_co2)

        norm_forest = None
        if forest_pct is not None:
            norm_forest = clamp(forest_pct / 100.0)
            norm_values.append(norm_forest)

        normalized_weight = clamp(average(norm_values)) if norm_values else 0.0

        # Country -[EMITS]-> EmissionsProfile
        out.append({
            "source":       country,
            "target":       node_name,
            "rel":          EMITS,
            "source_label": COUNTRY_LABEL,
            "target_label": EMISSIONS_PROFILE_LABEL,
            "properties": {
                "co2_pc":            co2_pc,
                "forest_pct":        forest_pct,
                "value":             0.0 if co2_pc is None else co2_pc,
                "normalized_weight": normalized_weight,
                "year":              year,
                "relation_type":     get_relation_type(EMITS),
            },
        })

        # Country -[DEPENDS_ON_RESOURCE]-> "Global Forest Commons"
        # only for countries with very low forest coverage
        if norm_forest is not None and norm_forest < 0.1:
            dep_score = clamp(1.0 - norm_forest)
            out.append({
                "source":       country,
                "target":       "Global Forest Commons",
                "rel":          DEPENDS_ON_RESOURCE,
                "source_label": COUNTRY_LABEL,
                "target_label": COUNTRY_LABEL,
                "properties": {
                    "value":             dep_score,
                    "normalized_weight": dep_score,
                    "year":              year,
                    "relation_type":     get_relation_type(DEPENDS_ON_RESOURCE),
                },
            })

    logger.info(
        "compute_emissions_relationships: input=%d output=%d edges",
        len(emissions_rows), len(out),
    )
    return out


# ── Temperature relationships ─────────────────────────────────────────────────

def compute_temperature_relationships(rows: list[dict]) -> list[dict]:
    """
    Produce Country -[HAS_RESOURCE_STRESS]-> LiveWeather edges.

    LiveWeather node name: f"LiveWeather_{country}"
    Temperature is min-max normalized across the dataset.
    Higher temperature → higher normalized stress score.
    Reference year is 2024 (NASA POWER climatology, no specific year).
    """
    if not rows:
        return []

    valid = [r for r in rows if r.get("country") and r.get("mean_temp_c") is not None
             and r["country"] not in SYNTHETIC_NODES]
    if not valid:
        return []

    temps   = [float(r["mean_temp_c"]) for r in valid]
    min_temp = min(temps)
    max_temp = max(temps)

    out: list[dict] = []

    for row in valid:
        country = row["country"]
        temp    = float(row["mean_temp_c"])
        norm_temp = clamp(min_max_normalize(temp, min_temp, max_temp))

        out.append({
            "source":       country,
            "target":       f"LiveWeather_{re.sub(r'[^A-Za-z0-9_]', '_', country)}",
            "rel":          HAS_RESOURCE_STRESS,
            "source_label": COUNTRY_LABEL,
            "target_label": LIVE_WEATHER_LABEL,
            "properties": {
                "warming":           norm_temp,
                "nasa_temp":         temp,
                "value":             temp,
                "normalized_weight": norm_temp,
                "year":              2024,
                "relation_type":     get_relation_type(HAS_RESOURCE_STRESS),
            },
        })

    logger.info(
        "compute_temperature_relationships: input=%d output=%d edges",
        len(rows), len(out),
    )
    return out


# ── Earthquake relationships ──────────────────────────────────────────────────

def compute_earthquake_relationships(rows: list[dict]) -> list[dict]:
    """
    From USGS aggregated earthquake rows produce:
        Country -[EXPERIENCED]-> ClimateEvent   (always)
        Country -[IS_HIGH_RISK_FOR]-> ClimateEvent  (only when risk >= RISK_HIGH_THRESHOLD)

    ClimateEvent node name: f"Earthquake_{country}_{year}"
    count and magnitude are normalized by their dataset-wide maximums.
    risk = weighted_score(count * 0.4 + magnitude * 0.6)
    """
    if not rows:
        return []

    valid = [r for r in rows
             if r.get("country") and r.get("year") is not None
             and r["country"] not in SYNTHETIC_NODES]
    if not valid:
        return []

    max_count = max_value([float(r.get("quake_count",    0.0)) for r in valid])
    max_mag   = max_value([float(r.get("max_magnitude",  0.0)) for r in valid])

    out: list[dict] = []

    for row in valid:
        country       = row["country"]
        year          = int(row["year"])
        count         = float(row.get("quake_count",   0.0))
        max_magnitude = float(row.get("max_magnitude", 0.0))

        event_name = _event_name("Earthquake", country, year)
        if not event_name:
            continue

        norm_count = clamp(normalize_by_max(count,         max_count))
        norm_mag   = clamp(normalize_by_max(max_magnitude, max_mag))
        risk       = clamp(
            weighted_score(
                {"count": norm_count, "magnitude": norm_mag},
                EARTHQUAKE_RISK_WEIGHTS,
            )
        )

        # Country -[EXPERIENCED]-> ClimateEvent
        out.append({
            "source":       country,
            "target":       event_name,
            "rel":          EXPERIENCED,
            "source_label": COUNTRY_LABEL,
            "target_label": CLIMATE_EVENT_LABEL,
            "properties": {
                "value":             count,
                "normalized_weight": norm_count,
                "year":              year,
                "risk":              risk,
                "disaster_type":     "Earthquake",
                "max_magnitude":     max_magnitude,
                "relation_type":     get_relation_type(EXPERIENCED),
            },
        })

        # Country -[IS_HIGH_RISK_FOR]-> ClimateEvent  (high risk only)
        if risk >= RISK_HIGH_THRESHOLD:
            out.append({
                "source":       country,
                "target":       event_name,
                "rel":          IS_HIGH_RISK_FOR,
                "source_label": COUNTRY_LABEL,
                "target_label": CLIMATE_EVENT_LABEL,
                "properties": {
                    "value":             risk,
                    "normalized_weight": risk,
                    "year":              year,
                    "relation_type":     get_relation_type(IS_HIGH_RISK_FOR),
                },
            })

    logger.info(
        "compute_earthquake_relationships: input=%d output=%d edges",
        len(rows), len(out),
    )
    return out


# ── Hazard risk (derived from EM-DAT + USGS) ─────────────────────────────────

def compute_hazard_risk(
    disaster_rows: list[dict],
    earthquake_rows: list[dict],
) -> list[dict]:
    """
    Derive hazard risk classifications entirely from ingested data.
    No hardcoded table, no external API.

    Step 1 — accumulate raw event counts per country per hazard type from EM-DAT:
                flood_count, drought_count, cyclone_count
              accumulate normalized risk scores from USGS per country:
                quake_score  (uses dataset-wide max for normalization)

    Step 2 — normalize each count by its dataset-wide maximum.

    Step 3 — classify each (country, hazard) using _classify_risk().

    Step 4 — emit edges:
        a. Country -[IS_HIGH_RISK_FOR]-> ClimateEvent
           for High + Medium classifications
        b. Country -[INCREASES_CONFLICT_RISK]-> Country
           top-50 pairs from countries with 2+ High hazards
        c. Country -[DISRUPTS_SUPPLY_CHAIN]-> "Global Supply Network"
           for countries with High flood OR High cyclone risk
    """
    # ── Step 1: accumulate ────────────────────────────────────────────────────
    flood_count:   dict[str, float] = defaultdict(float)
    drought_count: dict[str, float] = defaultdict(float)
    cyclone_count: dict[str, float] = defaultdict(float)

    for row in disaster_rows:
        country = row.get("country")
        dtype   = row.get("disaster_type")
        if not country or country in SYNTHETIC_NODES:
            continue
        if dtype == "Flood":
            flood_count[country]   += 1.0
        elif dtype == "Drought":
            drought_count[country] += 1.0
        elif dtype == "Storm":
            cyclone_count[country] += 1.0

    # Earthquake risk: normalize using dataset-wide maxes (fix for incorrect
    # hardcoded denominators in the original version)
    quake_scores_raw: dict[str, float] = defaultdict(float)
    if earthquake_rows:
        valid_eq   = [r for r in earthquake_rows if r.get("country")
                      and r["country"] not in SYNTHETIC_NODES]
        max_count_eq = max_value([float(r.get("quake_count",   0.0)) for r in valid_eq])
        max_mag_eq   = max_value([float(r.get("max_magnitude", 0.0)) for r in valid_eq])

        for row in valid_eq:
            country = row["country"]
            norm_count = clamp(normalize_by_max(float(row.get("quake_count",   0.0)), max_count_eq))
            norm_mag   = clamp(normalize_by_max(float(row.get("max_magnitude", 0.0)), max_mag_eq))
            risk_score = clamp(
                weighted_score(
                    {"count": norm_count, "magnitude": norm_mag},
                    EARTHQUAKE_RISK_WEIGHTS,
                )
            )
            # keep the highest risk score seen across years for this country
            quake_scores_raw[country] = max(quake_scores_raw[country], risk_score)

    # ── Step 2: normalize event counts ───────────────────────────────────────
    hazard_maps: dict[str, dict[str, float]] = {
        "Flood":      flood_count,
        "Drought":    drought_count,
        "Cyclone":    cyclone_count,
        "Earthquake": quake_scores_raw,   # already normalized [0,1]
    }

    # Union of all real countries seen across any hazard source
    all_countries: set[str] = (
        set().union(*[set(m.keys()) for m in hazard_maps.values()])
        - SYNTHETIC_NODES
    )

    # For Flood/Drought/Cyclone normalize by max count;
    # Earthquake scores are already in [0,1] so max should be ≤ 1.
    norm_scores:     dict[tuple[str, str], float] = {}
    classifications: dict[tuple[str, str], str]   = {}

    for hazard, cmap in hazard_maps.items():
        max_val = max_value(list(cmap.values())) if cmap else 0.0
        for country in all_countries:
            raw   = cmap.get(country, 0.0)
            score = clamp(normalize_by_max(raw, max_val)) if max_val > 0 else 0.0
            norm_scores[(country, hazard)]     = score
            classifications[(country, hazard)] = _classify_risk(score)

    # ── Step 3: emit IS_HIGH_RISK_FOR edges ───────────────────────────────────
    out: list[dict] = []

    # Track cumulative High-hazard score per country for conflict-risk pairs
    high_hazard_totals: dict[str, float] = defaultdict(float)
    high_hazard_counts: dict[str, int]   = defaultdict(int)

    for (country, hazard), score in norm_scores.items():
        level = classifications[(country, hazard)]
        if level not in {"High", "Medium"}:
            continue

        risk_node_name = f"{hazard}Risk_{re.sub(r'[^A-Za-z0-9_]', '_', country)}"

        out.append({
            "source":       country,
            "target":       risk_node_name,
            "rel":          IS_HIGH_RISK_FOR,
            "source_label": COUNTRY_LABEL,
            "target_label": CLIMATE_EVENT_LABEL,
            "properties": {
                "value":             RISK_SCORE_MAP[level],
                "normalized_weight": score,
                "year":              2024,
                "hazard_type":       hazard,
                "risk_level":        level,
                "relation_type":     get_relation_type(IS_HIGH_RISK_FOR),
            },
        })

        if level == "High":
            high_hazard_totals[country] += score
            high_hazard_counts[country] += 1

    # ── Step 4b: INCREASES_CONFLICT_RISK (Country → Country) ─────────────────
    # Only countries with ≥ 2 distinct High hazards qualify
    multi_high_risk = [
        c for c, cnt in high_hazard_counts.items() if cnt >= 2
    ]

    pair_scores: list[tuple[float, str, str]] = []
    for country_a, country_b in combinations(multi_high_risk, 2):
        combined = clamp(
            (high_hazard_totals[country_a] + high_hazard_totals[country_b]) / 2.0
        )
        pair_scores.append((combined, country_a, country_b))

    pair_scores.sort(key=lambda x: x[0], reverse=True)

    for score, country_a, country_b in pair_scores[:50]:
        out.append({
            "source":       country_a,
            "target":       country_b,
            "rel":          INCREASES_CONFLICT_RISK,
            "source_label": COUNTRY_LABEL,
            "target_label": COUNTRY_LABEL,
            "properties": {
                "value":             score,
                "normalized_weight": score,
                "year":              2024,
                "relation_type":     get_relation_type(INCREASES_CONFLICT_RISK),
            },
        })

    # ── Step 4c: DISRUPTS_SUPPLY_CHAIN (Country → synthetic node) ─────────────
    for country in all_countries:
        flood_score   = norm_scores.get((country, "Flood"),   0.0)
        cyclone_score = norm_scores.get((country, "Cyclone"), 0.0)
        flood_level   = classifications.get((country, "Flood"),   "Low")
        cyclone_level = classifications.get((country, "Cyclone"), "Low")

        if flood_level == "High" or cyclone_level == "High":
            score = clamp(max(flood_score, cyclone_score))
            out.append({
                "source":       country,
                "target":       "Global Supply Network",
                "rel":          DISRUPTS_SUPPLY_CHAIN,
                "source_label": COUNTRY_LABEL,
                "target_label": COUNTRY_LABEL,
                "properties": {
                    "value":             score,
                    "normalized_weight": score,
                    "year":              2024,
                    "relation_type":     get_relation_type(DISRUPTS_SUPPLY_CHAIN),
                },
            })

    logger.info(
        "compute_hazard_risk: countries=%d is_high_risk=%d "
        "conflict_pairs=%d supply_chain=%d total_edges=%d",
        len(all_countries),
        sum(1 for e in out if e["rel"] == IS_HIGH_RISK_FOR),
        sum(1 for e in out if e["rel"] == INCREASES_CONFLICT_RISK),
        sum(1 for e in out if e["rel"] == DISRUPTS_SUPPLY_CHAIN),
        len(out),
    )
    return out


# ── Orchestrator ──────────────────────────────────────────────────────────────

def compute_all(data: dict[str, list[dict]]) -> dict[str, list[dict]]:
    """
    Run all compute functions in dependency order.

    Args:
        data: output of transform_all() with keys:
              disasters, emissions, temperature, earthquakes

    Returns dict with keys:
        disasters, emissions, temperature, earthquakes, hazard
    """
    disasters   = compute_disaster_relationships(data.get("disasters",   []))
    emissions   = compute_emissions_relationships(data.get("emissions",  []))
    temperature = compute_temperature_relationships(data.get("temperature", []))
    earthquakes = compute_earthquake_relationships(data.get("earthquakes", []))

    # hazard_risk reads raw transformed rows (not the computed edges above)
    # so it can derive risk classifications from event counts and quake scores
    hazard = compute_hazard_risk(
        disaster_rows=data.get("disasters",   []),
        earthquake_rows=data.get("earthquakes", []),
    )

    result = {
        "disasters":   disasters,
        "emissions":   emissions,
        "temperature": temperature,
        "earthquakes": earthquakes,
        "hazard":      hazard,
    }

    total_edges = sum(len(v) for v in result.values())
    logger.info(
        "compute_all complete: disasters=%d emissions=%d temperature=%d "
        "earthquakes=%d hazard=%d total=%d",
        len(disasters), len(emissions), len(temperature),
        len(earthquakes), len(hazard), total_edges,
    )
    return result