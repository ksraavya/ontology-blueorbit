from __future__ import annotations
import sys
sys.path.insert(0, '.')

import logging
from typing import Any

from common.db import Neo4jConnection

logger = logging.getLogger(__name__)


# =========================================================
# HELPERS
# =========================================================

def _safe(value: Any, default: Any = None) -> Any:
    return value if value is not None else default


def _run(query: str, params: dict | None = None) -> list[dict]:
    conn = Neo4jConnection()
    try:
        return conn.run_query(query, params or {})
    finally:
        conn.close()


# =========================================================
# COUNTRY CLIMATE PROFILE
# =========================================================

def get_country_climate_profile(
    country: str,
    year: int = 2024,
) -> dict:
    """
    Full climate intelligence picture for a single country.
    Combines all climate scores, emissions, temperature stress,
    disaster history summary, hazard risks, and supply chain impact.
    """
    scores_rows = _run(
        """
        MATCH (c:Country {name: $name})
        RETURN
            c.climate_risk_score                AS climate_risk_score,
            c.climate_vulnerability_score       AS climate_vulnerability_score,
            c.disaster_frequency_score          AS disaster_frequency_score,
            c.disaster_exposure_score           AS disaster_exposure_score,
            c.climate_damage_score              AS climate_damage_score,
            c.climate_deaths_score              AS climate_deaths_score,
            c.supply_chain_risk_score           AS supply_chain_risk_score,
            c.emissions_score                   AS emissions_score,
            c.resource_stress_score             AS resource_stress_score
        """,
        {"name": country},
    )
    if not scores_rows:
        return {}

    scores = scores_rows[0]

    # Emissions profile (latest available year)
    emissions_rows = _run(
        """
        MATCH (c:Country {name: $name})-[r:EMITS]->(e:EmissionsProfile)
        WITH r ORDER BY r.year DESC
        RETURN
            r.year          AS year,
            r.co2_pc        AS co2_per_capita,
            r.forest_pct    AS forest_pct,
            r.normalized_weight AS emissions_score
        LIMIT 5
        """,
        {"name": country},
    )

    # Temperature stress
    temp_rows = _run(
        """
        MATCH (c:Country {name: $name})-[r:HAS_RESOURCE_STRESS]->(w:LiveWeather)
        RETURN
            r.nasa_temp     AS mean_temp_celsius,
            r.warming       AS warming_stress_score
        """,
        {"name": country},
    )
    temp = temp_rows[0] if temp_rows else {}

    # Disaster summary (all time)
    disaster_summary = _run(
        """
        MATCH (c:Country {name: $name})-[r:EXPERIENCED]->(e:ClimateEvent)
        RETURN
            r.disaster_type                     AS disaster_type,
            count(e)                            AS event_count,
            toInteger(sum(r.deaths))            AS total_deaths,
            round(avg(r.risk), 4)               AS avg_risk
        ORDER BY total_deaths DESC
        """,
        {"name": country},
    )

    # Hazard risk classifications
    hazard_risks = _run(
        """
        MATCH (c:Country {name: $name})-[r:IS_HIGH_RISK_FOR]->(e:ClimateEvent)
        RETURN
            r.hazard_type       AS hazard_type,
            r.risk_level        AS risk_level,
            round(r.value, 4)   AS risk_score
        ORDER BY r.value DESC
        """,
        {"name": country},
    )

    # Supply chain disruption targets
    supply_chain_rows = _run(
        """
        MATCH (c:Country {name: $name})-[r:DISRUPTS_SUPPLY_CHAIN]->(target:Country)
        RETURN
            target.name             AS disrupted_country,
            round(r.value, 4)       AS disruption_score,
            r.source_event          AS trigger_event,
            r.confidence            AS confidence
        ORDER BY r.value DESC
        LIMIT 10
        """,
        {"name": country},
    )

    # Conflict risk propagation
    conflict_rows = _run(
        """
        MATCH (c:Country {name: $name})-[r:INCREASES_CONFLICT_RISK]->(target:Country)
        RETURN
            target.name             AS at_risk_country,
            round(r.value, 4)       AS conflict_score,
            r.source_event          AS trigger_event
        ORDER BY r.value DESC
        LIMIT 10
        """,
        {"name": country},
    )

    # Economy affected
    economy_rows = _run(
        """
        MATCH (c:Country {name: $name})-[r:AFFECTS_ECONOMY]->(c2:Country)
        RETURN
            c2.name             AS economy_country,
            round(r.value, 4)   AS impact_score,
            r.mechanism         AS mechanism
        ORDER BY r.value DESC
        LIMIT 5
        """,
        {"name": country},
    )

    return {
        "country": country,
        "year":    year,
        "scores": {
            "climate_risk":          _safe(scores.get("climate_risk_score")),
            "climate_vulnerability": _safe(scores.get("climate_vulnerability_score")),
            "disaster_frequency":    _safe(scores.get("disaster_frequency_score")),
            "disaster_exposure":     _safe(scores.get("disaster_exposure_score")),
            "climate_damage":        _safe(scores.get("climate_damage_score")),
            "climate_deaths":        _safe(scores.get("climate_deaths_score")),
            "supply_chain_risk":     _safe(scores.get("supply_chain_risk_score")),
            "emissions_score":       _safe(scores.get("emissions_score")),
            "resource_stress":       _safe(scores.get("resource_stress_score")),
        },
        "temperature": {
            "mean_temp_celsius":   _safe(temp.get("mean_temp_celsius")),
            "warming_stress_score": _safe(temp.get("warming_stress_score")),
        },
        "emissions_history":    emissions_rows,
        "disaster_summary":     disaster_summary,
        "hazard_risks":         hazard_risks,
        "supply_chain_impact":  supply_chain_rows,
        "conflict_risk_impact": conflict_rows,
        "economy_impact":       economy_rows,
    }


# =========================================================
# DISASTER HISTORY
# =========================================================

def get_disaster_history(
    country: str,
    start_year: int = 2000,
    end_year: int = 2024,
    disaster_type: str | None = None,
) -> list[dict]:
    """
    Disaster history for a country across a year range.
    Optionally filter by disaster type.
    """
    if disaster_type:
        return _run(
            """
            MATCH (c:Country {name: $name})-[r:EXPERIENCED]->(e:ClimateEvent)
            WHERE e.year >= $start AND e.year <= $end
              AND r.disaster_type = $dtype
            RETURN
                e.name                  AS event,
                e.year                  AS year,
                r.disaster_type         AS disaster_type,
                toInteger(r.deaths)     AS deaths,
                round(r.risk, 4)        AS risk_score
            ORDER BY e.year DESC, r.deaths DESC
            """,
            {"name": country, "start": start_year,
             "end": end_year, "dtype": disaster_type},
        )
    return _run(
        """
        MATCH (c:Country {name: $name})-[r:EXPERIENCED]->(e:ClimateEvent)
        WHERE e.year >= $start AND e.year <= $end
        RETURN
            e.name                  AS event,
            e.year                  AS year,
            r.disaster_type         AS disaster_type,
            toInteger(r.deaths)     AS deaths,
            round(r.risk, 4)        AS risk_score
        ORDER BY e.year DESC, r.deaths DESC
        """,
        {"name": country, "start": start_year, "end": end_year},
    )


def get_disaster_fatalities(
    country: str,
    start_year: int = 2000,
    end_year: int = 2024,
) -> list[dict]:
    """
    Fatalities from climate events for a country across years.
    """
    return _run(
        """
        MATCH (e:ClimateEvent)-[r:RESULTED_IN_FATALITIES]->(c:Country {name: $name})
        WHERE e.year >= $start AND e.year <= $end
        RETURN
            e.name                      AS event,
            e.year                      AS year,
            toInteger(r.value)          AS deaths,
            round(r.normalized_weight, 4) AS severity_score
        ORDER BY r.value DESC
        """,
        {"name": country, "start": start_year, "end": end_year},
    )


def get_disaster_damage(
    country: str,
    start_year: int = 2000,
    end_year: int = 2024,
) -> list[dict]:
    """
    Economic damage from climate events for a country.
    """
    return _run(
        """
        MATCH (e:ClimateEvent)-[r:CAUSED_DAMAGE]->(c:Country {name: $name})
        WHERE e.year >= $start AND e.year <= $end
        RETURN
            e.name                          AS event,
            e.year                          AS year,
            round(r.value / 1000000, 2)     AS damage_million_usd,
            round(r.normalized_weight, 4)   AS damage_score
        ORDER BY r.value DESC
        """,
        {"name": country, "start": start_year, "end": end_year},
    )


def get_people_affected(
    country: str,
    start_year: int = 2000,
    end_year: int = 2024,
) -> list[dict]:
    """
    People affected by climate events for a country.
    """
    return _run(
        """
        MATCH (c:Country {name: $name})-[r:AFFECTED_BY]->(e:ClimateEvent)
        WHERE e.year >= $start AND e.year <= $end
        RETURN
            e.name                      AS event,
            e.year                      AS year,
            r.disaster_type             AS disaster_type,
            toInteger(r.value)          AS people_affected,
            round(r.normalized_weight, 4) AS severity_score
        ORDER BY r.value DESC
        """,
        {"name": country, "start": start_year, "end": end_year},
    )


# =========================================================
# EMISSIONS
# =========================================================

def get_emissions_trend(
    country: str,
    start_year: int = 2014,
    end_year: int = 2024,
) -> list[dict]:
    """
    CO2 emissions and forest coverage trend for a country.
    """
    return _run(
        """
        MATCH (c:Country {name: $name})-[r:EMITS]->(e:EmissionsProfile)
        WHERE r.year >= $start AND r.year <= $end
        RETURN
            r.year                          AS year,
            round(r.co2_pc, 4)              AS co2_per_capita_tons,
            round(r.forest_pct, 2)          AS forest_pct,
            round(r.normalized_weight, 4)   AS emissions_score
        ORDER BY r.year ASC
        """,
        {"name": country, "start": start_year, "end": end_year},
    )


def get_top_emitters(limit: int = 20) -> list[dict]:
    """
    Countries ranked by CO2 per capita (latest year).
    """
    return _run(
        """
        MATCH (c:Country)-[r:EMITS]->(e:EmissionsProfile)
        WHERE r.co2_pc IS NOT NULL
        WITH c, r ORDER BY r.year DESC
        WITH c, collect(r)[0] AS latest
        RETURN
            c.name                          AS country,
            latest.year                     AS year,
            round(latest.co2_pc, 3)         AS co2_per_capita_tons,
            round(latest.forest_pct, 2)     AS forest_pct,
            round(latest.normalized_weight, 4) AS emissions_score
        ORDER BY latest.co2_pc DESC
        LIMIT $limit
        """,
        {"limit": limit},
    )


def get_most_deforested(limit: int = 20) -> list[dict]:
    """
    Countries with lowest forest coverage (latest year).
    """
    return _run(
        """
        MATCH (c:Country)-[r:EMITS]->(e:EmissionsProfile)
        WHERE r.forest_pct IS NOT NULL
        WITH c, r ORDER BY r.year DESC
        WITH c, collect(r)[0] AS latest
        WHERE latest.forest_pct IS NOT NULL
        RETURN
            c.name                      AS country,
            latest.year                 AS year,
            round(latest.forest_pct, 2) AS forest_pct,
            round(latest.co2_pc, 3)     AS co2_per_capita
        ORDER BY latest.forest_pct ASC
        LIMIT $limit
        """,
        {"limit": limit},
    )


def get_emissions_comparison(
    countries: list[str],
    year: int = 2024,
) -> list[dict]:
    """
    Emissions comparison across multiple countries for a given year.
    """
    return _run(
        """
        MATCH (c:Country)-[r:EMITS]->(e:EmissionsProfile)
        WHERE c.name IN $countries AND r.year = $year
        RETURN
            c.name                  AS country,
            round(r.co2_pc, 3)      AS co2_per_capita,
            round(r.forest_pct, 2)  AS forest_pct
        ORDER BY r.co2_pc DESC
        """,
        {"countries": countries, "year": year},
    )


# =========================================================
# TEMPERATURE
# =========================================================

def get_temperature_ranking(
    order: str = "hottest",
    limit: int = 20,
) -> list[dict]:
    """
    Countries ranked by mean temperature.
    order=hottest → highest temp first.
    order=coldest → lowest temp first.
    """
    direction = "DESC" if order == "hottest" else "ASC"
    return _run(
        f"""
        MATCH (c:Country)-[r:HAS_RESOURCE_STRESS]->(w:LiveWeather)
        RETURN
            c.name                  AS country,
            round(r.nasa_temp, 2)   AS mean_temp_celsius,
            round(r.warming, 4)     AS warming_stress_score
        ORDER BY r.nasa_temp {direction}
        LIMIT $limit
        """,
        {"limit": limit},
    )


# =========================================================
# HAZARD RISK
# =========================================================

def get_hazard_risk(
    country: str,
) -> list[dict]:
    """
    All hazard risk classifications for a country.
    """
    return _run(
        """
        MATCH (c:Country {name: $name})-[r:IS_HIGH_RISK_FOR]->(e:ClimateEvent)
        RETURN
            e.name                  AS risk_category,
            r.hazard_type           AS hazard_type,
            r.risk_level            AS risk_level,
            round(r.value, 4)       AS risk_score
        ORDER BY r.value DESC
        """,
        {"name": country},
    )


def get_countries_by_hazard(
    hazard_type: str,
    risk_level: str = "High",
    limit: int = 30,
) -> list[dict]:
    """
    Countries at given risk level for a specific hazard type.
    hazard_type: Flood | Drought | Cyclone | Earthquake
    risk_level:  High | Medium | Low
    """
    return _run(
        """
        MATCH (c:Country)-[r:IS_HIGH_RISK_FOR]->(e:ClimateEvent)
        WHERE r.hazard_type = $hazard
          AND r.risk_level = $level
        RETURN
            c.name                  AS country,
            r.risk_level            AS risk_level,
            round(r.value, 4)       AS risk_score
        ORDER BY r.value DESC
        LIMIT $limit
        """,
        {"hazard": hazard_type, "level": risk_level, "limit": limit},
    )


def get_multi_hazard_countries(
    min_high_hazards: int = 2,
    limit: int = 20,
) -> list[dict]:
    """
    Countries with multiple High-level hazard risks.
    """
    return _run(
        """
        MATCH (c:Country)-[r:IS_HIGH_RISK_FOR]->(e:ClimateEvent)
        WHERE r.risk_level = "High"
        RETURN
            c.name                              AS country,
            count(DISTINCT r.hazard_type)       AS high_risk_count,
            collect(DISTINCT r.hazard_type)     AS hazard_types,
            round(avg(r.value), 4)              AS avg_risk_score
        HAVING high_risk_count >= $min_hazards
        ORDER BY high_risk_count DESC, avg_risk_score DESC
        LIMIT $limit
        """,
        {"min_hazards": min_high_hazards, "limit": limit},
    )


# =========================================================
# EARTHQUAKE SPECIFIC
# =========================================================

def get_earthquake_history(
    country: str,
) -> list[dict]:
    """
    USGS earthquake data for a country by year.
    """
    return _run(
        """
        MATCH (c:Country {name: $name})-[r:EXPERIENCED]->(e:ClimateEvent)
        WHERE r.disaster_type = "Earthquake"
        RETURN
            e.year                          AS year,
            toInteger(r.value)              AS quake_count,
            round(r.max_magnitude, 2)       AS max_magnitude,
            round(r.risk, 4)                AS risk_score
        ORDER BY e.year DESC
        """,
        {"name": country},
    )


def get_highest_seismic_risk(limit: int = 20) -> list[dict]:
    """
    Countries with highest earthquake risk scores.
    """
    return _run(
        """
        MATCH (c:Country)-[r:IS_HIGH_RISK_FOR]->(e:ClimateEvent)
        WHERE r.hazard_type = "Earthquake"
          AND r.risk_level = "High"
        RETURN
            c.name                  AS country,
            round(r.value, 4)       AS risk_score
        ORDER BY r.value DESC
        LIMIT $limit
        """,
        {"limit": limit},
    )


# =========================================================
# RANKINGS
# =========================================================

def get_climate_risk_ranking(limit: int = 20) -> list[dict]:
    """
    Countries ranked by overall climate risk score.
    """
    return _run(
        """
        MATCH (c:Country)
        WHERE c.climate_risk_score IS NOT NULL
        RETURN
            c.name                                          AS country,
            round(c.climate_risk_score, 4)                 AS climate_risk,
            round(c.disaster_frequency_score, 4)           AS frequency,
            round(c.climate_damage_score, 4)               AS damage,
            round(c.climate_deaths_score, 4)               AS deaths_score,
            round(coalesce(c.supply_chain_risk_score, 0), 4) AS supply_chain_risk
        ORDER BY c.climate_risk_score DESC
        LIMIT $limit
        """,
        {"limit": limit},
    )


def get_most_disaster_prone(limit: int = 20) -> list[dict]:
    """
    Countries with highest disaster frequency score.
    """
    return _run(
        """
        MATCH (c:Country)
        WHERE c.disaster_frequency_score IS NOT NULL
        RETURN
            c.name                                  AS country,
            round(c.disaster_frequency_score, 4)   AS frequency_score,
            round(c.climate_risk_score, 4)          AS climate_risk
        ORDER BY c.disaster_frequency_score DESC
        LIMIT $limit
        """,
        {"limit": limit},
    )


def get_deadliest_events(
    limit: int = 20,
    start_year: int = 2000,
    end_year: int = 2024,
) -> list[dict]:
    """
    Deadliest climate events globally across a year range.
    """
    return _run(
        """
        MATCH (c:Country)-[r:EXPERIENCED]->(e:ClimateEvent)
        WHERE r.deaths > 0
          AND e.year >= $start AND e.year <= $end
        RETURN
            c.name                  AS country,
            e.name                  AS event,
            e.year                  AS year,
            r.disaster_type         AS disaster_type,
            toInteger(r.deaths)     AS deaths,
            round(r.risk, 4)        AS risk_score
        ORDER BY r.deaths DESC
        LIMIT $limit
        """,
        {"limit": limit, "start": start_year, "end": end_year},
    )


# =========================================================
# COUNTRY → COUNTRY IMPACT
# =========================================================

def get_supply_chain_disruptions(
    country: str | None = None,
    limit: int = 20,
) -> list[dict]:
    """
    Supply chain disruption edges.
    If country provided, filter to that country as source.
    """
    if country:
        return _run(
            """
            MATCH (a:Country {name: $name})-[r:DISRUPTS_SUPPLY_CHAIN]->(b:Country)
            RETURN
                a.name                  AS source_country,
                b.name                  AS disrupted_country,
                round(r.value, 4)       AS disruption_score,
                r.source_event          AS trigger_event,
                r.disaster_type         AS disaster_type,
                r.confidence            AS confidence,
                r.year                  AS year
            ORDER BY r.value DESC
            LIMIT $limit
            """,
            {"name": country, "limit": limit},
        )
    return _run(
        """
        MATCH (a:Country)-[r:DISRUPTS_SUPPLY_CHAIN]->(b:Country)
        WHERE r.source_event IS NOT NULL
        RETURN
            a.name                  AS source_country,
            b.name                  AS disrupted_country,
            round(r.value, 4)       AS disruption_score,
            r.source_event          AS trigger_event,
            r.disaster_type         AS disaster_type,
            r.confidence            AS confidence
        ORDER BY r.value DESC
        LIMIT $limit
        """,
        {"limit": limit},
    )


def get_conflict_risk_propagation(
    country: str | None = None,
    limit: int = 20,
) -> list[dict]:
    """
    Conflict risk propagation edges from climate events.
    """
    if country:
        return _run(
            """
            MATCH (a:Country {name: $name})-[r:INCREASES_CONFLICT_RISK]->(b:Country)
            RETURN
                a.name              AS source_country,
                b.name              AS at_risk_country,
                round(r.value, 4)   AS conflict_score,
                r.source_event      AS trigger_event,
                r.disaster_type     AS disaster_type
            ORDER BY r.value DESC
            LIMIT $limit
            """,
            {"name": country, "limit": limit},
        )
    return _run(
        """
        MATCH (a:Country)-[r:INCREASES_CONFLICT_RISK]->(b:Country)
        WHERE r.source_event IS NOT NULL
        RETURN
            a.name              AS source_country,
            b.name              AS at_risk_country,
            round(r.value, 4)   AS conflict_score,
            r.source_event      AS trigger_event,
            r.disaster_type     AS disaster_type
        ORDER BY r.value DESC
        LIMIT $limit
        """,
        {"limit": limit},
    )


def get_resource_dependencies(
    country: str | None = None,
    limit: int = 20,
) -> list[dict]:
    """
    Resource dependency edges (deforestation related).
    """
    if country:
        return _run(
            """
            MATCH (a:Country {name: $name})-[r:DEPENDS_ON_RESOURCE]->(b:Country)
            RETURN
                a.name              AS dependent_country,
                b.name              AS resource_country,
                round(r.value, 4)   AS dependency_score,
                r.mechanism         AS mechanism,
                r.year              AS year
            ORDER BY r.value DESC
            LIMIT $limit
            """,
            {"name": country, "limit": limit},
        )
    return _run(
        """
        MATCH (a:Country)-[r:DEPENDS_ON_RESOURCE]->(b:Country)
        RETURN
            a.name              AS dependent_country,
            b.name              AS resource_country,
            round(r.value, 4)   AS dependency_score,
            r.mechanism         AS mechanism
        ORDER BY r.value DESC
        LIMIT $limit
        """,
        {"limit": limit},
    )


# =========================================================
# BILATERAL CLIMATE RELATIONSHIP
# =========================================================

def get_bilateral_climate(
    country_a: str,
    country_b: str,
) -> dict:
    """
    Full bilateral climate relationship between two countries.
    Shared hazard risks, supply chain links, conflict risk links.
    """
    shared_hazards = _run(
        """
        MATCH (a:Country {name: $a})-[r1:IS_HIGH_RISK_FOR]->(e1:ClimateEvent)
        MATCH (b:Country {name: $b})-[r2:IS_HIGH_RISK_FOR]->(e2:ClimateEvent)
        WHERE r1.hazard_type = r2.hazard_type
          AND r1.risk_level = "High"
          AND r2.risk_level = "High"
        RETURN
            r1.hazard_type      AS shared_hazard,
            round(r1.value, 3)  AS country_a_risk,
            round(r2.value, 3)  AS country_b_risk
        ORDER BY shared_hazard
        """,
        {"a": country_a, "b": country_b},
    )

    a_disrupts_b = _run(
        """
        MATCH (a:Country {name: $a})-[r:DISRUPTS_SUPPLY_CHAIN]->(b:Country {name: $b})
        RETURN
            round(r.value, 4)   AS disruption_score,
            r.source_event      AS trigger_event,
            r.confidence        AS confidence
        """,
        {"a": country_a, "b": country_b},
    )

    b_disrupts_a = _run(
        """
        MATCH (b:Country {name: $b})-[r:DISRUPTS_SUPPLY_CHAIN]->(a:Country {name: $a})
        RETURN
            round(r.value, 4)   AS disruption_score,
            r.source_event      AS trigger_event,
            r.confidence        AS confidence
        """,
        {"a": country_a, "b": country_b},
    )

    conflict_links = _run(
        """
        MATCH (a:Country {name: $a})-[r:INCREASES_CONFLICT_RISK]->(b:Country {name: $b})
        RETURN round(r.value, 4) AS conflict_score, r.source_event AS trigger
        UNION ALL
        MATCH (b:Country {name: $b})-[r:INCREASES_CONFLICT_RISK]->(a:Country {name: $a})
        RETURN round(r.value, 4) AS conflict_score, r.source_event AS trigger
        """,
        {"a": country_a, "b": country_b},
    )

    return {
        "country_a":         country_a,
        "country_b":         country_b,
        "shared_hazards":    shared_hazards,
        "a_disrupts_b":      a_disrupts_b,
        "b_disrupts_a":      b_disrupts_a,
        "conflict_links":    conflict_links,
    }


# =========================================================
# SEARCH
# =========================================================

def search_countries_by_climate(
    query: str,
    limit: int = 10,
) -> list[dict]:
    """
    Search countries by partial name, return with climate risk score.
    """
    return _run(
        """
        MATCH (c:Country)
        WHERE toLower(c.name) CONTAINS toLower($query)
          AND c.climate_risk_score IS NOT NULL
        RETURN
            c.name                          AS country,
            round(c.climate_risk_score, 4)  AS climate_risk_score
        ORDER BY c.climate_risk_score DESC
        LIMIT $limit
        """,
        {"query": query, "limit": limit},
    )