from __future__ import annotations

import logging
from typing import Any

from common.db import Neo4jConnection

logger = logging.getLogger(__name__)


def _run(query: str, params: dict | None = None) -> list[dict]:
    conn = Neo4jConnection()
    try:
        return conn.run_query(query, params or {})
    finally:
        conn.close()


# =========================================================
# GLOBAL RISK
# =========================================================

def get_global_risk_ranking(limit: int = 30) -> list[dict]:
    """
    Countries ranked by global_risk_score descending.
    Returns breakdown of all three component scores.
    """
    return _run(
        """
        MATCH (c:Country)
        WHERE c.global_risk_score IS NOT NULL
        RETURN
            c.name                                          AS country,
            round(c.global_risk_score, 4)                  AS global_risk,
            round(coalesce(c.trade_vulnerability_score, 0), 4)   AS trade_vulnerability,
            round(coalesce(c.conflict_risk_score, 0), 4)         AS conflict_risk,
            round(coalesce(c.climate_vulnerability_score, 0), 4) AS climate_vulnerability
        ORDER BY c.global_risk_score DESC
        LIMIT $limit
        """,
        {"limit": limit},
    )


def get_country_composite_profile(country: str) -> dict:
    """
    Full composite intelligence profile for a single country.
    Combines all composite scores + component breakdowns.
    """
    rows = _run(
        """
        MATCH (c:Country {name: $name})
        RETURN
            c.name                                              AS country,
            c.global_risk_score                                AS global_risk,
            c.strategic_influence_score                        AS strategic_influence,
            c.overall_vulnerability_score                      AS overall_vulnerability,
            c.primary_vulnerability                            AS primary_vulnerability,
            c.trade_vulnerability_score                        AS trade_vulnerability,
            c.energy_vulnerability_score                       AS energy_vulnerability,
            c.conflict_risk_score                              AS conflict_risk,
            c.climate_vulnerability_score                      AS climate_vulnerability,
            c.economic_influence_score                         AS economic_influence,
            c.military_strength_score                          AS military_strength,
            c.geopolitical_influence_score                     AS geopolitical_influence,
            c.defense_composite_score                          AS defense_composite,
            c.economic_power_score                             AS economic_power,
            c.political_stability_score                        AS political_stability,
            c.diplomatic_centrality_score                      AS diplomatic_centrality,
            c.live_risk_score                                  AS live_risk,
            c.nuclear_status                                   AS nuclear_status,
            c.un_p5                                            AS un_p5,
            c.is_regional_power                                AS is_regional_power,
            c.bloc_id                                          AS bloc_id
        """,
        {"name": country},
    )
    return rows[0] if rows else {}


def get_strategic_influence_ranking(limit: int = 30) -> list[dict]:
    """
    Countries ranked by strategic_influence_score descending.
    Returns economic, military, and geopolitical component breakdown.
    """
    return _run(
        """
        MATCH (c:Country)
        WHERE c.strategic_influence_score IS NOT NULL
        RETURN
            c.name                                                  AS country,
            round(c.strategic_influence_score, 4)                  AS strategic_influence,
            round(coalesce(c.economic_influence_score, 0), 4)      AS economic_influence,
            round(coalesce(c.military_strength_score, 0), 4)       AS military_strength,
            round(coalesce(c.geopolitical_influence_score, 0), 4)  AS geopolitical_influence,
            c.nuclear_status                                        AS nuclear_status,
            c.un_p5                                                 AS un_p5
        ORDER BY c.strategic_influence_score DESC
        LIMIT $limit
        """,
        {"limit": limit},
    )


def get_vulnerability_ranking(
    limit: int = 30,
    primary: str | None = None,
) -> list[dict]:
    """
    Countries ranked by overall_vulnerability_score descending.
    Optionally filter by primary_vulnerability dimension
    ("trade" | "energy" | "conflict" | "climate").
    """
    if primary:
        return _run(
            """
            MATCH (c:Country)
            WHERE c.overall_vulnerability_score IS NOT NULL
              AND c.primary_vulnerability = $primary
            RETURN
                c.name                                                  AS country,
                round(c.overall_vulnerability_score, 4)                AS overall_vulnerability,
                c.primary_vulnerability                                 AS primary_vulnerability,
                round(coalesce(c.trade_vulnerability_score, 0), 4)     AS trade,
                round(coalesce(c.energy_vulnerability_score, 0), 4)    AS energy,
                round(coalesce(c.conflict_risk_score, 0), 4)           AS conflict,
                round(coalesce(c.climate_vulnerability_score, 0), 4)   AS climate
            ORDER BY c.overall_vulnerability_score DESC
            LIMIT $limit
            """,
            {"primary": primary, "limit": limit},
        )
    return _run(
        """
        MATCH (c:Country)
        WHERE c.overall_vulnerability_score IS NOT NULL
        RETURN
            c.name                                                  AS country,
            round(c.overall_vulnerability_score, 4)                AS overall_vulnerability,
            c.primary_vulnerability                                 AS primary_vulnerability,
            round(coalesce(c.trade_vulnerability_score, 0), 4)     AS trade,
            round(coalesce(c.energy_vulnerability_score, 0), 4)    AS energy,
            round(coalesce(c.conflict_risk_score, 0), 4)           AS conflict,
            round(coalesce(c.climate_vulnerability_score, 0), 4)   AS climate
        ORDER BY c.overall_vulnerability_score DESC
        LIMIT $limit
        """,
        {"limit": limit},
    )


def get_influence_network(year: int = 2024, limit: int = 50) -> list[dict]:
    """
    IS_INFLUENTIAL_TO relationships — who influences whom and how strongly.
    Useful for force-directed graph visualization of power networks.
    """
    return _run(
        """
        MATCH (a:Country)-[r:IS_INFLUENTIAL_TO]->(b:Country)
        WHERE r.year = $year
        RETURN
            a.name                                      AS influencer,
            b.name                                      AS influenced,
            round(r.normalized_weight, 4)               AS influence_score,
            round(coalesce(a.strategic_influence_score, 0), 4) AS influencer_power
        ORDER BY r.normalized_weight DESC
        LIMIT $limit
        """,
        {"year": year, "limit": limit},
    )


def get_risk_vs_influence_matrix(limit: int = 50) -> list[dict]:
    """
    2D matrix of global_risk_score vs strategic_influence_score.
    Identifies four quadrants:
        high influence + low risk  → stable powers
        high influence + high risk → contested powers
        low influence  + high risk → fragile states
        low influence  + low risk  → stable minor states
    """
    return _run(
        """
        MATCH (c:Country)
        WHERE c.global_risk_score IS NOT NULL
          AND c.strategic_influence_score IS NOT NULL
        RETURN
            c.name                                      AS country,
            round(c.global_risk_score, 4)              AS global_risk,
            round(c.strategic_influence_score, 4)      AS strategic_influence,
            round(c.overall_vulnerability_score, 4)    AS vulnerability,
            c.primary_vulnerability                    AS primary_vulnerability,
            CASE
                WHEN c.strategic_influence_score >= 0.5 AND c.global_risk_score < 0.4
                THEN "stable_power"
                WHEN c.strategic_influence_score >= 0.5 AND c.global_risk_score >= 0.4
                THEN "contested_power"
                WHEN c.strategic_influence_score < 0.5 AND c.global_risk_score >= 0.4
                THEN "fragile_state"
                ELSE "stable_minor"
            END AS quadrant
        ORDER BY c.strategic_influence_score DESC, c.global_risk_score DESC
        LIMIT $limit
        """,
        {"limit": limit},
    )


def get_most_exposed_countries(limit: int = 20) -> list[dict]:
    """
    Countries with simultaneously high vulnerability AND high global risk.
    These are the most exposed states in the system.
    Score = average of overall_vulnerability and global_risk.
    """
    return _run(
        """
        MATCH (c:Country)
        WHERE c.overall_vulnerability_score IS NOT NULL
          AND c.global_risk_score IS NOT NULL
        WITH c,
             (c.overall_vulnerability_score + c.global_risk_score) / 2.0 AS exposure
        ORDER BY exposure DESC
        LIMIT $limit
        RETURN
            c.name                                              AS country,
            round(exposure, 4)                                 AS exposure_score,
            round(c.overall_vulnerability_score, 4)            AS vulnerability,
            round(c.global_risk_score, 4)                      AS global_risk,
            c.primary_vulnerability                            AS primary_vulnerability,
            round(coalesce(c.strategic_influence_score, 0), 4) AS strategic_influence
        """,
        {"limit": limit},
    )


def get_composite_scores_coverage() -> dict:
    """
    Diagnostic: how many countries have each composite score.
    Useful for verifying all module analytics ran before composite.
    """
    rows = _run(
        """
        MATCH (c:Country)
        RETURN
            count(c)                                                            AS total_countries,
            sum(CASE WHEN c.global_risk_score IS NOT NULL THEN 1 ELSE 0 END)          AS has_global_risk,
            sum(CASE WHEN c.strategic_influence_score IS NOT NULL THEN 1 ELSE 0 END)  AS has_influence,
            sum(CASE WHEN c.overall_vulnerability_score IS NOT NULL THEN 1 ELSE 0 END) AS has_vulnerability,
            sum(CASE WHEN c.trade_vulnerability_score IS NOT NULL THEN 1 ELSE 0 END)  AS has_trade_vuln,
            sum(CASE WHEN c.conflict_risk_score IS NOT NULL THEN 1 ELSE 0 END)        AS has_conflict_risk,
            sum(CASE WHEN c.climate_vulnerability_score IS NOT NULL THEN 1 ELSE 0 END) AS has_climate_vuln,
            sum(CASE WHEN c.economic_influence_score IS NOT NULL THEN 1 ELSE 0 END)   AS has_econ_influence,
            sum(CASE WHEN c.military_strength_score IS NOT NULL THEN 1 ELSE 0 END)    AS has_military,
            sum(CASE WHEN c.geopolitical_influence_score IS NOT NULL THEN 1 ELSE 0 END) AS has_geopolitical
        """
    )
    return rows[0] if rows else {}


def compare_countries(countries: list[str]) -> list[dict]:
    """
    Side-by-side composite comparison for a list of country names.
    Returns all composite + component scores for each.
    """
    return _run(
        """
        MATCH (c:Country)
        WHERE c.name IN $countries
        RETURN
            c.name                                                  AS country,
            round(coalesce(c.global_risk_score, 0), 4)             AS global_risk,
            round(coalesce(c.strategic_influence_score, 0), 4)     AS strategic_influence,
            round(coalesce(c.overall_vulnerability_score, 0), 4)   AS vulnerability,
            round(coalesce(c.economic_influence_score, 0), 4)      AS economic_influence,
            round(coalesce(c.military_strength_score, 0), 4)       AS military_strength,
            round(coalesce(c.geopolitical_influence_score, 0), 4)  AS geopolitical_influence,
            round(coalesce(c.trade_vulnerability_score, 0), 4)     AS trade_vulnerability,
            round(coalesce(c.energy_vulnerability_score, 0), 4)    AS energy_vulnerability,
            round(coalesce(c.conflict_risk_score, 0), 4)           AS conflict_risk,
            round(coalesce(c.climate_vulnerability_score, 0), 4)   AS climate_vulnerability,
            c.primary_vulnerability                                 AS primary_vulnerability,
            c.nuclear_status                                        AS nuclear_status,
            c.un_p5                                                 AS p5
        ORDER BY c.strategic_influence_score DESC
        """,
        {"countries": countries},
    )