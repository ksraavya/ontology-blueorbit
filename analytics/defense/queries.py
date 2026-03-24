"""
analytics/defense/queries.py
=============================
All query functions for the Defence Intelligence Module API.

Contains:
  - 5 original functions copied from modules/defense/analytics.py
  - 3 new functions using analytics score properties

Used by modules/defense/routes.py
"""

from __future__ import annotations

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from typing import Any, Dict, List, Optional

from common.db import Neo4jConnection


# ── HELPERS ─────────────────────────────────────────────────────────────────

def _run(query: str, params: dict | None = None) -> List[Dict[str, Any]]:
    conn = Neo4jConnection()
    try:
        return conn.run_query(query, params or {})
    finally:
        conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# ORIGINAL 5 FUNCTIONS  (copied from modules/defense/analytics.py)
# ══════════════════════════════════════════════════════════════════════════════

def get_spending_trend(country: str) -> List[Dict[str, Any]]:
    """Defence spending year-by-year for a given country."""
    return _run("""
        MATCH (c:Country {name: $country})-[r:SPENDS_ON_DEFENSE]->(y:Year)
        RETURN y.year AS year,
               round(r.value, 2) AS spending_usd_millions,
               round(r.normalized_weight, 4) AS normalized_weight
        ORDER BY year
    """, {"country": country})


def get_top_defense_spenders(limit: int = 10) -> List[Dict[str, Any]]:
    """Top N defence spenders in the most recent year."""
    return _run("""
        MATCH (c:Country)-[r:SPENDS_ON_DEFENSE]->(y:Year)
        WITH max(y.year) AS latest_year
        MATCH (c:Country)-[r:SPENDS_ON_DEFENSE]->(y:Year {year: latest_year})
        RETURN c.name AS country,
               y.year AS year,
               round(r.value, 2) AS spending_usd_millions,
               round(r.normalized_weight, 4) AS normalized_weight
        ORDER BY r.value DESC
        LIMIT $limit
    """, {"limit": limit})


def get_top_arms_exporters(limit: int = 10) -> List[Dict[str, Any]]:
    """Top N arms exporters by peak year-level market share (all periods)."""
    return _run("""
        MATCH (c:Country)-[r:EXPORTS_ARMS]->(y:Year)
        WITH c.name AS country,
             max(r.dependency) AS peak_share,
             round(avg(r.dependency) * 100, 2) AS avg_share_pct,
             count(r) AS active_years,
             collect(DISTINCT r.period) AS periods
        WHERE peak_share IS NOT NULL
        RETURN country,
               round(peak_share * 100, 1) AS peak_market_share_pct,
               avg_share_pct AS avg_market_share_pct,
               active_years,
               periods
        ORDER BY peak_share DESC
        LIMIT $limit
    """, {"limit": limit})


def get_conflict_summary(country: str) -> List[Dict[str, Any]]:
    """Conflict statistics per year for a given country."""
    return _run("""
        MATCH (c:Country {name: $country})-[r:HAS_CONFLICT_STATS]->(y:Year)
        RETURN y.year AS year,
               r.total_fatalities AS total_fatalities,
               r.civilian_fatalities AS civilian_fatalities,
               r.violence_events AS violence_events,
               r.civilian_events AS civilian_events,
               r.fatality_trend AS fatality_trend,
               round(r.normalized_weight, 4) AS normalized_weight
        ORDER BY year
    """, {"country": country})


def get_most_conflict_prone(limit: int = 10) -> List[Dict[str, Any]]:
    """Top N most conflict-affected countries by total fatalities."""
    return _run("""
        MATCH (c:Country)-[r:HAS_CONFLICT_STATS]->(y:Year)
        RETURN c.name AS country,
               sum(r.total_fatalities) AS total_fatalities,
               sum(r.violence_events) AS total_violence_events,
               sum(r.civilian_fatalities) AS total_civilian_fatalities,
               count(DISTINCT y.year) AS years_with_data,
               collect(DISTINCT r.fatality_trend)[0] AS trend
        ORDER BY total_fatalities DESC
        LIMIT $limit
    """, {"limit": limit})


# ══════════════════════════════════════════════════════════════════════════════
# NEW FUNCTIONS  — use analytics score properties + IMPORTS_ARMS
# ══════════════════════════════════════════════════════════════════════════════

def get_country_defense_profile(country: str) -> Optional[Dict[str, Any]]:
    """
    Full simulator-ready profile for one country.
    Combines all analytics scores, raw metrics, enrichment data,
    arms import/export data, and live signals.
    """
    results = _run("""
        MATCH (c:Country {name: $country})
        OPTIONAL MATCH (c)-[s:SPENDS_ON_DEFENSE]->(y1:Year)
        WHERE y1.year = (
            MATCH (cc:Country {name: $country})-[:SPENDS_ON_DEFENSE]->(yy:Year)
            RETURN max(yy.year) LIMIT 1
        )
        OPTIONAL MATCH (c)-[cf:HAS_CONFLICT_STATS]->(y2:Year)
        WHERE y2.year = (
            MATCH (cc2:Country {name: $country})-[:HAS_CONFLICT_STATS]->(yy2:Year)
            RETURN max(yy2.year) LIMIT 1
        )
        OPTIONAL MATCH (c)-[:MEMBER_OF]->(a:Alliance)
        OPTIONAL MATCH (c)-[:BELONGS_TO]->(reg:Region)
        OPTIONAL MATCH (c)-[co:CO_MENTIONED_WITH]->(hostile:Country)
        WHERE co.dominant_context = 'hostile'
        OPTIONAL MATCH (imp)-[ia:IMPORTS_ARMS]->(yimp:Year)
        WHERE imp = c AND yimp.year >= 2018
        RETURN c.name AS country,
               c.composite_threat_score AS composite_threat,
               c.military_strength_score AS military_strength,
               c.defense_spending_score AS spending_score,
               c.conflict_risk_score AS conflict_risk,
               c.arms_export_score AS arms_export_score,
               c.defense_burden_score AS burden_score,
               c.live_risk_score AS live_risk_score,
               c.nuclear_status AS nuclear_status,
               c.un_p5 AS un_p5,
               c.is_regional_power AS is_regional_power,
               c.dominant_region AS dominant_region,
               round(s.normalized_weight, 4) AS latest_spending_weight,
               round(cf.normalized_weight, 4) AS latest_conflict_weight,
               cf.fatality_trend AS conflict_trend,
               collect(DISTINCT a.name) AS alliances,
               reg.name AS region,
               collect(DISTINCT hostile.name) AS hostile_co_mentions,
               round(avg(ia.dependency), 4) AS avg_import_dependency
        LIMIT 1
    """, {"country": country})

    # Fallback: simpler query if complex one fails
    if not results:
        results = _run("""
            MATCH (c:Country {name: $country})
            OPTIONAL MATCH (c)-[:MEMBER_OF]->(a:Alliance)
            OPTIONAL MATCH (c)-[:BELONGS_TO]->(reg:Region)
            OPTIONAL MATCH (c)-[co:CO_MENTIONED_WITH]->(hostile:Country)
            WHERE co.dominant_context = 'hostile'
            WITH c, reg,
                 collect(DISTINCT a.name) AS alliances,
                 collect(DISTINCT hostile.name) AS hostile_partners
            OPTIONAL MATCH (c)-[s:SPENDS_ON_DEFENSE]->(y1:Year)
            WITH c, reg, alliances, hostile_partners, s, y1
            ORDER BY y1.year DESC
            WITH c, reg, alliances, hostile_partners, head(collect(s)) AS latest_s
            OPTIONAL MATCH (c)-[cf:HAS_CONFLICT_STATS]->(y2:Year)
            WITH c, reg, alliances, hostile_partners, latest_s, cf, y2
            ORDER BY y2.year DESC
            WITH c, reg, alliances, hostile_partners, latest_s,
                 head(collect(cf)) AS latest_cf
            OPTIONAL MATCH (c)-[ia:IMPORTS_ARMS]->(yimp:Year)
            WHERE yimp.year >= 2018
            WITH c, reg, alliances, hostile_partners, latest_s, latest_cf,
                 avg(ia.dependency) AS avg_import_dep
            RETURN c.name AS country,
                   c.composite_threat_score AS composite_threat,
                   c.military_strength_score AS military_strength,
                   c.defense_spending_score AS spending_score,
                   c.conflict_risk_score AS conflict_risk,
                   c.arms_export_score AS arms_export_score,
                   c.defense_burden_score AS burden_score,
                   c.live_risk_score AS live_risk_score,
                   c.nuclear_status AS nuclear_status,
                   c.un_p5 AS un_p5,
                   c.is_regional_power AS is_regional_power,
                   round(latest_s.normalized_weight, 4) AS latest_spending_weight,
                   round(latest_cf.normalized_weight, 4) AS latest_conflict_weight,
                   latest_cf.fatality_trend AS conflict_trend,
                   alliances,
                   reg.name AS region,
                   hostile_partners AS hostile_co_mentions,
                   round(avg_import_dep, 4) AS avg_import_dependency
        """, {"country": country})

    return results[0] if results else None


def get_military_comparison(countries: List[str] | None = None,
                             limit: int = 15) -> List[Dict[str, Any]]:
    """
    Comparative military intelligence across countries.
    Uses all analytics score properties plus raw arms trade data.
    Optionally filter to a specific list of countries.
    """
    if countries:
        return _run("""
            MATCH (c:Country)
            WHERE c.name IN $countries
              AND c.composite_threat_score IS NOT NULL
            OPTIONAL MATCH (c)-[ia:IMPORTS_ARMS]->(yia:Year)
            WHERE yia.year >= 2018
            WITH c, avg(ia.dependency) AS avg_import_dep
            OPTIONAL MATCH (c)-[ea:EXPORTS_ARMS]->(yea:Year)
            WHERE yea.year >= 2018
            WITH c, avg_import_dep, avg(ea.dependency) AS avg_export_dep
            RETURN c.name AS country,
                   round(c.composite_threat_score, 4) AS composite_threat,
                   round(c.military_strength_score, 4) AS military_strength,
                   round(c.defense_spending_score, 4) AS spending_score,
                   round(c.conflict_risk_score, 4) AS conflict_risk,
                   round(c.arms_export_score, 4) AS arms_export_score,
                   round(c.defense_burden_score, 4) AS burden_score,
                   c.nuclear_status AS nuclear_status,
                   c.un_p5 AS un_p5,
                   round(avg_export_dep * 100, 2) AS avg_export_share_pct,
                   round(avg_import_dep * 100, 2) AS avg_import_share_pct
            ORDER BY c.composite_threat_score DESC
        """, {"countries": countries})
    else:
        return _run("""
            MATCH (c:Country)
            WHERE c.composite_threat_score IS NOT NULL
            OPTIONAL MATCH (c)-[ia:IMPORTS_ARMS]->(yia:Year)
            WHERE yia.year >= 2018
            WITH c, avg(ia.dependency) AS avg_import_dep
            OPTIONAL MATCH (c)-[ea:EXPORTS_ARMS]->(yea:Year)
            WHERE yea.year >= 2018
            WITH c, avg_import_dep, avg(ea.dependency) AS avg_export_dep
            RETURN c.name AS country,
                   round(c.composite_threat_score, 4) AS composite_threat,
                   round(c.military_strength_score, 4) AS military_strength,
                   round(c.defense_spending_score, 4) AS spending_score,
                   round(c.conflict_risk_score, 4) AS conflict_risk,
                   round(c.arms_export_score, 4) AS arms_export_score,
                   round(c.defense_burden_score, 4) AS burden_score,
                   c.nuclear_status AS nuclear_status,
                   c.un_p5 AS un_p5,
                   round(avg_export_dep * 100, 2) AS avg_export_share_pct,
                   round(avg_import_dep * 100, 2) AS avg_import_share_pct
            ORDER BY c.composite_threat_score DESC
            LIMIT $limit
        """, {"limit": limit})


def get_alliance_network(alliance: str | None = None) -> List[Dict[str, Any]]:
    """
    Alliance membership with defence intelligence for each member.
    If alliance is None, returns all alliances with aggregate stats.
    """
    if alliance:
        return _run("""
            MATCH (c:Country)-[:MEMBER_OF]->(a:Alliance {name: $alliance})
            OPTIONAL MATCH (c)-[s:SPENDS_ON_DEFENSE]->(y:Year)
            WHERE y.year = 2023
            WITH a, c, s
            RETURN a.name AS alliance,
                   c.name AS country,
                   round(c.composite_threat_score, 4) AS composite_threat,
                   round(c.military_strength_score, 4) AS military_strength,
                   c.nuclear_status AS nuclear_status,
                   c.un_p5 AS un_p5,
                   round(s.value, 0) AS spending_2023_usd_millions,
                   round(s.normalized_weight, 4) AS spending_weight
            ORDER BY c.composite_threat_score DESC NULLS LAST
        """, {"alliance": alliance})
    else:
        return _run("""
            MATCH (c:Country)-[:MEMBER_OF]->(a:Alliance)
            OPTIONAL MATCH (c)-[s:SPENDS_ON_DEFENSE]->(y:Year {year: 2023})
            WITH a.name AS alliance,
                 count(DISTINCT c) AS members,
                 sum(coalesce(s.value, 0)) AS total_spending_usd_millions,
                 avg(c.composite_threat_score) AS avg_threat_score,
                 avg(c.military_strength_score) AS avg_military_score,
                 count(CASE WHEN c.nuclear_status IS NOT NULL THEN 1 END)
                     AS nuclear_members
            RETURN alliance,
                   members,
                   round(total_spending_usd_millions / 1000, 1)
                       AS total_spending_billion,
                   round(avg_threat_score, 4) AS avg_composite_threat,
                   round(avg_military_score, 4) AS avg_military_strength,
                   nuclear_members
            ORDER BY total_spending_usd_millions DESC
        """)


def get_top_arms_importers(limit: int = 10) -> List[Dict[str, Any]]:
    """
    Top N arms importers by average annual market share.
    Uses IMPORTS_ARMS relationship (all periods combined).
    """
    return _run("""
        MATCH (c:Country)-[r:IMPORTS_ARMS]->(y:Year)
        WITH c.name AS country,
             max(r.dependency) AS peak_share,
             round(avg(r.dependency) * 100, 2) AS avg_share_pct,
             count(r) AS active_years,
             collect(DISTINCT r.period) AS periods
        WHERE peak_share IS NOT NULL
        RETURN country,
               round(peak_share * 100, 1) AS peak_import_share_pct,
               avg_share_pct AS avg_import_share_pct,
               active_years,
               periods
        ORDER BY peak_share DESC
        LIMIT $limit
    """, {"limit": limit})


def get_arms_import_profile(country: str) -> List[Dict[str, Any]]:
    """
    Arms import history for a specific country (all years).
    """
    return _run("""
        MATCH (c:Country {name: $country})-[r:IMPORTS_ARMS]->(y:Year)
        RETURN y.year AS year,
               r.period AS period,
               round(r.value, 1) AS tiv_millions,
               round(r.dependency * 100, 2) AS market_share_pct,
               round(r.normalized_weight, 4) AS normalized_weight
        ORDER BY year
    """, {"country": country})


def get_threat_classification(year: int = 2022, limit: int = 20) -> List[Dict[str, Any]]:
    """
    Three-module threat classification combining defence, economy, geopolitics.
    Requires Economy module (HAS_GDP) and Geopolitics module (HAS_POLITICAL_SYSTEM).
    """
    return _run("""
        MATCH (c:Country)-[s:SPENDS_ON_DEFENSE]->(y:Year {year: $year})
        MATCH (c)-[g:HAS_GDP]->(m:Metric {name: 'GDP'})
        MATCH (c)-[p:HAS_POLITICAL_SYSTEM]->(ps:PoliticalSystem)
        WHERE g.year = $year AND g.value > 0 AND s.value > 0
        WITH c, s, p, ps, head(collect(g)) AS best_g
        ORDER BY p.normalized_weight DESC
        WITH c, s, best_g,
             collect(ps.name)[0] AS system,
             collect(p.normalized_weight)[0] AS gov
        RETURN c.name AS country,
               round((s.value * 1e6) / best_g.value * 100, 2)
                   AS defense_pct_of_gdp,
               system AS political_system,
               round(gov, 3) AS democracy_score,
               round(c.composite_threat_score, 4) AS composite_threat,
               CASE
                   WHEN (s.value * 1e6) / best_g.value > 0.04 AND gov < 0.5
                   THEN 'HIGH RISK — autocracy on war footing'
                   WHEN (s.value * 1e6) / best_g.value > 0.04 AND gov >= 0.5
                   THEN 'ACTIVE CONFLICT — democracy under threat'
                   WHEN (s.value * 1e6) / best_g.value > 0.02 AND gov < 0.5
                   THEN 'ELEVATED — autocracy militarising'
                   ELSE 'NORMAL'
               END AS threat_classification
        ORDER BY defense_pct_of_gdp DESC
        LIMIT $limit
    """, {"year": year, "limit": limit})


def get_simulator_ready_countries(limit: int = 50) -> List[Dict[str, Any]]:
    """
    All countries that have a composite_threat_score and are ready for simulation.
    live_risk_score is included if present but NOT required.
    """
    return _run("""
        MATCH (c:Country)
        WHERE c.composite_threat_score IS NOT NULL
        OPTIONAL MATCH (c)-[:MEMBER_OF]->(a:Alliance)
        OPTIONAL MATCH (c)-[:BELONGS_TO]->(reg:Region)
        WITH c, reg,
             collect(DISTINCT a.name) AS alliances
        RETURN c.name AS country,
               round(c.composite_threat_score, 4) AS composite_threat,
               round(c.military_strength_score, 4) AS military_strength,
               round(c.conflict_risk_score, 4) AS conflict_risk,
               round(c.defense_spending_score, 4) AS spending_score,
               round(c.arms_export_score, 4) AS arms_export_score,
               coalesce(round(c.live_risk_score, 3), 0.0) AS live_risk,
               c.nuclear_status AS nuclear,
               c.un_p5 AS p5,
               alliances,
               reg.name AS region
        ORDER BY c.composite_threat_score DESC
        LIMIT $limit
    """, {"limit": limit})