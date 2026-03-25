from __future__ import annotations

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
# COUNTRY PROFILE
# =========================================================

def get_country_economic_profile(
    country: str,
    year: int = 2024,
) -> dict:
    """
    Full economic intelligence picture for a single country.
    Combines all score properties, latest GDP, trade balance,
    top partners, energy suppliers, sanctions status,
    and trade agreement count.
    """
    scores_rows = _run(
        """
        MATCH (c:Country {name: $name})
        RETURN
          c.economic_power_score          AS economic_power_score,
          c.economic_power_year           AS economic_power_year,
          c.trade_vulnerability_score     AS trade_vulnerability_score,
          c.energy_vulnerability_score    AS energy_vulnerability_score,
          c.partner_diversification_score AS partner_diversification_score,
          c.trade_balance_score           AS trade_balance_score,
          c.trade_balance_trend           AS trade_balance_trend,
          c.inflation_stability_score     AS inflation_stability_score,
          c.avg_inflation                 AS avg_inflation,
          c.inflation_trend               AS inflation_trend,
          c.trade_integration_score       AS trade_integration_score,
          c.economic_influence_score      AS economic_influence_score
        """,
        {"name": country},
    )
    if not scores_rows:
        return {}

    scores = scores_rows[0]

    # Latest GDP
    gdp_rows = _run(
        """
        MATCH (c:Country {name: $name})-[r:HAS_GDP]->(:Metric)
        WHERE r.year = $year
        RETURN r.value AS gdp_usd, r.normalized_weight AS gdp_normalized
        """,
        {"name": country, "year": year},
    )
    gdp = gdp_rows[0] if gdp_rows else {}

    # Latest trade balance
    balance_rows = _run(
        """
        MATCH (c:Country {name: $name})-[r:HAS_TRADE_BALANCE]->(:Metric)
        WHERE r.year = $year
        RETURN r.value AS balance_usd,
               r.exports AS exports_usd,
               r.imports AS imports_usd
        """,
        {"name": country, "year": year},
    )
    balance = balance_rows[0] if balance_rows else {}

    # Latest inflation
    inflation_rows = _run(
        """
        MATCH (c:Country {name: $name})-[r:HAS_INFLATION]->(:Metric)
        WHERE r.year = $year
        RETURN r.value AS inflation_pct
        """,
        {"name": country, "year": year},
    )
    inflation = inflation_rows[0] if inflation_rows else {}

    # Top 3 export partners (derived: IS_MAJOR_EXPORT_PARTNER_OF, rank 1-3)
    partners_rows = _run(
        """
        MATCH (c:Country {name: $name})-[r:IS_MAJOR_EXPORT_PARTNER_OF]->(p:Country)
        WHERE r.year = $year
        RETURN p.name AS partner,
               r.rank AS rank,
               r.dependency AS dependency,
               r.normalized_weight AS normalized_weight
        ORDER BY r.rank ASC
        LIMIT 3
        """,
        {"name": country, "year": year},
    )

    # Top 3 energy suppliers
    energy_rows = _run(
        """
        MATCH (c:Country {name: $name})-[r:IMPORTS_ENERGY_FROM]->(s:Country)
        WHERE r.year = $year
        RETURN s.name AS supplier,
               r.dependency AS dependency,
               r.value AS value_usd
        ORDER BY r.dependency DESC
        LIMIT 3
        """,
        {"name": country, "year": year},
    )

    # Sanctions status — who sanctions this country
    sanctions_rows = _run(
        """
        MATCH (sanctioner:Country)-[r:IMPOSED_SANCTIONS_ON]->(c:Country {name: $name})
        RETURN sanctioner.name AS sanctioned_by
        """,
        {"name": country},
    )

    # Trade agreement count
    agreement_rows = _run(
        """
        MATCH (c:Country {name: $name})-[:HAS_TRADE_AGREEMENT_WITH]->(p:Country)
        RETURN count(p) AS agreement_count
        """,
        {"name": country},
    )
    agreement_count = agreement_rows[0].get("agreement_count", 0) if agreement_rows else 0

    # High dependency flags (derived: HAS_HIGH_DEPENDENCY_ON, threshold > 0.7)
    high_dep_rows = _run(
        """
        MATCH (c:Country {name: $name})-[r:HAS_HIGH_DEPENDENCY_ON]->(p:Country)
        WHERE r.year = $year
        RETURN p.name AS depends_on, r.dependency AS dependency
        ORDER BY r.dependency DESC
        """,
        {"name": country, "year": year},
    )

    return {
        "country": country,
        "year": year,
        "scores": {
            "economic_power":          _safe(scores.get("economic_power_score")),
            "trade_vulnerability":     _safe(scores.get("trade_vulnerability_score")),
            "energy_vulnerability":    _safe(scores.get("energy_vulnerability_score")),
            "partner_diversification": _safe(scores.get("partner_diversification_score")),
            "trade_balance_health":    _safe(scores.get("trade_balance_score")),
            "inflation_stability":     _safe(scores.get("inflation_stability_score")),
            "trade_integration":       _safe(scores.get("trade_integration_score")),
            "economic_influence":      _safe(scores.get("economic_influence_score")),
        },
        "trends": {
            "trade_balance_trend": _safe(scores.get("trade_balance_trend")),
            "inflation_trend":     _safe(scores.get("inflation_trend")),
            "avg_inflation_pct":   _safe(scores.get("avg_inflation")),
        },
        "macro": {
            "gdp_usd":           _safe(gdp.get("gdp_usd")),
            "gdp_normalized":    _safe(gdp.get("gdp_normalized")),
            "trade_balance_usd": _safe(balance.get("balance_usd")),
            "exports_usd":       _safe(balance.get("exports_usd")),
            "imports_usd":       _safe(balance.get("imports_usd")),
            "inflation_pct":     _safe(inflation.get("inflation_pct")),
        },
        "top_trade_partners":  partners_rows,
        "top_energy_suppliers": energy_rows,
        "high_dependencies":   high_dep_rows,
        "sanctions": {
            "is_sanctioned": len(sanctions_rows) > 0,
            "sanctioned_by": [r["sanctioned_by"] for r in sanctions_rows],
        },
        "trade_agreements": {
            "count": agreement_count,
        },
    }


# =========================================================
# DEPENDENCY ANALYSIS
# =========================================================

def get_trade_dependencies(
    country: str,
    year: int = 2024,
    min_dependency: float = 0.05,
) -> list[dict]:
    """
    All trade dependencies for a country above threshold,
    sorted by dependency descending.

    Uses HAS_TRADE_DEPENDENCY_ON (derived from EXPORTS_TO) —
    each edge represents how much of country's total exports
    go to a given partner, i.e. the country's export concentration
    dependency on that partner. A high value means the country
    is heavily reliant on that partner as an export destination.
    """
    return _run(
        """
        MATCH (c:Country {name: $name})-[r:HAS_TRADE_DEPENDENCY_ON]->(p:Country)
        WHERE r.year = $year AND r.dependency >= $min_dep
        RETURN p.name AS partner,
               r.dependency AS dependency,
               r.value AS trade_value_usd,
               r.normalized_weight AS normalized_weight
        ORDER BY r.dependency DESC
        """,
        {"name": country, "year": year, "min_dep": min_dependency},
    )


def get_energy_dependencies(
    country: str,
    year: int = 2024,
) -> list[dict]:
    """
    All energy import dependencies for a country,
    sorted by dependency descending.
    Each row shows how much of this country's total energy
    imports come from a given supplier.
    """
    return _run(
        """
        MATCH (c:Country {name: $name})-[r:IMPORTS_ENERGY_FROM]->(s:Country)
        WHERE r.year = $year
        RETURN s.name AS supplier,
               r.dependency AS dependency,
               r.value AS value_usd,
               r.normalized_weight AS normalized_weight
        ORDER BY r.dependency DESC
        """,
        {"name": country, "year": year},
    )


def get_high_dependency_network(
    country: str,
) -> dict:
    """
    Countries this country has high dependency on (> 0.7),
    and countries that have high dependency on this country (reverse).
    Uses the derived HAS_HIGH_DEPENDENCY_ON relationship.
    """
    depends_on = _run(
        """
        MATCH (c:Country {name: $name})-[r:HAS_HIGH_DEPENDENCY_ON]->(p:Country)
        RETURN p.name AS country, r.dependency AS dependency, r.year AS year
        ORDER BY r.dependency DESC
        """,
        {"name": country},
    )

    depended_on_by = _run(
        """
        MATCH (p:Country)-[r:HAS_HIGH_DEPENDENCY_ON]->(c:Country {name: $name})
        RETURN p.name AS country, r.dependency AS dependency, r.year AS year
        ORDER BY r.dependency DESC
        """,
        {"name": country},
    )

    return {
        "country": country,
        "high_dependency_on":       depends_on,
        "countries_depend_on_this": depended_on_by,
    }


# =========================================================
# BILATERAL RELATIONSHIP
# =========================================================

def get_bilateral_trade(
    country_a: str,
    country_b: str,
) -> dict:
    """
    Full bilateral trade relationship between two countries.
    Includes trade flows in both directions across all years,
    energy flows, shared agreements, and sanctions.
    """
    years = list(range(2018, 2025))

    # A exports to B
    a_to_b = _run(
        """
        MATCH (a:Country {name: $a})-[r:EXPORTS_TO]->(b:Country {name: $b})
        WHERE r.year IN $years
        RETURN r.year AS year,
               r.value AS value_usd,
               r.dependency AS a_dependency_on_b,
               r.normalized_weight AS normalized_weight
        ORDER BY r.year
        """,
        {"a": country_a, "b": country_b, "years": years},
    )

    # B exports to A
    b_to_a = _run(
        """
        MATCH (b:Country {name: $b})-[r:EXPORTS_TO]->(a:Country {name: $a})
        WHERE r.year IN $years
        RETURN r.year AS year,
               r.value AS value_usd,
               r.dependency AS b_dependency_on_a,
               r.normalized_weight AS normalized_weight
        ORDER BY r.year
        """,
        {"a": country_a, "b": country_b, "years": years},
    )

    # Energy: A imports from B
    energy_a_from_b = _run(
        """
        MATCH (a:Country {name: $a})-[r:IMPORTS_ENERGY_FROM]->(b:Country {name: $b})
        RETURN r.year AS year,
               r.value AS value_usd,
               r.dependency AS dependency
        ORDER BY r.year
        """,
        {"a": country_a, "b": country_b},
    )

    # Energy: B imports from A
    energy_b_from_a = _run(
        """
        MATCH (b:Country {name: $b})-[r:IMPORTS_ENERGY_FROM]->(a:Country {name: $a})
        RETURN r.year AS year,
               r.value AS value_usd,
               r.dependency AS dependency
        ORDER BY r.year
        """,
        {"a": country_a, "b": country_b},
    )

    # Bilateral trade volume across years
    volume = _run(
        """
        MATCH (a:Country {name: $a})-[r:HAS_TRADE_VOLUME_WITH]-(b:Country {name: $b})
        RETURN r.year AS year,
               r.value AS total_volume_usd,
               r.normalized_weight AS normalized_weight
        ORDER BY r.year
        """,
        {"a": country_a, "b": country_b},
    )

    # Shared trade agreements
    shared_agreements = _run(
        """
        MATCH (a:Country {name: $a})-[r:HAS_TRADE_AGREEMENT_WITH]->(b:Country {name: $b})
        RETURN r.agreement_name AS agreement, r.source AS source
        """,
        {"a": country_a, "b": country_b},
    )

    # Sanctions between them (both directions)
    sanctions = _run(
        """
        MATCH (a:Country {name: $a})-[r:IMPOSED_SANCTIONS_ON]->(b:Country {name: $b})
        RETURN $a AS sanctioner, $b AS sanctioned, r.source AS source
        UNION ALL
        MATCH (b:Country {name: $b})-[r:IMPOSED_SANCTIONS_ON]->(a:Country {name: $a})
        RETURN $b AS sanctioner, $a AS sanctioned, r.source AS source
        """,
        {"a": country_a, "b": country_b},
    )

    return {
        "country_a": country_a,
        "country_b": country_b,
        "exports_a_to_b":       a_to_b,
        "exports_b_to_a":       b_to_a,
        "energy_a_imports_from_b": energy_a_from_b,
        "energy_b_imports_from_a": energy_b_from_a,
        "bilateral_volume":     volume,
        "shared_agreements":    shared_agreements,
        "sanctions":            sanctions,
    }


# =========================================================
# RANKINGS
# =========================================================

def get_top_economies(limit: int = 20) -> list[dict]:
    """
    Countries ranked by economic_influence_score descending.
    """
    return _run(
        """
        MATCH (c:Country)
        WHERE c.economic_influence_score IS NOT NULL
        RETURN c.name AS country,
               c.economic_influence_score      AS economic_influence,
               c.economic_power_score          AS economic_power,
               c.trade_integration_score       AS trade_integration,
               c.partner_diversification_score AS diversification
        ORDER BY c.economic_influence_score DESC
        LIMIT $limit
        """,
        {"limit": limit},
    )


def get_most_trade_vulnerable(limit: int = 20) -> list[dict]:
    """
    Countries ranked by trade_vulnerability_score descending (most vulnerable first).
    """
    return _run(
        """
        MATCH (c:Country)
        WHERE c.trade_vulnerability_score IS NOT NULL
        RETURN c.name AS country,
               c.trade_vulnerability_score     AS trade_vulnerability,
               c.energy_vulnerability_score    AS energy_vulnerability,
               c.partner_diversification_score AS diversification,
               c.economic_influence_score      AS economic_influence
        ORDER BY c.trade_vulnerability_score DESC
        LIMIT $limit
        """,
        {"limit": limit},
    )


def get_most_energy_vulnerable(limit: int = 20) -> list[dict]:
    """
    Countries ranked by energy_vulnerability_score descending (most vulnerable first).
    """
    return _run(
        """
        MATCH (c:Country)
        WHERE c.energy_vulnerability_score IS NOT NULL
        RETURN c.name AS country,
               c.energy_vulnerability_score AS energy_vulnerability,
               c.trade_vulnerability_score  AS trade_vulnerability,
               c.economic_influence_score   AS economic_influence
        ORDER BY c.energy_vulnerability_score DESC
        LIMIT $limit
        """,
        {"limit": limit},
    )


def get_inflation_ranking(
    order: str = "worst",
    limit: int = 20,
) -> list[dict]:
    """
    Countries ranked by inflation stability score.
    order='worst' → most unstable (lowest score) first.
    order='best'  → most stable (highest score) first.
    """
    direction = "ASC" if order == "worst" else "DESC"
    return _run(
        f"""
        MATCH (c:Country)
        WHERE c.inflation_stability_score IS NOT NULL
        RETURN c.name AS country,
               c.inflation_stability_score AS stability_score,
               c.avg_inflation             AS avg_inflation_pct,
               c.inflation_trend           AS trend
        ORDER BY c.inflation_stability_score {direction}
        LIMIT $limit
        """,
        {"limit": limit},
    )


def get_trade_surplus_ranking(limit: int = 20) -> list[dict]:
    """
    Countries ranked by trade balance health score descending.
    """
    return _run(
        """
        MATCH (c:Country)
        WHERE c.trade_balance_score IS NOT NULL
        RETURN c.name AS country,
               c.trade_balance_score AS balance_score,
               c.trade_balance_trend AS trend
        ORDER BY c.trade_balance_score DESC
        LIMIT $limit
        """,
        {"limit": limit},
    )


# =========================================================
# GDP ANALYSIS
# =========================================================

def get_gdp_trend(
    country: str,
    start_year: int = 2000,
    end_year: int = 2024,
) -> list[dict]:
    """
    GDP values for a country across a year range.
    """
    return _run(
        """
        MATCH (c:Country {name: $name})-[r:HAS_GDP]->(:Metric)
        WHERE r.year >= $start AND r.year <= $end
        RETURN r.year AS year,
               r.value AS gdp_usd,
               r.normalized_weight AS normalized_weight
        ORDER BY r.year
        """,
        {"name": country, "start": start_year, "end": end_year},
    )


def get_gdp_comparison(
    countries: list[str],
    year: int = 2024,
) -> list[dict]:
    """
    GDP comparison across multiple countries for a given year.
    """
    return _run(
        """
        MATCH (c:Country)-[r:HAS_GDP]->(:Metric)
        WHERE c.name IN $countries AND r.year = $year
        RETURN c.name AS country,
               r.value AS gdp_usd,
               r.normalized_weight AS normalized_weight
        ORDER BY r.value DESC
        """,
        {"countries": countries, "year": year},
    )


# =========================================================
# INFLATION & TRADE BALANCE HISTORY
# =========================================================

def get_inflation_history(
    country: str,
    start_year: int = 2000,
    end_year: int = 2024,
) -> list[dict]:
    """
    Inflation history for a country across years.
    """
    return _run(
        """
        MATCH (c:Country {name: $name})-[r:HAS_INFLATION]->(:Metric)
        WHERE r.year >= $start AND r.year <= $end
        RETURN r.year AS year, r.value AS inflation_pct
        ORDER BY r.year
        """,
        {"name": country, "start": start_year, "end": end_year},
    )


def get_trade_balance_history(
    country: str,
    start_year: int = 2018,
    end_year: int = 2024,
) -> list[dict]:
    """
    Trade balance history for a country across years.
    """
    return _run(
        """
        MATCH (c:Country {name: $name})-[r:HAS_TRADE_BALANCE]->(:Metric)
        WHERE r.year >= $start AND r.year <= $end
        RETURN r.year AS year,
               r.value AS balance_usd,
               r.exports AS exports_usd,
               r.imports AS imports_usd
        ORDER BY r.year
        """,
        {"name": country, "start": start_year, "end": end_year},
    )


# =========================================================
# SANCTIONS & AGREEMENTS
# =========================================================

def get_sanctions_network(country: str) -> dict:
    """
    Full sanctions picture for a country —
    who sanctions it and who it sanctions.
    """
    sanctioned_by = _run(
        """
        MATCH (s:Country)-[r:IMPOSED_SANCTIONS_ON]->(c:Country {name: $name})
        RETURN s.name AS sanctioner, r.source AS source, r.year AS year
        """,
        {"name": country},
    )

    sanctions_imposed_on = _run(
        """
        MATCH (c:Country {name: $name})-[r:IMPOSED_SANCTIONS_ON]->(t:Country)
        RETURN t.name AS target, r.source AS source, r.year AS year
        """,
        {"name": country},
    )

    return {
        "country": country,
        "is_sanctioned":        len(sanctioned_by) > 0,
        "sanctioned_by":        sanctioned_by,
        "sanctions_imposed_on": sanctions_imposed_on,
    }


def get_trade_agreement_partners(country: str) -> list[dict]:
    """
    All active trade agreement partners for a country.
    """
    return _run(
        """
        MATCH (c:Country {name: $name})-[r:HAS_TRADE_AGREEMENT_WITH]->(p:Country)
        RETURN p.name AS partner,
               r.agreement_name AS agreement,
               r.source AS source
        ORDER BY r.agreement_name
        """,
        {"name": country},
    )


def get_shared_agreements(country_a: str, country_b: str) -> list[dict]:
    """
    Trade agreements shared by two countries.
    """
    return _run(
        """
        MATCH (a:Country {name: $a})-[r:HAS_TRADE_AGREEMENT_WITH]->(b:Country {name: $b})
        RETURN r.agreement_name AS agreement, r.source AS source
        ORDER BY r.agreement_name
        """,
        {"a": country_a, "b": country_b},
    )


# =========================================================
# ENERGY ANALYSIS
# =========================================================

def get_energy_supplier_profile(
    supplier: str,
    year: int = 2024,
) -> list[dict]:
    """
    All countries that import energy from a given supplier,
    with their dependency values. Shows the supplier's
    geopolitical leverage — high dependency means the importer
    is strategically exposed.
    """
    return _run(
        """
        MATCH (importer:Country)-[r:IMPORTS_ENERGY_FROM]->(s:Country {name: $name})
        WHERE r.year = $year
        RETURN importer.name AS importer,
               r.dependency AS dependency,
               r.value AS value_usd,
               r.normalized_weight AS normalized_weight
        ORDER BY r.dependency DESC
        """,
        {"name": supplier, "year": year},
    )


def get_major_energy_exporters(year: int = 2024, limit: int = 10) -> list[dict]:
    """
    Countries ranked by total energy export value for a given year.
    """
    return _run(
        """
        MATCH (c:Country)-[r:EXPORTS_ENERGY_TO]->(:Country)
        WHERE r.year = $year
        RETURN c.name AS country,
               sum(r.value) AS total_export_value_usd,
               count(r) AS importer_count
        ORDER BY total_export_value_usd DESC
        LIMIT $limit
        """,
        {"year": year, "limit": limit},
    )


# =========================================================
# TRADE VOLUME
# =========================================================

def get_top_trade_pairs(year: int = 2024, limit: int = 20) -> list[dict]:
    """
    Highest bilateral trade volume pairs for a given year.
    HAS_TRADE_VOLUME_WITH is undirected (written as A→B where A < B
    alphabetically), so we match both directions with -[r]-
    and deduplicate by only returning rows where a.name < b.name.
    """
    return _run(
        """
        MATCH (a:Country)-[r:HAS_TRADE_VOLUME_WITH]-(b:Country)
        WHERE r.year = $year AND a.name < b.name
        RETURN a.name AS country_a,
               b.name AS country_b,
               r.value AS total_volume_usd,
               r.normalized_weight AS normalized_weight
        ORDER BY r.value DESC
        LIMIT $limit
        """,
        {"year": year, "limit": limit},
    )


def get_trade_volume_trend(
    country_a: str,
    country_b: str,
) -> list[dict]:
    """
    Bilateral trade volume trend across all available years.
    """
    return _run(
        """
        MATCH (a:Country {name: $a})-[r:HAS_TRADE_VOLUME_WITH]-(b:Country {name: $b})
        RETURN r.year AS year,
               r.value AS total_volume_usd,
               r.normalized_weight AS normalized_weight
        ORDER BY r.year
        """,
        {"a": country_a, "b": country_b},
    )


# =========================================================
# MAJOR EXPORT PARTNERS NETWORK
# =========================================================

def get_major_partners_network(year: int = 2024) -> list[dict]:
    """
    Top export partner relationships across all countries,
    rank 1 only — useful for graph/force-directed visualization.
    """
    return _run(
        """
        MATCH (a:Country)-[r:IS_MAJOR_EXPORT_PARTNER_OF]->(b:Country)
        WHERE r.year = $year AND r.rank = 1
        RETURN a.name AS exporter,
               b.name AS top_partner,
               r.dependency AS dependency,
               r.normalized_weight AS normalized_weight
        ORDER BY r.dependency DESC
        """,
        {"year": year},
    )


# =========================================================
# SEARCH
# =========================================================

def search_countries(query: str, limit: int = 10) -> list[dict]:
    """
    Search for countries by partial name match.
    Returns matches sorted by economic influence score descending.
    Useful for autocomplete in frontend.
    """
    return _run(
        """
        MATCH (c:Country)
        WHERE toLower(c.name) CONTAINS toLower($query)
          AND c.economic_influence_score IS NOT NULL
        RETURN c.name AS country,
               c.economic_influence_score AS influence_score
        ORDER BY c.economic_influence_score DESC
        LIMIT $limit
        """,
        {"query": query, "limit": limit},
    )