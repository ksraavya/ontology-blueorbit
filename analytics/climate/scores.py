from __future__ import annotations
import sys
sys.path.insert(0, '.')

import logging
import time
from typing import Any

from common.db import Neo4jConnection
from common.intelligence.aggregation import average, max_value
from common.intelligence.composite import weighted_score
from common.intelligence.normalization import clamp, normalize_by_max

logger = logging.getLogger(__name__)


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def compute_disaster_frequency_score(years: list[int] | None = None) -> int:
    if years is None:
        years = list(range(2000, 2025))

    conn = Neo4jConnection()
    try:
        query = """
            MATCH (c:Country)-[r:EXPERIENCED]->(e:ClimateEvent)
            WHERE e.year IN $years
            RETURN c.name AS country, count(e) AS event_count
        """
        rows = conn.run_query(query, {"years": years})
        if not rows:
            logger.warning("compute_disaster_frequency_score: no rows returned")
            return 0

        counts = {
            row["country"]: float(row["event_count"])
            for row in rows
            if row.get("country") and row.get("event_count") is not None
        }
        max_count = max_value(list(counts.values()))

        updated = 0
        for country, count in counts.items():
            score = clamp(normalize_by_max(count, max_count))
            conn.run_query(
                """
                MATCH (c:Country {name: $country})
                SET c.disaster_frequency_score = $score
                """,
                {"country": country, "score": score},
            )
            updated += 1

        logger.info("compute_disaster_frequency_score: updated=%d", updated)
        return updated
    finally:
        conn.close()


def compute_climate_damage_score(years: list[int] | None = None) -> int:
    if years is None:
        years = list(range(2000, 2025))

    conn = Neo4jConnection()
    try:
        query = """
            MATCH (c:Country)-[r:EXPERIENCED]->(e:ClimateEvent)
            WHERE e.year IN $years
              AND r.disaster_type <> 'Earthquake'
            RETURN c.name AS country, sum(coalesce(r.value, 0.0)) AS total_damage
        """
        rows = conn.run_query(query, {"years": years})
        if not rows:
            logger.warning("compute_climate_damage_score: no rows returned")
            return 0

        damage_by_country = {
            row["country"]: float(row["total_damage"])
            for row in rows
            if row.get("country") and row.get("total_damage") is not None
        }
        max_damage = max_value(list(damage_by_country.values()))

        updated = 0
        for country, total in damage_by_country.items():
            score = clamp(normalize_by_max(total, max_damage))
            conn.run_query(
                """
                MATCH (c:Country {name: $country})
                SET c.climate_damage_score = $score
                """,
                {"country": country, "score": score},
            )
            updated += 1

        logger.info("compute_climate_damage_score: updated=%d", updated)
        return updated
    finally:
        conn.close()


def compute_climate_deaths_score(years: list[int] | None = None) -> int:
    if years is None:
        years = list(range(2000, 2025))

    conn = Neo4jConnection()
    try:
        query = """
            MATCH (e:ClimateEvent)-[r:RESULTED_IN_FATALITIES]->(c:Country)
            WHERE e.year IN $years
            RETURN c.name AS country, sum(coalesce(r.value, 0.0)) AS total_deaths
        """
        rows = conn.run_query(query, {"years": years})
        if not rows:
            logger.warning("compute_climate_deaths_score: no rows returned")
            return 0

        deaths_by_country = {
            row["country"]: float(row["total_deaths"])
            for row in rows
            if row.get("country") and row.get("total_deaths") is not None
        }
        max_deaths = max_value(list(deaths_by_country.values()))

        updated = 0
        for country, total in deaths_by_country.items():
            score = clamp(normalize_by_max(total, max_deaths))
            conn.run_query(
                """
                MATCH (c:Country {name: $country})
                SET c.climate_deaths_score = $score
                """,
                {"country": country, "score": score},
            )
            updated += 1

        logger.info("compute_climate_deaths_score: updated=%d", updated)
        return updated
    finally:
        conn.close()


def compute_climate_risk_score() -> int:
    """
    Composite score. Requires frequency, damage, deaths scores already written.
    Writes alias for composite layer at the end.
    """
    conn = Neo4jConnection()
    try:
        query = """
            MATCH (c:Country)
            WHERE c.disaster_frequency_score IS NOT NULL
              AND c.climate_damage_score IS NOT NULL
              AND c.climate_deaths_score IS NOT NULL
            RETURN c.name AS country,
                   c.disaster_frequency_score AS frequency,
                   c.climate_damage_score AS damage,
                   c.climate_deaths_score AS deaths
        """
        rows = conn.run_query(query)
        if not rows:
            logger.warning(
                "compute_climate_risk_score: no countries with all three component scores"
            )
            return 0

        updated = 0
        for row in rows:
            country   = row.get("country")
            frequency = _safe_float(row.get("frequency"))
            damage    = _safe_float(row.get("damage"))
            deaths    = _safe_float(row.get("deaths"))

            if not country or frequency is None or damage is None or deaths is None:
                continue

            score = clamp(
                weighted_score(
                    {"frequency": frequency, "damage": damage, "deaths": deaths},
                    {"frequency": 0.3,       "damage": 0.4,   "deaths": 0.3},
                )
            )
            conn.run_query(
                """
                MATCH (c:Country {name: $country})
                SET c.climate_risk_score = $score
                """,
                {"country": country, "score": score},
            )
            updated += 1

        # --- Claude Alias Mapping Layer ---
        logger.info("compute_climate_risk_score: writing aliases for %d countries", updated)
        conn.run_query("""
            MATCH (c:Country)
            WHERE c.climate_risk_score IS NOT NULL
            SET c.climate_vulnerability_score = c.climate_risk_score,
                c.disaster_exposure_score = c.disaster_frequency_score,
                c.emissions_score = coalesce(c.climate_damage_score, 0.0),
                c.resource_stress_score = coalesce(c.supply_chain_risk_score, 0.0)
        """)
        # ----------------------------------

        logger.info("compute_climate_risk_score: updated=%d", updated)
        return updated
    finally:
        conn.close()


def compute_supply_chain_risk_score() -> int:
    conn = Neo4jConnection()
    try:
        query = """
            MATCH (c:Country)-[r:DISRUPTS_SUPPLY_CHAIN]->(:Country)
            RETURN c.name AS country,
                   max(coalesce(r.normalized_weight, 0.0)) AS max_disruption
        """
        rows = conn.run_query(query)
        if not rows:
            logger.warning("compute_supply_chain_risk_score: no DISRUPTS_SUPPLY_CHAIN edges found")
            return 0

        updated = 0
        for row in rows:
            country          = row.get("country")
            max_disruption   = _safe_float(row.get("max_disruption"))
            if not country or max_disruption is None:
                continue

            score = clamp(max_disruption)
            conn.run_query(
                """
                MATCH (c:Country {name: $country})
                SET c.supply_chain_risk_score = $score
                """,
                {"country": country, "score": score},
            )
            updated += 1

        logger.info("compute_supply_chain_risk_score: updated=%d", updated)
        return updated
    finally:
        conn.close()


def compute_all_climate_scores(
    years: list[int] | None = None,
) -> dict[str, int]:
    if years is None:
        years = list(range(2000, 2025))

    start = time.perf_counter()
    counts: dict[str, int] = {}

    logger.info("compute_all_climate_scores: stage frequency")
    counts["disaster_frequency"] = compute_disaster_frequency_score(years)

    logger.info("compute_all_climate_scores: stage damage")
    counts["climate_damage"] = compute_climate_damage_score(years)

    logger.info("compute_all_climate_scores: stage deaths")
    counts["climate_deaths"] = compute_climate_deaths_score(years)

    logger.info("compute_all_climate_scores: stage risk (and aliases)")
    counts["climate_risk"] = compute_climate_risk_score()

    logger.info("compute_all_climate_scores: stage supply chain")
    counts["supply_chain_risk"] = compute_supply_chain_risk_score()

    elapsed = time.perf_counter() - start
    logger.info("compute_all_climate_scores: done in %.2fs counts=%s", elapsed, counts)
    return counts