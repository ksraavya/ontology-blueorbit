from __future__ import annotations
import sys
sys.path.insert(0, '.')

"""
analytics/climate/derived.py

Writes derived relationships back to the graph based on
climate scores already computed by analytics/climate/scores.py.

Relationships written:
    (Country)-[:AFFECTS_ECONOMY]->(Country)       ← self-referential climate→economy signal
    (Country)-[:DEPENDS_ON_RESOURCE]->(Country)   ← enriched from existing edges

Rule: Uses GraphOps.create_relationship() only — no raw Cypher MERGE for relationships.
Rule: Reads cross-domain scores (trade_vulnerability) from Neo4j, not from Python imports.
Rule: All scores clamped to [0.0, 1.0].
"""

import logging
import time
from typing import Any

from common.db import Neo4jConnection
from common.graph_ops import GraphOps
from common.intelligence.normalization import clamp, normalize_by_max
from common.intelligence.composite import weighted_score
from common.intelligence.aggregation import max_value
from common.ontology import AFFECTS_ECONOMY, DEPENDS_ON_RESOURCE

logger = logging.getLogger(__name__)


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def compute_affects_economy_edges(threshold: float = 0.5) -> int:
    """
    Writes (Country)-[:AFFECTS_ECONOMY]->(Country) self-referential edges.

    Logic per spec:
        High climate_vulnerability_score + high trade_vulnerability_score
        = climate disrupts that country's own economy.

    Only writes edges where climate_vulnerability_score > threshold.

    Edge properties:
        value              = climate_vulnerability_score
        normalized_weight  = climate_vulnerability_score
        year               = 2024
        mechanism          = "climate"

    Returns number of edges written.
    """
    conn = Neo4jConnection()
    try:
        # Read both climate and economy scores from graph
        # trade_vulnerability_score is written by analytics/economy/scores.py
        query = """
            MATCH (c:Country)
            WHERE c.climate_vulnerability_score IS NOT NULL
              AND c.climate_vulnerability_score > $threshold
            RETURN
                c.name                          AS country,
                c.climate_vulnerability_score   AS climate_vuln,
                coalesce(c.trade_vulnerability_score, 0.0) AS trade_vuln
        """
        rows = conn.run_query(query, {"threshold": threshold})

        if not rows:
            logger.warning(
                "compute_affects_economy_edges: no countries above threshold=%.2f "
                "— run analytics/climate/scores.py first",
                threshold,
            )
            return 0

        ops = GraphOps(conn)
        written = 0

        for row in rows:
            country     = row.get("country")
            climate_vuln = _safe_float(row.get("climate_vuln"))
            trade_vuln   = _safe_float(row.get("trade_vuln"))

            if not country or climate_vuln is None:
                continue

            # Combined impact signal: climate vulnerability amplified by
            # trade vulnerability (more trade = more disruption)
            trade_vuln_safe = trade_vuln if trade_vuln is not None else 0.0
            impact_score = clamp(
                weighted_score(
                    {"climate": climate_vuln, "trade": trade_vuln_safe},
                    {"climate": 0.7, "trade": 0.3},
                )
            )

            logger.debug(
                "compute_affects_economy_edges: country=%s climate=%.4f "
                "trade=%.4f impact=%.4f",
                country, climate_vuln, trade_vuln_safe, impact_score,
            )

            # Self-referential: country's climate affects its own economy
            ops.create_relationship(
                source=country,
                target=country,
                rel_type=AFFECTS_ECONOMY,
                properties={
                    "value":             impact_score,
                    "normalized_weight": impact_score,
                    "year":              2024,
                    "mechanism":         "climate",
                    "climate_vuln":      climate_vuln,
                    "trade_vuln":        trade_vuln_safe,
                },
            )
            written += 1

        logger.info(
            "compute_affects_economy_edges: threshold=%.2f written=%d",
            threshold, written,
        )
        return written

    finally:
        conn.close()


def compute_resource_dependency_edges() -> int:
    """
    Enriches existing DEPENDS_ON_RESOURCE relationships with
    additional properties derived from emissions and forest scores.

    Reads existing DEPENDS_ON_RESOURCE edges (written by climate compute layer)
    and re-writes them via GraphOps to ensure they conform to the standard
    edge schema and carry enriched properties.

    Returns number of edges enriched/written.
    """
    conn = Neo4jConnection()
    try:
        # Read existing DEPENDS_ON_RESOURCE edges plus emissions context
        query = """
            MATCH (c:Country)-[r:DEPENDS_ON_RESOURCE]->(target:Country)
            RETURN
                c.name                                          AS source,
                target.name                                     AS target,
                coalesce(r.value, 0.0)                         AS dep_value,
                coalesce(r.year, 2024)                         AS year,
                coalesce(c.emissions_score, 0.0)               AS emissions_score,
                coalesce(c.resource_stress_score, 0.0)         AS resource_stress
        """
        rows = conn.run_query(query)

        if not rows:
            logger.warning(
                "compute_resource_dependency_edges: no DEPENDS_ON_RESOURCE "
                "edges found — run modules/climate/runner.py first"
            )
            return 0

        ops    = GraphOps(conn)
        written = 0

        for row in rows:
            source          = row.get("source")
            target          = row.get("target")
            dep_value       = _safe_float(row.get("dep_value")) or 0.0
            year            = int(row.get("year") or 2024)
            emissions_score = _safe_float(row.get("emissions_score")) or 0.0
            resource_stress = _safe_float(row.get("resource_stress")) or 0.0

            if not source or not target:
                continue

            # Enrich: dependency score amplified by resource stress
            enriched_score = clamp(
                weighted_score(
                    {"dependency": dep_value, "stress": resource_stress},
                    {"dependency": 0.6,       "stress": 0.4},
                )
            )

            logger.debug(
                "compute_resource_dependency_edges: %s -> %s "
                "dep=%.4f stress=%.4f enriched=%.4f",
                source, target, dep_value, resource_stress, enriched_score,
            )

            ops.create_relationship(
                source=source,
                target=target,
                rel_type=DEPENDS_ON_RESOURCE,
                properties={
                    "value":             enriched_score,
                    "normalized_weight": enriched_score,
                    "year":              year,
                    "mechanism":         "climate_resource",
                    "emissions_score":   emissions_score,
                    "resource_stress":   resource_stress,
                },
            )
            written += 1

        logger.info(
            "compute_resource_dependency_edges: enriched=%d edges",
            written,
        )
        return written

    finally:
        conn.close()


def compute_all_derived() -> dict[str, int]:
    """
    Runs all derived functions in dependency order.

    Dependency order:
        1. compute_affects_economy_edges()
           Reads: c.climate_vulnerability_score (scores.py must run first)
                  c.trade_vulnerability_score   (economy analytics must run first)

        2. compute_resource_dependency_edges()
           Reads: existing DEPENDS_ON_RESOURCE edges (climate module must run first)
                  c.emissions_score, c.resource_stress_score (scores.py must run first)

    Returns:
        {function_name: edges_written}
    """
    start = time.perf_counter()
    counts: dict[str, int] = {}

    logger.info("compute_all_derived: stage compute_affects_economy_edges")
    counts["compute_affects_economy_edges"] = compute_affects_economy_edges()

    logger.info("compute_all_derived: stage compute_resource_dependency_edges")
    counts["compute_resource_dependency_edges"] = compute_resource_dependency_edges()

    elapsed = time.perf_counter() - start
    total   = sum(counts.values())

    logger.info(
        "compute_all_derived: done in %.2fs total_edges=%d counts=%s",
        elapsed, total, counts,
    )
    return counts

