from __future__ import annotations

import logging
from typing import Any

from common.db import Neo4jConnection
from common.graph_ops import GraphOps
from common.intelligence.composite import weighted_score
from common.intelligence.normalization import clamp
from common.config import INFLUENCE_WEIGHTS, DEFAULT_INFLUENCE_THRESHOLD
from common.ontology import IS_INFLUENTIAL_TO

logger = logging.getLogger(__name__)


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def compute_strategic_influence_score(year: int = 2024) -> int:
    """
    Compute strategic_influence_score for every country that has all three
    component scores written by their respective module analytics.

    Reads from graph:
        c.economic_influence_score      (economy analytics)
        c.military_strength_score       (defense analytics)
        c.geopolitical_influence_score  (geopolitics analytics)

    Computes:
        score = weighted_score(
            metrics={
                "economic":    c.economic_influence_score,
                "defense":     c.military_strength_score,
                "geopolitical": c.geopolitical_influence_score,
            },
            weights=INFLUENCE_WEIGHTS  # {"economic": 0.4, "defense": 0.3, "geopolitical": 0.3}
        )

    Writes:
        c.strategic_influence_score (0-1)
        (Country)-[:IS_INFLUENTIAL_TO]->(Country) for top influenced countries
    """
    conn = Neo4jConnection()
    try:
        query = """
            MATCH (c:Country)
            WHERE c.economic_influence_score IS NOT NULL
              AND c.military_strength_score IS NOT NULL
              AND c.geopolitical_influence_score IS NOT NULL
            RETURN
                c.name                          AS country,
                c.economic_influence_score      AS economic,
                c.military_strength_score       AS defense,
                c.geopolitical_influence_score  AS geopolitical
        """
        rows = conn.run_query(query)

        if not rows:
            logger.warning(
                "compute_strategic_influence_score: no countries with all three component "
                "scores. Ensure economy, defense, and geopolitics analytics have all run first."
            )
            return 0

        scores: dict[str, float] = {}
        for row in rows:
            country     = row.get("country")
            economic    = _safe_float(row.get("economic"))
            defense     = _safe_float(row.get("defense"))
            geopolitical = _safe_float(row.get("geopolitical"))

            if not country or economic is None or defense is None or geopolitical is None:
                continue

            score = clamp(
                weighted_score(
                    metrics={
                        "economic":     economic,
                        "defense":      defense,
                        "geopolitical": geopolitical,
                    },
                    weights=INFLUENCE_WEIGHTS,
                )
            )

            logger.debug(
                "compute_strategic_influence_score: country=%s economic=%.4f "
                "defense=%.4f geopolitical=%.4f score=%.4f",
                country, economic, defense, geopolitical, score,
            )

            conn.run_query(
                """
                MATCH (c:Country {name: $country})
                SET c.strategic_influence_score = $score,
                    c.strategic_influence_year  = $year
                """,
                {"country": country, "score": score, "year": year},
            )
            scores[country] = score

        # ── Write IS_INFLUENTIAL_TO edges ─────────────────────────────────────
        # For each high-influence country, find their top trade/diplomatic partners
        # and create IS_INFLUENTIAL_TO edges toward them.
        ops = GraphOps(conn)
        edges_written = 0

        high_influence = [
            c for c, s in scores.items() if s >= DEFAULT_INFLUENCE_THRESHOLD
        ]

        for influencer in high_influence:
            # Top partners by trade volume
            partner_rows = conn.run_query(
                """
                MATCH (a:Country {name: $name})-[r:HAS_TRADE_VOLUME_WITH]-(b:Country)
                WHERE r.year = $year
                RETURN b.name AS partner, r.normalized_weight AS weight
                ORDER BY r.normalized_weight DESC
                LIMIT 5
                """,
                {"name": influencer, "year": year},
            )

            influencer_score = scores[influencer]
            seen: set[str] = set()

            for pr in partner_rows:
                partner = pr.get("partner")
                weight  = _safe_float(pr.get("weight")) or 0.0
                if not partner or partner in seen or partner == influencer:
                    continue
                seen.add(partner)

                edge_score = clamp(influencer_score * weight)
                if edge_score < 0.05:
                    continue

                ops.create_relationship(
                    source=influencer,
                    target=partner,
                    rel_type=IS_INFLUENTIAL_TO,
                    properties={
                        "value":             edge_score,
                        "normalized_weight": edge_score,
                        "year":              year,
                        "domain":            "composite",
                        "confidence":        0.8,
                    },
                )
                edges_written += 1

        updated = len(scores)
        logger.info(
            "compute_strategic_influence_score: updated=%d is_influential_to_edges=%d",
            updated, edges_written,
        )
        return updated
    finally:
        conn.close()