from __future__ import annotations

import logging
import time
from typing import Any

from common.db import Neo4jConnection
from common.intelligence.composite import weighted_score
from common.intelligence.normalization import clamp
from common.config import GLOBAL_RISK_WEIGHTS

logger = logging.getLogger(__name__)


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def compute_global_risk_score(year: int = 2024) -> int:
    """
    Compute global_risk_score for every country that has all three
    component scores written by their respective module analytics.

    Reads from graph:
        c.trade_vulnerability_score    (economy analytics)
        c.conflict_risk_score          (defense analytics)
        c.climate_vulnerability_score  (climate analytics)

    Computes:
        score = weighted_score(
            metrics={
                "trade":   c.trade_vulnerability_score,
                "defense": c.conflict_risk_score,
                "climate": c.climate_vulnerability_score,
            },
            weights=GLOBAL_RISK_WEIGHTS  # {"trade": 0.4, "defense": 0.3, "climate": 0.3}
        )

    Writes: c.global_risk_score (0-1)
    """
    conn = Neo4jConnection()
    try:
        query = """
            MATCH (c:Country)
            WHERE c.trade_vulnerability_score IS NOT NULL
              AND c.conflict_risk_score IS NOT NULL
              AND c.climate_vulnerability_score IS NOT NULL
            RETURN
                c.name                          AS country,
                c.trade_vulnerability_score     AS trade,
                c.conflict_risk_score           AS defense,
                c.climate_vulnerability_score   AS climate
        """
        rows = conn.run_query(query, {"year": year})

        if not rows:
            logger.warning(
                "compute_global_risk_score: no countries with all three component scores. "
                "Ensure economy, defense, and climate analytics have all run first."
            )
            return 0

        updated = 0
        for row in rows:
            country = row.get("country")
            trade   = _safe_float(row.get("trade"))
            defense = _safe_float(row.get("defense"))
            climate = _safe_float(row.get("climate"))

            if not country or trade is None or defense is None or climate is None:
                continue

            score = clamp(
                weighted_score(
                    metrics={
                        "trade":   trade,
                        "defense": defense,
                        "climate": climate,
                    },
                    weights=GLOBAL_RISK_WEIGHTS,
                )
            )

            logger.debug(
                "compute_global_risk_score: country=%s trade=%.4f defense=%.4f "
                "climate=%.4f score=%.4f",
                country, trade, defense, climate, score,
            )

            conn.run_query(
                """
                MATCH (c:Country {name: $country})
                SET c.global_risk_score = $score,
                    c.global_risk_year  = $year
                """,
                {"country": country, "score": score, "year": year},
            )
            updated += 1

        logger.info("compute_global_risk_score: updated=%d", updated)
        return updated
    finally:
        conn.close()