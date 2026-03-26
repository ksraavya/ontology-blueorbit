from __future__ import annotations

import logging
import time

from analytics.composite.global_risk import compute_global_risk_score
from analytics.composite.influence import compute_strategic_influence_score
from analytics.composite.vulnerability import compute_overall_vulnerability

logger = logging.getLogger(__name__)


def run(year: int = 2024) -> dict[str, int]:
    """
    Orchestrate all composite analytics in correct dependency order.

    MUST be called only after all four module analytics have run:
        - analytics/economy/runner.py
        - analytics/defense/runner.py
        - analytics/geopolitics/runner.py
        - analytics/climate/runner.py

    Composite reads node properties written by those runners.
    Running before they complete will silently score 0 countries.

    Order:
        1. global_risk         — reads trade, conflict, climate vulnerability
        2. strategic_influence — reads economic, military, geopolitical influence
        3. overall_vulnerability — reads all four vulnerability dimensions

    Args:
        year: Reference year passed to each stage.

    Returns:
        Dict of {stage_name: countries_updated}
    """
    logger.info("Composite analytics starting (year=%d)", year)
    total_start = time.perf_counter()
    counts: dict[str, int] = {}

    logger.info("--- Stage 1: Global Risk Score ---")
    stage_start = time.perf_counter()
    counts["compute_global_risk_score"] = compute_global_risk_score(year)
    logger.info(
        "Global risk complete in %.2fs: %d countries",
        time.perf_counter() - stage_start,
        counts["compute_global_risk_score"],
    )

    logger.info("--- Stage 2: Strategic Influence Score ---")
    stage_start = time.perf_counter()
    counts["compute_strategic_influence_score"] = compute_strategic_influence_score(year)
    logger.info(
        "Strategic influence complete in %.2fs: %d countries",
        time.perf_counter() - stage_start,
        counts["compute_strategic_influence_score"],
    )

    logger.info("--- Stage 3: Overall Vulnerability Score ---")
    stage_start = time.perf_counter()
    counts["compute_overall_vulnerability"] = compute_overall_vulnerability(year)
    logger.info(
        "Overall vulnerability complete in %.2fs: %d countries",
        time.perf_counter() - stage_start,
        counts["compute_overall_vulnerability"],
    )

    total_elapsed = time.perf_counter() - total_start
    total_countries = sum(counts.values())
    logger.info(
        "Composite analytics complete in %.2fs — total writes: %d | stages: %s",
        total_elapsed, total_countries, counts,
    )
    return counts


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    result = run()
    for stage, count in result.items():
        print(f"  {stage}: {count} countries updated")