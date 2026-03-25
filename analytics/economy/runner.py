from __future__ import annotations
 
import logging
import time
 
from analytics.economy.scores import compute_all_economic_scores
from analytics.economy.derived import compute_all_derived
 
logger = logging.getLogger(__name__)
 
 
def run(year: int = 2024) -> None:
    """
    Orchestrates all economy analytics in correct dependency order.
 
    Scores must be computed before derived edges because derived.py
    reads node properties written by scores.py.
 
    Args:
        year: Reference year for snapshot metrics and derived edges.
              Historical range (2018..year) is computed automatically
              inside compute_all_economic_scores().
    """
    logger.info("Economy analytics starting (reference year=%d)", year)
    total_start = time.perf_counter()
 
    # ── Stage 1: Compute and write all score properties to Country nodes ──
    logger.info("--- Stage: Economic Scores ---")
    scores_start = time.perf_counter()
 
    scores_result = compute_all_economic_scores(
        years=list(range(2018, year + 1)),
        latest_year=year,
    )
 
    scores_elapsed = time.perf_counter() - scores_start
    logger.info(
        "Scores complete in %.2fs: %s",
        scores_elapsed,
        scores_result,
    )
 
    # ── Stage 2: Write derived relationships back to graph ────────────────
    logger.info("--- Stage: Derived Relationships ---")
    derived_start = time.perf_counter()
 
    derived_result = compute_all_derived(year=year)
 
    derived_elapsed = time.perf_counter() - derived_start
    logger.info(
        "Derived complete in %.2fs: %s",
        derived_elapsed,
        derived_result,
    )
 
    # ── Summary ───────────────────────────────────────────────────────────
    total_elapsed = time.perf_counter() - total_start
    total_scores = sum(scores_result.values())
    total_derived = sum(derived_result.values())
 
    logger.info(
        "Economy analytics complete in %.2fs — "
        "scores updated: %d countries, derived relationships written: %d",
        total_elapsed,
        total_scores,
        total_derived,
    )
 
 
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    run()