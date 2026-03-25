import sys

sys.path.insert(0, ".")

import logging
import time

from analytics.geopolitics.scores import compute_all_geopolitics_scores
from analytics.geopolitics.derived import compute_all_derived

logger = logging.getLogger(__name__)


def run(years: list[int] | None = None) -> None:
    if years is None:
        years = list(range(2010, 2025))

    logger.info("Geopolitics analytics starting")
    total_start = time.perf_counter()

    # --- Stage 0: Ensure base graph analytics have run ---
    logger.info("--- Stage 0: Base Graph Analytics (centrality + blocs) ---")
    try:
        # Import inside function to avoid circular import issues.
        from modules.geopolitics.analytics import (
            compute_alignment_scores,
            compute_centrality,
            detect_blocs,
        )

        compute_alignment_scores()
        compute_centrality()
        detect_blocs()
        logger.info("Stage 0 complete")
    except Exception as e:
        logger.error("Stage 0 failed: %s", e)
        logger.warning("Continuing -- centrality/blocs may already be in graph")

    # --- Stage 1: Scores ---
    logger.info("--- Stage 1: Geopolitics Scores ---")
    scores_start = time.perf_counter()
    scores_result = compute_all_geopolitics_scores(years=years)
    scores_elapsed = time.perf_counter() - scores_start
    logger.info("Scores complete in %.2fs: %s", scores_elapsed, scores_result)

    # --- Stage 2: Derived Relationships ---
    logger.info("--- Stage 2: Derived Relationships ---")
    derived_start = time.perf_counter()
    derived_result = compute_all_derived()
    derived_elapsed = time.perf_counter() - derived_start
    logger.info("Derived complete in %.2fs: %s", derived_elapsed, derived_result)

    # --- Summary ---
    total_elapsed = time.perf_counter() - total_start
    total_scores = sum(scores_result.values())
    total_derived = sum(derived_result.values())
    logger.info(
        "Geopolitics analytics complete in %.2fs -- "
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