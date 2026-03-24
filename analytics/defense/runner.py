import sys

sys.path.insert(0, ".")
import logging
from analytics.defense.scores import compute_all_defense_scores
from analytics.defense.derived import (
    compute_high_conflict_edges,
    compute_arms_influence_edges,
)

logger = logging.getLogger(__name__)


def run_defense_analytics() -> None:
    logger.info("=== Defense Analytics: Starting ===")

    # Step 1: Compute all score properties on Country nodes
    logger.info("--- Step 1: Computing defense scores ---")
    score_results = compute_all_defense_scores()
    logger.info("Score results: %s", score_results)

    # Step 2: Compute derived relationships
    logger.info("--- Step 2: Computing derived relationships ---")
    n_conflict = compute_high_conflict_edges(threshold=0.35, year=2024)
    n_influence = compute_arms_influence_edges(top_n=5, year=2024)

    logger.info(
        "Derived results: conflict_edges=%d influence_edges=%d",
        n_conflict,
        n_influence,
    )
    logger.info("=== Defense Analytics: Complete ===")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    run_defense_analytics()
