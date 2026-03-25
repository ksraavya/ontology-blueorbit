from __future__ import annotations
import sys
sys.path.insert(0, '.')

import logging
import time

from modules.climate.ingest import load_all
from modules.climate.transform import transform_all
from modules.climate.compute import compute_all
from modules.climate.load import load_all_to_graph
from modules.climate.bridge import run_bridge

logger = logging.getLogger(__name__)


def run(year: int = 2024, years: list[int] | None = None) -> None:
    logger.info("Climate pipeline starting")
    start = time.perf_counter()

    # Import scores here so a missing analytics module doesn't block ingest
    try:
        from analytics.climate.scores import compute_all_climate_scores
    except ImportError as exc:
        logger.warning("analytics.climate.scores not available: %s", exc)
        compute_all_climate_scores = None  # type: ignore[assignment]

    logger.info("--- Stage 1: Ingest ---")
    raw = load_all(years)

    logger.info("--- Stage 2: Transform ---")
    clean = transform_all(raw)

    logger.info("--- Stage 3: Compute ---")
    enriched = compute_all(clean)

    logger.info("--- Stage 4: Load to Graph ---")
    load_all_to_graph(enriched)

    logger.info("--- Stage 5: Bridge (Impact Propagation) ---")
    bridge_result = run_bridge(year=year)
    logger.info("Bridge result: %s", bridge_result)

    if compute_all_climate_scores is not None:
        logger.info("--- Stage 6: Climate Scores ---")
        score_years = years or list(range(2000, year + 1))
        score_result = compute_all_climate_scores(years=score_years)
        logger.info("Score result: %s", score_result)
    else:
        logger.warning("--- Stage 6: Skipped (analytics module not found) ---")

    elapsed = time.perf_counter() - start
    logger.info("Climate pipeline complete in %.2fs", elapsed)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    run()