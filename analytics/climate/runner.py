from __future__ import annotations
import sys
sys.path.insert(0, '.')

import logging
import time

from analytics.climate.scores import compute_all_climate_scores

logger = logging.getLogger(__name__)


def run(years: list[int] | None = None) -> None:
    logger.info("Climate analytics runner starting")
    start = time.perf_counter()

    result = compute_all_climate_scores(years=years)

    elapsed = time.perf_counter() - start
    logger.info(
        "Climate analytics complete in %.2fs — %s",
        elapsed, result,
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    run()