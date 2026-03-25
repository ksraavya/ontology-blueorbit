import sys
sys.path.insert(0, ".")

import logging
import time
from typing import Any

from common.db import Neo4jConnection
from common.intelligence.aggregation import average, max_value
from common.intelligence.normalization import normalize_by_max, clamp
from common.intelligence.composite import weighted_score
from common.intelligence.growth import growth_trend

logger = logging.getLogger(__name__)


def compute_diplomatic_centrality_score() -> int:
    conn: Neo4jConnection | None = None
    try:
        conn = Neo4jConnection()

        rows = conn.run_query(
            """
            MATCH (c:Country)
            WHERE c.centrality IS NOT NULL
            RETURN c.name AS country, c.centrality AS centrality
            """
        )

        centralities = [row["centrality"] for row in rows]
        max_c = max_value(centralities)

        nodes: list[dict[str, Any]] = []
        for row in rows:
            country = row["country"]
            score = normalize_by_max(row["centrality"], max_c)
            score = clamp(score, 0.0, 1.0)
            logger.debug(
                "compute_diplomatic_centrality_score: country=%s centrality=%s score=%s",
                country,
                row["centrality"],
                score,
            )
            nodes.append({"name": country, "score": score})

        write_query = """
        UNWIND $nodes AS node
        MATCH (c:Country {name: node.name})
        SET c.diplomatic_centrality_score = node.score
        """
        conn.run_query(write_query, {"nodes": nodes})

        count = len(nodes)
        logger.info("compute_diplomatic_centrality_score: updated=%s", count)
        return count
    finally:
        if conn is not None:
            conn.close()


def compute_political_stability_score(years: list[int] | None = None) -> int:
    if years is None:
        years = list(range(2010, 2025))

    conn: Neo4jConnection | None = None
    try:
        conn = Neo4jConnection()

        rows = conn.run_query(
            """
            MATCH (c:Country)-[r:HAS_POLITICAL_SYSTEM]->(:PoliticalSystem)
            WHERE r.year IN $years
            WITH c, r ORDER BY r.year
            RETURN c.name AS country,
                   collect(r.score) AS scores,
                   collect(r.year) AS years_list
            """,
            {"years": years},
        )

        nodes: list[dict[str, Any]] = []
        for row in rows:
            country = row["country"]
            scores_list = [float(s) for s in row["scores"] if s is not None]
            if len(scores_list) == 0:
                continue

            avg_score = average(scores_list)
            trend = growth_trend(scores_list)
            stability_bonus = 0.1 if trend == "stable" else 0.0
            score = clamp(avg_score + stability_bonus, 0.0, 1.0)
            system_trend = trend

            logger.debug(
                "compute_political_stability_score: country=%s avg=%s trend=%s bonus=%s score=%s",
                country,
                avg_score,
                system_trend,
                stability_bonus,
                score,
            )
            nodes.append({"name": country, "score": score, "trend": system_trend})

        write_query = """
        UNWIND $nodes AS node
        MATCH (c:Country {name: node.name})
        SET c.political_stability_score = node.score,
            c.political_system_trend = node.trend
        """
        conn.run_query(write_query, {"nodes": nodes})

        count = len(nodes)
        logger.info("compute_political_stability_score: updated=%s", count)
        return count
    finally:
        if conn is not None:
            conn.close()


def compute_bloc_alignment_score() -> int:
    conn: Neo4jConnection | None = None
    try:
        conn = Neo4jConnection()

        alliance_rows = conn.run_query(
            """
            MATCH (c:Country)-[:MEMBER_OF]->(a:Alliance)
            RETURN c.name AS country, count(a) AS alliance_count
            """
        )

        bloc_rows = conn.run_query(
            """
            MATCH (c:Country)
            WHERE c.bloc_id IS NOT NULL
            RETURN c.name AS country
            """
        )

        alliance_counts: dict[str, int] = {r["country"]: int(r["alliance_count"]) for r in alliance_rows}
        has_bloc_set: set[str] = {r["country"] for r in bloc_rows}

        max_alliances = max_value(list(alliance_counts.values())) if alliance_counts else 1.0

        countries = set(alliance_counts.keys()) | has_bloc_set
        nodes: list[dict[str, Any]] = []
        for country in countries:
            alliance_count = alliance_counts.get(country, 0)
            norm_alliances = normalize_by_max(float(alliance_count), max_alliances)
            has_bloc = 1.0 if country in has_bloc_set else 0.0
            score = clamp(
                weighted_score(
                    {"alliances": norm_alliances, "bloc": has_bloc},
                    {"alliances": 0.7, "bloc": 0.3},
                ),
                0.0,
                1.0,
            )

            logger.debug(
                "compute_bloc_alignment_score: country=%s alliances=%s norm_alliances=%s has_bloc=%s score=%s",
                country,
                alliance_count,
                norm_alliances,
                has_bloc,
                score,
            )
            nodes.append(
                {"name": country, "score": score, "alliance_count": alliance_count}
            )

        write_query = """
        UNWIND $nodes AS node
        MATCH (c:Country {name: node.name})
        SET c.bloc_alignment_score = node.score,
            c.alliance_count = node.alliance_count
        """
        conn.run_query(write_query, {"nodes": nodes})

        count = len(nodes)
        logger.info("compute_bloc_alignment_score: updated=%s", count)
        return count
    finally:
        if conn is not None:
            conn.close()


def compute_sanctions_vulnerability_score() -> int:
    conn: Neo4jConnection | None = None
    try:
        conn = Neo4jConnection()

        sanctioned_rows = conn.run_query(
            """
            MATCH ()-[s:IMPOSED_SANCTIONS_ON]->(target:Country)
            RETURN DISTINCT target.name AS country
            """
        )
        sanctioned_countries: set[str] = {r["country"] for r in sanctioned_rows}

        all_rows = conn.run_query(
            """
            MATCH (c:Country)
            RETURN c.name AS country
            """
        )

        nodes: list[dict[str, Any]] = []
        for row in all_rows:
            country = row["country"]
            score = 1.0 if country in sanctioned_countries else 0.0
            is_sanctioned = True if country in sanctioned_countries else False
            # score already in [0, 1], but clamp anyway to satisfy spec.
            score = clamp(score, 0.0, 1.0)
            logger.debug(
                "compute_sanctions_vulnerability_score: country=%s sanctioned=%s score=%s",
                country,
                is_sanctioned,
                score,
            )
            nodes.append(
                {"name": country, "score": score, "is_sanctioned": is_sanctioned}
            )

        write_query = """
        UNWIND $nodes AS node
        MATCH (c:Country {name: node.name})
        SET c.sanctions_vulnerability_score = node.score,
            c.is_sanctioned = node.is_sanctioned
        """
        conn.run_query(write_query, {"nodes": nodes})

        count = len(nodes)
        logger.info(
            "compute_sanctions_vulnerability_score: sanctioned=%s countries, updated=%s",
            len(sanctioned_countries),
            count,
        )
        return count
    finally:
        if conn is not None:
            conn.close()


def compute_geopolitical_influence_score() -> int:
    conn: Neo4jConnection | None = None
    try:
        conn = Neo4jConnection()

        rows = conn.run_query(
            """
            MATCH (c:Country)
            WHERE c.diplomatic_centrality_score IS NOT NULL
              AND c.political_stability_score IS NOT NULL
              AND c.bloc_alignment_score IS NOT NULL
            RETURN c.name AS country,
                   c.diplomatic_centrality_score AS centrality,
                   c.political_stability_score AS stability,
                   c.bloc_alignment_score AS alignment
            """
        )

        nodes: list[dict[str, Any]] = []
        for row in rows:
            country = row["country"]
            score = weighted_score(
                metrics={
                    "centrality": row["centrality"],
                    "stability": row["stability"],
                    "alignment": row["alignment"],
                },
                weights={
                    "centrality": 0.5,
                    "stability": 0.3,
                    "alignment": 0.2,
                },
            )
            score = clamp(score, 0.0, 1.0)
            logger.debug(
                "compute_geopolitical_influence_score: country=%s centrality=%s stability=%s alignment=%s score=%s",
                country,
                row["centrality"],
                row["stability"],
                row["alignment"],
                score,
            )
            nodes.append({"name": country, "score": score})

        write_query = """
        UNWIND $nodes AS node
        MATCH (c:Country {name: node.name})
        SET c.geopolitical_influence_score = node.score
        """
        conn.run_query(write_query, {"nodes": nodes})

        count = len(nodes)
        logger.info("compute_geopolitical_influence_score: updated=%s", count)
        return count
    finally:
        if conn is not None:
            conn.close()


def compute_all_geopolitics_scores(years: list[int] | None = None) -> dict[str, int]:
    if years is None:
        years = list(range(2010, 2025))

    start = time.perf_counter()
    results: dict[str, int] = {}

    stage_name = "compute_diplomatic_centrality_score"
    logger.info("compute_all_geopolitics_scores: stage %s", stage_name)
    results[stage_name] = compute_diplomatic_centrality_score()

    stage_name = "compute_political_stability_score"
    logger.info("compute_all_geopolitics_scores: stage %s", stage_name)
    results[stage_name] = compute_political_stability_score(years)

    stage_name = "compute_bloc_alignment_score"
    logger.info("compute_all_geopolitics_scores: stage %s", stage_name)
    results[stage_name] = compute_bloc_alignment_score()

    stage_name = "compute_sanctions_vulnerability_score"
    logger.info("compute_all_geopolitics_scores: stage %s", stage_name)
    results[stage_name] = compute_sanctions_vulnerability_score()

    stage_name = "compute_geopolitical_influence_score"
    logger.info("compute_all_geopolitics_scores: stage %s", stage_name)
    results[stage_name] = compute_geopolitical_influence_score()

    elapsed = time.perf_counter() - start
    logger.info("compute_all_geopolitics_scores: total_elapsed_seconds=%s", elapsed)
    return results


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    result = compute_all_geopolitics_scores()
    for stage, count in result.items():
        print(f"  {stage}: {count} countries updated")