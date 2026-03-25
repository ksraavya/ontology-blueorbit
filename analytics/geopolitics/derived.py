import sys

sys.path.insert(0, ".")

import logging
from typing import Any

from common.db import Neo4jConnection
from common.graph_ops import GraphOps
from common.intelligence.normalization import clamp

logger = logging.getLogger(__name__)


def compute_aligned_with_edges(min_score: float = 0.5) -> int:
    """
    Create GDELT-based ALIGNED_WITH edges (diplomatic basis).
    """
    count = 0
    min_score_clamped = clamp(float(min_score), 0.0, 1.0)

    conn = Neo4jConnection()
    try:
        ops = GraphOps(conn)
        query = """
        MATCH (a:Country)-[r:DIPLOMATIC_INTERACTION]->(b:Country)
        WHERE r.alignment_score >= $min_score
        WITH a.name AS country_a, b.name AS country_b,
             MAX(r.alignment_score) AS alignment_score
        RETURN country_a, country_b, alignment_score
        """
        rows = conn.run_query(query, {"min_score": min_score_clamped})

        for row in rows:
            score = clamp(float(row["alignment_score"]), 0.0, 1.0)
            ops.create_relationship(
                source=row["country_a"],
                target=row["country_b"],
                rel_type="ALIGNED_WITH",
                properties={
                    "value": score,
                    "normalized_weight": score,
                    "year": 2024,
                    "basis": "diplomatic",
                    "confidence": clamp(0.8, 0.0, 1.0),
                },
            )
            count += 1
    finally:
        conn.close()

    logger.info(
        "compute_aligned_with_edges: min_score=%s wrote=%s", min_score_clamped, count
    )
    return count


def compute_part_of_bloc_edges() -> int:
    """
    Create PART_OF_BLOC edges for countries that share the same bloc_id.
    """
    count = 0
    num_blocs = 0

    conn = Neo4jConnection()
    try:
        ops = GraphOps(conn)
        query = """
        MATCH (c:Country)
        WHERE c.bloc_id IS NOT NULL
        RETURN c.name AS country, c.bloc_id AS bloc_id
        ORDER BY c.bloc_id
        """
        rows = conn.run_query(query)

        bloc_members: dict[Any, list[str]] = {}
        for row in rows:
            bloc_id = row["bloc_id"]
            bloc_members.setdefault(bloc_id, []).append(row["country"])

        num_blocs = sum(1 for v in bloc_members.values() if len(v) >= 2)
        for bloc_id, members in bloc_members.items():
            if len(members) < 2:
                continue

            # Sort to make pair generation deterministic and ensure a < b
            members_sorted = sorted(members)
            for i in range(len(members_sorted)):
                a = members_sorted[i]
                for j in range(i + 1, len(members_sorted)):
                    b = members_sorted[j]
                    if a >= b:
                        continue

                    ops.create_relationship(
                        source=a,
                        target=b,
                        rel_type="PART_OF_BLOC",
                        properties={
                            "value": clamp(1.0, 0.0, 1.0),
                            "normalized_weight": clamp(1.0, 0.0, 1.0),
                            "bloc_id": int(bloc_id),
                            "year": 2024,
                            "confidence": clamp(0.8, 0.0, 1.0),
                        },
                    )
                    count += 1
    finally:
        conn.close()

    logger.info(
        "compute_part_of_bloc_edges: blocs=%s edges_written=%s",
        num_blocs,
        count,
    )
    return count


def compute_opposes_edges() -> int:
    """
    Create OPPOSES edges based on sanctions first, then diplomatic low alignment.
    """
    count = 0
    seen: set[tuple[str, str]] = set()
    sanctions_written = 0
    diplomatic_written = 0

    conn = Neo4jConnection()
    try:
        ops = GraphOps(conn)

        # Step A — sanctions-based opposition
        sanctions_query = """
        MATCH (sanctioner:Country)-[:IMPOSED_SANCTIONS_ON]->(target:Country)
        RETURN sanctioner.name AS country_a, target.name AS country_b,
               "sanctions" AS basis
        """
        sanctions_rows = conn.run_query(sanctions_query)

        for row in sanctions_rows:
            country_a = row["country_a"]
            country_b = row["country_b"]
            pair_key = (country_a, country_b)
            if pair_key in seen:
                continue
            seen.add(pair_key)

            ops.create_relationship(
                source=country_a,
                target=country_b,
                rel_type="OPPOSES",
                properties={
                    "value": clamp(1.0, 0.0, 1.0),
                    "normalized_weight": clamp(1.0, 0.0, 1.0),
                    "basis": row["basis"],
                    "year": 2024,
                    "confidence": clamp(0.8, 0.0, 1.0),
                },
            )
            count += 1
            sanctions_written += 1

        # Step B — diplomatic opposition (very low alignment)
        diplomatic_query = """
        MATCH (a:Country)-[r:DIPLOMATIC_INTERACTION]->(b:Country)
        WHERE r.alignment_score IS NOT NULL AND r.alignment_score < 0.2
        WITH a.name AS country_a, b.name AS country_b,
             MAX(r.alignment_score) AS score
        RETURN country_a, country_b, "diplomatic" AS basis
        """
        diplomatic_rows = conn.run_query(diplomatic_query)

        for row in diplomatic_rows:
            country_a = row["country_a"]
            country_b = row["country_b"]
            pair_key = (country_a, country_b)
            if pair_key in seen:
                continue
            seen.add(pair_key)

            ops.create_relationship(
                source=country_a,
                target=country_b,
                rel_type="OPPOSES",
                properties={
                    "value": clamp(1.0, 0.0, 1.0),
                    "normalized_weight": clamp(1.0, 0.0, 1.0),
                    "basis": row["basis"],
                    "year": 2024,
                    "confidence": clamp(0.8, 0.0, 1.0),
                },
            )
            count += 1
            diplomatic_written += 1
    finally:
        conn.close()

    logger.info(
        "compute_opposes_edges: sanctions_pairs=%s diplomatic_pairs=%s total_written=%s",
        sanctions_written,
        diplomatic_written,
        count,
    )
    return count


def compute_all_derived() -> dict[str, int]:
    """
    Run all derived geopolitics relationship computations in order.
    """
    results: dict[str, int] = {}

    stages: list[tuple[str, Any]] = [
        ("compute_aligned_with_edges", compute_aligned_with_edges),
        ("compute_part_of_bloc_edges", compute_part_of_bloc_edges),
        ("compute_opposes_edges", compute_opposes_edges),
    ]

    for stage_name, fn in stages:
        logger.info("compute_all_derived: stage %s", stage_name)
        results[stage_name] = fn()

    return results


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    result = compute_all_derived()
    for stage, count in result.items():
        print(f"  {stage}: {count} edges written")