from __future__ import annotations

import logging

from common.db import Neo4jConnection
from common.graph_ops import GraphOps
from common.intelligence.aggregation import max_value
from common.ontology import (
    DEPENDS_ON_ENERGY_FROM,
    HAS_HIGH_DEPENDENCY_ON,
    HAS_TRADE_DEPENDENCY_ON,
    IS_MAJOR_EXPORT_PARTNER_OF,
    get_relation_type,
)
from common.config import DEFAULT_HIGH_DEPENDENCY_THRESHOLD

logger = logging.getLogger(__name__)

# Prevent unused-import lint for mandatory aggregation import.
_ = max_value

def _batch_upsert_relationships(
    conn: Neo4jConnection,
    rows: list[dict],
    rel_type: str,
    src_label: str = "Country",
    tgt_label: str = "Country",
    batch_size: int = 500,
) -> int:
    """
    Bulk write relationships using UNWIND — avoids deadlocks
    and is orders of magnitude faster than per-row writes.
    """
    from common.config import EDGE_SCHEMA
    wrote = 0
    for start in range(0, len(rows), batch_size):
        batch = rows[start: start + batch_size]
        records = []
        for row in batch:
            props = EDGE_SCHEMA.copy()
            props.update(row["properties"])
            if "year" in props and props["year"] is not None:
                props["year"] = int(props["year"])
            records.append({
                "source": row["source"],
                "target": row["target"],
                "props": props,
            })
        query = f"""
        UNWIND $rows AS row
        MERGE (a:{src_label} {{name: row.source}})
        MERGE (b:{tgt_label} {{name: row.target}})
        MERGE (a)-[r:{rel_type} {{year: toInteger(row.props.year)}}]->(b)
        SET r += row.props
        SET r.year = toInteger(row.props.year)
        """
        conn.run_query(query, {"rows": records})
        wrote += len(batch)
    return wrote


def compute_high_dependency_edges(
    threshold: float | None = None,
    year: int = 2024,
) -> int:
    if threshold is None:
        threshold = DEFAULT_HIGH_DEPENDENCY_THRESHOLD

    conn = Neo4jConnection()
    try:
        query = """
            MATCH (a:Country)-[r:EXPORTS_TO]->(b:Country)
            WHERE r.year = $year AND r.dependency >= $threshold
            RETURN a.name AS source, b.name AS target,
                   r.dependency AS dependency
        """
        rows = conn.run_query(query, {"year": year, "threshold": threshold})

        batch_rows = [
            {
                "source": row["source"],
                "target": row["target"],
                "properties": {
                    "value": row["dependency"],
                    "normalized_weight": row["dependency"],
                    "dependency": row["dependency"],
                    "year": year,
                },
            }
            for row in rows
            if row.get("source") and row.get("target")
        ]

        wrote = _batch_upsert_relationships(
            conn, batch_rows, HAS_HIGH_DEPENDENCY_ON
        )
        logger.info(
            "compute_high_dependency_edges: year=%d threshold=%s wrote=%d",
            year, threshold, wrote,
        )
        return wrote
    finally:
        conn.close()


def compute_major_partner_edges(
    top_n: int = 3,
    year: int = 2024,
) -> int:
    conn = Neo4jConnection()
    try:
        query = """
            MATCH (a:Country)-[r:EXPORTS_TO]->(b:Country)
            WHERE r.year = $year
            WITH a, b, r
            ORDER BY r.normalized_weight DESC
            WITH a, collect({
                target: b.name,
                nw: r.normalized_weight,
                dep: r.dependency
            })[0..$top_n] AS top_partners
            UNWIND top_partners AS partner
            RETURN a.name AS source,
                   partner.target AS target,
                   partner.nw AS normalized_weight,
                   partner.dep AS dependency
        """
        rows = conn.run_query(query, {"year": year, "top_n": top_n})

        # Track rank per source country
        rank_counter: dict[str, int] = {}
        batch_rows = []
        for row in rows:
            if not row.get("source") or not row.get("target"):
                continue
            source = row["source"]
            rank_counter[source] = rank_counter.get(source, 0) + 1
            rank = rank_counter[source]
            batch_rows.append({
                "source": source,
                "target": row["target"],
                "properties": {
                    "value": row["normalized_weight"],
                    "normalized_weight": row["normalized_weight"],
                    "dependency": row["dependency"],
                    "year": year,
                    "rank": rank,
                },
            })

        wrote = _batch_upsert_relationships(
            conn, batch_rows, IS_MAJOR_EXPORT_PARTNER_OF
        )
        logger.info(
            "compute_major_partner_edges: year=%d top_n=%d wrote=%d",
            year, top_n, wrote,
        )
        return wrote
    finally:
        conn.close()


def compute_trade_dependency_edges(year: int = 2024) -> int:
    conn = Neo4jConnection()
    try:
        query = """
            MATCH (a:Country)-[r:EXPORTS_TO]->(b:Country)
            WHERE r.year = $year
            RETURN a.name AS source, b.name AS target,
                   r.dependency AS dependency,
                   r.normalized_weight AS normalized_weight
        """
        rows = conn.run_query(query, {"year": year})

        batch_rows = [
            {
                "source": row["source"],
                "target": row["target"],
                "properties": {
                    "value": row["dependency"],
                    "normalized_weight": row["normalized_weight"],
                    "dependency": row["dependency"],
                    "year": year,
                },
            }
            for row in rows
            if row.get("source") and row.get("target")
        ]

        wrote = _batch_upsert_relationships(
            conn, batch_rows, HAS_TRADE_DEPENDENCY_ON
        )
        logger.info(
            "compute_trade_dependency_edges: year=%d wrote=%d", year, wrote
        )
        return wrote
    finally:
        conn.close()


def compute_energy_dependency_edges(year: int = 2024) -> int:
    conn = Neo4jConnection()
    try:
        query = """
            MATCH (a:Country)-[r:IMPORTS_ENERGY_FROM]->(b:Country)
            WHERE r.year = $year
            RETURN a.name AS source, b.name AS target,
                   r.dependency AS dependency,
                   r.normalized_weight AS normalized_weight
        """
        rows = conn.run_query(query, {"year": year})

        batch_rows = [
            {
                "source": row["source"],
                "target": row["target"],
                "properties": {
                    "value": row["dependency"],
                    "normalized_weight": row["normalized_weight"],
                    "dependency": row["dependency"],
                    "year": year,
                },
            }
            for row in rows
            if row.get("source") and row.get("target")
        ]

        wrote = _batch_upsert_relationships(
            conn, batch_rows, DEPENDS_ON_ENERGY_FROM
        )
        logger.info(
            "compute_energy_dependency_edges: year=%d wrote=%d", year, wrote
        )
        return wrote
    finally:
        conn.close()

def compute_all_derived(
    year: int = 2024,
) -> dict[str, int]:
    high_dep = compute_high_dependency_edges(year=year)
    major_partners = compute_major_partner_edges(year=year)
    trade_dep = compute_trade_dependency_edges(year=year)
    energy_dep = compute_energy_dependency_edges(year=year)

    total = high_dep + major_partners + trade_dep + energy_dep
    logger.info(
        "compute_all_derived: year=%d total_derived_written=%d",
        year,
        total,
    )
    return {
        "compute_high_dependency_edges": high_dep,
        "compute_major_partner_edges": major_partners,
        "compute_trade_dependency_edges": trade_dep,
        "compute_energy_dependency_edges": energy_dep,
    }

