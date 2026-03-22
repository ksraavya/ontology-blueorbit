from __future__ import annotations

import logging
from typing import Any

from common.db import Neo4jConnection
from common.graph_ops import GraphOps

logger = logging.getLogger(__name__)

BATCH_SIZE = 500


def _build_props(item: dict[str, Any]) -> dict[str, Any]:
    """
    Merge EDGE_SCHEMA defaults with item properties.
    Mirrors what GraphOps._apply_edge_schema() does
    so batched writes stay schema-consistent.
    """
    from common.config import EDGE_SCHEMA
    final = EDGE_SCHEMA.copy()
    final.update(item["properties"])
    return final


def _batch_write(
    conn: Neo4jConnection,
    batch: list[dict[str, Any]],
) -> int:
    """
    Write a batch of relationships using a single UNWIND query.
    Returns number of relationships written.
    Groups by (source_label, target_label, rel_type) so the
    Cypher label is static — Neo4j requires label names in the
    query string, not as parameters.
    """
    from collections import defaultdict

    # Group by (source_label, target_label, rel_type)
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for item in batch:
        key = (item["source_label"], item["target_label"], item["rel"])
        groups[key].append(item)

    wrote = 0
    for (src_label, tgt_label, rel_type), items in groups.items():
        rows = [
            {
                "source": item["source"],
                "target": item["target"],
                "props": _build_props(item),
            }
            for item in items
        ]

        query = f"""
        UNWIND $rows AS row
        MERGE (a:{src_label} {{name: row.source}})
        MERGE (b:{tgt_label} {{name: row.target}})
        MERGE (a)-[r:{rel_type}]->(b)
        SET r += row.props
        """

        conn.run_query(query, {"rows": rows})
        wrote += len(rows)

    return wrote


def load_to_graph(data: list[dict]) -> None:
    """
    Load a list of enriched relationship dicts into Neo4j.
    Uses batched UNWIND writes (BATCH_SIZE rows per query)
    instead of one-at-a-time writes — critical for AuraDB
    where per-call latency dominates.
    """
    if not data:
        logger.warning("load_to_graph called with empty data list")
        return

    conn = Neo4jConnection()
    try:
        wrote = 0
        failed = 0

        for start in range(0, len(data), BATCH_SIZE):
            batch = data[start: start + BATCH_SIZE]
            try:
                wrote += _batch_write(conn, batch)
            except Exception as exc:
                failed += len(batch)
                logger.error(
                    "Batch write failed (rows %d-%d): %s",
                    start,
                    start + len(batch),
                    exc,
                )

        logger.info(
            "load_to_graph: wrote=%d failed=%d total=%d",
            wrote,
            failed,
            len(data),
        )
    finally:
        conn.close()


def load_all_to_graph(enriched: dict[str, list[dict]]) -> None:
    logger.info(
        "Starting graph load: macro=%d trade=%d energy=%d "
        "volume=%d balance=%d",
        len(enriched.get("macro", [])),
        len(enriched.get("trade", [])),
        len(enriched.get("energy", [])),
        len(enriched.get("volume", [])),
        len(enriched.get("balance", [])),
    )
    load_to_graph(enriched["macro"])
    load_to_graph(enriched["trade"])
    load_to_graph(enriched["energy"])
    load_to_graph(enriched["volume"])
    load_to_graph(enriched.get("balance", []))
    # orgs intentionally skipped — handled by geopolitics module
    logger.info("Graph load complete")