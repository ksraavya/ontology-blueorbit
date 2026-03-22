from __future__ import annotations

import logging

from common.db import Neo4jConnection
from common.graph_ops import GraphOps

logger = logging.getLogger(__name__)


def load_to_graph(data: list[dict]) -> None:
    if not data:
        logger.warning("load_to_graph called with empty data list")
        return

    conn = Neo4jConnection()
    graph_ops = GraphOps(conn)
    try:
        wrote = 0
        for item in data:
            try:
                graph_ops.create_relationship(
                    source=item["source"],
                    target=item["target"],
                    rel_type=item["rel"],
                    properties=item["properties"],
                    source_label=item["source_label"],
                    target_label=item["target_label"],
                )
                wrote += 1
            except Exception as exc:
                logger.error(
                    "Relationship write failed: source=%r target=%r rel=%r error=%s",
                    item.get("source"),
                    item.get("target"),
                    item.get("rel"),
                    exc,
                )
        logger.info("load_to_graph: wrote %d relationships", wrote)
    finally:
        conn.close()


def load_all_to_graph(enriched: dict[str, list[dict]]) -> None:
    logger.info(
        "Starting graph load: macro=%d trade=%d energy=%d volume=%d",
        len(enriched.get("macro", [])),
        len(enriched.get("trade", [])),
        len(enriched.get("energy", [])),
        len(enriched.get("volume", [])),
    )
    load_to_graph(enriched["macro"])
    load_to_graph(enriched["trade"])
    load_to_graph(enriched["energy"])
    load_to_graph(enriched["volume"])
    # orgs intentionally skipped — handled by geopolitics module
    logger.info("Graph load complete")
