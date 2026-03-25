from __future__ import annotations
import sys
sys.path.insert(0, '.')

import logging
from collections import defaultdict
from itertools import islice
from typing import Iterable

from common.config import EDGE_SCHEMA
from common.db import Neo4jConnection
from common.intelligence.normalization import clamp
from modules.climate.constants import BATCH_SIZE

logger = logging.getLogger(__name__)

# Node properties to set ON CREATE per label type
_NODE_PROPS: dict[str, list[str]] = {
    "ClimateEvent":      ["year", "disaster_type", "risk", "deaths", "hazard_type"],
    "EmissionsProfile":  ["co2_pc", "forest_pct", "year"],
    "LiveWeather":       ["warming", "nasa_temp", "year"],
    "Country":           [],
}


def _build_props(props: dict) -> dict:
    out = EDGE_SCHEMA.copy()
    out.update(props or {})
    out["value"] = 0.0 if out.get("value") is None else float(out["value"])
    out["normalized_weight"] = clamp(
        0.0 if out.get("normalized_weight") is None
        else float(out["normalized_weight"])
    )
    return out


def _chunked(items: list[dict], size: int) -> Iterable[list[dict]]:
    iterator = iter(items)
    while True:
        chunk = list(islice(iterator, size))
        if not chunk:
            break
        yield chunk


def _on_create_set_clause(alias: str, label: str) -> str:
    props = _NODE_PROPS.get(label, [])
    if not props:
        return ""
    lines = "\n".join(
        f"      ON CREATE SET {alias}.{p} = row.properties.{p}"
        for p in props
    )
    return lines


def _batch_write(conn: Neo4jConnection, batch: list[dict]) -> int:
    # Group by (source_label, target_label, rel_type) — labels must be
    # static strings in Cypher, they cannot be parameters.
    groups: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    for edge in batch:
        if not edge.get("source") or not edge.get("target"):
            continue
        key = (
            edge["source_label"],
            edge["target_label"],
            edge["rel"],
        )
        groups[key].append(edge)

    wrote = 0
    for (src_label, tgt_label, rel_type), edges in groups.items():
        rows = [
            {
                "source": e["source"],
                "target": e["target"],
                "properties": _build_props(e["properties"]),
            }
            for e in edges
        ]

        tgt_on_create = _on_create_set_clause("t", tgt_label)

        query = f"""
UNWIND $rows AS row
MERGE (s:{src_label} {{name: row.source}})
MERGE (t:{tgt_label} {{name: row.target}})
{tgt_on_create}
MERGE (s)-[r:{rel_type}]->(t)
SET r += row.properties
"""
        conn.run_query(query, {"rows": rows})
        wrote += len(rows)

    return wrote


def load_to_graph(edges: list[dict]) -> int:
    if not edges:
        return 0
    conn = Neo4jConnection()
    try:
        wrote = 0
        failed = 0
        for batch in _chunked(edges, BATCH_SIZE):
            try:
                wrote += _batch_write(conn, batch)
            except Exception:
                failed += len(batch)
                logger.error("Batch write failed", exc_info=True)
        logger.info("load_to_graph: wrote=%d failed=%d total=%d", wrote, failed, len(edges))
        return wrote
    finally:
        conn.close()


def load_all_to_graph(enriched: dict[str, list[dict]]) -> None:
    logger.info(
        "Climate graph load starting: disasters=%d emissions=%d "
        "temperature=%d earthquakes=%d hazard=%d",
        len(enriched.get("disasters", [])),
        len(enriched.get("emissions", [])),
        len(enriched.get("temperature", [])),
        len(enriched.get("earthquakes", [])),
        len(enriched.get("hazard", [])),
    )
    for key in ("disasters", "emissions", "temperature", "earthquakes", "hazard"):
        rows = enriched.get(key, [])
        if rows:
            load_to_graph(rows)
            logger.info("Loaded %s: %d rows", key, len(rows))
    logger.info("Climate graph load complete")