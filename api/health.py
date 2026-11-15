from __future__ import annotations

"""
GET /health — is the API up, is Neo4j reachable, what data is loaded.

The frontend's "Data status" panel reads this so an empty or half-loaded
graph shows up as "economy: not loaded" instead of blank charts.
Label / relationship counts come from Neo4j's count store, so they are
O(1) per type even on a large graph.
"""

import time
from typing import Any
from urllib.parse import urlparse

from fastapi import APIRouter

from common import llm
from common.db import Neo4jConnection

router = APIRouter(tags=["health"])

_cache: tuple[float, dict[str, Any]] = (0.0, {})
_TTL_S = 30

# Which relationship types prove each domain's pipeline has run.
DOMAIN_MARKERS: dict[str, list[str]] = {
    "economy":     ["EXPORTS_TO", "HAS_TRADE_VOLUME_WITH", "HAS_TRADE_DEPENDENCY_ON", "HAS_GDP",
                    "IMPORTS_ENERGY_FROM", "IMPOSED_SANCTIONS_ON", "HAS_TRADE_AGREEMENT_WITH"],
    "defense":     ["SPENDS_ON_DEFENSE", "EXPORTS_ARMS", "HAS_CONFLICT_STATS", "MEMBER_OF"],
    "geopolitics": ["DIPLOMATIC_INTERACTION", "ALIGNED_WITH", "OPPOSES", "HAS_POLITICAL_SYSTEM"],
    "climate":     ["EXPERIENCED", "EMITS", "HAS_RESOURCE_STRESS", "DISRUPTS_SUPPLY_CHAIN"],
    "live_news":   ["CO_MENTIONED_WITH", "HAS_CONFLICT_SIGNAL"],
}

SCORE_MARKERS = [
    "global_risk_score", "geopolitical_influence_score", "trade_vulnerability_score",
    "economic_influence_score", "conflict_risk_score", "climate_risk_score",
    "defense_spending_score", "diplomatic_centrality_score",
]


def _snapshot() -> dict[str, Any]:
    conn = Neo4jConnection()
    labels = [r["label"] for r in conn.run_query("CALL db.labels() YIELD label RETURN label")]
    rel_types = [r["relationshipType"] for r in conn.run_query(
        "CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType")]

    node_counts = {}
    for lbl in labels:
        q = f"MATCH (n:`{lbl}`) RETURN count(n) AS c"
        node_counts[lbl] = conn.run_query(q)[0]["c"]
    rel_counts = {}
    for t in rel_types:
        q = f"MATCH ()-[r:`{t}`]->() RETURN count(r) AS c"
        rel_counts[t] = conn.run_query(q)[0]["c"]

    score_coverage = {}
    if node_counts.get("Country"):
        props = ", ".join(f"count(c.{p}) AS {p}" for p in SCORE_MARKERS)
        score_coverage = conn.run_query(f"MATCH (c:Country) RETURN {props}")[0]

    domains = {}
    for domain, markers in DOMAIN_MARKERS.items():
        present = {m: rel_counts.get(m, 0) for m in markers}
        domains[domain] = {
            "loaded": any(v > 0 for v in present.values()),
            "edges": present,
        }

    years = conn.run_query(
        "MATCH ()-[r:HAS_TRADE_VOLUME_WITH]->() WHERE r.year IS NOT NULL "
        "RETURN min(toInteger(r.year)) AS min_year, max(toInteger(r.year)) AS max_year"
    )
    return {
        "node_counts": node_counts,
        "relationship_counts": rel_counts,
        "total_nodes": sum(node_counts.values()),
        "total_relationships": sum(rel_counts.values()),
        "countries": node_counts.get("Country", 0),
        "score_coverage": score_coverage,
        "domains": domains,
        "trade_years": years[0] if years else {},
    }


@router.get("/health")
def health() -> dict[str, Any]:
    global _cache
    import os
    uri = os.getenv("NEO4J_URI", "")
    out: dict[str, Any] = {
        "api": "ok",
        "neo4j": {"host": urlparse(uri).hostname, "status": "unknown"},
        "llm": llm.status(),
    }
    loaded_at, snap = _cache
    try:
        if not snap or time.time() - loaded_at > _TTL_S:
            snap = _snapshot()
            _cache = (time.time(), snap)
        out["neo4j"]["status"] = "ok"
        out["graph"] = snap
        meta = Neo4jConnection().run_query("MATCH (m:Meta {key: 'graph'}) RETURN m.built_at AS t")
        out["built_at"] = meta[0]["t"] if meta else None
    except Exception as exc:
        out["neo4j"]["status"] = "error"
        out["neo4j"]["error"] = str(exc)[:300]
    return out
