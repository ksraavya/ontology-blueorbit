"""
Geopolitics query module for Neo4j knowledge graph.

Provides read-only Cypher queries for country geopolitics, diplomatic networks,
blocs, and centrality rankings.
"""

from __future__ import annotations

import sys
from typing import Any, Dict, List

sys.path.insert(0, ".")

from common.db import Neo4jConnection


def get_country_geopolitics(country_name: str) -> Dict[str, Any]:
    """
    Fetch geopolitics summary for a country.

    Returns democracy score, system type, centrality, bloc_id, and the top 5
    diplomatic partners by alignment_score.
    """
    conn = Neo4jConnection()
    try:
        rows = conn.run_query(
            """
            MATCH (c:Country {name: $name})
            OPTIONAL MATCH (c)-[ps:HAS_POLITICAL_SYSTEM]->(p:PoliticalSystem)
            WITH c, AVG(ps.score) AS democracy_score, COLLECT(DISTINCT p.name)[0] AS system_type
            OPTIONAL MATCH (c)-[d:DIPLOMATIC_INTERACTION]->(partner:Country)
            WHERE d.alignment_score IS NOT NULL
            WITH c, democracy_score, system_type, partner, MAX(d.alignment_score) AS best_score
            WHERE partner IS NOT NULL
            WITH c, democracy_score, system_type, partner, best_score
            ORDER BY best_score DESC
            WITH c, democracy_score, system_type,
                 COLLECT(DISTINCT {partner: partner.name, score: best_score})[0..5] AS top_partners
            RETURN c.name AS country,
                   democracy_score,
                   system_type,
                   c.centrality AS centrality,
                   c.bloc_id AS bloc_id,
                   top_partners
            """,
            {"name": country_name},
        )
        return rows[0] if rows else {}
    finally:
        conn.close()


def get_diplomatic_network(min_score: float = 0.3) -> List[Dict[str, Any]]:
    """
    Return all country pairs with alignment_score above the threshold.

    Intended for network visualization.
    """
    conn = Neo4jConnection()
    try:
        return conn.run_query(
            """
            MATCH (a:Country)-[r:DIPLOMATIC_INTERACTION]->(b:Country)
            WHERE r.alignment_score > $min_score
            WITH a, b, MAX(r.alignment_score) AS alignment_score
            RETURN a.name AS country1,
                   b.name AS country2,
                   alignment_score
            ORDER BY alignment_score DESC
            """,
            {"min_score": min_score},
        )
    finally:
        conn.close()


def get_blocs() -> List[Dict[str, Any]]:
    """
    Return countries grouped by bloc_id.

    Each item has bloc_id and a list of member country names.
    """
    conn = Neo4jConnection()
    try:
        rows = conn.run_query(
            """
            MATCH (c:Country)
            WHERE c.bloc_id IS NOT NULL
            WITH c.bloc_id AS bloc_id, COLLECT(c.name) AS members
            RETURN bloc_id, members
            ORDER BY bloc_id
            """
        )
        return [{"bloc_id": r["bloc_id"], "members": sorted(r["members"])} for r in rows]
    finally:
        conn.close()


def get_top_central_countries(limit: int = 20) -> List[Dict[str, Any]]:
    """
    Return top N countries by centrality (eigenvector centrality) descending.
    """
    conn = Neo4jConnection()
    try:
        return conn.run_query(
            """
            MATCH (c:Country)
            WHERE c.centrality IS NOT NULL
            RETURN c.name AS country, c.centrality AS centrality
            ORDER BY c.centrality DESC
            LIMIT $limit
            """,
            {"limit": limit},
        )
    finally:
        conn.close()


if __name__ == "__main__":
    test_country = "India"

    print("--- get_country_geopolitics ---")
    result = get_country_geopolitics(test_country)
    print(result)
    print()

    print("--- get_diplomatic_network(min_score=0.3) ---")
    network = get_diplomatic_network(min_score=0.3)
    print(f"Found {len(network)} edges")
    for edge in network[:5]:
        print(f"  {edge}")
    print()

    print("--- get_blocs ---")
    blocs = get_blocs()
    for b in blocs:
        print(f"  Bloc {b['bloc_id']}: {b['members'][:5]}...")
    print()

    print("--- get_top_central_countries(limit=20) ---")
    top = get_top_central_countries(limit=20)
    for i, row in enumerate(top, 1):
        print(f"  {i}. {row['country']}: {row['centrality']:.6f}")
