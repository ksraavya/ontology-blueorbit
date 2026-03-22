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


def get_country_voting_alignment(
    country_name: str,
    year: int | None = None,
) -> List[Dict[str, Any]]:
    """
    Returns top 10 countries most aligned with given country
    based on UN General Assembly voting similarity.

    If year is provided filter by that specific year.
    If year is None return most recent year available.

    source filter: r.source = "UNGA"
    """
    conn = Neo4jConnection()
    try:
        if year is None:
            return conn.run_query(
                """
                MATCH (a:Country {name: $name})-[r:ALIGNED_WITH]->(b:Country)
                WHERE r.source = "UNGA"
                WITH a, b, r
                ORDER BY r.year DESC, r.vote_similarity DESC
                WITH a, b, head(collect(r)) AS latest
                RETURN b.name AS country,
                       latest.vote_similarity AS vote_similarity,
                       latest.year AS year,
                       latest.agreements AS agreements,
                       latest.total_votes AS total_votes
                ORDER BY vote_similarity DESC
                LIMIT 10
                """,
                {"name": country_name},
            )
        return conn.run_query(
            """
            MATCH (a:Country {name: $name})-[r:ALIGNED_WITH]->(b:Country)
            WHERE r.source = "UNGA"
            AND r.year = $year
            RETURN b.name AS country,
                   r.vote_similarity AS vote_similarity,
                   r.year AS year,
                   r.agreements AS agreements,
                   r.total_votes AS total_votes
            ORDER BY vote_similarity DESC
            LIMIT 10
            """,
            {"name": country_name, "year": year},
        )
    finally:
        conn.close()


def get_voting_blocs(
    year: int | None = None,
    min_similarity: float = 0.85,
) -> List[Dict[str, Any]]:
    """
    Returns country pairs with very high UN voting similarity.

    Default min_similarity = 0.85 meaning countries that voted
    the same way on at least 85% of resolutions they both
    participated in.

    If year provided filter by that year.
    If year is None return latest year available.

    source filter: r.source = "UNGA"
    """
    conn = Neo4jConnection()
    try:
        if year is None:
            return conn.run_query(
                """
                MATCH (a:Country)-[r:ALIGNED_WITH]->(b:Country)
                WHERE r.source = "UNGA"
                AND r.vote_similarity >= $min_similarity
                WITH a, b, r
                ORDER BY r.year DESC
                WITH a, b, head(collect(r)) AS latest
                WHERE latest.vote_similarity >= $min_similarity
                RETURN a.name AS country_a,
                       b.name AS country_b,
                       latest.vote_similarity AS vote_similarity,
                       latest.year AS year,
                       latest.agreements AS agreements,
                       latest.total_votes AS total_votes
                ORDER BY vote_similarity DESC
                LIMIT 50
                """,
                {"min_similarity": min_similarity},
            )
        return conn.run_query(
            """
            MATCH (a:Country)-[r:ALIGNED_WITH]->(b:Country)
            WHERE r.source = "UNGA"
            AND r.vote_similarity >= $min_similarity
            AND r.year = $year
            RETURN a.name AS country_a,
                   b.name AS country_b,
                   r.vote_similarity AS vote_similarity,
                   r.year AS year,
                   r.agreements AS agreements,
                   r.total_votes AS total_votes
            ORDER BY vote_similarity DESC
            LIMIT 50
            """,
            {"min_similarity": min_similarity, "year": year},
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

    print("\n=== India Voting Alignment (latest year) ===")
    results = get_country_voting_alignment("India")
    for r in results:
        print(f"  {r['country']}: {r['vote_similarity']} ({r['year']})")

    print("\n=== India Voting Alignment (2024) ===")
    results = get_country_voting_alignment("India", year=2024)
    for r in results:
        print(f"  {r['country']}: {r['vote_similarity']}")

    print("\n=== Tight Voting Blocs (>= 0.85 similarity) ===")
    blocs = get_voting_blocs(min_similarity=0.85)
    for b in blocs[:10]:
        print(f"  {b['country_a']} — {b['country_b']}: {b['vote_similarity']}")

    print("\n=== Very Tight Blocs (>= 0.95 similarity) ===")
    blocs = get_voting_blocs(min_similarity=0.95)
    for b in blocs:
        print(f"  {b['country_a']} — {b['country_b']}: {b['vote_similarity']}")
