"""
Analytics-layer geopolitics queries.
Extends the 4 original module queries with new profile, bilateral,
sanctions, and opposition functions for the API layer.
"""
from __future__ import annotations

import sys
from typing import Any, Dict, List, Optional

sys.path.insert(0, ".")

from common.db import Neo4jConnection


# =========================================================
# COPIED FROM modules/geopolitics/queries.py (DO NOT MODIFY ORIGINALS)
# =========================================================

def get_country_geopolitics(country_name: str) -> Dict[str, Any]:
    """
    Fetch geopolitics summary for a country.
    Returns democracy score, system type, centrality, bloc_id,
    and the top 5 diplomatic partners by alignment_score.
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


# =========================================================
# NEW FUNCTIONS
# =========================================================

def get_country_geopolitics_profile(country_name: str) -> Dict[str, Any]:
    """
    Rich geopolitics profile for a country using all score properties.

    Returns:
    - Democracy score and political system classification
    - Centrality score and bloc membership
    - Top 5 diplomatic partners (GDELT alignment)
    - Top 5 UN voting allies (UNGA similarity)
    - Alliance memberships (from defense module)
    - Region membership
    - Nuclear status and P5 status (from defense module)
    """
    conn = Neo4jConnection()
    try:
        # Core profile
        profile_rows = conn.run_query(
            """
            MATCH (c:Country {name: $name})
            OPTIONAL MATCH (c)-[ps:HAS_POLITICAL_SYSTEM]->(p:PoliticalSystem)
            WITH c, AVG(ps.score) AS democracy_score,
                 COLLECT(DISTINCT p.name)[0] AS system_type
            RETURN c.name AS country,
                   democracy_score,
                   system_type,
                   c.centrality AS centrality,
                   c.bloc_id AS bloc_id,
                   c.nuclear_status AS nuclear_status,
                   c.is_nuclear AS is_nuclear,
                   c.un_p5 AS un_p5,
                   c.is_regional_power AS is_regional_power,
                   c.live_risk_score AS live_risk_score
            """,
            {"name": country_name},
        )

        if not profile_rows:
            return {}

        profile = dict(profile_rows[0])

        # Top diplomatic partners (GDELT)
        gdelt_rows = conn.run_query(
            """
            MATCH (c:Country {name: $name})-[d:DIPLOMATIC_INTERACTION]->(partner:Country)
            WHERE d.alignment_score IS NOT NULL
            WITH partner, MAX(d.alignment_score) AS score
            RETURN partner.name AS country, score
            ORDER BY score DESC
            LIMIT 5
            """,
            {"name": country_name},
        )
        profile["top_diplomatic_partners"] = gdelt_rows

        # Top UNGA voting allies
        unga_rows = conn.run_query(
            """
            MATCH (c:Country {name: $name})-[r:ALIGNED_WITH]->(partner:Country)
            WHERE r.source = "UNGA"
            WITH partner, r ORDER BY r.year DESC, r.vote_similarity DESC
            WITH partner, HEAD(COLLECT(r)) AS latest
            RETURN partner.name AS country,
                   latest.vote_similarity AS vote_similarity,
                   latest.year AS year
            ORDER BY vote_similarity DESC
            LIMIT 5
            """,
            {"name": country_name},
        )
        profile["top_unga_allies"] = unga_rows

        # Alliance memberships
        alliance_rows = conn.run_query(
            """
            MATCH (c:Country {name: $name})-[:MEMBER_OF]->(a:Alliance)
            RETURN a.name AS alliance
            """,
            {"name": country_name},
        )
        profile["alliances"] = [r["alliance"] for r in alliance_rows]

        # Region
        region_rows = conn.run_query(
            """
            MATCH (c:Country {name: $name})-[:BELONGS_TO]->(r:Region)
            RETURN r.name AS region
            """,
            {"name": country_name},
        )
        profile["region"] = region_rows[0]["region"] if region_rows else None

        return profile
    finally:
        conn.close()


def get_bilateral_geopolitics(country_a: str, country_b: str) -> Dict[str, Any]:
    """
    Full bilateral geopolitics relationship between two countries.

    Returns:
    - GDELT diplomatic alignment score (both directions)
    - UNGA voting similarity (latest year)
    - Shared alliance memberships
    - Sanctions (if any direction)
    - Political system similarity
    - Bloc membership (same or different)
    """
    conn = Neo4jConnection()
    try:
        result: Dict[str, Any] = {
            "country_a": country_a,
            "country_b": country_b,
        }

        # GDELT diplomatic scores (both directions)
        diplo_rows = conn.run_query(
            """
            MATCH (a:Country {name: $ca})-[r:DIPLOMATIC_INTERACTION]->(b:Country {name: $cb})
            RETURN MAX(r.alignment_score) AS a_to_b_score,
                   MAX(r.weight) AS a_to_b_weight
            """,
            {"ca": country_a, "cb": country_b},
        )
        if diplo_rows and diplo_rows[0]["a_to_b_score"] is not None:
            result["gdelt_a_to_b"] = diplo_rows[0]["a_to_b_score"]

        diplo_rows_rev = conn.run_query(
            """
            MATCH (a:Country {name: $cb})-[r:DIPLOMATIC_INTERACTION]->(b:Country {name: $ca})
            RETURN MAX(r.alignment_score) AS b_to_a_score
            """,
            {"ca": country_a, "cb": country_b},
        )
        if diplo_rows_rev and diplo_rows_rev[0]["b_to_a_score"] is not None:
            result["gdelt_b_to_a"] = diplo_rows_rev[0]["b_to_a_score"]

        # UNGA voting similarity (latest year available)
        unga_rows = conn.run_query(
            """
            MATCH (a:Country {name: $ca})-[r:ALIGNED_WITH]->(b:Country {name: $cb})
            WHERE r.source = "UNGA"
            RETURN r.vote_similarity AS vote_similarity,
                   r.year AS year,
                   r.agreements AS agreements,
                   r.total_votes AS total_votes
            ORDER BY r.year DESC
            LIMIT 1
            """,
            {"ca": country_a, "cb": country_b},
        )
        if not unga_rows:
            # Try reverse direction
            unga_rows = conn.run_query(
                """
                MATCH (a:Country {name: $cb})-[r:ALIGNED_WITH]->(b:Country {name: $ca})
                WHERE r.source = "UNGA"
                RETURN r.vote_similarity AS vote_similarity,
                       r.year AS year,
                       r.agreements AS agreements,
                       r.total_votes AS total_votes
                ORDER BY r.year DESC
                LIMIT 1
                """,
                {"ca": country_a, "cb": country_b},
            )
        result["unga_voting"] = unga_rows[0] if unga_rows else None

        # Shared alliances
        shared_alliances = conn.run_query(
            """
            MATCH (a:Country {name: $ca})-[:MEMBER_OF]->(al:Alliance)
            MATCH (b:Country {name: $cb})-[:MEMBER_OF]->(al)
            RETURN al.name AS alliance
            """,
            {"ca": country_a, "cb": country_b},
        )
        result["shared_alliances"] = [r["alliance"] for r in shared_alliances]

        # Sanctions (either direction)
        sanctions_rows = conn.run_query(
            """
            MATCH (a:Country {name: $ca})-[s:IMPOSED_SANCTIONS_ON]->(b:Country {name: $cb})
            RETURN "a_sanctions_b" AS direction
            UNION
            MATCH (a:Country {name: $cb})-[s:IMPOSED_SANCTIONS_ON]->(b:Country {name: $ca})
            RETURN "b_sanctions_a" AS direction
            """,
            {"ca": country_a, "cb": country_b},
        )
        result["sanctions"] = [r["direction"] for r in sanctions_rows]

        # Political system scores
        ps_rows = conn.run_query(
            """
            MATCH (a:Country {name: $ca})-[r:HAS_POLITICAL_SYSTEM]->()
            WITH AVG(r.score) AS score_a
            MATCH (b:Country {name: $cb})-[r2:HAS_POLITICAL_SYSTEM]->()
            WITH score_a, AVG(r2.score) AS score_b
            RETURN score_a, score_b,
                   abs(score_a - score_b) AS political_distance
            """,
            {"ca": country_a, "cb": country_b},
        )
        if ps_rows:
            result["political_similarity"] = ps_rows[0]

        # Bloc membership
        bloc_rows = conn.run_query(
            """
            MATCH (a:Country {name: $ca}), (b:Country {name: $cb})
            RETURN a.bloc_id AS bloc_a, b.bloc_id AS bloc_b,
                   a.bloc_id = b.bloc_id AS same_bloc
            """,
            {"ca": country_a, "cb": country_b},
        )
        if bloc_rows:
            result["bloc_info"] = bloc_rows[0]

        return result
    finally:
        conn.close()


def get_sanctions_network(country_name: str) -> Dict[str, Any]:
    """
    Sanctions network for a country.

    Returns:
    - Countries this country has sanctioned (outgoing)
    - Countries that have sanctioned this country (incoming)
    """
    conn = Neo4jConnection()
    try:
        # Sanctions imposed BY this country
        imposed = conn.run_query(
            """
            MATCH (c:Country {name: $name})-[s:IMPOSED_SANCTIONS_ON]->(target:Country)
            RETURN target.name AS country,
                   s.year AS year,
                   s.source AS source
            ORDER BY target.name
            """,
            {"name": country_name},
        )

        # Sanctions imposed ON this country
        received = conn.run_query(
            """
            MATCH (sanctioner:Country)-[s:IMPOSED_SANCTIONS_ON]->(c:Country {name: $name})
            RETURN sanctioner.name AS country,
                   s.year AS year,
                   s.source AS source
            ORDER BY sanctioner.name
            """,
            {"name": country_name},
        )

        return {
            "country": country_name,
            "sanctions_imposed_on": imposed,
            "sanctions_received_from": received,
            "total_imposed": len(imposed),
            "total_received": len(received),
        }
    finally:
        conn.close()


def get_opposition_network(country_name: str) -> Dict[str, Any]:
    """
    Opposition network for a country — countries with low alignment.

    Returns:
    - Countries with lowest UNGA voting similarity (strong disagreement)
    - Countries in different diplomatic blocs
    - Any explicit OPPOSES relationships
    """
    conn = Neo4jConnection()
    try:
        # Lowest UNGA voting similarity (most opposed on votes)
        low_unga = conn.run_query(
            """
            MATCH (c:Country {name: $name})-[r:ALIGNED_WITH]->(other:Country)
            WHERE r.source = "UNGA" AND r.total_votes >= 10
            WITH other, r ORDER BY r.year DESC
            WITH other, HEAD(COLLECT(r)) AS latest
            WHERE latest.vote_similarity < 0.5
            RETURN other.name AS country,
                   latest.vote_similarity AS vote_similarity,
                   latest.year AS year
            ORDER BY vote_similarity ASC
            LIMIT 10
            """,
            {"name": country_name},
        )

        # Countries in a different diplomatic bloc
        diff_bloc = conn.run_query(
            """
            MATCH (c:Country {name: $name})
            WHERE c.bloc_id IS NOT NULL
            MATCH (other:Country)
            WHERE other.bloc_id IS NOT NULL
              AND other.bloc_id <> c.bloc_id
              AND other.centrality IS NOT NULL
            RETURN other.name AS country,
                   other.bloc_id AS bloc_id,
                   other.centrality AS centrality
            ORDER BY other.centrality DESC
            LIMIT 10
            """,
            {"name": country_name},
        )

        # Explicit OPPOSES relationships (if any in graph)
        explicit_opposes = conn.run_query(
            """
            MATCH (c:Country {name: $name})-[:OPPOSES]->(other:Country)
            RETURN other.name AS country, "explicit_opposition" AS type
            UNION
            MATCH (other:Country)-[:OPPOSES]->(c:Country {name: $name})
            RETURN other.name AS country, "opposed_by" AS type
            """,
            {"name": country_name},
        )

        return {
            "country": country_name,
            "lowest_unga_alignment": low_unga,
            "different_bloc_major_players": diff_bloc,
            "explicit_opposition": explicit_opposes,
        }
    finally:
        conn.close()