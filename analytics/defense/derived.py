from __future__ import annotations

import sys

sys.path.insert(0, ".")

import logging

from common.db import Neo4jConnection
from common.graph_ops import GraphOps
from common.intelligence.normalization import clamp
from common.ontology import ALIGNED_WITH, IS_HIGH_RISK_FOR, IS_INFLUENTIAL_TO

logger = logging.getLogger(__name__)

# Imported per module contract/prompt for ontology compatibility.
_ = ALIGNED_WITH


def compute_high_conflict_edges(threshold: float = 0.35, year: int = 2024) -> int:
    """
    Create IS_HIGH_RISK_FOR edges between high-conflict countries in same region.
    """
    conn = Neo4jConnection()
    try:
        query = """
        MATCH (c:Country)-[:BELONGS_TO]->(r:Region)<-[:BELONGS_TO]-(other:Country)
        WHERE c.conflict_risk_score >= $threshold
          AND other.conflict_risk_score >= $threshold
          AND c.name <> other.name
          AND r IS NOT NULL
        RETURN c.name AS country_a, other.name AS country_b,
               r.name AS region,
               c.conflict_risk_score AS score_a,
               other.conflict_risk_score AS score_b
        """
        rows = conn.run_query(query, {"threshold": threshold})

        ops = GraphOps(conn)
        created = 0
        for row in rows:
            score_a = float(row.get("score_a", 0.0) or 0.0)
            score_b = float(row.get("score_b", 0.0) or 0.0)
            combined_risk = clamp((score_a + score_b) / 2.0, 0.0, 1.0)

            ops.create_relationship(
                source=row["country_a"],
                target=row["country_b"],
                rel_type=IS_HIGH_RISK_FOR,
                properties={
                    "value": combined_risk,
                    "normalized_weight": combined_risk,
                    "confidence": 0.8,
                    "year": year,
                    "region": row["region"],
                },
            )
            created += 1

        logger.info("IS_HIGH_RISK_FOR edges created: %d", created)
        return created
    finally:
        conn.close()


def compute_arms_influence_edges(top_n: int = 5, year: int = 2024) -> int:
    """
    Creates IS_INFLUENTIAL_TO relationships from top arms exporters
    to countries in the same alliances (MEMBER_OF).
    """
    conn = Neo4jConnection()
    try:
        # Step 1: Get top N exporters by arms_export_score
        query_top = """
        MATCH (c:Country)
        WHERE c.arms_export_score IS NOT NULL
        RETURN c.name AS country, c.arms_export_score AS score
        ORDER BY score DESC LIMIT $top_n
        """
        top_exporters = conn.run_query(query_top, {"top_n": top_n})
        created = 0

        for exporter in top_exporters:
            exporter_name = exporter["country"]
            influence_score = float(exporter.get("score", 0.0) or 0.0)

            # Step 2: Find all countries in same alliances as this exporter
            query_alliance_partners = """
            MATCH (exporter:Country {name: $name})-[:MEMBER_OF]->(a:Alliance)<-[:MEMBER_OF]-(partner:Country)
            WHERE partner.name <> $name
            RETURN DISTINCT partner.name AS partner,
                            a.name AS alliance,
                            $score AS arms_score
            """
            partners = conn.run_query(
                query_alliance_partners,
                {"name": exporter_name, "score": influence_score},
            )

            seen_partners = set()
            ops = GraphOps(conn)
            for partner in partners:
                partner_name = partner["partner"]
                if partner_name in seen_partners:
                    continue
                seen_partners.add(partner_name)

                final_influence = clamp(influence_score, 0.0, 1.0)
                ops.create_relationship(
                    source=exporter_name,
                    target=partner_name,
                    rel_type=IS_INFLUENTIAL_TO,
                    properties={
                        "value": final_influence,
                        "normalized_weight": final_influence,
                        "confidence": 0.8,
                        "year": year,
                        "domain": "defense",
                        "context": partner.get("alliance"),
                    },
                )
                created += 1

        logger.info("IS_INFLUENTIAL_TO (defense) edges: %d", created)
        return created
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    n1 = compute_high_conflict_edges()
    n2 = compute_arms_influence_edges()
    print(f"High conflict edges: {n1}")
    print(f"Arms influence edges: {n2}")
