from __future__ import annotations

from typing import Dict, Any, Optional

from common.db import Neo4jConnection
from common.entity_mapper import normalize_entity
from common.ontology import is_valid_relationship
from common.config import EDGE_SCHEMA


"""
Graph Operations Layer

Responsibilities:
- Enforce schema
- Normalize entities
- Validate relationships
- Provide safe, consistent graph writes

Modules MUST use this instead of raw Cypher.
"""


class GraphOps:

    def __init__(self, conn: Neo4jConnection):
        self.conn = conn

    # =========================================================
    # NODE OPERATIONS
    # =========================================================

    def upsert_country(self, name: str) -> None:
        """
        Ensure a country node exists.
        """
        normalized = normalize_entity(name, entity_type="country")

        query = """
        MERGE (c:Country {name: $name})
        RETURN c
        """

        self.conn.run_query(query, {"name": normalized})

    def upsert_node(self, label: str, name: str) -> None:
        """
        Generic node creation (future-proof).
        """
        normalized = normalize_entity(name)

        query = f"""
        MERGE (n:{label} {{name: $name}})
        RETURN n
        """

        self.conn.run_query(query, {"name": normalized})

    # =========================================================
    # RELATIONSHIP OPERATIONS
    # =========================================================

    def create_relationship(
        self,
        source: str,
        target: str,
        rel_type: str,
        properties: Optional[Dict[str, Any]] = None,
        source_label: str = "Country",
        target_label: str = "Country",
    ) -> None:
        """
        Create or update a relationship with enforced schema.
        """

        if not is_valid_relationship(rel_type):
            raise ValueError(f"Invalid relationship type: {rel_type}")

        # Normalize entities
        source_norm = normalize_entity(source)
        target_norm = normalize_entity(target)

        # Ensure nodes exist
        self.upsert_node(source_label, source_norm)
        self.upsert_node(target_label, target_norm)

        # Apply schema defaults
        props = self._apply_edge_schema(properties or {})

        query = f"""
        MATCH (a:{source_label} {{name: $source}})
        MATCH (b:{target_label} {{name: $target}})
        MERGE (a)-[r:{rel_type}]->(b)
        SET r += $props
        RETURN r
        """

        self.conn.run_query(query, {
            "source": source_norm,
            "target": target_norm,
            "props": props,
        })

    # =========================================================
    # INTERNAL HELPERS
    # =========================================================

    def _apply_edge_schema(self, properties: Dict[str, Any]) -> Dict[str, Any]:
        """
        Ensure all edges follow standard schema.
        """
        final_props = EDGE_SCHEMA.copy()

        for key, value in properties.items():
            final_props[key] = value

        return final_props
