from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from neo4j import GraphDatabase


"""
Neo4j Connection Layer

Responsibilities:
- Manage connection lifecycle
- Execute queries safely
- Provide reusable query interface
"""

load_dotenv()


class Neo4jConnection:

    def __init__(self) -> None:
        self._uri: str = os.getenv("NEO4J_URI", "").strip()
        self._user: str = os.getenv("NEO4J_USER", "").strip()
        self._password: str = os.getenv("NEO4J_PASSWORD", "")

        if not self._uri:
            raise ValueError("Missing NEO4J_URI")
        if not self._user:
            raise ValueError("Missing NEO4J_USER")
        if not self._password:
            raise ValueError("Missing NEO4J_PASSWORD")

        self._driver = GraphDatabase.driver(
            self._uri,
            auth=(self._user, self._password),
        )

        self._verify_connection()

    def _verify_connection(self) -> None:
        try:
            self._driver.verify_connectivity()
        except Exception as exc:
            raise RuntimeError("Neo4j connection failed") from exc

    def close(self) -> None:
        if self._driver:
            self._driver.close()

    def run_query(
        self,
        query: str,
        parameters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute Cypher query safely.
        """
        if not query or not query.strip():
            raise ValueError("Query must be non-empty")

        params = parameters or {}

        try:
            with self._driver.session() as session:
                result = session.run(query, params)
                return [record.data() for record in result]
        except Exception as exc:
            raise RuntimeError("Neo4j query failed") from exc
