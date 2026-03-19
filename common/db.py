# ===== common/db.py =====

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

try:
    from dotenv import load_dotenv
except ImportError as exc:  # pragma: no cover
    raise ImportError("Missing dependency: python-dotenv is required for common/db.py") from exc

try:
    from neo4j import GraphDatabase
except ImportError as exc:  # pragma: no cover
    raise ImportError("Missing dependency: neo4j is required for common/db.py") from exc


# Load environment variables
load_dotenv()


class Neo4jConnection:
    """
    Shared Neo4j connection helper.

    Responsibilities:
    - Load credentials from environment
    - Manage driver lifecycle
    - Provide simple query execution interface
    """

    def __init__(self) -> None:
        self._uri: str = os.getenv("NEO4J_URI", "").strip()
        self._user: str = os.getenv("NEO4J_USER", "").strip()
        self._password: str = os.getenv("NEO4J_PASSWORD", "")

        if not self._uri:
            raise ValueError("Missing required environment variable: NEO4J_URI")
        if not self._user:
            raise ValueError("Missing required environment variable: NEO4J_USER")
        if not self._password:
            raise ValueError("Missing required environment variable: NEO4J_PASSWORD")

        self._driver = GraphDatabase.driver(
            self._uri,
            auth=(self._user, self._password),
        )

        # Optional but useful: verify connection immediately
        try:
            self._driver.verify_connectivity()
        except Exception as exc:
            raise RuntimeError("Failed to connect to Neo4j. Check credentials and URI.") from exc

    def close(self) -> None:
        """Close the Neo4j driver."""
        if hasattr(self, "_driver") and self._driver:
            self._driver.close()
        self._driver = None  # type: ignore

    def __del__(self) -> None:  # pragma: no cover
        try:
            self.close()
        except Exception:
            pass

    def run_query(
        self,
        query: str,
        parameters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute a Cypher query and return results as a list of dictionaries.

        - Safe session handling
        - Supports both read and write queries
        """

        if not isinstance(query, str) or not query.strip():
            raise ValueError("query must be a non-empty string")

        params: Dict[str, Any] = parameters or {}

        try:
            with self._driver.session() as session:
                result = session.run(query, params)
                return [record.data() for record in result]
        except Exception as exc:
            raise RuntimeError("Neo4j query failed. Check query or connection.") from exc