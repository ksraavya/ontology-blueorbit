from __future__ import annotations

import os
import threading
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from neo4j import Driver, GraphDatabase


"""
Neo4j Connection Layer

Responsibilities:
- Manage connection lifecycle
- Execute queries safely
- Provide reusable query interface

Shared driver
-------------
A neo4j Driver is a connection POOL and is thread-safe; the Neo4j docs
recommend exactly one per process. Previously every Neo4jConnection()
built a new driver and ran verify_connectivity() (a network round trip),
and the simulation code does that ~40 times per request. Now every
Neo4jConnection() reuses one module-level driver, so existing call sites
(`conn = Neo4jConnection(); ...; conn.close()`) keep working unchanged
but no longer pay connection setup on each call.
"""

load_dotenv()

_driver: Optional[Driver] = None
_driver_lock = threading.Lock()


def _read_credentials() -> tuple[str, str, str]:
    uri = os.getenv("NEO4J_URI", "").strip()
    user = os.getenv("NEO4J_USER", "").strip()
    password = os.getenv("NEO4J_PASSWORD", "")

    if not uri:
        raise ValueError("Missing NEO4J_URI")
    if not user:
        raise ValueError("Missing NEO4J_USER")
    if not password:
        raise ValueError("Missing NEO4J_PASSWORD")
    return uri, user, password


def get_driver() -> Driver:
    """Return the process-wide driver, creating and verifying it once."""
    global _driver
    if _driver is not None:
        return _driver
    with _driver_lock:
        if _driver is None:
            uri, user, password = _read_credentials()
            driver = GraphDatabase.driver(
                uri,
                auth=(user, password),
                # Queries against labels a pipeline has not loaded yet are
                # normal here; don't spam the log with "label does not exist".
                notifications_min_severity="OFF",
            )
            try:
                driver.verify_connectivity()
            except Exception as exc:
                driver.close()
                raise RuntimeError(f"Neo4j connection failed ({uri}): {exc}") from exc
            _driver = driver
    return _driver


def close_driver() -> None:
    """Close the shared driver. Call once on application shutdown."""
    global _driver
    with _driver_lock:
        if _driver is not None:
            _driver.close()
            _driver = None


class Neo4jConnection:

    def __init__(self) -> None:
        self._driver = get_driver()

    def close(self) -> None:
        # Intentionally a no-op: the driver is shared by the whole process.
        # Use close_driver() at shutdown instead.
        return None

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
            # Keep the real Neo4j message: "Neo4j query failed" alone made
            # syntax errors and missing-property bugs impossible to debug.
            raise RuntimeError(f"Neo4j query failed: {exc}") from exc
