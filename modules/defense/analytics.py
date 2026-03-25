from __future__ import annotations

import sys
from pathlib import Path
from typing import List, Dict, Any

# Ensure `common` is importable when running this file directly.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common.db import Neo4jConnection


def get_spending_trend(country_name: str) -> list:
    try:
        conn = Neo4jConnection()
        try:
            query = """
            MATCH (c:Country {name: $name})-[r:SPENDS_ON_DEFENSE]->(y:Year)
            RETURN y.year AS year, r.amount_usd_millions AS amount
            ORDER BY year
            """
            return conn.run_query(query, {"name": country_name})
        finally:
            conn.close()
    except Exception:
        return []


def get_top_defense_spenders(limit: int = 10) -> list:
    try:
        conn = Neo4jConnection()
        try:
            query = """
            MATCH (c:Country)-[r:SPENDS_ON_DEFENSE]->(y:Year)
            WHERE y.year = 2023
            RETURN c.name AS country, r.amount_usd_millions AS spending_2023
            ORDER BY spending_2023 DESC LIMIT $limit
            """
            return conn.run_query(query, {"limit": limit})
        finally:
            conn.close()
    except Exception:
        return []


def get_top_arms_exporters(limit: int = 10) -> list:
    try:
        conn = Neo4jConnection()
        try:
            query = """
            MATCH (c:Country)-[r:EXPORTS_ARMS]->(y:Year)
            RETURN c.name AS country, sum(r.tiv_millions) AS total_tiv
            ORDER BY total_tiv DESC LIMIT $limit
            """
            return conn.run_query(query, {"limit": limit})
        finally:
            conn.close()
    except Exception:
        return []


def get_conflict_summary(country_name: str) -> list:
    try:
        conn = Neo4jConnection()
        try:
            query = """
            MATCH (c:Country {name: $name})-[r:HAS_CONFLICT_STATS]->(y:Year)
            RETURN y.year AS year,
                   r.violence_events AS events,
                   r.total_fatalities AS fatalities
            ORDER BY year
            """
            return conn.run_query(query, {"name": country_name})
        finally:
            conn.close()
    except Exception:
        return []


def get_most_conflict_prone(limit: int = 10) -> list:
    try:
        conn = Neo4jConnection()
        try:
            query = """
            MATCH (c:Country)-[r:HAS_CONFLICT_STATS]->(y:Year)
            RETURN c.name AS country,
                   sum(r.total_fatalities) AS total_fatalities,
                   sum(r.violence_events) AS total_events
            ORDER BY total_fatalities DESC LIMIT $limit
            """
            return conn.run_query(query, {"limit": limit})
        finally:
            conn.close()
    except Exception:
        return []

