from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List

# Ensure `common` and other repo root modules are importable when running this
# file directly (Python's sys.path otherwise points to modules/defense).
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def insert_defense_spending(df) -> None:
    from common.db import Neo4jConnection

    conn = None
    try:
        rows: List[Dict[str, Any]] = df.to_dict("records")

        query = """
        UNWIND $rows AS row
        MERGE (c:Country {name: row.country})
        MERGE (y:Year {year: toInteger(row.year)})
        MERGE (c)-[r:SPENDS_ON_DEFENSE]->(y)
        SET r.amount_usd_millions = toFloat(row.expenditure_usd_millions),
            r.currency = 'USD',
            r.source = 'SIPRI Milex'
        """

        conn = Neo4jConnection()

        inserted_total = 0
        for i in range(0, len(rows), 500):
            batch = rows[i : i + 500]
            batch_idx = i // 500 + 1
            try:
                conn.run_query(query, {"rows": batch})
                inserted_total += len(batch)
                print(f"Inserted batch {batch_idx}")
            except Exception as e:
                print(f"Batch {batch_idx} failed: {e}")

        print(f"Total rows inserted: {inserted_total}")
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def verify_spending_insert():
    from common.db import Neo4jConnection

    conn = None
    try:
        conn = Neo4jConnection()

        query = """
        MATCH (c:Country)-[r:SPENDS_ON_DEFENSE]->(y:Year)
        RETURN c.name AS country,
               count(r) AS years_with_data,
               max(r.amount_usd_millions) AS peak_spending
        ORDER BY peak_spending DESC
        LIMIT 10
        """

        result = conn.run_query(query)
        print(result)
        return result
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def insert_arms_exports(df) -> None:
    from common.db import Neo4jConnection

    conn = None
    try:
        rows: List[Dict[str, Any]] = df.to_dict("records")

        query = """
        UNWIND $rows AS row
        MERGE (c:Country {name: row.country})
        MERGE (y:Year {year: toInteger(row.year)})
        MERGE (c)-[r:EXPORTS_ARMS]->(y)
        SET r.tiv_millions = toFloat(row.tiv_millions),
            r.period = row.period,
            r.source = 'SIPRI TIV'
        """

        conn = Neo4jConnection()

        inserted_total = 0
        for i in range(0, len(rows), 500):
            batch = rows[i : i + 500]
            batch_idx = i // 500 + 1
            try:
                conn.run_query(query, {"rows": batch})
                inserted_total += len(batch)
                print(f"Inserted batch {batch_idx}")
            except Exception as e:
                print(f"Batch {batch_idx} failed: {e}")

        print(f"Total rows inserted: {inserted_total}")
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def verify_arms_insert():
    from common.db import Neo4jConnection

    conn = None
    try:
        conn = Neo4jConnection()

        query = """
        MATCH (c:Country)-[r:EXPORTS_ARMS]->(y:Year)
        RETURN c.name AS country,
               sum(r.tiv_millions) AS total_tiv,
               count(r) AS years_active
        ORDER BY total_tiv DESC LIMIT 10
        """

        result = conn.run_query(query)
        print(result)
        return result
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def insert_conflict_stats(df) -> None:
    from common.db import Neo4jConnection

    conn = None
    try:
        rows: List[Dict[str, Any]] = df.to_dict("records")

        query = """
        UNWIND $rows AS row
        MERGE (c:Country {name: row.country})
        MERGE (y:Year {year: toInteger(row.year)})
        MERGE (c)-[r:HAS_CONFLICT_STATS]->(y)
        SET r.violence_events     = toInteger(row.violence_events),
            r.civilian_events     = toInteger(row.civilian_events),
            r.civilian_fatalities = toInteger(row.civilian_fatalities),
            r.total_fatalities    = toInteger(row.total_fatalities),
            r.source              = 'ACLED'
        """

        conn = Neo4jConnection()

        inserted_total = 0
        for i in range(0, len(rows), 500):
            batch = rows[i : i + 500]
            batch_idx = i // 500 + 1
            try:
                conn.run_query(query, {"rows": batch})
                inserted_total += len(batch)
                print(f"Inserted batch {batch_idx}")
            except Exception as e:
                print(f"Batch {batch_idx} failed: {e}")

        print(f"Total rows inserted: {inserted_total}")
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def verify_conflict_insert():
    from common.db import Neo4jConnection

    conn = None
    try:
        conn = Neo4jConnection()

        query = """
        MATCH (c:Country)-[r:HAS_CONFLICT_STATS]->(y:Year)
        RETURN c.name AS country,
               sum(r.total_fatalities) AS total_fatalities,
               sum(r.violence_events) AS total_events
        ORDER BY total_fatalities DESC LIMIT 10
        """

        result = conn.run_query(query)
        print(result)
        return result
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
