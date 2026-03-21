import sys
import os
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from common.db import Neo4jConnection

def run(query, params=None):
    conn = Neo4jConnection()
    try:
        results = conn.run_query(query, params or {})
        if not results:
            print("No results returned.")
        for row in results:
            print(row)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

# ── FULL HEALTH CHECK ──────────────────────────────────────
query = """
MATCH (c:Country)-[:BELONGS_TO]->(reg)
MATCH (c)-[cf:HAS_CONFLICT_STATS]->(y:Year)
WHERE y.year >= 2015
RETURN reg.name AS region,
       count(DISTINCT c) AS countries_in_region,
       sum(cf.total_fatalities) AS total_fatalities_since_2015,
       sum(cf.violence_events) AS total_violence_events,
       round(avg(cf.total_fatalities), 0) AS avg_fatalities_per_country
ORDER BY total_fatalities_since_2015 DESC
"""
# ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Running Health Check Query...")
    run(query)
