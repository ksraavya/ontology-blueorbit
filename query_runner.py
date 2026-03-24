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


query = """
MATCH (c:Country)
WHERE c.military_strength_score IS NOT NULL
RETURN c.name AS country,
       round(c.military_strength_score, 4) AS military_strength,
       round(c.defense_spending_score, 4) AS spending_score,
       round(c.arms_export_score, 4) AS arms_score,
       c.nuclear_status AS nuclear,
       c.un_p5 AS p5
ORDER BY c.military_strength_score DESC LIMIT 10
"""


if __name__ == "__main__":
    print("Running Health Check Query...")
    run(query)
