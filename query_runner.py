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
MERGE (reg:Region {name: 'Eastern Europe'})
WITH reg
MATCH (c:Country {name: 'Ukraine'})
MERGE (c)-[:BELONGS_TO]->(reg)
WITH reg
MERGE (reg2:Region {name: 'Middle East'})
WITH reg2
MATCH (c2:Country {name: 'Syrian Arab Republic'})
MERGE (c2)-[:BELONGS_TO]->(reg2)
RETURN 'done' AS status
"""


if __name__ == "__main__":
    print("Running Health Check Query...")
    run(query)
