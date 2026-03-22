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
MATCH (c:Country)-[r:SPENDS_ON_DEFENSE]->(y:Year)
RETURN count(r) AS spending
"""
if __name__ == "__main__":
    print("Running Health Check Query...")
    run(query)
