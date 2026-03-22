import sys
sys.path.insert(0, '.')

from common.db import Neo4jConnection

try:
    conn = Neo4jConnection()
    result = conn.run_query("MATCH (n) RETURN COUNT(n) AS total")
    print(f"✅ Neo4j connection successful. Total nodes in DB: {result[0]['total']}")
    conn.close()
except Exception as e:
    print(f"❌ Connection failed: {e}")