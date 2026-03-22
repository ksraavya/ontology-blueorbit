import sys
sys.path.insert(0, '.')

from common.db import Neo4jConnection

conn = Neo4jConnection()

total = conn.run_query("MATCH (n) RETURN COUNT(n) AS total")
print(f"Total nodes: {total[0]['total']}")

countries = conn.run_query("MATCH (c:Country) RETURN COUNT(c) AS total")
print(f"Country nodes: {countries[0]['total']}")

political = conn.run_query("MATCH (p:PoliticalSystem) RETURN COUNT(p) AS total")
print(f"PoliticalSystem nodes: {political[0]['total']}")

conn.close()