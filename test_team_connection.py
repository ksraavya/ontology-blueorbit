import sys
sys.path.insert(0, '.')

from dotenv import load_dotenv
load_dotenv()

from common.db import Neo4jConnection

try:
    conn = Neo4jConnection()
    result = conn.run_query("RETURN 1 AS ok")
    print("Connected to team Neo4j successfully:", result)
    
    # Check what's already in team database
    nodes = conn.run_query(
        "MATCH (n) RETURN labels(n) AS label, count(n) AS count ORDER BY count DESC"
    )
    print("\nExisting nodes in team database:")
    for row in nodes:
        print(f"  {row['label']}: {row['count']}")
    
    rels = conn.run_query(
        "MATCH ()-[r]->() RETURN type(r) AS rel_type, count(r) AS count ORDER BY count DESC"
    )
    print("\nExisting relationships in team database:")
    for row in rels:
        print(f"  {row['rel_type']}: {row['count']}")
        
    conn.close()
except Exception as e:
    print(f"Connection failed: {e}")