from __future__ import annotations
from typing import List, Dict, Any
from common.db import Neo4jConnection
from common.graph_ops import GraphOps

"""Defense module — load layer. 
Inserts enriched data into Neo4j. 
Uses graph_ops for all writes — never raw Cypher. 
Exception: Year nodes use direct Cypher because 
graph_ops uses {name} property but Year nodes 
use {year} integer property."""

def _insert_country_year_relationship(conn: Neo4jConnection, country: str, year: int, rel_type: str, properties: Dict[str, Any]) -> None:
    """
    Direct Cypher helper for Year-related relationships.
    Year nodes use {year: integer} property which graph_ops doesn't support.
    """
    query = f"""
    MERGE (c:Country {{name: $country}})
    MERGE (y:Year {{year: toInteger($year)}})
    MERGE (c)-[r:{rel_type}]->(y)
    SET r += $props
    """
    conn.run_query(query, {
        "country": country,
        "year": year,
        "props": properties
    })

def load_milex(rows: List[Dict[str, Any]]) -> int:
    """
    Inserts enriched military expenditure data into Neo4j.
    """
    if not rows:
        return 0

    conn = Neo4jConnection()
    total_inserted = 0
    batch_size = 500

    try:
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            try:
                for row in batch:
                    properties = {
                        'value': row['value'],
                        'normalized_weight': row['normalized_weight'],
                        'year': row['year'],
                        'confidence': row['confidence'],
                        'currency': 'USD',
                        'source': 'SIPRI Milex'
                    }
                    _insert_country_year_relationship(
                        conn, 
                        row['country'], 
                        row['year'], 
                        'SPENDS_ON_DEFENSE', 
                        properties
                    )
                total_inserted += len(batch)
                print(f"Spending batch {(i // batch_size) + 1}: {total_inserted} rows")
            except Exception as e:
                print(f"Error in spending batch {(i // batch_size) + 1}: {e}")
                continue
    finally:
        conn.close()

    return total_inserted

def load_arms(rows: List[Dict[str, Any]]) -> int:
    """
    Inserts enriched arms export data into Neo4j.
    """
    if not rows:
        return 0

    conn = Neo4jConnection()
    total_inserted = 0
    batch_size = 500

    try:
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            try:
                for row in batch:
                    properties = {
                        'value': row['value'],
                        'normalized_weight': row['normalized_weight'],
                        'dependency': row['dependency'],
                        'year': row['year'],
                        'confidence': row['confidence'],
                        'period': row['period'],
                        'source': 'SIPRI TIV'
                    }
                    _insert_country_year_relationship(
                        conn, 
                        row['country'], 
                        row['year'], 
                        'EXPORTS_ARMS', 
                        properties
                    )
                total_inserted += len(batch)
                print(f"Arms batch {(i // batch_size) + 1}: {total_inserted} rows")
            except Exception as e:
                print(f"Error in arms batch {(i // batch_size) + 1}: {e}")
                continue
    finally:
        conn.close()

    return total_inserted

def load_acled(rows: List[Dict[str, Any]]) -> int:
    """
    Inserts enriched ACLED conflict data into Neo4j.
    """
    if not rows:
        return 0

    conn = Neo4jConnection()
    total_inserted = 0
    batch_size = 500

    try:
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            try:
                for row in batch:
                    properties = {
                        'value': row['value'],
                        'normalized_weight': row['normalized_weight'],
                        'year': row['year'],
                        'confidence': row['confidence'],
                        'violence_events': row['violence_events'],
                        'civilian_events': row['civilian_events'],
                        'civilian_fatalities': row['civilian_fatalities'],
                        'total_fatalities': row['total_fatalities'],
                        'fatality_trend': row['fatality_trend'],
                        'source': 'ACLED'
                    }
                    _insert_country_year_relationship(
                        conn, 
                        row['country'], 
                        row['year'], 
                        'HAS_CONFLICT_STATS', 
                        properties
                    )
                total_inserted += len(batch)
                print(f"ACLED batch {(i // batch_size) + 1}: {total_inserted} rows")
            except Exception as e:
                print(f"Error in ACLED batch {(i // batch_size) + 1}: {e}")
                continue
    finally:
        conn.close()

    return total_inserted

def verify_loads() -> None:
    """
    Verifies that data was correctly loaded into Neo4j.
    """
    conn = Neo4jConnection()
    try:
        print("\n--- Load Verification ---")
        
        # Query 1: Spending
        q1 = """
        MATCH (c:Country)-[r:SPENDS_ON_DEFENSE]->(y:Year) 
        RETURN count(r) AS total, 
               avg(r.normalized_weight) AS avg_weight
        """
        res1 = conn.run_query(q1)
        if res1:
            print(f"Spending: Total relationships = {res1[0]['total']}, Avg Normalized Weight = {res1[0]['avg_weight']:.4f}")

        # Query 2: Arms
        q2 = """
        MATCH (c:Country)-[r:EXPORTS_ARMS]->(y:Year) 
        RETURN count(r) AS total, 
               avg(r.normalized_weight) AS avg_weight, 
               avg(r.dependency) AS avg_dependency
        """
        res2 = conn.run_query(q2)
        if res2:
            print(f"Arms: Total relationships = {res2[0]['total']}, Avg Normalized Weight = {res2[0]['avg_weight']:.4f}, Avg Dependency = {res2[0]['avg_dependency']:.4f}")

        # Query 3: ACLED
        q3 = """
        MATCH (c:Country)-[r:HAS_CONFLICT_STATS]->(y:Year) 
        RETURN count(r) AS total, 
               avg(r.normalized_weight) AS avg_weight, 
               count(CASE WHEN r.fatality_trend = 'increasing' 
                     THEN 1 END) AS increasing_countries
        """
        res3 = conn.run_query(q3)
        if res3:
            print(f"ACLED: Total relationships = {res3[0]['total']}, Avg Normalized Weight = {res3[0]['avg_weight']:.4f}, Increasing Fatality Trends = {res3[0]['increasing_countries']}")

    finally:
        conn.close()

if __name__ == '__main__':
    # For standalone testing, you would typically run the pipeline.
    # But we can call verify_loads to see current state.
    verify_loads()
