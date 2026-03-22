"""
Insert geopolitical data into the Neo4j knowledge graph.
"""

from __future__ import annotations
import sys
sys.path.insert(0, '.')
import pandas as pd

from common.db import Neo4jConnection
from common.graph_ops import GraphOps
from common.ontology import HAS_POLITICAL_SYSTEM


def insert_political_systems(df: pd.DataFrame) -> None:
    """
    Insert political system classifications from V-Dem data into the graph.

    For each (country, year) row, determines system type from v2x_polyarchy
    (Democracy if >= 0.5, else Autocracy), ensures Country and PoliticalSystem
    nodes exist, and creates a HAS_POLITICAL_SYSTEM relationship with score
    and year properties.
    """
    conn = Neo4jConnection()
    try:
        ops = GraphOps(conn)
        for row in df.itertuples():
            system_type = "Democracy" if row.v2x_polyarchy >= 0.5 else "Autocracy"
            ops.upsert_country(row.country)
            ops.upsert_node("PoliticalSystem", system_type)
            ops.create_relationship(
                source=row.country,
                target=system_type,
                rel_type=HAS_POLITICAL_SYSTEM,
                properties={
                    "score": row.v2x_polyarchy,
                    "year": row.year,
                },
                source_label="Country",
                target_label="PoliticalSystem",
            )
    finally:
        conn.close()

    print(f"Inserted {len(df)} political system records")


def insert_diplomatic_edges(df: pd.DataFrame) -> None:
    """
    Insert diplomatic interaction edges from GDELT data into the graph.

    Adds a weight column based on event_code prefix (08→1.0, 05→0.6, 04→0.3,
    else→0.1), then bulk-inserts Country nodes and DIPLOMATIC_INTERACTION
    relationships via UNWIND for performance.
    """
    def _weight(e: str) -> float:
        s = str(e)
        if s.startswith("08"):
            return 1.0
        if s.startswith("05"):
            return 0.6
        if s.startswith("04"):
            return 0.3
        return 0.1

    df = df.copy()
    df["weight"] = df["event_code"].apply(_weight)

    rows = [
        {
            "actor1": row.actor1,
            "actor2": row.actor2,
            "event_code": row.event_code,
            "date": row.date,
            "weight": row.weight,
        }
        for row in df.itertuples()
    ]

    conn = Neo4jConnection()
    try:
        query = """
        UNWIND $rows AS row
        MERGE (a:Country {name: row.actor1})
        MERGE (b:Country {name: row.actor2})
        MERGE (a)-[r:DIPLOMATIC_INTERACTION {event_code: row.event_code}]->(b)
        SET r.date = row.date,
            r.weight = row.weight
        """
        conn.run_query(query, {"rows": rows})
    finally:
        conn.close()

    print(f"Inserted {len(df)} diplomatic edges")


if __name__ == "__main__":
    import sys
    sys.path.insert(0, '.')

    from modules.geopolitics.loader import load_vdem, load_gdelt
    from modules.geopolitics.cleaner import clean_vdem, clean_gdelt

    df_vdem = load_vdem()
    df_vdem_clean = clean_vdem(df_vdem)
    insert_political_systems(df_vdem_clean)

    df_gdelt = load_gdelt()
    df_gdelt_clean = clean_gdelt(df_gdelt)
    insert_diplomatic_edges(df_gdelt_clean)