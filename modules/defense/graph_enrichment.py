import sys
import os

# Sys path fix to reach project root from modules/defense/
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from common.db import Neo4jConnection

REGION_DATA = {
    "South Asia": [
        "India", "Pakistan", "Bangladesh", "Afghanistan",
        "Sri Lanka", "Nepal", "Bhutan", "Maldives"
    ],
    "Middle East": [
        "Israel", "Iran", "Iraq", "Syria", "Yemen",
        "Saudi Arabia", "Jordan", "Lebanon", "Kuwait",
        "United Arab Emirates", "Qatar", "Bahrain", "Oman"
    ],
    "Eastern Europe": [
        "Ukraine", "Russian Federation", "Belarus",
        "Poland", "Romania", "Bulgaria", "Hungary",
        "Czechia", "Slovakia"
    ],
    "Western Europe": [
        "Germany", "France", "United Kingdom", "Italy",
        "Spain", "Netherlands", "Belgium", "Norway",
        "Sweden", "Denmark", "Finland", "Portugal"
    ],
    "East Asia": [
        "China", "Japan", "Korea, Republic of",
        "Korea, Democratic People's Republic of", "Taiwan",
        "Mongolia"
    ],
    "Southeast Asia": [
        "Myanmar", "Thailand", "Vietnam", "Indonesia",
        "Philippines", "Malaysia", "Singapore", "Cambodia"
    ],
    "North America": [
        "United States", "Canada", "Mexico"
    ],
    "South America": [
        "Brazil", "Colombia", "Argentina", "Venezuela",
        "Peru", "Chile", "Ecuador", "Bolivia"
    ],
    "Central Asia": [
        "Kazakhstan", "Uzbekistan", "Turkmenistan",
        "Tajikistan", "Kyrgyzstan"
    ],
    "Africa": [
        "Ethiopia", "Nigeria", "Somalia", "Sudan",
        "South Sudan", "Mali", "Niger", "Chad",
        "Democratic Republic of the Congo", "Egypt",
        "Libya", "Morocco", "Algeria", "Tunisia",
        "South Africa", "Kenya", "Uganda", "Mozambique"
    ]
}

ALLIANCE_DATA = {
    "NATO": [
        "United States", "United Kingdom", "France", "Germany",
        "Italy", "Spain", "Canada", "Norway", "Denmark",
        "Netherlands", "Belgium", "Portugal", "Poland",
        "Romania", "Bulgaria", "Hungary", "Czechia", "Slovakia",
        "Finland", "Sweden"
    ],
    "BRICS": [
        "Brazil", "Russian Federation", "India", "China",
        "South Africa", "Egypt", "Ethiopia", "Iran",
        "United Arab Emirates"
    ],
    "SCO": [
        "China", "Russian Federation", "India", "Pakistan",
        "Kazakhstan", "Uzbekistan", "Kyrgyzstan", "Tajikistan",
        "Iran"
    ],
    "QUAD": [
        "United States", "India", "Japan", "Australia"
    ],
    "ASEAN": [
        "Indonesia", "Malaysia", "Philippines", "Singapore",
        "Thailand", "Vietnam", "Myanmar", "Cambodia"
    ],
    "African Union": [
        "Ethiopia", "Nigeria", "South Africa", "Kenya",
        "Egypt", "Algeria", "Morocco", "Ghana", "Tanzania"
    ],
    "Arab League": [
        "Egypt", "Saudi Arabia", "Iraq", "Syria", "Jordan",
        "Lebanon", "Libya", "Yemen", "Algeria", "Morocco",
        "Tunisia", "Kuwait", "United Arab Emirates", "Qatar",
        "Bahrain", "Oman"
    ],
    "CSTO": [
        "Russian Federation", "Belarus", "Kazakhstan",
        "Kyrgyzstan", "Tajikistan", "Armenia"
    ]
}

def insert_regions():
    """Insert Region nodes and link them to countries."""
    print("--- Inserting Regions ---")
    conn = None
    try:
        rows = [
            {"region": region, "country": country}
            for region, countries in REGION_DATA.items()
            for country in countries
        ]
        
        query = """
        UNWIND $rows AS row
        MERGE (r:Region {name: row.region})
        MERGE (c:Country {name: row.country})
        MERGE (c)-[:BELONGS_TO]->(r)
        """
        
        conn = Neo4jConnection()
        conn.run_query(query, {"rows": rows})
        print(f"Successfully linked {len(rows)} countries to their regions.")
    except Exception as e:
        print(f"Error inserting regions: {e}")
    finally:
        if conn:
            conn.close()

def insert_alliances():
    """Insert Alliance nodes and link them to countries."""
    print("\n--- Inserting Alliances ---")
    conn = None
    try:
        rows = [
            {"alliance": alliance, "country": country}
            for alliance, countries in ALLIANCE_DATA.items()
            for country in countries
        ]
        
        query = """
        UNWIND $rows AS row
        MERGE (a:Alliance {name: row.alliance})
        MERGE (c:Country {name: row.country})
        MERGE (c)-[:MEMBER_OF]->(a)
        """
        
        conn = Neo4jConnection()
        conn.run_query(query, {"rows": rows})
        print(f"Successfully linked {len(rows)} countries to their alliances.")
    except Exception as e:
        print(f"Error inserting alliances: {e}")
    finally:
        if conn:
            conn.close()

def verify_enrichment():
    """Verify Region and Alliance data in Neo4j."""
    print("\n--- Verification Results ---")
    conn = None
    try:
        conn = Neo4jConnection()
        
        print("Regions Summary:")
        q1 = """
        MATCH (c:Country)-[:BELONGS_TO]->(r:Region)
        RETURN r.name AS region, count(c) AS countries
        ORDER BY countries DESC
        """
        res1 = conn.run_query(q1)
        for row in res1:
            print(f"- {row['region']}: {row['countries']} countries")
            
        print("\nAlliances Summary:")
        q2 = """
        MATCH (c:Country)-[:MEMBER_OF]->(a:Alliance)
        RETURN a.name AS alliance, count(c) AS members
        ORDER BY members DESC
        """
        res2 = conn.run_query(q2)
        for row in res2:
            print(f"- {row['alliance']}: {row['members']} members")
            
    except Exception as e:
        print(f"Error verifying enrichment: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    insert_regions()
    insert_alliances()
    verify_enrichment()
