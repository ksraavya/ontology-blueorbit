import sys
import os

# Sys path fix to reach project root from modules/defense/
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from common.db import Neo4jConnection
from common.entity_mapper import normalize_entity

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

# Full memberships as of 2026 (names are normalised before writing, so
# common spellings are fine). Earlier lists were partial (NATO had 20 of 32,
# the African Union 9 of 55), which skewed every alliance scenario.
ALLIANCE_DATA = {
    "NATO": [
        "Albania", "Belgium", "Bulgaria", "Canada", "Croatia", "Czechia", "Denmark",
        "Estonia", "Finland", "France", "Germany", "Greece", "Hungary", "Iceland",
        "Italy", "Latvia", "Lithuania", "Luxembourg", "Montenegro", "Netherlands",
        "North Macedonia", "Norway", "Poland", "Portugal", "Romania", "Slovakia",
        "Slovenia", "Spain", "Sweden", "Turkey", "United Kingdom", "United States",
    ],
    "BRICS": [
        "Brazil", "Russia", "India", "China", "South Africa", "Egypt", "Ethiopia",
        "Iran", "United Arab Emirates", "Indonesia",
    ],
    "SCO": [
        "China", "Russia", "India", "Pakistan", "Kazakhstan", "Kyrgyzstan",
        "Tajikistan", "Uzbekistan", "Iran", "Belarus",
    ],
    "QUAD": ["United States", "India", "Japan", "Australia"],
    "ASEAN": [
        "Brunei", "Cambodia", "Indonesia", "Laos", "Malaysia", "Myanmar",
        "Philippines", "Singapore", "Thailand", "Vietnam", "Timor-Leste",
    ],
    "African Union": [
        "Algeria", "Angola", "Benin", "Botswana", "Burkina Faso", "Burundi",
        "Cabo Verde", "Cameroon", "Central African Republic", "Chad", "Comoros",
        "Congo", "Democratic Republic of the Congo", "Djibouti", "Egypt",
        "Equatorial Guinea", "Eritrea", "Eswatini", "Ethiopia", "Gabon", "Gambia",
        "Ghana", "Guinea", "Guinea-Bissau", "Ivory Coast", "Kenya", "Lesotho",
        "Liberia", "Libya", "Madagascar", "Malawi", "Mali", "Mauritania",
        "Mauritius", "Morocco", "Mozambique", "Namibia", "Niger", "Nigeria",
        "Rwanda", "Sao Tome and Principe", "Senegal", "Seychelles", "Sierra Leone",
        "Somalia", "South Africa", "South Sudan", "Sudan", "Tanzania", "Togo",
        "Tunisia", "Uganda", "Zambia", "Zimbabwe", "Western Sahara",
    ],
    "Arab League": [
        "Algeria", "Bahrain", "Comoros", "Djibouti", "Egypt", "Iraq", "Jordan",
        "Kuwait", "Lebanon", "Libya", "Mauritania", "Morocco", "Oman", "Palestine",
        "Qatar", "Saudi Arabia", "Somalia", "Sudan", "Syria", "Tunisia",
        "United Arab Emirates", "Yemen",
    ],
    "CSTO": ["Russia", "Belarus", "Kazakhstan", "Kyrgyzstan", "Tajikistan", "Armenia"],
}

def insert_regions():
    """Insert Region nodes and link them to countries."""
    print("--- Inserting Regions ---")
    conn = None
    try:
        rows = [
            {"region": region, "country": normalize_entity(country)}
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
            {"alliance": alliance, "country": normalize_entity(country)}
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
