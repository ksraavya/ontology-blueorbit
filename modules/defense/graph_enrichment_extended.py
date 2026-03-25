from common.db import Neo4jConnection

# Hardcoded Dictionaries
NUCLEAR_STATES = { 
    'United States': 'confirmed', 
    'Russian Federation': 'confirmed', 
    'China': 'confirmed', 
    'United Kingdom': 'confirmed', 
    'France': 'confirmed', 
    'India': 'confirmed', 
    'Pakistan': 'confirmed', 
    'Israel': 'undeclared', 
    'Korea, Democratic People\'s Republic of': 'confirmed',
    'North Korea': 'confirmed'
} 

UN_SECURITY_COUNCIL_PERMANENT = [ 
    'United States', 'Russian Federation', 'China', 
    'United Kingdom', 'France' 
] 

REGIONAL_POWERS = { 
    'Middle East': 'Saudi Arabia', 
    'South Asia': 'India', 
    'East Asia': 'China', 
    'Southeast Asia': 'Indonesia', 
    'South America': 'Brazil', 
    'Africa': 'Nigeria', 
    'Eastern Europe': 'Russian Federation', 
    'Central Asia': 'Kazakhstan' 
} 

def enrich_country_properties():
    """
    Enriches Country nodes in Neo4j with nuclear status, UN P5 status, and regional power status.
    """
    db = Neo4jConnection()
    try:
        updated_count = 0
        
        # 1. Update Nuclear Status
        print("Enriching Nuclear Status...")
        for name, status in NUCLEAR_STATES.items():
            query = """
            MATCH (c:Country {name: $name})
            SET c.nuclear_status = $status,
                c.is_nuclear = true
            RETURN count(c) as updated
            """
            result = db.run_query(query, {'name': name, 'status': status})
            if result and result[0]['updated'] > 0:
                updated_count += result[0]['updated']

        # 2. Update UN Security Council P5 Status
        print("Enriching UN Security Council P5 Status...")
        for name in UN_SECURITY_COUNCIL_PERMANENT:
            query = """
            MATCH (c:Country {name: $name})
            SET c.un_p5 = true
            RETURN count(c) as updated
            """
            result = db.run_query(query, {'name': name})
            if result and result[0]['updated'] > 0:
                updated_count += result[0]['updated']

        # 3. Update Regional Powers
        print("Enriching Regional Powers...")
        for region, country in REGIONAL_POWERS.items():
            query = """
            MATCH (c:Country {name: $country})
            SET c.is_regional_power = true,
                c.dominant_region = $region
            RETURN count(c) as updated
            """
            result = db.run_query(query, {'country': country, 'region': region})
            if result and result[0]['updated'] > 0:
                updated_count += result[0]['updated']

        print(f"\nSuccess: Updated properties for {updated_count} country references.")
        
    finally:
        db.close()

if __name__ == '__main__':
    print("Starting Extended Graph Enrichment...")
    enrich_country_properties()
