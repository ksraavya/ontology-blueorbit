import os 
import requests 
import pandas as pd 
from datetime import datetime, timedelta 
from dotenv import load_dotenv 
 
load_dotenv() 
 
ACLED_EMAIL = os.getenv("ACLED_EMAIL") 
ACLED_PASSWORD = os.getenv("ACLED_PASSWORD") 
TOKEN_URL = "https://acleddata.com/oauth/token" 
API_URL = "https://acleddata.com/api/acled/read" 
 
 
def get_access_token(): 
    """ 
    Get OAuth access token using email and password. 
    Token is valid for 24 hours. 
    """ 
    payload = { 
        "username": ACLED_EMAIL, 
        "password": ACLED_PASSWORD, 
        "grant_type": "password", 
        "client_id": "acled" 
    } 
    try: 
        response = requests.post( 
            TOKEN_URL, 
            headers={"Content-Type": "application/x-www-form-urlencoded"}, 
            data=payload 
        ) 
        response.raise_for_status() 
        token = response.json().get("access_token") 
        if not token: 
            print("ERROR: No access token in response:", response.json()) 
            return None 
        print("Token acquired successfully.") 
        return token 
    except Exception as e: 
        print(f"ERROR getting token: {e}") 
        return None 
 
 
def fetch_recent_acled(days_back=30): 
    """ 
    Fetch recent conflict events from ACLED API. 
    Returns cleaned dataframe. 
    """ 
    token = get_access_token() 
    if not token: 
        print("Cannot fetch data without token.") 
        return pd.DataFrame() 
 
    today = datetime.today().strftime("%Y-%m-%d") 
    start_date = (datetime.today() - timedelta(days=days_back)).strftime("%Y-%m-%d") 
 
    # Correct syntax for BETWEEN: event_date=START|END&event_date_where=BETWEEN
    params = { 
        "event_date": f"{start_date}|{today}",
        "event_date_where": "BETWEEN",
        "fields": "event_id_cnty|country|event_type|fatalities|event_date", 
        "limit": 1000,
        "_format": "json"
    } 
 
    # Strictly follow the Python example from the documentation
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    } 
 
    try: 
        print(f"Fetching data from {API_URL}...")
        response = requests.get(API_URL, headers=headers, params=params) 
        if response.status_code == 403:
            print(f"403 Forbidden Error: {response.text}")
            print("HINT: This usually means the account exists but has not been authorized for programmatic API access.")
            print("Please check your myACLED dashboard or contact access@acleddata.com")
        response.raise_for_status() 
        data = response.json().get("data", []) 
        if not data: 
            print("No data returned from ACLED.") 
            return pd.DataFrame() 
 
        df = pd.DataFrame(data) 
        df = df.rename(columns={ 
            "event_id_cnty": "event_id", 
            "event_date": "date", 
            "event_type": "event_type", 
            "fatalities": "fatalities", 
            "country": "country" 
        }) 
        df["fatalities"] = pd.to_numeric(df["fatalities"], errors="coerce").fillna(0).astype(int) 
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d") 
        df = df.dropna(subset=["country", "date"]) 
 
        print(f"Fetched {len(df)} events from {start_date} to {today}") 
        print(f"Unique countries: {df['country'].nunique()}") 
        print(df.head(5)) 
        return df 
 
    except Exception as e: 
        print(f"ERROR fetching ACLED data: {e}") 
        return pd.DataFrame() 
 
 
def insert_live_acled(df): 
    """ 
    Insert live ACLED events into Neo4j. 
    Uses MERGE for Country, CREATE for each unique event. 
    """ 
    import sys 
    import os 
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))) 
 
    from common.db import Neo4jConnection 
    from common.country_mapper import normalize_country 
 
    df["country"] = df["country"].apply(normalize_country) 
    df = df.dropna(subset=["country"]) 
 
    rows = df.to_dict("records") 
    conn = Neo4jConnection() 
 
    query = """ 
    UNWIND $rows AS row 
    MERGE (c:Country {name: row.country}) 
    CREATE (e:ConflictEvent { 
        event_id: row.event_id, 
        event_type: row.event_type, 
        date: row.date, 
        fatalities: toInteger(row.fatalities) 
    }) 
    MERGE (c)-[:INVOLVED_IN]->(e) 
    """ 
 
    total = 0 
    for i in range(0, len(rows), 500): 
        batch = rows[i:i+500] 
        try: 
            conn.run_query(query, {"rows": batch}) 
            total += len(batch) 
            print(f"Inserted batch {i//500 + 1} — {total} rows so far") 
        except Exception as e: 
            print(f"Batch {i//500 + 1} failed: {e}") 
 
    conn.close() 
    print(f"Done. Total inserted: {total}") 
 
 
if __name__ == "__main__": 
    df = fetch_recent_acled(days_back=30) 
    if not df.empty: 
        insert_live_acled(df)
