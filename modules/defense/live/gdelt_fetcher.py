import requests 
import pandas as pd 
from datetime import datetime, timedelta 
import sys 
import os 
import time
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))) 
 
from common.db import Neo4jConnection 
from common.country_mapper import normalize_country 
 
GDELT_URL = "https://api.gdeltproject.org/api/v2/doc/doc" 
 
DEFENCE_KEYWORDS = [ 
    "military conflict", "armed attack", "war", "airstrike", 
    "ceasefire", "troops", "missile", "bombing", "sanctions", 
    "defense spending", "arms deal", "weapons", "insurgency" 
] 
 
def fetch_gdelt_events(keyword, days_back=7): 
    """ 
    Fetch news articles from GDELT for a given keyword. 
    Returns dataframe with title, url, country, date, source. 
    """ 
    today = datetime.today() 
    start = today - timedelta(days=days_back) 
 
    start_str = start.strftime("%Y%m%d%H%M%S") 
    end_str = today.strftime("%Y%m%d%H%M%S") 
 
    params = { 
        "query": keyword, 
        "mode": "artlist", 
        "maxrecords": 250, 
        "startdatetime": start_str, 
        "enddatetime": end_str, 
        "format": "json" 
    } 
 
    try: 
        response = requests.get(GDELT_URL, params=params, timeout=30) 
        response.raise_for_status() 
        data = response.json() 
        articles = data.get("articles", []) 
        if not articles: 
            print(f"No articles for keyword: {keyword}") 
            return pd.DataFrame() 
 
        df = pd.DataFrame(articles) 
        df = df[["title", "url", "domain", "seendate", "sourcecountry"]].copy() 
        df = df.rename(columns={ 
            "domain": "source", 
            "seendate": "published", 
            "sourcecountry": "country" 
        }) 
        df["keyword"] = keyword 
        df["country"] = df["country"].apply( 
            lambda x: normalize_country(x) if pd.notna(x) and x else None 
        ) 
        df = df.dropna(subset=["country", "title"]) 
        df["published"] = pd.to_datetime( 
            df["published"], format="%Y%m%dT%H%M%SZ", errors="coerce" 
        ).dt.strftime("%Y-%m-%d") 
        df = df.dropna(subset=["published"]) 
        return df 
 
    except Exception as e: 
        print(f"ERROR fetching GDELT for '{keyword}': {e}") 
        return pd.DataFrame() 
 
 
def fetch_all_defence_news(days_back=7): 
    """ 
    Fetch articles for all defence keywords and combine. 
    """ 
    all_dfs = [] 
    for i, keyword in enumerate(DEFENCE_KEYWORDS): 
        print(f"Fetching: {keyword}") 
        df = fetch_gdelt_events(keyword, days_back) 
        if not df.empty: 
            all_dfs.append(df) 
        
        # Add a 6-second delay between requests to avoid 429 errors
        if i < len(DEFENCE_KEYWORDS) - 1:
            print("Waiting 15 seconds to respect rate limits...")
            time.sleep(15)
 
    if not all_dfs: 
        print("No data fetched.") 
        return pd.DataFrame() 
 
    combined = pd.concat(all_dfs, ignore_index=True) 
    combined = combined.drop_duplicates(subset=["url"]) 
    print(f"\nTotal unique articles: {len(combined)}") 
    print(f"Unique countries: {combined['country'].nunique()}") 
    print(combined[["country", "title", "published"]].head(10)) 
    return combined 
 
 
def insert_gdelt_news(df): 
    """ 
    Insert GDELT news articles into Neo4j. 
    Links articles to Country nodes via MENTIONED_IN. 
    """ 
    rows = df.to_dict("records") 
    conn = Neo4jConnection() 
 
    query = """ 
    UNWIND $rows AS row 
    MERGE (c:Country {name: row.country}) 
    CREATE (n:NewsArticle { 
        title: row.title, 
        url: row.url, 
        source: row.source, 
        published: row.published, 
        keyword: row.keyword 
    }) 
    MERGE (c)-[:MENTIONED_IN]->(n) 
    """ 
 
    total = 0 
    for i in range(0, len(rows), 200): 
        batch = rows[i:i+200] 
        try: 
            conn.run_query(query, {"rows": batch}) 
            total += len(batch) 
            print(f"Inserted batch {i//200 + 1} — {total} rows so far") 
        except Exception as e: 
            print(f"Batch {i//200 + 1} failed: {e}") 
 
    conn.close() 
    print(f"Done. Total inserted: {total}") 
 
 
def verify_gdelt_insert(): 
    """ 
    Verify news articles are in Neo4j. 
    """ 
    conn = Neo4jConnection() 
    query = """ 
    MATCH (c:Country)-[:MENTIONED_IN]->(n:NewsArticle) 
    RETURN c.name AS country, count(n) AS articles 
    ORDER BY articles DESC 
    LIMIT 10 
    """ 
    results = conn.run_query(query) 
    conn.close() 
    for row in results: 
        print(row) 
 
 
if __name__ == "__main__": 
    df = fetch_all_defence_news(days_back=7) 
    if not df.empty: 
        insert_gdelt_news(df) 
        print("\n=== VERIFICATION ===") 
        verify_gdelt_insert()
