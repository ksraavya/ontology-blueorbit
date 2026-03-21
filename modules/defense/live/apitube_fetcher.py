import requests
import pandas as pd
import time
import os
import sys
from dotenv import load_dotenv

# Sys path fix to reach common/ from modules/defense/live/
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from common.db import Neo4jConnection
from common.country_mapper import normalize_country

load_dotenv()

APITUBE_API_KEY = os.getenv("APITUBE_API_KEY")
BASE_URL = "https://api.apitube.io/v1/news/everything"

KEYWORDS = ["military", "defense", "war", "conflict", "sanctions", "weapons", "airstrike", "troops"]

def fetch_apitube_news():
    """
    Loops through keywords and fetches news from APITube.
    """
    all_articles = []
    headers = {"X-API-Key": APITUBE_API_KEY}
    
    total_fetched = 0
    
    for keyword in KEYWORDS:
        print(f"Fetching APITube news for keyword: {keyword}...")
        params = {
            "title": keyword,
            "language.code": "en",
            "per_page": 20
        }
        
        try:
            response = requests.get(BASE_URL, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            results = data.get("results", [])
            
            keyword_count = 0
            for art in results:
                title = art.get("title")
                if not title:
                    continue
                
                article = {
                    "title": title,
                    "url": art.get("url"),
                    "summary": art.get("description", ""),
                    "published": art.get("published_at", "")[:10],
                    "source": art.get("source", {}).get("name", "Unknown"),
                    "keyword": keyword
                }
                all_articles.append(article)
                keyword_count += 1
            
            print(f"Fetched {keyword_count} articles for '{keyword}'")
            total_fetched += keyword_count
            time.sleep(3) # Respect rate limits
            
        except Exception as e:
            print(f"ERROR fetching APITube for '{keyword}': {e}")
            time.sleep(5)
            continue
            
    print(f"\nTotal articles fetched from APITube: {total_fetched}")
    
    if not all_articles:
        return pd.DataFrame()
        
    df = pd.DataFrame(all_articles)
    df = df.dropna(subset=["title"])
    df = df[df["title"].str.strip() != ""]
    df = df.drop_duplicates(subset=["url"])
    
    print(f"Total unique articles: {len(df)}")
    return df

def match_and_insert_apitube(df):
    """
    Matches articles to countries and inserts into Neo4j.
    """
    # Fetch country names
    conn = Neo4jConnection()
    try:
        query = "MATCH (c:Country) RETURN c.name AS name"
        results = conn.run_query(query)
        country_list = [r['name'] for r in results]
    finally:
        conn.close()
        
    matched_data = []
    
    for _, row in df.iterrows():
        text_to_search = f"{row['title']} {row['summary']}".lower()
        
        for country in country_list:
            if country.lower() in text_to_search:
                matched_row = {
                    "country": country,
                    "title": row['title'],
                    "url": row['url'],
                    "published": row['published'],
                    "source": row['source'],
                    "keyword": row['keyword']
                }
                matched_data.append(matched_row)
                
    if not matched_data:
        print("No country matches found for APITube articles.")
        return
        
    matched_df = pd.DataFrame(matched_data)
    matched_df = matched_df.drop_duplicates(subset=["url", "country"])
    
    print(f"Total matched pairs: {len(matched_df)}")
    print(f"Unique countries found: {matched_df['country'].nunique()}")
    
    # Insert into Neo4j
    rows = matched_df.to_dict("records")
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
    
    total_inserted = 0
    batch_size = 200
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        try:
            conn.run_query(query, {"rows": batch})
            total_inserted += len(batch)
            print(f"Inserted batch {i//batch_size + 1} — {total_inserted} rows so far")
        except Exception as e:
            print(f"Batch {i//batch_size + 1} failed: {e}")
            
    conn.close()
    print(f"Done. Total APITube rows inserted: {total_inserted}")

def verify_apitube_insert():
    """
    Runs verify query and prints results.
    """
    conn = Neo4jConnection()
    try:
        query = """
        MATCH (c:Country)-[:MENTIONED_IN]->(n:NewsArticle)
        WHERE NOT n.keyword = 'rss'
        RETURN c.name AS country, count(n) AS articles
        ORDER BY articles DESC LIMIT 10
        """
        results = conn.run_query(query)
        print("\n=== TOP 10 COUNTRIES IN APITUBE/GDELT NEWS ===")
        for row in results:
            print(f"{row['country']}: {row['articles']} articles")
    finally:
        conn.close()

if __name__ == '__main__':
    df = fetch_apitube_news()
    if not df.empty:
        match_and_insert_apitube(df)
        verify_apitube_insert()
