import feedparser
import pandas as pd
import time
import os
import sys
from datetime import datetime, timedelta

# Sys path fix to reach common/ from modules/defense/live/
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from common.db import Neo4jConnection
from common.country_mapper import normalize_country

RSS_FEEDS = {
    "defensenews_land": "https://www.defensenews.com/arc/outboundfeeds/rss/category/land/",
    "defensenews_air": "https://www.defensenews.com/arc/outboundfeeds/rss/category/air/",
    "defensenews_naval": "https://www.defensenews.com/arc/outboundfeeds/rss/category/naval/",
    "defensenews_cyber": "https://www.defensenews.com/arc/outboundfeeds/rss/category/cyber/",
    "defensenews_global": "https://www.defensenews.com/arc/outboundfeeds/rss/category/global/",
    "defensenews_pentagon": "https://www.defensenews.com/arc/outboundfeeds/rss/category/pentagon/"
}

def fetch_rss_feeds():
    """
    Loops through all feeds using feedparser.parse(url)
    Only keeps articles from the last 7 days.
    """
    all_articles = []
    now = datetime.now()
    seven_days_ago = now - timedelta(days=7)
    
    total_overall = 0
    
    for source_name, url in RSS_FEEDS.items():
        print(f"Fetching feed: {source_name}...")
        feed = feedparser.parse(url)
        feed_count = 0
        
        for entry in feed.entries:
            published_dt = None
            
            # Safely parse published date
            if hasattr(entry, 'published_parsed'):
                try:
                    published_dt = datetime(*entry.published_parsed[:6])
                except Exception:
                    pass
            
            if not published_dt:
                # Fallback if published_parsed fails
                try:
                    # Some feeds use different date formats
                    published_dt = datetime.strptime(entry.published[:25].strip(), '%a, %d %b %Y %H:%M:%S')
                except Exception:
                    continue
            
            # Only keep last 7 days
            if published_dt >= seven_days_ago:
                article = {
                    "title": entry.title,
                    "url": entry.link,
                    "summary": getattr(entry, 'summary', ''),
                    "published": published_dt.strftime("%Y-%m-%d"),
                    "source": source_name
                }
                all_articles.append(article)
                feed_count += 1
        
        print(f"Fetched {feed_count} articles from {source_name}")
        total_overall += feed_count
        time.sleep(2) # Respect rate limits
        
    print(f"\nTotal overall articles fetched: {total_overall}")
    
    if not all_articles:
        return pd.DataFrame()
        
    df = pd.DataFrame(all_articles)
    df = df.drop_duplicates(subset=["url"])
    print(f"Total unique articles: {len(df)}")
    return df

def match_countries(df):
    """
    Connects to Neo4j and fetches all existing Country node names.
    Checks if any country name appears in title + summary combined.
    """
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
                    "keyword": "rss"
                }
                matched_data.append(matched_row)
                
    if not matched_data:
        print("No country matches found.")
        return pd.DataFrame()
        
    matched_df = pd.DataFrame(matched_data)
    matched_df = matched_df.drop_duplicates(subset=["url", "country"])
    
    print(f"Total matched pairs: {len(matched_df)}")
    print(f"Unique country count: {matched_df['country'].nunique()}")
    return matched_df

def insert_rss_news(df):
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
    batch_size = 200
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        try:
            conn.run_query(query, {"rows": batch})
            total += len(batch)
            print(f"Inserted batch {i//batch_size + 1} — {total} rows so far")
        except Exception as e:
            print(f"Batch {i//batch_size + 1} failed: {e}")
            
    conn.close()
    print(f"Done. Total inserted: {total}")

def verify_rss_insert():
    """
    Runs verify query and prints results.
    """
    conn = Neo4jConnection()
    try:
        query = """
        MATCH (c:Country)-[:MENTIONED_IN]->(n:NewsArticle)
        WHERE n.keyword = 'rss'
        RETURN c.name AS country, count(n) AS articles
        ORDER BY articles DESC LIMIT 10
        """
        results = conn.run_query(query)
        print("\n=== TOP 10 COUNTRIES IN RSS NEWS ===")
        for row in results:
            print(f"{row['country']}: {row['articles']} articles")
    finally:
        conn.close()

if __name__ == '__main__':
    df = fetch_rss_feeds()
    if not df.empty:
        matched = match_countries(df)
        if not matched.empty:
            insert_rss_news(matched)
            verify_rss_insert()
