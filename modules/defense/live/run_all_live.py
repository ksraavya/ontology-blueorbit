import sys
import os
from datetime import datetime

# Sys path fix to reach project root from modules/defense/live/
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from common.db import Neo4jConnection
from modules.defense.live.gdelt_fetcher import fetch_all_defence_news, insert_gdelt_news
from modules.defense.live.rss_fetcher import fetch_rss_feeds, match_countries, insert_rss_news
from modules.defense.live.apitube_fetcher import fetch_apitube_news, match_and_insert_apitube
from modules.defense.live.news_enrichment import (
    enrich_news_in_graph, build_co_mention_network, 
    extract_secondary_countries, correlate_live_with_historical, 
    cleanup_news_nodes
)

def run_all_live_updates():
    start_time = datetime.now()
    print(f"=== LIVE DATA REFRESH STARTED AT: {start_time} ===\n")

    # STEP 0: Cleanup stale news articles
    print("=== STEP 0: Cleanup stale news articles ===") 
    try:
        conn = Neo4jConnection() 
        result = conn.run_query("MATCH (n) WHERE n:NewsArticle OR n:ConflictSignal DETACH DELETE n RETURN count(n) AS deleted") 
        print(f"Deleted stale articles: {result}") 
        conn.close() 
    except Exception as e:
        print(f"Step 0 Cleanup Failed: {e}")

    # STEP 1: GDELT
    print("--- STEP 1: GDELT NEWS ---")
    try:
        df_gdelt = fetch_all_defence_news(days_back=7)
        if not df_gdelt.empty:
            insert_gdelt_news(df_gdelt)
        else:
            print("No new GDELT articles found.")
    except Exception as e:
        print(f"GDELT Step Failed: {e}")

    print("\n")

    # STEP 2: RSS Feeds
    print("--- STEP 2: RSS DEFENSE NEWS ---")
    try:
        df_rss = fetch_rss_feeds()
        if not df_rss.empty:
            matched_rss = match_countries(df_rss)
            if not matched_rss.empty:
                insert_rss_news(matched_rss)
            else:
                print("No country matches found in RSS feeds.")
        else:
            print("No new RSS articles found.")
    except Exception as e:
        print(f"RSS Step Failed: {e}")

    print("\n")

    # STEP 3: APITube
    print("--- STEP 3: APITUBE NEWS ---")
    try:
        df_apitube = fetch_apitube_news()
        if not df_apitube.empty:
            match_and_insert_apitube(df_apitube)
        else:
            print("No new APITube articles found.")
    except Exception as e:
        print(f"APITube Step Failed: {e}")

    print("\n")

    # STEP 4: News Enrichment
    print("--- STEP 4: NEWS ENRICHMENT & CORRELATION ---")
    try:
        print("Enriching articles with categories...")
        enrich_news_in_graph()
        
        print("Extracting secondary countries from titles...")
        extract_secondary_countries()
        
        print("Building co-mention network...")
        build_co_mention_network()
        
        print("Correlating live signals with historical data...")
        correlate_live_with_historical()
        
        print("Cleaning up staging NewsArticle nodes...")
        cleanup_news_nodes()
        
    except Exception as e:
        print(f"Enrichment Step Failed: {e}")

    print("\n")

    # FINAL SUMMARY
    print("--- FINAL SUMMARY ---")
    try:
        conn = Neo4jConnection()
        query = """
        MATCH (c:Country)
        WHERE c.live_risk_score IS NOT NULL
        RETURN count(c) AS countries_with_risk_scores,
               avg(c.live_risk_score) AS avg_risk_score
        """
        results = conn.run_query(query)
        conn.close()
        
        if results:
            summary = results[0]
            print(f"Countries with Risk Scores: {summary['countries_with_risk_scores']}")
            print(f"Average Live Risk Score: {round(summary['avg_risk_score'], 4)}")
    except Exception as e:
        print(f"Summary Query Failed: {e}")

    # FINAL CLEANUP: Removing news nodes and conflict signals
    print("=== FINAL CLEANUP: Removing news article and conflict signal nodes ===") 
    try:
        conn = Neo4jConnection() 
        result = conn.run_query("MATCH (n) WHERE n:NewsArticle OR n:ConflictSignal DETACH DELETE n RETURN count(n) AS deleted") 
        print(f"Cleaned up news and signal nodes: {result}") 
        conn.close() 
    except Exception as e:
        print(f"Final Cleanup Failed: {e}")

    end_time = datetime.now()
    duration = end_time - start_time
    print(f"\n=== LIVE DATA REFRESH COMPLETED AT: {end_time} ===")
    print(f"=== TOTAL TIME TAKEN: {duration} ===")

if __name__ == '__main__':
    run_all_live_updates()
