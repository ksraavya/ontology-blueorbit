from common.db import Neo4jConnection
from datetime import date, timedelta, datetime

# Keyword Dictionaries
CONFLICT_KEYWORDS = [
    'war', 'attack', 'airstrike', 'missile', 'troops',
    'invasion', 'offensive', 'battle', 'ceasefire',
    'casualties', 'bombing', 'military operation'
]

DIPLOMACY_KEYWORDS = [
    'agreement', 'treaty', 'talks', 'summit', 'sanctions',
    'deal', 'alliance', 'cooperation', 'bilateral',
    'negotiations', 'peace', 'diplomatic'
]

ARMS_KEYWORDS = [
    'weapons', 'arms deal', 'defense contract', 'fighter jet',
    'missile system', 'military equipment', 'ammunition',
    'procurement', 'defense spending', 'military aid'
]

ESCALATION_KEYWORDS = [
    'escalation', 'tension', 'threat', 'warning', 'nuclear',
    'mobilization', 'ultimatum', 'provocation', 'standoff'
]

TOP_COUNTRIES = [ 
    'United States', 'Russia', 'China', 'Ukraine', 
    'Israel', 'Iran', 'India', 'Pakistan', 'Turkey', 
    'Saudi Arabia', 'North Korea', 'South Korea', 
    'Germany', 'France', 'United Kingdom', 'Japan', 
    'Brazil', 'Syria', 'Yemen', 'Afghanistan', 
    'Taiwan', 'Poland', 'NATO', 'Belarus', 'Georgia', 
    'Azerbaijan', 'Armenia', 'Ethiopia', 'Sudan', 
    'Nigeria', 'Egypt', 'Libya', 'Iraq', 'Lebanon', 
    'Palestine', 'Qatar', 'UAE', 'Indonesia', 
    'Philippines', 'Vietnam', 'Myanmar', 'Thailand', 
    'Bangladesh', 'Sri Lanka', 'Nepal', 'Kazakhstan', 
    'Uzbekistan', 'Serbia', 'Kosovo', 'Hungary' 
] 

ALIASES = { 
    'US': 'United States', 
    'USA': 'United States', 
    'UK': 'United Kingdom', 
    'USSR': 'Russian Federation', 
    'Korean': 'Korea', 
    'Iranian': 'Iran', 
    'Israeli': 'Israel', 
    'Chinese': 'China', 
    'Russian': 'Russian Federation', 
    'Indian': 'India', 
    'Pakistani': 'Pakistan', 
    'Ukrainian': 'Ukraine', 
    'Turkish': 'Turkey', 
    'Palestinian': 'Palestine', 
    'Syrian': 'Syrian Arab Republic', 
    'Iraqi': 'Iraq', 
    'Saudi': 'Saudi Arabia', 
    'Taliban': 'Afghanistan', 
    'Hamas': 'Palestine', 
    'Hezbollah': 'Lebanon', 
    'Houthi': 'Yemen', 
    'Wagner': 'Russian Federation' 
} 

def classify_article(title, description):
    """
    Combines title + description into one text string.
    Checks how many keywords from each category appear.
    Returns the category with most matches.
    If no keywords match, returns 'general'.
    """
    text = f"{title or ''} {description or ''}".lower()
    
    scores = {
        'conflict': sum(1 for k in CONFLICT_KEYWORDS if k in text),
        'diplomacy': sum(1 for k in DIPLOMACY_KEYWORDS if k in text),
        'arms_trade': sum(1 for k in ARMS_KEYWORDS if k in text),
        'escalation': sum(1 for k in ESCALATION_KEYWORDS if k in text)
    }
    
    # Filter out categories with 0 matches
    matched_categories = {cat: score for cat, score in scores.items() if score > 0}
    
    if not matched_categories:
        return 'general'
    
    # Return category with the highest score
    return max(matched_categories, key=matched_categories.get)

def enrich_news_in_graph():
    """
    Fetches all news articles from Neo4j that do not yet have a 'category' property.
    For each article, calls classify_article() on its title and description fields.
    Updates the article node and creates ConflictSignal nodes for specific categories.
    """
    db = Neo4jConnection()
    try:
        # 1. Fetch articles without category (trying both labels used in codebase)
        query_fetch = """
        MATCH (a)
        WHERE (a:NewsArticle OR a:Article) AND a.category IS NULL
        RETURN elementId(a) as id, a.title as title, 
               coalesce(a.description, a.summary, "") as description, 
               a.published as date
        """
        articles = db.run_query(query_fetch)
        print(f"Enriching {len(articles)} articles...")

        for art in articles:
            category = classify_article(art['title'], art['description'])
            
            # 2. Update article node
            query_update = """
            MATCH (a) WHERE elementId(a) = $id
            SET a.category = $category,
                a.enriched = true
            """
            db.run_query(query_update, {'id': art['id'], 'category': category})
            
            # 3. Create ConflictSignal if category is conflict or escalation
            if category in ['conflict', 'escalation']:
                # Note: We need to find countries mentioned in this article
                query_signal = """
                MATCH (c:Country)-[:MENTIONED_IN]->(a)
                WHERE elementId(a) = $id
                MERGE (s:ConflictSignal {date: a.published})
                MERGE (c)-[:HAS_CONFLICT_SIGNAL]->(s)
                SET s.source = 'news',
                    s.category = $category,
                    s.article_count = coalesce(s.article_count, 0) + 1
                """
                db.run_query(query_signal, {'id': art['id'], 'category': category})
        
        print("Enrichment complete.")
    finally:
        db.close()

def get_news_intelligence_summary():
    """
    Returns for each country:
    - total news mentions this week
    - breakdown by category (conflict/diplomacy/arms/escalation)
    - escalation_score = (conflict_count + escalation_count) / total_mentions
    """
    db = Neo4jConnection()
    try:
        # Using Cypher to calculate everything
        query = """
        MATCH (c:Country)-[:MENTIONED_IN]->(a)
        WHERE (a:NewsArticle OR a:Article) 
        AND date(a.published) >= date() - duration({days: 7})
        WITH c, a
        RETURN c.name AS country,
               count(a) AS total_mentions,
               sum(CASE WHEN a.category = 'conflict' THEN 1 ELSE 0 END) AS conflict_mentions,
               sum(CASE WHEN a.category = 'escalation' THEN 1 ELSE 0 END) AS escalation_mentions,
               sum(CASE WHEN a.category = 'diplomacy' THEN 1 ELSE 0 END) AS diplomacy_mentions,
               sum(CASE WHEN a.category = 'arms_trade' THEN 1 ELSE 0 END) AS arms_mentions
        ORDER BY total_mentions DESC LIMIT 20
        """
        results = db.run_query(query)
        
        # Calculate escalation score in Python for clarity, though could be done in Cypher
        summary = []
        for r in results:
            total = r['total_mentions']
            if total > 0:
                esc_score = (r['conflict_mentions'] + r['escalation_mentions']) / total
            else:
                esc_score = 0
            
            r['escalation_score'] = round(esc_score, 2)
            summary.append(r)
            
        return summary
    finally:
        db.close()

def build_co_mention_network():
    """
    Finds pairs of countries that appear in the same article in the last 30 days.
    Creates a CO_MENTIONED_WITH relationship between them with category context.
    """
    db = Neo4jConnection()
    try:
        query = """
        MATCH (c1:Country)-[:MENTIONED_IN]->(a)<-[:MENTIONED_IN]-(c2:Country)
        WHERE (a:NewsArticle OR a:Article)
        AND c1.name < c2.name 
        AND a.category IS NOT NULL
        AND date(a.published) >= date() - duration({days: 30})
        WITH c1, c2, 
             count(a) AS co_mentions, 
             sum(CASE WHEN a.category = 'conflict' THEN 1 ELSE 0 END) AS conflict_context, 
             sum(CASE WHEN a.category = 'escalation' THEN 1 ELSE 0 END) AS escalation_context, 
             sum(CASE WHEN a.category = 'diplomacy' THEN 1 ELSE 0 END) AS diplomacy_context, 
             sum(CASE WHEN a.category = 'arms_trade' THEN 1 ELSE 0 END) AS arms_context 
        WHERE co_mentions >= 2 
        MERGE (c1)-[r:CO_MENTIONED_WITH]->(c2) 
        SET r.count = co_mentions, 
            r.conflict_context = conflict_context, 
            r.escalation_context = escalation_context, 
            r.diplomacy_context = diplomacy_context, 
            r.arms_context = arms_context, 
            r.dominant_context = CASE 
                WHEN conflict_context + escalation_context >= diplomacy_context + arms_context 
                THEN 'hostile' 
                ELSE 'cooperative' 
            END, 
            r.last_updated = date()
        RETURN count(r) as relationship_count
        """
        result = db.run_query(query)
        print(f"Built co-mention network: {result[0]['relationship_count']} relationships updated.")
    finally:
        db.close()

def correlate_live_with_historical():
    """
    Joins live news signals with historical conflict stats.
    Calculates a combined risk score and updates the Country node.
    """
    db = Neo4jConnection()
    try:
        query = """
        MATCH (c:Country)-[:HAS_CONFLICT_SIGNAL]->(s:ConflictSignal) 
        WITH c, count(s) AS live_signals 
        MATCH (c)-[cf:HAS_CONFLICT_STATS]->(y:Year) 
        WHERE y.year >= 2020 
        WITH c, 
             live_signals, 
             sum(cf.total_fatalities) AS historical_fatalities, 
             cf.fatality_trend AS trend, 
             avg(cf.normalized_weight) AS avg_conflict_weight 
        RETURN c.name AS country, 
               live_signals, 
               historical_fatalities, 
               trend, 
               round(avg_conflict_weight, 4) AS historical_intensity, 
               round( 
                   (toFloat(live_signals) / 10 * 0.4) + 
                   (avg_conflict_weight * 0.6) 
               , 3) AS combined_risk_score 
        ORDER BY combined_risk_score DESC LIMIT 15
        """
        results = db.run_query(query)
        print(f"Correlating live signals with historical data for {len(results)} countries...")

        for r in results:
            update_query = """
            MATCH (c:Country {name: $country})
            SET c.live_risk_score = $score,
                c.risk_last_updated = datetime()
            """
            db.run_query(update_query, {
                'country': r['country'],
                'score': r['combined_risk_score']
            })
        
        return results
    finally:
        db.close()

def extract_secondary_countries():
    """
    Finds articles that currently mention only one country and tries to extract a 
    second country from the title using a list of top countries and aliases.
    Uses batch processing for efficiency.
    """
    db = Neo4jConnection()
    try:
        # Query articles that have exactly one country mention
        query_fetch = """
        MATCH (c:Country)-[:MENTIONED_IN]->(a) 
        WITH a, collect(c.name) AS mentioned_countries 
        WHERE size(mentioned_countries) = 1 
        RETURN elementId(a) AS article_id, 
               a.title AS title, 
               mentioned_countries[0] AS primary_country
        """
        articles = db.run_query(query_fetch)
        print(f"Scanning {len(articles)} articles for secondary country mentions...")

        updates = []
        for art in articles:
            title = art['title']
            primary_country = art['primary_country']
            article_id = art['article_id']
            
            second_country = None
            
            # Check for direct matches in TOP_COUNTRIES
            for country in TOP_COUNTRIES:
                if country != primary_country and country.lower() in title.lower():
                    second_country = country
                    break
            
            # If no direct match, check for aliases
            if not second_country:
                for alias, real_name in ALIASES.items():
                    if real_name != primary_country and alias.lower() in title.lower():
                        second_country = real_name
                        break
            
            if second_country:
                updates.append({
                    'article_id': article_id,
                    'second_country': second_country
                })
        
        if updates:
            print(f"Found {len(updates)} potential new links. Applying batch update...")
            batch_query = """
            UNWIND $updates AS update
            MATCH (c:Country {name: update.second_country})
            MATCH (a) WHERE elementId(a) = update.article_id
            MERGE (c)-[:MENTIONED_IN]->(a)
            RETURN count(*) as count
            """
            # Batch size of 500
            total_created = 0
            for i in range(0, len(updates), 500):
                batch = updates[i:i+500]
                result = db.run_query(batch_query, {'updates': batch})
                total_created += result[0]['count'] if result else 0
            
            print(f"Extraction complete: {total_created} new country-article links created.")
            
            # Rebuild the network with enriched data
            if total_created > 0:
                print("Rebuilding co-mention network...")
                build_co_mention_network()
        else:
            print("No new secondary mentions found.")
            
    finally:
        db.close()

def cleanup_news_nodes():
    """
    Deletes all NewsArticle and ConflictSignal nodes and their relationships from the graph.
    These are staging nodes and are no longer needed after enrichment.
    """
    db = Neo4jConnection()
    try:
        query = """
        MATCH (n) 
        WHERE n:NewsArticle OR n:ConflictSignal
        DETACH DELETE n 
        RETURN count(n) AS deleted 
        """
        result = db.run_query(query)
        deleted_count = result[0]['deleted'] if result else 0
        print(f"Cleanup complete: Deleted {deleted_count} news/signal nodes.")
        return deleted_count
    finally:
        db.close()


if __name__ == "__main__":
    print("Starting News Enrichment Process...")
    enrich_news_in_graph()
    extract_secondary_countries() # Added extraction step
    build_co_mention_network()
    correlate_live_with_historical()
    
    print("\n--- Intelligence Summary (Last 7 Days) ---")

    summary = get_news_intelligence_summary()
    for item in summary:
        print(f"{item['country']}: {item['total_mentions']} mentions | Esc Score: {item['escalation_score']} | "
              f"Conflict: {item['conflict_mentions']}, Esc: {item['escalation_mentions']}, "
              f"Diplomacy: {item['diplomacy_mentions']}")
