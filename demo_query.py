import sys, os
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from common.db import Neo4jConnection

conn = Neo4jConnection()

def show(title, results):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")
    if not results:
        print("  No results.")
        return
    for r in results:
        print(" ", r)

# ── QUERY 1 ─────────────────────────────────────────────────
# Countries spending MORE on defense but still experiencing
# MORE conflict — the "money can't buy peace" insight
show("MONEY CAN'T BUY PEACE — High Spenders, High Casualties 2022",
conn.run_query("""
MATCH (c:Country)-[s:SPENDS_ON_DEFENSE]->(y:Year {year: 2022})
MATCH (c)-[cf:HAS_CONFLICT_STATS]->(y)
WHERE s.amount_usd_millions > 500
RETURN c.name AS country,
       round(s.amount_usd_millions) AS defense_budget_usd_millions,
       cf.total_fatalities AS fatalities_2022,
       round(toFloat(cf.total_fatalities) / s.amount_usd_millions, 4)
           AS fatalities_per_million_usd_spent
ORDER BY fatalities_per_million_usd_spent DESC
LIMIT 10
"""))

# ── QUERY 2 ─────────────────────────────────────────────────
# Countries that INCREASED defense spending every single year
# for 5 consecutive years — true militarization trend
show("MILITARIZATION TREND — Unbroken Spending Increase 2018-2023",
conn.run_query("""
MATCH (c:Country)-[r:SPENDS_ON_DEFENSE]->(y:Year)
WHERE y.year >= 2018 AND y.year <= 2023
WITH c, y.year AS yr, r.amount_usd_millions AS amt
ORDER BY c.name, yr
WITH c, collect(amt) AS amounts
WHERE size(amounts) = 6
  AND all(i IN range(0, size(amounts)-2)
      WHERE amounts[i] < amounts[i+1])
RETURN c.name AS country,
       round(amounts[0]) AS spending_2018,
       round(amounts[5]) AS spending_2023,
       round(((amounts[5] - amounts[0]) / amounts[0]) * 100, 1)
           AS total_increase_percent
ORDER BY total_increase_percent DESC
LIMIT 10
"""))

# ── QUERY 3 ─────────────────────────────────────────────────
# The arms export dominance — what share of global arms
# trade did the top 5 countries control?
show("ARMS DOMINANCE — Share of Global Arms Exports 1950-2000",
conn.run_query("""
MATCH (c:Country)-[r:EXPORTS_ARMS]->(y:Year)
WITH c.name AS country, sum(r.tiv_millions) AS total_tiv
WITH collect({country: country, tiv: total_tiv}) AS all_data,
     sum(total_tiv) AS world_total
UNWIND all_data AS row
RETURN row.country AS country,
       round(row.tiv) AS total_tiv_millions,
       round((row.tiv / world_total) * 100, 2) AS global_share_percent
ORDER BY total_tiv_millions DESC
LIMIT 8
"""))

# ── QUERY 4 ─────────────────────────────────────────────────
# Conflict surge detection — countries where fatalities
# MORE THAN DOUBLED from one year to the next
show("CONFLICT SURGE — Countries Where Fatalities Doubled Year-on-Year",
conn.run_query("""
MATCH (c:Country)-[r1:HAS_CONFLICT_STATS]->(y1:Year)
MATCH (c)-[r2:HAS_CONFLICT_STATS]->(y2:Year)
WHERE y2.year = y1.year + 1
  AND r1.total_fatalities > 100
  AND r2.total_fatalities > r1.total_fatalities * 2
RETURN c.name AS country,
       y1.year AS from_year,
       y2.year AS to_year,
       r1.total_fatalities AS fatalities_before,
       r2.total_fatalities AS fatalities_after,
       round(toFloat(r2.total_fatalities) / r1.total_fatalities, 2)
           AS multiplier
ORDER BY multiplier DESC
LIMIT 10
"""))

# ── QUERY 5 ─────────────────────────────────────────────────
# Defense spending as a geopolitical bloc comparison —
# NATO vs BRICS vs SCO total spending power 2023
show("BLOC POWER — NATO vs BRICS vs SCO Defense Spending 2023",
conn.run_query("""
MATCH (c:Country)-[m:MEMBER_OF]->(b)
MATCH (c)-[s:SPENDS_ON_DEFENSE]->(y:Year {year: 2023})
WHERE b.name IN ['NATO', 'BRICS', 'SCO']
RETURN b.name AS bloc,
       count(DISTINCT c) AS member_countries,
       round(sum(s.amount_usd_millions)) AS total_spending_usd_millions,
       round(avg(s.amount_usd_millions)) AS avg_per_country
ORDER BY total_spending_usd_millions DESC
"""))

# ── QUERY 6 ─────────────────────────────────────────────────
# Most conflict-affected regions — aggregate by region node
show("REGIONAL CONFLICT — Which Region Has Suffered Most",
conn.run_query("""
MATCH (c:Country)-[:BELONGS_TO]->(reg)
MATCH (c)-[cf:HAS_CONFLICT_STATS]->(y:Year)
WHERE y.year >= 2015
RETURN reg.name AS region,
       count(DISTINCT c) AS countries_in_region,
       sum(cf.total_fatalities) AS total_fatalities_since_2015,
       sum(cf.violence_events) AS total_violence_events,
       round(avg(cf.total_fatalities), 0) AS avg_fatalities_per_country
ORDER BY total_fatalities_since_2015 DESC
"""))

# ── QUERY 7 ─────────────────────────────────────────────────
# Countries in the news RIGHT NOW that are also
# top conflict zones — live signal meets historical data
show("LIVE ALERT — Current News Hotspots Matching Historical Conflict Zones",
conn.run_query("""
MATCH (c:Country)-[:MENTIONED_IN]->(a)
WITH c, count(a) AS news_mentions
WHERE news_mentions >= 2
MATCH (c)-[cf:HAS_CONFLICT_STATS]->(y:Year)
WHERE y.year >= 2020
WITH c, news_mentions,
     sum(cf.total_fatalities) AS recent_fatalities
WHERE recent_fatalities > 1000
RETURN c.name AS country,
       news_mentions AS current_news_mentions,
       recent_fatalities AS fatalities_since_2020
ORDER BY news_mentions DESC, recent_fatalities DESC
LIMIT 10
"""))

# ── QUERY 8 ─────────────────────────────────────────────────
# The "sleeping giants" — countries with massive defense
# budgets but almost no recorded conflict
show("SLEEPING GIANTS — High Defense Budget, Almost No Conflict",
conn.run_query("""
MATCH (c:Country)-[s:SPENDS_ON_DEFENSE]->(y:Year {year: 2022})
MATCH (c)-[cf:HAS_CONFLICT_STATS]->(y)
WHERE s.amount_usd_millions > 5000
  AND cf.total_fatalities < 100
RETURN c.name AS country,
       round(s.amount_usd_millions) AS defense_spending_usd_millions,
       cf.total_fatalities AS fatalities_2022
ORDER BY defense_spending_usd_millions DESC
LIMIT 10
"""))

# ── QUERY 9 ─────────────────────────────────────────────────
# Decade comparison — how has global conflict changed
# from 2000s to 2010s to 2020s
show("DECADE COMPARISON — Global Conflict Intensity Over Time",
conn.run_query("""
MATCH (c:Country)-[cf:HAS_CONFLICT_STATS]->(y:Year)
WITH
  sum(CASE WHEN y.year >= 2000 AND y.year < 2010
      THEN cf.total_fatalities ELSE 0 END) AS fatalities_2000s,
  sum(CASE WHEN y.year >= 2010 AND y.year < 2020
      THEN cf.total_fatalities ELSE 0 END) AS fatalities_2010s,
  sum(CASE WHEN y.year >= 2020
      THEN cf.total_fatalities ELSE 0 END) AS fatalities_2020s,
  sum(CASE WHEN y.year >= 2000 AND y.year < 2010
      THEN cf.violence_events ELSE 0 END) AS events_2000s,
  sum(CASE WHEN y.year >= 2010 AND y.year < 2020
      THEN cf.violence_events ELSE 0 END) AS events_2010s,
  sum(CASE WHEN y.year >= 2020
      THEN cf.violence_events ELSE 0 END) AS events_2020s
RETURN fatalities_2000s, fatalities_2010s, fatalities_2020s,
       events_2000s, events_2010s, events_2020s
"""))

# ── QUERY 10 ─────────────────────────────────────────────────
# The ultimate country risk profile — combines spending,
# conflict, news mentions, and alliance membership
show("COUNTRY RISK PROFILE — India",
conn.run_query("""
MATCH (c:Country {name: 'India'})
OPTIONAL MATCH (c)-[s:SPENDS_ON_DEFENSE]->(y1:Year {year: 2023})
OPTIONAL MATCH (c)-[cf:HAS_CONFLICT_STATS]->(y2:Year {year: 2022})
OPTIONAL MATCH (c)-[ex:EXPORTS_ARMS]->(y3:Year)
OPTIONAL MATCH (c)-[:MEMBER_OF]->(b)
OPTIONAL MATCH (c)-[:MENTIONED_IN]->(a)
RETURN c.name AS country,
       round(s.amount_usd_millions) AS defense_budget_2023,
       cf.total_fatalities AS conflict_fatalities_2022,
       cf.violence_events AS violence_events_2022,
       round(sum(ex.tiv_millions)) AS total_arms_exported,
       collect(DISTINCT b.name) AS alliances,
       count(DISTINCT a) AS news_mentions
"""))

conn.close()
print("\n" + "="*60)
print("  Demo queries complete.")
print("="*60)