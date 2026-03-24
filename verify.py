"""
Analytics Layer + Arms 2001-2025 Verification
==============================================
Verifies:
  - analytics/defense/scores.py  (5 score properties on Country nodes)
  - analytics/defense/derived.py (IS_HIGH_RISK_FOR + IS_INFLUENTIAL_TO edges)
  - EXPORTS_ARMS 2001-2025 new data insertion
  - Full data integrity and logical consistency across all three periods

Usage:
    python analytics_verify.py

Run this AFTER:
    python rerun_arms_2001.py
    python analytics/defense/runner.py
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from common.db import Neo4jConnection


def run(label, query, params=None):
    conn = Neo4jConnection()
    print(f"\n{'='*65}")
    print(f"  {label}")
    print(f"{'='*65}")
    try:
        results = conn.run_query(query, params or {})
        if not results:
            print("  ⚠  No results returned.")
        else:
            for r in results:
                print(" ", r)
    except Exception as e:
        print(f"  ✗ ERROR: {e}")
    finally:
        conn.close()


def section(title):
    print(f"\n\n{'#'*65}")
    print(f"  {title}")
    print(f"{'#'*65}")


# ══════════════════════════════════════════════════════════════════
# SECTION 1 — ARMS DATA: ALL THREE PERIODS
# ══════════════════════════════════════════════════════════════════

section("SECTION 1 — EXPORTS_ARMS: ALL THREE PERIODS OVERVIEW")

run("All periods — record count, exporter count, year range", """
MATCH (c:Country)-[r:EXPORTS_ARMS]->(y:Year)
RETURN r.period AS period,
       count(r) AS records,
       count(DISTINCT c) AS exporters,
       min(y.year) AS from_year,
       max(y.year) AS to_year
ORDER BY from_year
""")

run("Total EXPORTS_ARMS in graph (all periods combined)", """
MATCH (c:Country)-[r:EXPORTS_ARMS]->(y:Year)
RETURN count(r) AS total_relationships,
       count(DISTINCT c) AS total_exporters,
       count(DISTINCT r.period) AS periods_present
""")

run("Is 2001-2025 period present? (must return rows)", """
MATCH (c:Country)-[r:EXPORTS_ARMS]->(y:Year)
WHERE r.period = '2001-2025'
RETURN count(r) AS arms_2001_2025_records,
       count(DISTINCT c) AS exporters,
       min(y.year) AS first_year,
       max(y.year) AS last_year
""")

run("Year coverage check — 2001-2025 period (should have all 25 years)", """
MATCH (c:Country)-[r:EXPORTS_ARMS]->(y:Year)
WHERE r.period = '2001-2025'
RETURN count(DISTINCT y.year) AS distinct_years_covered,
       collect(DISTINCT y.year) AS years_present
""")


# ══════════════════════════════════════════════════════════════════
# SECTION 2 — ARMS DATA: 2001-2025 MATHEMATICAL INTEGRITY
# ══════════════════════════════════════════════════════════════════

section("SECTION 2 — ARMS 2001-2025: FIELD INTEGRITY")

run("All fields present on 2001-2025 records (zero NULLs expected)", """
MATCH (c:Country)-[r:EXPORTS_ARMS]->(y:Year)
WHERE r.period = '2001-2025'
RETURN count(r) AS total,
       count(r.value) AS has_value,
       count(r.normalized_weight) AS has_weight,
       count(r.dependency) AS has_dependency,
       count(CASE WHEN r.value IS NULL THEN 1 END) AS null_values,
       count(CASE WHEN r.normalized_weight IS NULL THEN 1 END) AS null_weights,
       count(CASE WHEN r.dependency IS NULL THEN 1 END) AS null_deps
""")

run("normalized_weight range check — must be 0 to 1 (no above_one)", """
MATCH (c:Country)-[r:EXPORTS_ARMS]->(y:Year)
WHERE r.period = '2001-2025'
RETURN round(max(r.normalized_weight), 4) AS max_weight,
       round(min(r.normalized_weight), 6) AS min_weight,
       round(avg(r.normalized_weight), 4) AS avg_weight,
       count(CASE WHEN r.normalized_weight > 1.0 THEN 1 END) AS above_one,
       count(CASE WHEN r.normalized_weight < 0.0 THEN 1 END) AS below_zero
""")

run("dependency year-level integrity — sum per year must be ~1.0 (worst deviations)", """
MATCH (c:Country)-[r:EXPORTS_ARMS]->(y:Year)
WHERE r.period = '2001-2025'
WITH y.year AS yr, sum(r.dependency) AS year_total
WHERE abs(year_total - 1.0) > 0.05
RETURN yr AS year,
       round(year_total, 4) AS dependency_sum,
       round(abs(year_total - 1.0), 4) AS deviation
ORDER BY deviation DESC LIMIT 10
""")

run("dependency stats for 2001-2025 (avg ~0.033 = healthy market)", """
MATCH (c:Country)-[r:EXPORTS_ARMS]->(y:Year)
WHERE r.period = '2001-2025'
RETURN count(r) AS total,
       count(r.dependency) AS with_dependency,
       round(max(r.dependency), 4) AS max_dep,
       round(avg(r.dependency), 4) AS avg_dep,
       count(CASE WHEN r.dependency > 1.0 THEN 1 END) AS above_one
""")

run("Top 10 by dependency in 2001-2025 (US should dominate)", """
MATCH (c:Country)-[r:EXPORTS_ARMS]->(y:Year)
WHERE r.period = '2001-2025'
RETURN c.name AS country,
       y.year AS year,
       round(r.dependency * 100, 1) AS market_share_pct,
       round(r.normalized_weight, 4) AS normalized_weight
ORDER BY r.dependency DESC LIMIT 10
""")


# ══════════════════════════════════════════════════════════════════
# SECTION 3 — ARMS: CROSS-PERIOD COMPARISON
# ══════════════════════════════════════════════════════════════════

section("SECTION 3 — ARMS: CROSS-PERIOD HISTORICAL ANALYSIS")

run("Market dominance — peak share per country across ALL periods", """
MATCH (c:Country)-[r:EXPORTS_ARMS]->(y:Year)
WITH c.name AS country,
     max(r.dependency) AS peak_share,
     round(avg(r.dependency) * 100, 2) AS avg_share_pct,
     count(r) AS active_years,
     collect(DISTINCT r.period) AS periods_active
WHERE peak_share > 0.08
RETURN country,
       round(peak_share * 100, 1) AS peak_market_share_pct,
       avg_share_pct,
       active_years,
       periods_active
ORDER BY peak_share DESC LIMIT 12
""")

run("US peak share by period — post-Soviet surge vs 2001-2025", """
MATCH (c:Country {name: 'United States'})-[r:EXPORTS_ARMS]->(y:Year)
WITH r.period AS period,
     max(r.dependency) AS peak_share,
     round(avg(r.dependency) * 100, 2) AS avg_pct,
     count(r) AS years_active
RETURN period,
       round(peak_share * 100, 1) AS peak_market_share_pct,
       avg_pct,
       years_active
ORDER BY period
""")

run("Russian Federation arms profile across periods", """
MATCH (c:Country {name: 'Russian Federation'})-[r:EXPORTS_ARMS]->(y:Year)
WITH r.period AS period,
     max(r.dependency) AS peak_share,
     round(avg(r.dependency) * 100, 2) AS avg_pct,
     count(r) AS years_active,
     min(y.year) AS first_year,
     max(y.year) AS last_year
RETURN period,
       first_year, last_year,
       round(peak_share * 100, 1) AS peak_pct,
       avg_pct,
       years_active
ORDER BY first_year
""")

run("No pre-1992 Russian Federation arms (historical fix still intact)", """
MATCH (c:Country {name: 'Russian Federation'})-[r:EXPORTS_ARMS]->(y:Year)
WHERE y.year < 1992
RETURN count(r) AS misattributed_pre_1992_records
""")

run("Soviet Union capped at 1991 (must be first_year>=1950 last_year<=1991)", """
MATCH (c:Country {name: 'Soviet Union'})-[r:EXPORTS_ARMS]->(y:Year)
RETURN min(y.year) AS first_year,
       max(y.year) AS last_year,
       count(r) AS total_records,
       round(max(r.dependency) * 100, 1) AS peak_market_pct
""")

run("Modern arms market 2001-2025 top exporters avg market share", """
MATCH (c:Country)-[r:EXPORTS_ARMS]->(y:Year)
WHERE r.period = '2001-2025'
WITH c.name AS country,
     round(avg(r.dependency) * 100, 2) AS avg_share_pct,
     round(max(r.dependency) * 100, 1) AS peak_pct,
     count(r) AS years_active
WHERE avg_share_pct > 1.0
RETURN country,
       avg_share_pct,
       peak_pct,
       years_active
ORDER BY avg_share_pct DESC LIMIT 10
""")


# ══════════════════════════════════════════════════════════════════
# SECTION 4 — ANALYTICS SCORES: COVERAGE AND BASIC HEALTH
# ══════════════════════════════════════════════════════════════════

section("SECTION 4 — ANALYTICS SCORES: COVERAGE ON COUNTRY NODES")

run("How many countries have each analytics score populated", """
MATCH (c:Country)
WHERE c.defense_spending_score IS NOT NULL
   OR c.military_strength_score IS NOT NULL
   OR c.conflict_risk_score IS NOT NULL
   OR c.arms_export_score IS NOT NULL
   OR c.composite_threat_score IS NOT NULL
RETURN count(CASE WHEN c.defense_spending_score IS NOT NULL THEN 1 END)
           AS has_spending_score,
       count(CASE WHEN c.military_strength_score IS NOT NULL THEN 1 END)
           AS has_military_score,
       count(CASE WHEN c.conflict_risk_score IS NOT NULL THEN 1 END)
           AS has_conflict_score,
       count(CASE WHEN c.arms_export_score IS NOT NULL THEN 1 END)
           AS has_arms_score,
       count(CASE WHEN c.composite_threat_score IS NOT NULL THEN 1 END)
           AS has_composite_score
""")

run("Countries with ALL five scores populated (most complete profiles)", """
MATCH (c:Country)
WHERE c.defense_spending_score IS NOT NULL
  AND c.military_strength_score IS NOT NULL
  AND c.conflict_risk_score IS NOT NULL
  AND c.arms_export_score IS NOT NULL
  AND c.composite_threat_score IS NOT NULL
RETURN count(c) AS countries_with_all_five_scores
""")

run("Countries with ONLY composite but missing component scores (flag)", """
MATCH (c:Country)
WHERE c.composite_threat_score IS NOT NULL
  AND (c.defense_spending_score IS NULL
    OR c.military_strength_score IS NULL
    OR c.conflict_risk_score IS NULL)
RETURN c.name AS country,
       c.composite_threat_score AS composite,
       c.defense_spending_score AS spending,
       c.military_strength_score AS military,
       c.conflict_risk_score AS conflict
ORDER BY c.composite_threat_score DESC LIMIT 10
""")


# ══════════════════════════════════════════════════════════════════
# SECTION 5 — ANALYTICS SCORES: MATHEMATICAL INTEGRITY
# ══════════════════════════════════════════════════════════════════

section("SECTION 5 — ANALYTICS SCORES: RANGE AND NULL CHECKS")

run("composite_threat_score — must be 0-1, zero NULLs among populated", """
MATCH (c:Country)
WHERE c.composite_threat_score IS NOT NULL
RETURN count(c) AS total,
       round(max(c.composite_threat_score), 4) AS max_composite,
       round(min(c.composite_threat_score), 4) AS min_composite,
       round(avg(c.composite_threat_score), 4) AS avg_composite,
       count(CASE WHEN c.composite_threat_score > 1.0 THEN 1 END)
           AS above_one,
       count(CASE WHEN c.composite_threat_score < 0.0 THEN 1 END)
           AS below_zero
""")

run("military_strength_score — range check (bonuses for nuclear/P5)", """
MATCH (c:Country)
WHERE c.military_strength_score IS NOT NULL
RETURN count(c) AS total,
       round(max(c.military_strength_score), 4) AS max_score,
       round(min(c.military_strength_score), 4) AS min_score,
       round(avg(c.military_strength_score), 4) AS avg_score,
       count(CASE WHEN c.military_strength_score > 1.0 THEN 1 END)
           AS above_one
""")

run("defense_spending_score — range check", """
MATCH (c:Country)
WHERE c.defense_spending_score IS NOT NULL
RETURN count(c) AS total,
       round(max(c.defense_spending_score), 4) AS max_score,
       round(min(c.defense_spending_score), 4) AS min_score,
       round(avg(c.defense_spending_score), 4) AS avg_score
""")

run("arms_export_score — range check (should be non-zero now with 2001-2025 data)", """
MATCH (c:Country)
WHERE c.arms_export_score IS NOT NULL
RETURN count(c) AS total,
       round(max(c.arms_export_score), 4) AS max_score,
       round(min(c.arms_export_score), 6) AS min_score,
       round(avg(c.arms_export_score), 4) AS avg_score,
       count(CASE WHEN c.arms_export_score = 0.0 THEN 1 END)
           AS countries_with_zero_arms_score,
       count(CASE WHEN c.arms_export_score > 0.0 THEN 1 END)
           AS countries_with_nonzero_arms_score
""")

run("conflict_risk_score — range check", """
MATCH (c:Country)
WHERE c.conflict_risk_score IS NOT NULL
RETURN count(c) AS total,
       round(max(c.conflict_risk_score), 4) AS max_score,
       round(min(c.conflict_risk_score), 4) AS min_score,
       round(avg(c.conflict_risk_score), 4) AS avg_score
""")

run("defense_burden_score — range check (spending as % GDP proxy)", """
MATCH (c:Country)
WHERE c.defense_burden_score IS NOT NULL
RETURN count(c) AS total,
       round(max(c.defense_burden_score), 4) AS max_score,
       round(min(c.defense_burden_score), 4) AS min_score,
       round(avg(c.defense_burden_score), 4) AS avg_score
""")


# ══════════════════════════════════════════════════════════════════
# SECTION 6 — ANALYTICS SCORES: LOGICAL CORRECTNESS
# ══════════════════════════════════════════════════════════════════

section("SECTION 6 — ANALYTICS SCORES: LOGICAL CORRECTNESS")

run("Top 15 by composite_threat_score (US must be at or near top)", """
MATCH (c:Country)
WHERE c.composite_threat_score IS NOT NULL
RETURN c.name AS country,
       round(c.composite_threat_score, 4) AS composite_threat,
       round(c.military_strength_score, 4) AS military,
       round(c.conflict_risk_score, 4) AS conflict,
       round(c.defense_spending_score, 4) AS spending,
       round(c.arms_export_score, 4) AS arms,
       round(c.live_risk_score, 3) AS live_risk
ORDER BY c.composite_threat_score DESC LIMIT 15
""")

run("Nuclear states must score higher than non-nuclear (sanity check)", """
MATCH (c:Country)
WHERE c.military_strength_score IS NOT NULL
WITH
  CASE WHEN c.nuclear_status IS NOT NULL THEN 'nuclear' ELSE 'non-nuclear' END
      AS nuclear_status,
  avg(c.military_strength_score) AS avg_military,
  count(c) AS countries
RETURN nuclear_status, countries, round(avg_military, 4) AS avg_military_score
ORDER BY avg_military DESC
""")

run("P5 members must have elevated composite scores", """
MATCH (c:Country)
WHERE c.composite_threat_score IS NOT NULL
  AND c.un_p5 = true
RETURN c.name AS country,
       round(c.composite_threat_score, 4) AS composite_threat,
       round(c.military_strength_score, 4) AS military
ORDER BY c.composite_threat_score DESC
""")

run("Threat distribution — critical/high/moderate/low buckets", """
MATCH (c:Country)
WHERE c.composite_threat_score IS NOT NULL
RETURN count(CASE WHEN c.composite_threat_score > 0.5 THEN 1 END)
           AS critical,
       count(CASE WHEN c.composite_threat_score > 0.3
                   AND c.composite_threat_score <= 0.5 THEN 1 END)
           AS high,
       count(CASE WHEN c.composite_threat_score > 0.1
                   AND c.composite_threat_score <= 0.3 THEN 1 END)
           AS moderate,
       count(CASE WHEN c.composite_threat_score <= 0.1 THEN 1 END)
           AS low
""")

run("Countries with increasing conflict trend should score higher", """
MATCH (c:Country)-[r:HAS_CONFLICT_STATS]->(y:Year)
WHERE c.conflict_risk_score IS NOT NULL
WITH c, r.fatality_trend AS trend
WITH CASE WHEN trend = 'increasing' THEN 'increasing'
          WHEN trend = 'decreasing' THEN 'decreasing'
          ELSE 'stable' END AS trend_bucket,
     avg(c.conflict_risk_score) AS avg_conflict_score,
     count(DISTINCT c) AS countries
RETURN trend_bucket,
       countries,
       round(avg_conflict_score, 4) AS avg_conflict_risk_score
ORDER BY avg_conflict_score DESC
""")

run("Arms export score should be non-zero for known major exporters", """
MATCH (c:Country)
WHERE c.name IN [
    'United States', 'Russian Federation', 'France',
    'Germany', 'United Kingdom', 'China', 'Israel', 'Italy'
]
RETURN c.name AS country,
       round(c.arms_export_score, 4) AS arms_export_score,
       round(c.military_strength_score, 4) AS military_strength,
       round(c.composite_threat_score, 4) AS composite_threat
ORDER BY c.arms_export_score DESC
""")


# ══════════════════════════════════════════════════════════════════
# SECTION 7 — DERIVED RELATIONSHIPS
# ══════════════════════════════════════════════════════════════════

section("SECTION 7 — DERIVED RELATIONSHIPS: IS_HIGH_RISK_FOR + IS_INFLUENTIAL_TO")

run("IS_HIGH_RISK_FOR edges — count and basic stats", """
MATCH ()-[r:IS_HIGH_RISK_FOR]->()
RETURN count(r) AS high_risk_edges,
       count(DISTINCT startNode(r)) AS source_countries,
       count(DISTINCT endNode(r)) AS target_countries,
       round(max(r.value), 4) AS max_risk_value,
       round(min(r.value), 4) AS min_risk_value,
       round(avg(r.value), 4) AS avg_risk_value
""")

run("IS_HIGH_RISK_FOR pairs — top 15 highest risk (same region, both high conflict)", """
MATCH (c1:Country)-[r:IS_HIGH_RISK_FOR]->(c2:Country)
OPTIONAL MATCH (c1)-[:BELONGS_TO]->(reg1)
OPTIONAL MATCH (c2)-[:BELONGS_TO]->(reg2)
RETURN c1.name AS country_a,
       c2.name AS country_b,
       round(r.value, 4) AS risk_value,
       coalesce(reg1.name, 'no region') AS region_a,
       coalesce(reg2.name, 'no region') AS region_b
ORDER BY r.value DESC LIMIT 15
""")

run("IS_HIGH_RISK_FOR edges — are both countries in same region? (must be true)", """
MATCH (c1:Country)-[r:IS_HIGH_RISK_FOR]->(c2:Country)
OPTIONAL MATCH (c1)-[:BELONGS_TO]->(reg1)
OPTIONAL MATCH (c2)-[:BELONGS_TO]->(reg2)
WITH c1, c2, r, reg1, reg2,
     CASE WHEN reg1 IS NOT NULL AND reg2 IS NOT NULL
               AND reg1.name = reg2.name THEN 'SAME REGION'
          WHEN reg1 IS NULL OR reg2 IS NULL THEN 'REGION NOT ASSIGNED'
          ELSE 'DIFFERENT REGION — CHECK' END AS region_check
RETURN region_check,
       count(r) AS edge_count
ORDER BY edge_count DESC
""")

run("IS_INFLUENTIAL_TO edges — count and basic stats", """
MATCH ()-[r:IS_INFLUENTIAL_TO {domain: 'defense'}]->()
RETURN count(r) AS influence_edges,
       count(DISTINCT startNode(r)) AS source_countries,
       count(DISTINCT endNode(r)) AS target_countries,
       round(max(r.normalized_weight), 4) AS max_influence,
       round(min(r.normalized_weight), 4) AS min_influence,
       round(avg(r.normalized_weight), 4) AS avg_influence
""")

run("IS_INFLUENTIAL_TO top 15 — most influential arms exporters", """
MATCH (c1:Country)-[r:IS_INFLUENTIAL_TO {domain: 'defense'}]->(c2:Country)
RETURN c1.name AS influencer,
       c2.name AS influenced_via,
       round(r.normalized_weight, 4) AS influence_weight
ORDER BY r.normalized_weight DESC LIMIT 15
""")

run("All derived relationship types currently in graph", """
MATCH ()-[r]->()
WHERE type(r) IN ['IS_HIGH_RISK_FOR', 'IS_INFLUENTIAL_TO']
RETURN type(r) AS rel_type,
       count(r) AS count,
       count(DISTINCT startNode(r)) AS sources
ORDER BY count DESC
""")


# ══════════════════════════════════════════════════════════════════
# SECTION 8 — ANALYTICS + ARMS: COMBINED CROSS-CHECKS
# ══════════════════════════════════════════════════════════════════

section("SECTION 8 — COMBINED: ANALYTICS SCORES vs RAW DATA CONSISTENCY")

run("Arms export score should correlate with EXPORTS_ARMS.dependency (2001-2025)", """
MATCH (c:Country)-[r:EXPORTS_ARMS]->(y:Year)
WHERE r.period = '2001-2025'
  AND c.arms_export_score IS NOT NULL
WITH c, avg(r.dependency) AS avg_dep
RETURN c.name AS country,
       round(avg_dep, 4) AS avg_dependency_2001_2025,
       round(c.arms_export_score, 4) AS arms_export_score
ORDER BY c.arms_export_score DESC LIMIT 10
""")

run("Defence spending score should match SPENDS_ON_DEFENSE data (2018-2024)", """
MATCH (c:Country)-[r:SPENDS_ON_DEFENSE]->(y:Year)
WHERE y.year >= 2018 AND y.year <= 2024
  AND c.defense_spending_score IS NOT NULL
WITH c, avg(r.normalized_weight) AS avg_weight
WITH c, avg_weight
ORDER BY avg_weight DESC
LIMIT 10
RETURN c.name AS country,
       round(avg_weight, 4) AS avg_spending_weight_2018_2024,
       round(c.defense_spending_score, 4) AS spending_score
ORDER BY c.defense_spending_score DESC
""")

run("Conflict risk score vs actual fatality trend (increasing = higher score?)", """
MATCH (c:Country)-[r:HAS_CONFLICT_STATS]->(y:Year)
WHERE y.year >= 2018 AND y.year <= 2024
  AND c.conflict_risk_score IS NOT NULL
WITH c, avg(r.normalized_weight) AS avg_conflict_weight,
     collect(DISTINCT r.fatality_trend) AS trends
RETURN c.name AS country,
       trends,
       round(avg_conflict_weight, 4) AS avg_conflict_weight,
       round(c.conflict_risk_score, 4) AS conflict_risk_score
ORDER BY c.conflict_risk_score DESC LIMIT 15
""")

run("composite_threat_score vs live_risk_score — should broadly align", """
MATCH (c:Country)
WHERE c.composite_threat_score IS NOT NULL
  AND c.live_risk_score IS NOT NULL
RETURN c.name AS country,
       round(c.composite_threat_score, 4) AS composite_threat,
       round(c.live_risk_score, 3) AS live_risk,
       round(abs(c.composite_threat_score - c.live_risk_score), 4)
           AS delta
ORDER BY c.composite_threat_score DESC LIMIT 15
""")


# ══════════════════════════════════════════════════════════════════
# SECTION 9 — FULL GRAPH HEALTH (ALL EXISTING DATA STILL INTACT)
# ══════════════════════════════════════════════════════════════════

section("SECTION 9 — FULL GRAPH HEALTH: EXISTING DATA STILL INTACT")

run("All relationship types and counts (complete inventory)", """
MATCH ()-[r]->()
WITH type(r) AS rel, count(r) AS cnt
RETURN rel, cnt
ORDER BY cnt DESC
""")

run("Core defence data intact — spending, arms, conflict", """
MATCH (c:Country)-[r:SPENDS_ON_DEFENSE]->(y:Year)
WITH count(r) AS spending, count(DISTINCT c) AS spending_countries
MATCH (c2:Country)-[r2:EXPORTS_ARMS]->(y2:Year)
WITH spending, spending_countries,
     count(r2) AS arms, count(DISTINCT c2) AS arms_countries
MATCH (c3:Country)-[r3:HAS_CONFLICT_STATS]->(y3:Year)
RETURN spending, spending_countries,
       arms AS total_arms_all_periods, arms_countries,
       count(r3) AS conflict, count(DISTINCT c3) AS conflict_countries
""")

run("Enrichment intact — regions, alliances, nuclear, P5", """
MATCH (c:Country)-[:BELONGS_TO]->(r:Region)
WITH count(DISTINCT c) AS in_region
MATCH (c2:Country)-[:MEMBER_OF]->(a:Alliance)
WITH in_region, count(DISTINCT c2) AS in_alliance
MATCH (c3:Country) WHERE c3.nuclear_status IS NOT NULL
WITH in_region, in_alliance, count(c3) AS nuclear_states
MATCH (c4:Country) WHERE c4.un_p5 = true
RETURN in_region, in_alliance, nuclear_states,
       count(c4) AS p5_members
""")

run("CO_MENTIONED_WITH still intact", """
MATCH (c1:Country)-[r:CO_MENTIONED_WITH]->(c2:Country)
RETURN count(r) AS total_pairs,
       count(CASE WHEN r.dominant_context = 'hostile' THEN 1 END)
           AS hostile_pairs,
       count(CASE WHEN r.dominant_context = 'cooperative' THEN 1 END)
           AS cooperative_pairs
""")

run("No duplicate Country nodes", """
MATCH (c:Country)
WITH c.name AS name, count(c) AS cnt
WHERE cnt > 1
RETURN name, cnt
ORDER BY cnt DESC LIMIT 5
""")

run("No staging nodes (NewsArticle/ConflictSignal should be 0)", """
MATCH (n)
WHERE n:NewsArticle OR n:ConflictSignal
RETURN count(n) AS staging_nodes_remaining
""")


# ══════════════════════════════════════════════════════════════════
# SECTION 10 — SIMULATOR READINESS CHECK
# ══════════════════════════════════════════════════════════════════

section("SECTION 10 — SIMULATOR READINESS: ANALYTICS FIELDS POPULATED")

run("Top 15 countries simulator-ready with full analytics profile", """
MATCH (c:Country)
WHERE c.composite_threat_score IS NOT NULL
  AND c.live_risk_score IS NOT NULL
WITH c
OPTIONAL MATCH (c)-[:MEMBER_OF]->(a:Alliance)
OPTIONAL MATCH (c)-[:BELONGS_TO]->(reg:Region)
RETURN c.name AS country,
       round(c.composite_threat_score, 4) AS composite_threat,
       round(c.live_risk_score, 3) AS live_risk,
       round(c.military_strength_score, 4) AS military_strength,
       round(c.conflict_risk_score, 4) AS conflict_risk,
       c.nuclear_status AS nuclear,
       c.un_p5 AS p5,
       collect(DISTINCT a.name) AS alliances,
       reg.name AS region
ORDER BY c.composite_threat_score DESC LIMIT 15
""")

run("Sample India full simulator profile (cross-check all fields)", """
MATCH (c:Country {name: 'India'})
OPTIONAL MATCH (c)-[s:SPENDS_ON_DEFENSE]->(y1:Year {year: 2023})
OPTIONAL MATCH (c)-[cf:HAS_CONFLICT_STATS]->(y2:Year {year: 2022})
OPTIONAL MATCH (c)-[:MEMBER_OF]->(a:Alliance)
OPTIONAL MATCH (c)-[:BELONGS_TO]->(reg:Region)
OPTIONAL MATCH (c)-[co:CO_MENTIONED_WITH]->(partner:Country)
WHERE co.dominant_context = 'hostile'
RETURN c.name AS country,
       c.composite_threat_score AS composite_threat,
       c.military_strength_score AS military_strength,
       c.defense_spending_score AS spending_score,
       c.conflict_risk_score AS conflict_risk,
       c.arms_export_score AS arms_score,
       c.live_risk_score AS live_risk,
       c.nuclear_status AS nuclear,
       c.un_p5 AS p5,
       round(s.normalized_weight, 4) AS raw_spending_weight,
       round(cf.normalized_weight, 4) AS raw_conflict_weight,
       collect(DISTINCT a.name) AS alliances,
       reg.name AS region,
       collect(DISTINCT partner.name) AS hostile_co_mentions
""")

run("IS_HIGH_RISK_FOR feeding cascade detector (should have pairs)", """
MATCH (c1:Country)-[r:IS_HIGH_RISK_FOR]->(c2:Country)
OPTIONAL MATCH (c1)-[:BELONGS_TO]->(reg1)
RETURN c1.name AS high_risk_country,
       c2.name AS at_risk_from,
       round(r.value, 4) AS risk_value,
       round(c1.conflict_risk_score, 4) AS c1_conflict_score,
       round(c2.conflict_risk_score, 4) AS c2_conflict_score,
       reg1.name AS shared_region
ORDER BY r.value DESC LIMIT 10
""")

print("\n\n" + "="*65)
print("  ANALYTICS + ARMS 2001-2025 VERIFICATION COMPLETE")
print("="*65)
print("""
PASS CRITERIA:
  Section 1  — 2001-2025 period present, 25 years covered
  Section 2  — zero NULLs, normalized_weight 0-1, dep sums ~1.0
  Section 3  — US leads 2001-2025, no pre-1992 Russian Federation
  Section 4  — all 5 score types populated on 100+ countries
  Section 5  — all scores 0-1, arms_export_score non-zero for major exporters
  Section 6  — US near top, nuclear states score higher, logical buckets
  Section 7  — IS_HIGH_RISK_FOR and IS_INFLUENTIAL_TO both have edges
  Section 8  — scores correlate with underlying raw data
  Section 9  — existing data intact, zero duplicates, zero staging nodes
  Section 10 — 15+ countries fully simulator-ready
""")