"""
Deep Intelligence Analysis — Advanced Query Suite
===================================================
Goes beyond basic verification into genuine intelligence
analysis. Every query tells a story from the data.

Usage: python deep_analysis.py
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
            print("  No results returned.")
        else:
            for r in results:
                print(" ", r)
    except Exception as e:
        print(f"  ERROR: {e}")
    finally:
        conn.close()


def section(title):
    print(f"\n\n{'#'*65}")
    print(f"  {title}")
    print(f"{'#'*65}")


# ═══════════════════════════════════════════════════════════════
# SECTION 1 — DEEP DEFENCE ANALYSIS
# ═══════════════════════════════════════════════════════════════

section("SECTION 1 — DEEP DEFENCE ANALYSIS")

run("Cold War arms duopoly by decade — US+USSR combined share", """
MATCH (c:Country)-[r:EXPORTS_ARMS]->(y:Year)
WHERE c.name IN ['United States', 'Soviet Union']
WITH y.year AS yr,
     sum(r.dependency) AS superpower_share
WITH
  CASE WHEN yr < 1960 THEN '1950s'
       WHEN yr < 1970 THEN '1960s'
       WHEN yr < 1980 THEN '1970s'
       WHEN yr < 1990 THEN '1980s'
       ELSE '1990s' END AS decade,
  avg(superpower_share) AS avg_combined_share,
  min(superpower_share) AS min_share,
  max(superpower_share) AS max_share
RETURN decade,
       round(avg_combined_share*100,1) AS avg_combined_pct,
       round(min_share*100,1) AS min_pct,
       round(max_share*100,1) AS max_pct
ORDER BY decade
""")

run("Post-Soviet power vacuum — who filled the gap 1992-2000", """
MATCH (c:Country)-[r:EXPORTS_ARMS]->(y:Year)
WHERE y.year >= 1992 AND y.year <= 2000
  AND c.name <> 'Soviet Union'
WITH c.name AS country,
     avg(r.dependency) AS avg_share,
     max(r.dependency) AS peak_share,
     count(r) AS active_years
WHERE avg_share > 0.02
RETURN country,
       round(avg_share*100,2) AS avg_market_share_pct,
       round(peak_share*100,1) AS peak_pct,
       active_years
ORDER BY avg_share DESC LIMIT 10
""")

run("Defence spending acceleration — fastest growing 2015-2024", """
MATCH (c:Country)-[s:SPENDS_ON_DEFENSE]->(y:Year)
WHERE y.year IN [2015, 2024]
WITH c, y.year AS yr, s.value AS val
ORDER BY c.name, yr
WITH c, collect(val) AS series
WHERE size(series) = 2
  AND series[0] > 0
WITH c,
     series[0] AS spend_2015,
     series[1] AS spend_2024,
     round((series[1]-series[0])/series[0]*100,1) AS growth_pct
WHERE spend_2015 > 500
RETURN c.name AS country,
       round(spend_2015,0) AS spending_2015_usd_millions,
       round(spend_2024,0) AS spending_2024_usd_millions,
       growth_pct AS growth_pct_2015_to_2024
ORDER BY growth_pct DESC LIMIT 15
""")

run("Nuclear states vs non-nuclear — defence spending comparison", """
MATCH (c:Country)-[s:SPENDS_ON_DEFENSE]->(y:Year {year: 2023})
WITH c,
     CASE WHEN c.nuclear_status IS NOT NULL
          THEN c.nuclear_status ELSE 'none' END AS nuclear,
     s.value AS spending,
     s.normalized_weight AS weight
RETURN nuclear AS nuclear_status,
       count(c) AS countries,
       round(sum(spending),0) AS total_spending_usd_millions,
       round(avg(spending),0) AS avg_spending_usd_millions,
       round(avg(weight),4) AS avg_normalized_weight
ORDER BY avg_spending_usd_millions DESC
""")

run("Conflict fatalities per dollar spent — most inefficient defence", """
MATCH (c:Country)-[s:SPENDS_ON_DEFENSE]->(y:Year {year: 2022})
MATCH (c)-[cf:HAS_CONFLICT_STATS]->(y)
WHERE s.value > 100
  AND cf.total_fatalities > 500
RETURN c.name AS country,
       round(s.value,0) AS defense_budget_usd_millions,
       cf.total_fatalities AS fatalities_2022,
       round(toFloat(cf.total_fatalities)/s.value,2)
           AS fatalities_per_million_usd,
       cf.fatality_trend AS trend
ORDER BY fatalities_per_million_usd DESC LIMIT 12
""")

run("P5 members — combined defence dominance", """
MATCH (c:Country)-[s:SPENDS_ON_DEFENSE]->(y:Year {year: 2023})
WITH
  CASE WHEN c.un_p5 = true THEN 'P5 Member'
       ELSE 'Non-P5' END AS category,
  sum(s.value) AS total_spending,
  count(DISTINCT c) AS countries,
  avg(s.normalized_weight) AS avg_power
RETURN category, countries,
       round(total_spending,0) AS total_usd_millions,
       round(avg_power,4) AS avg_normalized_power,
       round(total_spending/1000000,2) AS total_trillion_usd
ORDER BY total_spending DESC
""")

run("Spending trend reversals — countries that cut then surged", """
MATCH (c:Country)-[s:SPENDS_ON_DEFENSE]->(y:Year)
WHERE y.year IN [2012, 2016, 2020, 2024]
WITH c, y.year AS yr, s.value AS val
ORDER BY c.name, yr
WITH c, collect(val) AS series
WHERE size(series) = 4
WITH c,
     series[0] AS v2012,
     series[1] AS v2016,
     series[2] AS v2020,
     series[3] AS v2024
WHERE v2016 < v2012
  AND v2024 > v2012 * 1.5
  AND v2012 > 200
RETURN c.name AS country,
       round(v2012,0) AS spend_2012,
       round(v2016,0) AS spend_2016_dip,
       round(v2020,0) AS spend_2020,
       round(v2024,0) AS spend_2024,
       round((v2024-v2012)/v2012*100,1) AS net_growth_pct
ORDER BY net_growth_pct DESC LIMIT 10
""")


# ═══════════════════════════════════════════════════════════════
# SECTION 2 — DEEP CONFLICT ANALYSIS
# ═══════════════════════════════════════════════════════════════

section("SECTION 2 — DEEP CONFLICT ANALYSIS")

run("Conflict intensity vs economic size — the poverty trap", """
MATCH (c:Country)-[cf:HAS_CONFLICT_STATS]->(y:Year {year: 2022})
MATCH (c)-[g:HAS_GDP]->(m:Metric {name: 'GDP'})
WHERE g.year = 2022
  AND cf.total_fatalities > 1000
  AND g.value > 0
WITH c,
     cf.total_fatalities AS fatalities,
     cf.normalized_weight AS conflict_weight,
     g.value AS gdp,
     g.normalized_weight AS gdp_power
WHERE gdp_power > 0
RETURN c.name AS country,
       round(gdp/1e9,2) AS gdp_billion,
       fatalities AS fatalities_2022,
       round(conflict_weight,4) AS conflict_intensity,
       round(gdp_power,4) AS economic_power,
       round(conflict_weight/gdp_power,2) AS conflict_to_gdp_ratio,
       CASE WHEN conflict_weight/gdp_power > 10
            THEN 'POVERTY TRAP'
            WHEN conflict_weight/gdp_power > 3
            THEN 'HIGH STRESS'
            ELSE 'PROPORTIONATE'
       END AS assessment
ORDER BY conflict_to_gdp_ratio DESC LIMIT 12
""")

run("Civilian targeting ratio — wars that target civilians", """
MATCH (c:Country)-[cf:HAS_CONFLICT_STATS]->(y:Year)
WHERE y.year >= 2018
  AND cf.total_fatalities > 200
  AND cf.violence_events > 0
WITH c,
     sum(cf.civilian_fatalities) AS total_civilian,
     sum(cf.total_fatalities) AS total_all,
     sum(cf.violence_events) AS total_events
WHERE total_all > 0
WITH c, total_civilian, total_all, total_events,
     toFloat(total_civilian)/total_all AS civilian_ratio
RETURN c.name AS country,
       total_civilian AS civilian_deaths_since_2018,
       total_all AS total_deaths_since_2018,
       round(civilian_ratio*100,1) AS civilian_pct_of_all_deaths,
       round(toFloat(total_civilian)/total_events,2)
           AS civilians_per_event,
       CASE WHEN civilian_ratio > 0.4
            THEN 'HIGH CIVILIAN TARGETING'
            WHEN civilian_ratio > 0.25
            THEN 'ELEVATED CIVILIAN RISK'
            ELSE 'MILITARY FOCUSED'
       END AS conflict_type
ORDER BY civilian_ratio DESC LIMIT 12
""")

run("Conflict surge detector — year on year doubles since 2018", """
MATCH (c:Country)-[r1:HAS_CONFLICT_STATS]->(y1:Year)
MATCH (c)-[r2:HAS_CONFLICT_STATS]->(y2:Year)
WHERE y2.year = y1.year + 1
  AND y1.year >= 2018
  AND r1.total_fatalities > 200
  AND r2.total_fatalities > r1.total_fatalities * 2.0
RETURN c.name AS country,
       y1.year AS from_year,
       y2.year AS to_year,
       r1.total_fatalities AS before,
       r2.total_fatalities AS after,
       round(toFloat(r2.total_fatalities)/r1.total_fatalities,1)
           AS surge_multiplier
ORDER BY surge_multiplier DESC LIMIT 12
""")

run("Decade comparison — is the world getting more violent?", """
MATCH (c:Country)-[cf:HAS_CONFLICT_STATS]->(y:Year)
WITH
  CASE WHEN y.year >= 2000 AND y.year < 2010 THEN '2000-2009'
       WHEN y.year >= 2010 AND y.year < 2020 THEN '2010-2019'
       WHEN y.year >= 2020 THEN '2020-present'
  END AS decade,
  sum(cf.total_fatalities) AS total_fatalities,
  sum(cf.violence_events) AS total_events,
  sum(cf.civilian_fatalities) AS civilian_deaths,
  count(DISTINCT c) AS countries_affected
WHERE decade IS NOT NULL
RETURN decade,
       total_fatalities,
       total_events,
       civilian_deaths,
       countries_affected,
       round(toFloat(total_fatalities)/countries_affected,0)
           AS avg_fatalities_per_country
ORDER BY decade
""")

run("Regional conflict concentration — which regions are hotspots", """
MATCH (c:Country)-[:BELONGS_TO]->(reg:Region)
MATCH (c)-[cf:HAS_CONFLICT_STATS]->(y:Year)
WHERE y.year >= 2020
WITH reg.name AS region,
     count(DISTINCT c) AS countries,
     sum(cf.total_fatalities) AS total_fatalities,
     sum(cf.violence_events) AS total_events,
     avg(cf.normalized_weight) AS avg_intensity
RETURN region,
       countries,
       total_fatalities,
       total_events,
       round(avg_intensity,4) AS avg_conflict_intensity,
       round(toFloat(total_fatalities)/countries,0)
           AS avg_fatalities_per_country
ORDER BY total_fatalities DESC
""")

run("Countries at peace — high spending, near-zero violence 2018-2023", """
MATCH (c:Country)-[s:SPENDS_ON_DEFENSE]->(y:Year)
WHERE y.year >= 2018 AND y.year <= 2023
WITH c, avg(s.value) AS avg_spending
WHERE avg_spending > 1000
MATCH (c)-[cf:HAS_CONFLICT_STATS]->(y2:Year)
WHERE y2.year >= 2018 AND y2.year <= 2023
WITH c, avg_spending,
     sum(cf.total_fatalities) AS total_fatalities_6yr
WHERE total_fatalities_6yr < 50
RETURN c.name AS country,
       round(avg_spending,0) AS avg_annual_spending_usd_millions,
       total_fatalities_6yr AS total_fatalities_6_years,
       round(avg_spending/1000,2) AS spending_billion_usd
ORDER BY avg_spending DESC LIMIT 10
""")


# ═══════════════════════════════════════════════════════════════
# SECTION 3 — DEEP ECONOMY ANALYSIS
# ═══════════════════════════════════════════════════════════════

section("SECTION 3 — DEEP ECONOMY ANALYSIS")

run("Trade web concentration — most connected trading nations", """
MATCH (c:Country)-[t:EXPORTS_TO]->(partner:Country)
WITH c,
     count(DISTINCT partner) AS export_partners,
     sum(t.value) AS total_exports,
     max(t.dependency) AS max_single_dependency
RETURN c.name AS country,
       export_partners,
       round(total_exports/1e9,2) AS total_exports_billion,
       round(max_single_dependency,4) AS highest_single_partner_dep,
       CASE WHEN max_single_dependency > 0.5
            THEN 'DANGEROUSLY CONCENTRATED'
            WHEN max_single_dependency > 0.3
            THEN 'HIGH CONCENTRATION'
            WHEN max_single_dependency > 0.15
            THEN 'MODERATE CONCENTRATION'
            ELSE 'DIVERSIFIED'
       END AS trade_risk
ORDER BY total_exports DESC LIMIT 15
""")

run("Energy chokepoints — most critical energy exporters", """
MATCH (c:Country)-[e:EXPORTS_ENERGY_TO]->(importer:Country)
WITH c,
     count(DISTINCT importer) AS import_countries,
     sum(e.value) AS total_energy_exports,
     avg(e.dependency) AS avg_importer_dependency
RETURN c.name AS country,
       import_countries AS countries_dependent,
       round(total_energy_exports/1e9,2) AS energy_exports_billion,
       round(avg_importer_dependency,4) AS avg_dependency_on_this_exporter,
       CASE WHEN import_countries > 30
            THEN 'CRITICAL GLOBAL CHOKEPOINT'
            WHEN import_countries > 15
            THEN 'REGIONAL ENERGY POWER'
            ELSE 'LOCAL SUPPLIER'
       END AS strategic_role
ORDER BY total_energy_exports DESC LIMIT 10
""")

run("Most energy vulnerable nations — high import dependency", """
MATCH (c:Country)-[e:IMPORTS_ENERGY_FROM]->(supplier:Country)
WITH c,
     count(DISTINCT supplier) AS supplier_count,
     max(e.dependency) AS max_dependency,
     sum(e.value) AS total_energy_imports
WHERE total_energy_imports > 1e9
RETURN c.name AS country,
       supplier_count AS energy_suppliers,
       round(max_dependency,4) AS highest_single_supplier_dep,
       round(total_energy_imports/1e9,2) AS energy_imports_billion,
       CASE WHEN max_dependency > 0.6 AND supplier_count < 5
            THEN 'CRITICAL ENERGY VULNERABILITY'
            WHEN max_dependency > 0.4
            THEN 'HIGH ENERGY RISK'
            ELSE 'MANAGEABLE'
       END AS energy_security_status
ORDER BY max_dependency DESC LIMIT 12
""")

run("GDP growth trajectory — economic momentum 2010-2022", """
MATCH (c:Country)-[g:HAS_GDP]->(m:Metric {name: 'GDP'})
WHERE g.year IN [2010, 2015, 2019, 2022]
WITH c, g.year AS yr, g.value AS val
ORDER BY c.name, yr
WITH c, collect({yr:yr, val:val}) AS pts
WHERE size(pts) = 4
WITH c,
     pts[0].val AS v2010,
     pts[1].val AS v2015,
     pts[2].val AS v2019,
     pts[3].val AS v2022
WHERE v2010 > 0
RETURN c.name AS country,
       round(v2010/1e9,1) AS gdp_2010_bn,
       round(v2022/1e9,1) AS gdp_2022_bn,
       round((v2022-v2010)/v2010*100,1) AS growth_pct_2010_2022,
       CASE WHEN (v2022-v2010)/v2010 > 2.0
            THEN 'TRANSFORMATIONAL GROWTH'
            WHEN (v2022-v2010)/v2010 > 1.0
            THEN 'STRONG GROWTH'
            WHEN (v2022-v2010)/v2010 > 0.3
            THEN 'MODERATE GROWTH'
            WHEN (v2022-v2010)/v2010 < 0
            THEN 'ECONOMIC CONTRACTION'
            ELSE 'SLOW GROWTH'
       END AS trajectory
ORDER BY growth_pct_2010_2022 DESC LIMIT 15
""")

run("Trade balance analysis — surplus vs deficit nations", """
MATCH (c:Country)-[tb:HAS_TRADE_BALANCE]->(m:Metric)
WHERE tb.year = 2022
  AND tb.exports IS NOT NULL
  AND tb.imports IS NOT NULL
WITH c,
     tb.exports AS exports,
     tb.imports AS imports,
     tb.value AS balance
WHERE exports > 1e9
RETURN c.name AS country,
       round(exports/1e9,2) AS exports_billion,
       round(imports/1e9,2) AS imports_billion,
       round(balance/1e9,2) AS trade_balance_billion,
       CASE WHEN balance > 0 THEN 'SURPLUS' ELSE 'DEFICIT' END
           AS position,
       round(balance/exports*100,1) AS balance_as_pct_of_exports
ORDER BY balance DESC LIMIT 15
""")

run("Sanctions effect — trade isolation of conflict states", """
MATCH (c:Country)-[cf:HAS_CONFLICT_STATS]->(y:Year {year: 2022})
WHERE cf.fatality_trend = 'increasing'
  AND cf.total_fatalities > 1000
OPTIONAL MATCH (c)-[t:EXPORTS_TO]->(partner:Country)
WITH c, cf,
     count(DISTINCT partner) AS export_partners,
     sum(t.value) AS export_value
RETURN c.name AS country,
       cf.total_fatalities AS fatalities_2022,
       coalesce(export_partners, 0) AS trade_partners,
       round(coalesce(export_value,0)/1e9,2) AS exports_billion,
       CASE WHEN coalesce(export_partners,0) < 10
            THEN 'TRADE ISOLATED'
            WHEN coalesce(export_partners,0) < 25
            THEN 'PARTIALLY ISOLATED'
            ELSE 'TRADE ACTIVE DESPITE CONFLICT'
       END AS trade_status
ORDER BY fatalities_2022 DESC LIMIT 12
""")


# ═══════════════════════════════════════════════════════════════
# SECTION 4 — DEEP GEOPOLITICS ANALYSIS
# ═══════════════════════════════════════════════════════════════

section("SECTION 4 — DEEP GEOPOLITICS ANALYSIS")

run("Democracy vs autocracy — bloc power comparison 2022", """
MATCH (c:Country)-[p:HAS_POLITICAL_SYSTEM]->(ps:PoliticalSystem)
MATCH (c)-[s:SPENDS_ON_DEFENSE]->(y:Year {year: 2022})
MATCH (c)-[g:HAS_GDP]->(m:Metric {name: 'GDP'})
WHERE g.year = 2022
WITH ps.name AS system,
     count(DISTINCT c) AS countries,
     sum(s.value) AS total_defense,
     sum(g.value) AS total_gdp,
     avg(p.normalized_weight) AS avg_demo_score
RETURN system,
       countries,
       round(total_defense/1e3,1) AS total_defense_billion,
       round(total_gdp/1e12,2) AS total_gdp_trillion,
       round(total_defense/total_gdp*100,2) AS defense_pct_of_gdp,
       round(avg_demo_score,3) AS avg_democracy_score
ORDER BY total_gdp DESC
""")

run("Diplomatic centrality — most connected countries in GDELT network", """
MATCH (c:Country)
WHERE c.centrality IS NOT NULL
OPTIONAL MATCH (c)-[p:HAS_POLITICAL_SYSTEM]->(ps:PoliticalSystem)
WITH c, ps, p
ORDER BY p.normalized_weight DESC
WITH c,
     collect(ps.name)[0] AS system,
     collect(p.normalized_weight)[0] AS gov_score
RETURN c.name AS country,
       round(c.centrality,6) AS eigenvector_centrality,
       coalesce(c.bloc_id, -1) AS detected_bloc,
       system AS political_system,
       round(coalesce(gov_score,0),3) AS democracy_score
ORDER BY c.centrality DESC LIMIT 15
""")

run("Alliance overlap — countries in multiple competing blocs", """
MATCH (c:Country)-[:MEMBER_OF]->(a:Alliance)
WITH c, collect(a.name) AS alliances
WHERE size(alliances) > 1
OPTIONAL MATCH (c)-[p:HAS_POLITICAL_SYSTEM]->(ps:PoliticalSystem)
WITH c, alliances, ps, p
ORDER BY p.normalized_weight DESC
WITH c, alliances,
     collect(ps.name)[0] AS system
RETURN c.name AS country,
       alliances,
       size(alliances) AS bloc_count,
       system AS political_system,
       CASE WHEN 'NATO' IN alliances AND 'SCO' IN alliances
            THEN 'EAST-WEST BRIDGE'
            WHEN 'NATO' IN alliances AND 'BRICS' IN alliances
            THEN 'NATO-BRICS OVERLAP'
            WHEN 'BRICS' IN alliances AND 'SCO' IN alliances
            THEN 'EASTERN BLOC CORE'
            WHEN 'QUAD' IN alliances AND 'BRICS' IN alliances
            THEN 'QUAD-BRICS TENSION'
            ELSE 'MULTI-ALIGNED'
       END AS geopolitical_position
ORDER BY bloc_count DESC
""")

run("Political system stability vs conflict correlation", """
MATCH (c:Country)-[p:HAS_POLITICAL_SYSTEM]->(ps:PoliticalSystem)
MATCH (c)-[cf:HAS_CONFLICT_STATS]->(y:Year)
WHERE y.year >= 2020
WITH c, ps, p,
     avg(cf.total_fatalities) AS avg_annual_fatalities,
     avg(cf.normalized_weight) AS avg_conflict_weight
WITH ps.name AS system,
     count(DISTINCT c) AS countries,
     avg(avg_annual_fatalities) AS avg_fatalities_per_country,
     avg(avg_conflict_weight) AS avg_conflict_intensity,
     avg(p.normalized_weight) AS avg_democracy_score
RETURN system,
       countries,
       round(avg_fatalities_per_country,0) AS avg_annual_fatalities,
       round(avg_conflict_intensity,4) AS avg_conflict_intensity,
       round(avg_democracy_score,3) AS avg_democracy_score
ORDER BY avg_conflict_intensity DESC
""")

run("Bloc-level diplomatic alignment scores", """
MATCH (c:Country)-[:MEMBER_OF]->(a:Alliance)
MATCH (c)-[d:DIPLOMATIC_INTERACTION]->(partner:Country)
WHERE d.alignment_score IS NOT NULL
WITH a.name AS alliance,
     avg(d.alignment_score) AS avg_alignment,
     count(d) AS total_interactions,
     count(DISTINCT c) AS active_members
RETURN alliance,
       active_members,
       total_interactions,
       round(avg_alignment,4) AS avg_diplomatic_alignment
ORDER BY avg_alignment DESC
""")


# ═══════════════════════════════════════════════════════════════
# SECTION 5 — DEEP CROSS-MODULE ANALYSIS
# ═══════════════════════════════════════════════════════════════

section("SECTION 5 — DEEP CROSS-MODULE ANALYSIS")

run("The full instability matrix — all four domains", """
MATCH (c:Country)-[s:SPENDS_ON_DEFENSE]->(y:Year {year: 2022})
MATCH (c)-[cf:HAS_CONFLICT_STATS]->(y)
MATCH (c)-[g:HAS_GDP]->(m:Metric {name: 'GDP'})
MATCH (c)-[p:HAS_POLITICAL_SYSTEM]->(ps:PoliticalSystem)
WHERE g.year = 2022 AND g.value > 0
WITH c, s, cf, g, p, ps
ORDER BY p.normalized_weight DESC
WITH c, s, cf, g,
     collect(ps.name)[0] AS system,
     collect(p.normalized_weight)[0] AS gov
WITH c, s, cf, g, system, gov,
     (s.normalized_weight * 0.25) AS defence_score,
     (cf.normalized_weight * 0.25) AS conflict_score,
     ((1 - gov) * 0.25) AS autocracy_score,
     (CASE WHEN g.normalized_weight > 0
           THEN (1 - g.normalized_weight) * 0.25
           ELSE 0.25 END) AS poverty_score
RETURN c.name AS country,
       round(defence_score,4) AS defence_component,
       round(conflict_score,4) AS conflict_component,
       round(autocracy_score,4) AS autocracy_component,
       round(poverty_score,4) AS poverty_component,
       round(defence_score+conflict_score+
             autocracy_score+poverty_score,4) AS instability_index,
       system AS political_system,
       round(gov,3) AS democracy_score
ORDER BY instability_index DESC LIMIT 15
""")

run("Resource curse hypothesis — oil exporters vs conflict", """
MATCH (c:Country)-[e:EXPORTS_ENERGY_TO]->(importer:Country)
WITH c, sum(e.value) AS energy_exports,
     count(DISTINCT importer) AS energy_customers
WHERE energy_exports > 5e9
MATCH (c)-[cf:HAS_CONFLICT_STATS]->(y:Year {year: 2022})
OPTIONAL MATCH (c)-[p:HAS_POLITICAL_SYSTEM]->(ps:PoliticalSystem)
WITH c, energy_exports, energy_customers, cf, ps, p
ORDER BY p.normalized_weight DESC
WITH c, energy_exports, energy_customers, cf,
     collect(ps.name)[0] AS system,
     collect(p.normalized_weight)[0] AS gov
RETURN c.name AS country,
       round(energy_exports/1e9,1) AS energy_exports_billion,
       energy_customers,
       cf.total_fatalities AS conflict_fatalities_2022,
       system AS political_system,
       round(coalesce(gov,0.5),3) AS democracy_score,
       CASE WHEN energy_exports > 5e9
             AND cf.total_fatalities > 1000
             AND coalesce(gov,0.5) < 0.4
            THEN 'RESOURCE CURSE ACTIVE'
            WHEN energy_exports > 5e9
             AND coalesce(gov,0.5) < 0.4
            THEN 'AUTOCRACY SUSTAINED BY ENERGY'
            ELSE 'STABLE ENERGY EXPORTER'
       END AS assessment
ORDER BY energy_exports DESC LIMIT 12
""")

run("Military-economic power gap — who punches above their weight", """
MATCH (c:Country)-[s:SPENDS_ON_DEFENSE]->(y:Year {year: 2022})
MATCH (c)-[g:HAS_GDP]->(m:Metric {name: 'GDP'})
WHERE g.year = 2022 AND g.value > 0
WITH c,
     s.normalized_weight AS defense_power,
     g.normalized_weight AS economic_power
WHERE defense_power > 0 AND economic_power > 0
WITH c, defense_power, economic_power,
     defense_power / economic_power AS military_economic_ratio
RETURN c.name AS country,
       round(defense_power,4) AS military_power,
       round(economic_power,4) AS economic_power,
       round(military_economic_ratio,2) AS military_vs_economy_ratio,
       CASE WHEN military_economic_ratio > 3
            THEN 'PUNCHES FAR ABOVE WEIGHT'
            WHEN military_economic_ratio > 1.5
            THEN 'ELEVATED MILITARY POSTURE'
            WHEN military_economic_ratio < 0.3
            THEN 'ECONOMIC GIANT MILITARY DWARF'
            ELSE 'BALANCED'
       END AS posture
ORDER BY military_economic_ratio DESC LIMIT 15
""")

run("NATO vs SCO vs BRICS — comprehensive bloc comparison 2022", """
MATCH (c:Country)-[:MEMBER_OF]->(a:Alliance)
WHERE a.name IN ['NATO', 'SCO', 'BRICS']
OPTIONAL MATCH (c)-[s:SPENDS_ON_DEFENSE]->(y:Year {year: 2022})
OPTIONAL MATCH (c)-[g:HAS_GDP]->(m:Metric {name: 'GDP'})
WHERE g.year = 2022
OPTIONAL MATCH (c)-[cf:HAS_CONFLICT_STATS]->(y2:Year {year: 2022})
OPTIONAL MATCH (c)-[p:HAS_POLITICAL_SYSTEM]->(ps:PoliticalSystem)
WITH a.name AS bloc, c, s, g, cf, p
ORDER BY p.normalized_weight DESC
WITH bloc, c, s, g, cf,
     collect(p.normalized_weight)[0] AS gov
WITH bloc,
     count(DISTINCT c) AS members,
     sum(coalesce(s.value,0)) AS total_defense,
     sum(coalesce(g.value,0)) AS total_gdp,
     sum(coalesce(cf.total_fatalities,0)) AS total_fatalities,
     avg(coalesce(gov,0.5)) AS avg_democracy
RETURN bloc,
       members,
       round(total_defense/1e3,1) AS defense_billion,
       round(total_gdp/1e12,2) AS gdp_trillion,
       total_fatalities AS conflict_fatalities_2022,
       round(avg_democracy,3) AS avg_democracy_score,
       round(total_defense/total_gdp*100,2) AS defense_burden_pct
ORDER BY gdp_trillion DESC
""")

run("Live risk vs historical baseline — what has changed", """
MATCH (c:Country)
WHERE c.live_risk_score IS NOT NULL
MATCH (c)-[cf:HAS_CONFLICT_STATS]->(y:Year)
WHERE y.year >= 2020
WITH c,
     c.live_risk_score AS live_score,
     avg(cf.normalized_weight) AS historical_avg
RETURN c.name AS country,
       round(live_score,3) AS live_risk_score,
       round(historical_avg,4) AS historical_conflict_intensity,
       round(live_score - historical_avg,4) AS deviation,
       CASE WHEN live_score > historical_avg * 1.5
            THEN 'ESCALATING — live above historical'
            WHEN live_score < historical_avg * 0.5
            THEN 'DE-ESCALATING — calmer than history'
            ELSE 'CONSISTENT WITH HISTORY'
       END AS signal_assessment
ORDER BY deviation DESC LIMIT 15
""")


# ═══════════════════════════════════════════════════════════════
# SECTION 6 — HISTORICAL PATTERNS AND ANOMALIES
# ═══════════════════════════════════════════════════════════════

section("SECTION 6 — HISTORICAL PATTERNS AND ANOMALIES")

run("Post-war spending patterns — countries that demilitarised", """
MATCH (c:Country)-[s:SPENDS_ON_DEFENSE]->(y:Year)
WHERE y.year IN [2000, 2005, 2010, 2015, 2020, 2023]
WITH c, y.year AS yr, s.value AS val
ORDER BY c.name, yr
WITH c, collect(val) AS series
WHERE size(series) = 6
  AND series[0] > 1000
  AND series[5] < series[0] * 0.7
RETURN c.name AS country,
       round(series[0],0) AS spending_2000,
       round(series[5],0) AS spending_2023,
       round((series[5]-series[0])/series[0]*100,1) AS change_pct
ORDER BY change_pct LIMIT 10
""")

run("Arms export longevity — countries active across all eras", """
MATCH (c:Country)-[r:EXPORTS_ARMS]->(y:Year)
WITH c,
     min(y.year) AS first_year,
     max(y.year) AS last_year,
     count(r) AS active_years,
     max(r.dependency) AS peak_share,
     avg(r.dependency) AS avg_share
WHERE active_years >= 40
RETURN c.name AS country,
       first_year,
       last_year,
       active_years,
       round(peak_share*100,1) AS peak_market_pct,
       round(avg_share*100,2) AS avg_market_pct
ORDER BY active_years DESC
""")

run("Conflict recurrence — countries that never find peace", """
MATCH (c:Country)-[cf:HAS_CONFLICT_STATS]->(y:Year)
WHERE cf.total_fatalities > 100
WITH c,
     count(DISTINCT y.year) AS conflict_years,
     min(y.year) AS first_conflict_year,
     max(y.year) AS last_conflict_year,
     sum(cf.total_fatalities) AS total_fatalities
WHERE conflict_years >= 15
  AND last_conflict_year > first_conflict_year
RETURN c.name AS country,
       first_conflict_year,
       last_conflict_year,
       conflict_years AS years_with_significant_conflict,
       total_fatalities,
       round(toFloat(conflict_years)/
             (last_conflict_year-first_conflict_year+1)*100,0)
           AS conflict_frequency_pct
ORDER BY conflict_years DESC LIMIT 12
""")

run("Great power competition — US vs China vs Russia 2010-2024", """
MATCH (c:Country)-[s:SPENDS_ON_DEFENSE]->(y:Year)
WHERE c.name IN ['United States','China','Russian Federation']
  AND y.year >= 2010
RETURN c.name AS country,
       y.year AS year,
       round(s.value,0) AS defense_usd_millions,
       round(s.normalized_weight,4) AS normalized_power
ORDER BY country, year
""")

run("The butterfly effect — smallest economies, biggest conflicts", """
MATCH (c:Country)-[g:HAS_GDP]->(m:Metric {name: 'GDP'})
MATCH (c)-[cf:HAS_CONFLICT_STATS]->(y:Year {year: 2022})
WHERE g.year = 2022
  AND g.value < 30e9
  AND cf.total_fatalities > 2000
RETURN c.name AS country,
       round(g.value/1e9,2) AS gdp_billion,
       cf.total_fatalities AS fatalities_2022,
       round(toFloat(cf.total_fatalities)/(g.value/1e9),0)
           AS fatalities_per_gdp_billion,
       cf.fatality_trend AS trend
ORDER BY fatalities_per_gdp_billion DESC LIMIT 10
""")

print("\n\n" + "="*65)
print("  DEEP ANALYSIS COMPLETE")
print("="*65)
print("""
KEY FINDINGS TO LOOK FOR:
  Section 1  — Cold War duopoly consistently 65-80% of arms market
  Section 2  — Civilian targeting reveals true conflict character
  Section 3  — Energy chokepoints show real geopolitical leverage
  Section 4  — Democracy vs autocracy bloc power gap
  Section 5  — Instability matrix shows true multi-domain risk
  Section 6  — Historical patterns confirm long-run trends
""")
