import sys 
sys.path.insert(0, ".") 
import logging 
from typing import List, Dict, Any, Union
from common.db import Neo4jConnection 
from common.intelligence.aggregation import average, max_value, sum_values 
from common.intelligence.normalization import normalize_by_max, clamp 
from common.intelligence.dependency import compute_dependency 
from common.intelligence.composite import weighted_score 
from common.intelligence.growth import growth_trend, average_growth 

logger = logging.getLogger(__name__) 

def compute_defense_spending_score(years=range(2018, 2025)) -> int:
    """
    Scores each country's defense spending relative to global max.
    """
    db = Neo4jConnection()
    try:
        query = """
        MATCH (c:Country)-[r:SPENDS_ON_DEFENSE]->(y:Year) 
        WHERE y.year IN $years 
        RETURN c.name AS country, y.year AS year, r.value AS amount
        """
        results = db.run_query(query, {"years": list(years)})
        
        if not results:
            logger.info("defense_spending_score: No data found for years %s", list(years))
            return 0
            
        # Group by country
        country_data = {}
        for row in results:
            name = row['country']
            if name not in country_data:
                country_data[name] = []
            country_data[name].append(row['amount'])
            
        # Calculate average per country
        country_averages = {}
        for name, amounts in country_data.items():
            avg_spending = average(amounts)
            country_averages[name] = avg_spending
            logger.debug("Country: %s, Avg Spending: %s", name, avg_spending)
            
        # Global max of averages
        global_max = max_value(list(country_averages.values()))
        
        # Calculate scores and prepare for update
        update_rows = []
        for name, avg_spending in country_averages.items():
            score = clamp(normalize_by_max(avg_spending, global_max), 0.0, 1.0)
            update_rows.append({"country": name, "score": score})
            
        # Write back to Neo4j
        write_query = """
        UNWIND $rows AS row 
        MATCH (c:Country {name: row.country}) 
        SET c.defense_spending_score = row.score
        """
        db.run_query(write_query, {"rows": update_rows})
        
        count = len(update_rows)
        logger.info("defense_spending_score: updated %d countries", count)
        return count
    finally:
        db.close()

def compute_military_strength_score(year=2024) -> int:
    """
    Composite military strength from spending + arms exports + nuclear/P5 status.
    """
    db = Neo4jConnection()
    try:
        # Query 1: Spending and Geopolitical status
        query_spending = """
        MATCH (c:Country)-[r:SPENDS_ON_DEFENSE]->(y:Year {year: $year}) 
        RETURN c.name AS country, r.normalized_weight AS spending_weight, 
               c.is_nuclear AS is_nuclear, c.un_p5 AS un_p5, 
               c.is_regional_power AS is_regional_power
        """
        spending_results = db.run_query(query_spending, {"year": year})
        
        # Query 2: Arms exports
        query_arms = """
        MATCH (c:Country)-[r:EXPORTS_ARMS]->(y:Year {year: $year}) 
        RETURN c.name AS country, r.normalized_weight AS arms_weight
        """
        arms_results = db.run_query(query_arms, {"year": year})
        
        # Build arms weight dictionary
        arms_dict = {row['country']: row['arms_weight'] for row in arms_results}
        
        update_rows = []
        for row in spending_results:
            name = row['country']
            spending_w = row['spending_weight'] or 0.0
            arms_w = arms_dict.get(name, 0.0)
            
            # Geopolitical multipliers
            nuclear_bonus = 0.15 if row.get('is_nuclear') else 0.0
            p5_bonus = 0.10 if row.get('un_p5') else 0.0
            regional_bonus = 0.05 if row.get('is_regional_power') else 0.0
            
            base_score = weighted_score( 
                {"spending": spending_w, "arms": arms_w}, 
                {"spending": 0.6, "arms": 0.4} 
            )
            score = clamp(base_score + nuclear_bonus + p5_bonus + regional_bonus, 0.0, 1.0)
            update_rows.append({"country": name, "score": score})
            logger.debug("Country: %s, Strength Score: %s", name, score)
            
        if not update_rows:
            logger.info("military_strength_score: No data found for year %d", year)
            return 0
            
        # Write back to Neo4j
        write_query = """
        UNWIND $rows AS row 
        MATCH (c:Country {name: row.country}) 
        SET c.military_strength_score = row.score
        """
        db.run_query(write_query, {"rows": update_rows})
        
        count = len(update_rows)
        logger.info("military_strength_score: updated %d countries", count)
        return count
    finally:
        db.close()

def compute_arms_export_score(years=range(1950, 2025)) -> int:
    """
    Scores each country's arms export influence over time.
    """
    db = Neo4jConnection()
    try:
        query = """
        MATCH (c:Country)-[r:EXPORTS_ARMS]->(y:Year) 
        WHERE y.year IN $years 
        RETURN c.name AS country, y.year AS year, r.value AS tiv
        """
        results = db.run_query(query, {"years": list(years)})
        
        if not results:
            logger.info("arms_export_score: No data found for years %s", list(years))
            return 0
            
        # Group by country
        country_data = {}
        for row in results:
            name = row['country']
            if name not in country_data:
                country_data[name] = []
            country_data[name].append(row['tiv'])
            
        # Calculate average per country
        country_averages = {}
        for name, tivs in country_data.items():
            avg_tiv = average(tivs)
            country_averages[name] = avg_tiv
            logger.debug("Country: %s, Avg TIV: %s", name, avg_tiv)
            
        # Global max of averages
        global_max_tiv = max_value(list(country_averages.values()))
        
        # Calculate scores and prepare for update
        update_rows = []
        for name, avg_tiv in country_averages.items():
            score = clamp(normalize_by_max(avg_tiv, global_max_tiv), 0.0, 1.0)
            update_rows.append({"country": name, "score": score})
            
        # Write back to Neo4j
        write_query = """
        UNWIND $rows AS row 
        MATCH (c:Country {name: row.country}) 
        SET c.arms_export_score = row.score
        """
        db.run_query(write_query, {"rows": update_rows})
        
        count = len(update_rows)
        logger.info("arms_export_score: updated %d countries", count)
        return count
    finally:
        db.close()

def compute_conflict_risk_score(years=range(2018, 2025)) -> int:
    """
    Scores each country's conflict risk level, adjusted for trend direction.
    """
    db = Neo4jConnection()
    try:
        query = """
        MATCH (c:Country)-[r:HAS_CONFLICT_STATS]->(y:Year) 
        WHERE y.year IN $years 
        RETURN c.name AS country, y.year AS year, 
               r.total_fatalities AS fatalities, 
               r.violence_events AS events
        """
        results = db.run_query(query, {"years": list(years)})
        
        if not results:
            logger.info("conflict_risk_score: No data found for years %s", list(years))
            return 0
            
        # Group by country and sort by year
        country_data = {}
        for row in results:
            name = row['country']
            if name not in country_data:
                country_data[name] = []
            country_data[name].append(row)
            
        # Metrics per country
        country_metrics = {}
        for name, records in country_data.items():
            # Sort records by year ascending
            sorted_records = sorted(records, key=lambda x: x['year'])
            
            fatalities_list = [r['fatalities'] for r in sorted_records]
            events_list = [r['events'] for r in sorted_records]
            
            avg_fatalities = average(fatalities_list)
            avg_events = average(events_list)
            
            # trend from sorted fatalities series
            trend = growth_trend(fatalities_list)
            
            country_metrics[name] = {
                "avg_fatalities": avg_fatalities,
                "avg_events": avg_events,
                "trend": trend,
                "fatality_series": fatalities_list
            }
            
        # Global maxes
        all_avg_fatalities = [m["avg_fatalities"] for m in country_metrics.values()]
        all_avg_events = [m["avg_events"] for m in country_metrics.values()]
        
        global_max_fatalities = max_value(all_avg_fatalities)
        global_max_events = max_value(all_avg_events)
        
        # Calculate scores and update rows
        update_rows = []
        for name, metrics in country_metrics.items():
            norm_fatalities = normalize_by_max(metrics["avg_fatalities"], global_max_fatalities)
            norm_events = normalize_by_max(metrics["avg_events"], global_max_events)
            
            trend_weight = 1.2 if metrics["trend"] == "increasing" else 0.8 if metrics["trend"] == "decreasing" else 1.0
            
            base_score = weighted_score( 
                {"fatalities": norm_fatalities, "events": norm_events}, 
                {"fatalities": 0.7, "events": 0.3} 
            )
            score = clamp(base_score * trend_weight, 0.0, 1.0)
            update_rows.append({"country": name, "score": score, "trend": metrics["trend"]})
            logger.debug("Country: %s, Risk Score: %s, Trend: %s", name, score, metrics["trend"])
            
        # Write back to Neo4j
        write_query = """
        UNWIND $rows AS row 
        MATCH (c:Country {name: row.country}) 
        SET c.conflict_risk_score = row.score, 
            c.conflict_fatality_trend = row.trend
        """
        db.run_query(write_query, {"rows": update_rows})
        
        count = len(update_rows)
        logger.info("conflict_risk_score: updated %d countries", count)
        return count
    finally:
        db.close()

def compute_defense_composite_score() -> int:
    """
    Computes a single composite defense score per country from all available
    defense score properties for fast profiling.
    """
    db = Neo4jConnection()
    try:
        query = """
        MATCH (c:Country)
        WHERE c.military_strength_score IS NOT NULL
           OR c.conflict_risk_score IS NOT NULL
           OR c.defense_spending_score IS NOT NULL
        RETURN c.name AS country,
               coalesce(c.military_strength_score, 0.0) AS military,
               coalesce(c.conflict_risk_score, 0.0) AS conflict,
               coalesce(c.defense_spending_score, 0.0) AS spending,
               coalesce(c.live_risk_score, 0.0) AS live_risk
        """
        results = db.run_query(query)

        if not results:
            logger.info("defense_composite_score: No eligible countries found")
            return 0

        update_rows = []
        for row in results:
            military = row.get("military", 0.0) or 0.0
            conflict = row.get("conflict", 0.0) or 0.0
            spending = row.get("spending", 0.0) or 0.0
            live_risk = row.get("live_risk", 0.0) or 0.0

            score = clamp(
                weighted_score(
                    {
                        "military": military,
                        "conflict": conflict,
                        "spending": spending,
                        "live_risk": live_risk,
                    },
                    {
                        "military": 0.35,
                        "conflict": 0.35,
                        "spending": 0.15,
                        "live_risk": 0.15,
                    },
                ),
                0.0,
                1.0,
            )
            update_rows.append({"country": row["country"], "score": score})

        write_query = """
        UNWIND $rows AS row
        MATCH (c:Country {name: row.country})
        SET c.defense_composite_score = row.score
        """
        db.run_query(write_query, {"rows": update_rows})

        count = len(update_rows)
        logger.info("defense_composite_score: updated %d countries", count)
        return count
    finally:
        db.close()

def compute_defense_burden_score(year=2024) -> int:
    """
    Defense spending as a proportion of GDP. Requires economy module GDP data.
    """
    db = Neo4jConnection()
    try:
        # Query join defense spending with GDP
        query = """
        MATCH (c:Country)-[s:SPENDS_ON_DEFENSE]->(y:Year {year: $year}) 
        MATCH (c)-[g:HAS_GDP]->(m:Metric {name: 'GDP'}) 
        WHERE g.year = $year AND g.value > 0 AND s.value > 0 
        RETURN c.name AS country, 
               s.value AS defense_usd, 
               g.value AS gdp_usd 
        """
        results = db.run_query(query, {"year": year})
        
        if not results:
            logger.warning("defense_burden_score: No GDP data exists yet for year %d. Economy module not run?", year)
            return 0
            
        # Calculate ratios
        ratios = []
        for row in results:
            # defense_usd is in USD millions, gdp_usd is in USD raw
            defense_usd_full = row['defense_usd'] * 1_000_000
            burden_ratio = compute_dependency(defense_usd_full, row['gdp_usd'])
            ratios.append({"country": row['country'], "ratio": burden_ratio})
            
        # Global max burden
        global_max_burden = max_value([r['ratio'] for r in ratios])
        
        # Calculate scores
        update_rows = []
        for r in ratios:
            score = clamp(normalize_by_max(r['ratio'], global_max_burden), 0.0, 1.0)
            update_rows.append({"country": r['country'], "score": score})
            logger.debug("Country: %s, Burden Score: %s", r['country'], score)
            
        # Write back to Neo4j
        write_query = """
        UNWIND $rows AS row 
        MATCH (c:Country {name: row.country}) 
        SET c.defense_burden_score = row.score
        """
        db.run_query(write_query, {"rows": update_rows})
        
        count = len(update_rows)
        logger.info("defense_burden_score: updated %d countries", count)
        return count
    finally:
        db.close()

def compute_all_defense_scores() -> Dict[str, int]:
    """
    Runs all 5 score functions in correct order and returns counts.
    """
    results = {} 
    results["defense_spending_score"] = compute_defense_spending_score() 
    results["arms_export_score"] = compute_arms_export_score() 
    results["conflict_risk_score"] = compute_conflict_risk_score() 
    results["military_strength_score"] = compute_military_strength_score() 
    results["defense_burden_score"] = compute_defense_burden_score() 
    results["defense_composite_score"] = compute_defense_composite_score()
    
    logger.info("Defense analytics complete: %s", results) 
    return results 

if __name__ == "__main__": 
    logging.basicConfig(level=logging.INFO) 
    compute_all_defense_scores()