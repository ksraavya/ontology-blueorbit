from __future__ import annotations
from typing import List, Dict, Any
from common.ontology import get_relation_type 
from common.intelligence.normalization import normalize_by_max 
from common.intelligence.dependency import compute_dependency 
from common.intelligence.aggregation import ( 
    max_value, sum_values) 
from common.intelligence.growth import ( 
    compute_growth_series, growth_trend) 
from common.config import DEFAULT_CONFIDENCE 

"""Defense module — compute layer. 
Adds normalized_weight, dependency, and other 
metrics to clean data using common/intelligence/. 
Uses ontology to determine which metrics to compute 
based on relationship type."""

def compute_milex_metrics(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Computes metrics for military expenditure data.
    SPENDS_ON_DEFENSE is a 'state' relation.
    """
    if not rows:
        return []

    # Step 1 — Verify relation type
    rel_type = get_relation_type('SPENDS_ON_DEFENSE')
    print(f"Relation type for SPENDS_ON_DEFENSE: {rel_type}")

    # Step 2 — Find the global maximum expenditure
    max_exp = max_value([r['expenditure_usd_millions'] for r in rows])
    print(f'Max defense spending: {max_exp}')

    # Step 3 — Enrich each row
    enriched_rows = []
    for row in rows:
        enriched_row = row.copy()
        enriched_row['normalized_weight'] = normalize_by_max(row['expenditure_usd_millions'], max_exp)
        enriched_row['value'] = row['expenditure_usd_millions']
        enriched_row['confidence'] = DEFAULT_CONFIDENCE
        enriched_row['rel_type'] = 'SPENDS_ON_DEFENSE'
        enriched_rows.append(enriched_row)

    return enriched_rows

def compute_arms_metrics(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Computes metrics for arms export data.
    EXPORTS_ARMS is a 'flow' relation.
    """
    if not rows:
        return []

    # Step 1 — Verify relation type
    rel_type = get_relation_type('EXPORTS_ARMS')
    print(f"Relation type for EXPORTS_ARMS: {rel_type}")

    # Step 2 — Compute global totals for normalization
    max_tiv = max_value([r['tiv_millions'] for r in rows])
    
    year_totals = {} 
    year_country_counts = {} 
    for row in rows: 
        yr = row['year'] 
        year_totals[yr] = year_totals.get(yr, 0) + row['tiv_millions'] 
        year_country_counts[yr] = year_country_counts.get(yr, 0) + 1 
    
    print(f"Max TIV: {max_tiv}")

    # Step 3 — Enrich each row
    enriched_rows = []
    for row in rows:
        enriched_row = row.copy()
        enriched_row['normalized_weight'] = normalize_by_max(row['tiv_millions'], max_tiv)
        
        year_count = year_country_counts.get(row['year'], 0) 
        year_total = year_totals.get(row['year'], 0) 
 
        if year_count >= 5 and year_total > 0: 
            dependency = compute_dependency(row['tiv_millions'], year_total) 
        else: 
            dependency = None 
            
        enriched_row['dependency'] = dependency
        enriched_row['value'] = row['tiv_millions']
        enriched_row['confidence'] = DEFAULT_CONFIDENCE
        enriched_row['rel_type'] = 'EXPORTS_ARMS'
        enriched_rows.append(enriched_row)

    valid_rows = [r for r in enriched_rows if r.get('dependency') is not None] 
    top = sorted(valid_rows, key=lambda x: x['dependency'], reverse=True)[:5] 
    print(f"Valid dependency rows: {len(valid_rows)} of {len(enriched_rows)}") 
    print("Top 5 by year-level dependency:") 
    for r in top: 
        print(f"  {r['country']} {r['year']}: {round(r['dependency'], 4)}") 
    print(f"Avg dependency: {sum(r['dependency'] for r in valid_rows) / len(valid_rows):.4f}") 

    return enriched_rows

def compute_acled_metrics(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Computes metrics for ACLED conflict data.
    HAS_CONFLICT_STATS is a 'state' relation.
    """
    if not rows:
        return []

    # Step 1 — Verify relation type
    rel_type = get_relation_type('HAS_CONFLICT_STATS')
    print(f"Relation type for HAS_CONFLICT_STATS: {rel_type}")

    # Step 2 — Find global max fatalities
    max_fat = max_value([r['total_fatalities'] for r in rows])
    print(f"Max total fatalities: {max_fat}")

    # Step 3 — Compute fatality trend per country
    country_data = {}
    for row in rows:
        country = row['country']
        if country not in country_data:
            country_data[country] = []
        country_data[country].append(row)

    country_trends = {}
    trend_counts = {'increasing': 0, 'decreasing': 0, 'stable': 0}
    
    for country, country_rows in country_data.items():
        # Sort by year ascending
        sorted_rows = sorted(country_rows, key=lambda x: x['year'])
        fatalities_series = [r['total_fatalities'] for r in sorted_rows]
        
        if len(fatalities_series) < 2:
            trend = 'stable'
        else:
            trend = growth_trend(fatalities_series)
            
        country_trends[country] = trend
        trend_counts[trend] += 1

    print(f"Fatality trends summary: {trend_counts}")

    # Step 4 — Enrich each row
    enriched_rows = []
    for row in rows:
        enriched_row = row.copy()
        enriched_row['normalized_weight'] = normalize_by_max(row['total_fatalities'], max_fat)
        enriched_row['value'] = row['total_fatalities']
        enriched_row['confidence'] = DEFAULT_CONFIDENCE
        enriched_row['rel_type'] = 'HAS_CONFLICT_STATS'
        enriched_row['fatality_trend'] = country_trends.get(row['country'], 'stable')
        enriched_rows.append(enriched_row)

    return enriched_rows

if __name__ == '__main__': 
    from modules.defense.ingest import ( 
        ingest_milex, ingest_arms_exports, ingest_acled) 
    from modules.defense.transform import ( 
        transform_milex, transform_arms, transform_acled) 
    print('Testing compute...') 
    try:
        milex_transformed = transform_milex(ingest_milex())
        m = compute_milex_metrics(milex_transformed) 
        if m:
            print('Sample milex enriched:', m[0]) 
            print('normalized_weight present:', 'normalized_weight' in m[0]) 
        
        arms_transformed = transform_arms(ingest_arms_exports())
        a = compute_arms_metrics(arms_transformed)
        if a:
            print('Sample arms enriched:', a[0])
            
        acled_transformed = transform_acled(ingest_acled())
        ac = compute_acled_metrics(acled_transformed)
        if ac:
            print('Sample acled enriched:', ac[0])
            
        print('All compute functions OK') 
    except Exception as e:
        print(f"Compute test failed: {e}")
        import traceback
        traceback.print_exc()
