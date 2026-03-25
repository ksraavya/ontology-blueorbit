from __future__ import annotations
import pandas as pd
from typing import List, Dict, Any
from common.entity_mapper import normalize_entity

"""Defense module — transformation layer. 
Cleans, filters, normalizes raw dataframes. 
Uses common.entity_mapper for country normalization. 
Returns list of clean dicts ready for compute."""

def transform_milex(raw_df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Transforms SIPRI military expenditure data.
    """
    df = raw_df.copy()

    # Step 1 — Define and filter region labels
    REGION_LABELS = [ 
        'Africa', 'North Africa', 'sub-Saharan Africa', 
        'Americas', 'North America', 
        'Central America and the Caribbean', 
        'South America', 'Asia and Oceania', 'Central Asia', 
        'East Asia', 'South Asia', 'South East Asia', 
        'Oceania', 'Europe', 'Eastern Europe', 
        'Western Europe', 'Middle East', 'World total', 'NATO' 
    ] 
    df = df[df['Country'].notna()]
    df = df[~df['Country'].isin(REGION_LABELS)]

    # Step 2 — Select Country and year columns 2000-2024
    year_cols = [c for c in df.columns 
                 if isinstance(c, int) and 2000 <= c <= 2024] 
    df = df[['Country'] + year_cols]

    # Step 3 — Melt from wide to long format
    df = df.melt(id_vars=['Country'], 
                 var_name='year', 
                 value_name='expenditure_usd_millions') 

    # Step 4 — Rename 'Country' to 'country'
    df = df.rename(columns={'Country': 'country'})

    # Step 5 — Apply normalize_entity to country column
    df['country'] = df['country'].apply( 
        lambda x: normalize_entity(str(x), entity_type='country')) 

    # Step 6 — Convert types
    df['year'] = pd.to_numeric(df['year'], errors='coerce')
    df = df.dropna(subset=['year'])
    df['year'] = df['year'].astype(int)

    df['expenditure_usd_millions'] = pd.to_numeric(df['expenditure_usd_millions'], errors='coerce')
    df = df.dropna(subset=['expenditure_usd_millions'])
    df = df[df['expenditure_usd_millions'] > 0]

    # Step 7 — Drop rows where country is NaN or empty
    df = df.dropna(subset=['country'])
    df = df[df['country'].str.strip() != ""]

    # Step 8 — Drop duplicates on (country, year) keep first
    df = df.drop_duplicates(subset=['country', 'year'], keep='first')

    # Step 9 — Sort by country, year ascending
    df = df.sort_values(by=['country', 'year'], ascending=True)

    result = df.to_dict('records')
    print(f'Milex transformed: {len(result)} rows, {len(set(r["country"] for r in result))} countries')
    return result

def transform_arms(raw_dict: dict[str, pd.DataFrame]) -> List[Dict[str, Any]]:
    """
    Transforms SIPRI arms transfer data.
    """
    processed_dfs = []
    
    file_configs = [
        ('1950_1980', '1950-1980'),
        ('1981_2000', '1981-2000')
    ]

    for key, period in file_configs:
        df = raw_dict[key].copy()

        # Step 1 — Rename column index 3 to 'country'
        df = df.rename(columns={df.columns[3]: 'country'}) 

        # Step 2 — Identify year columns (1940-2030)
        year_cols = [c for c in df.columns 
                     if str(c).strip().isdigit() 
                     and 1940 <= int(str(c).strip()) <= 2030] 

        # Step 3 — Select only ['country'] + year_cols
        df = df[['country'] + year_cols]

        # Step 4 — Filter out sub-variant rows (' N', ' S', ' R')
        df = df[df['country'].notna()] 
        df = df[~df['country'].str.strip().str.endswith((' N', ' S', ' R'))] 

        # Step 5 — Filter out non-country summary rows
        INVALID = ['Total', 'total', 'TOTAL', 'World', 'world', 
                   'Other', 'Supplier', 'Recipient', ''] 
        df = df[~df['country'].isin(INVALID)] 

        # Step 6 — Melt to long format
        df = df.melt(id_vars=['country'], var_name='year', value_name='tiv_millions') 

        # Step 7 — Apply normalize_entity to country column
        df['country'] = df['country'].apply( 
            lambda x: normalize_entity(str(x), entity_type='country')) 

        # Step 8 — Convert types
        df['year'] = pd.to_numeric(df['year'].astype(str).str.strip(), errors='coerce')
        df['tiv_millions'] = pd.to_numeric(df['tiv_millions'], errors='coerce')
        
        df = df.dropna(subset=['country', 'year', 'tiv_millions'])
        df['year'] = df['year'].astype(int)
        df = df[df['tiv_millions'] > 0]

        # Step 9 — Add period column
        df['period'] = period
        processed_dfs.append(df)

    # Combine dataframes
    combined_df = pd.concat(processed_dfs, ignore_index=True)

    # Step 10 — Drop duplicates on (country, year) keeping highest tiv_millions
    combined_df = combined_df.sort_values(by='tiv_millions', ascending=False)
    combined_df = combined_df.drop_duplicates(subset=['country', 'year'], keep='first')
    combined_df = combined_df.sort_values(by=['country', 'year'], ascending=True)

    result = combined_df.to_dict('records')
    print(f'Arms transformed: {len(result)} rows, {len(set(r["country"] for r in result))} countries')
    return result

def transform_acled(raw_dict: dict[str, pd.DataFrame]) -> List[Dict[str, Any]]:
    """
    Transforms ACLED conflict data.
    """
    # Step 1 — Rename columns immediately after loading
    violence = raw_dict['violence_events'].copy().rename(columns={
        'COUNTRY': 'country', 'YEAR': 'year', 'EVENTS': 'violence_events'
    })
    civ_fatal = raw_dict['civilian_fatalities'].copy().rename(columns={
        'COUNTRY': 'country', 'YEAR': 'year', 'FATALITIES': 'civilian_fatalities'
    })
    all_fatal = raw_dict['all_fatalities'].copy().rename(columns={
        'COUNTRY': 'country', 'YEAR': 'year', 'FATALITIES': 'total_fatalities'
    })
    civ_events = raw_dict['civilian_events'].copy().rename(columns={
        'COUNTRY': 'country', 'YEAR': 'year', 'EVENTS': 'civilian_events'
    })

    # Step 2 — Merge all four on ['country', 'year']
    df = violence.merge(civ_fatal, on=['country', 'year'], how='outer') 
    df = df.merge(all_fatal, on=['country', 'year'], how='outer') 
    df = df.merge(civ_events, on=['country', 'year'], how='outer') 

    # Step 3 — Fill all NaN in numeric columns with 0
    numeric_cols = ['violence_events', 'civilian_fatalities', 'total_fatalities', 'civilian_events']
    df[numeric_cols] = df[numeric_cols].fillna(0)

    # Step 4 — Apply normalize_entity to country column
    df['country'] = df['country'].apply( 
        lambda x: normalize_entity(str(x), entity_type='country')) 

    # Step 5 — Convert year to int. Drop rows where year conversion fails.
    df['year'] = pd.to_numeric(df['year'], errors='coerce')
    df = df.dropna(subset=['country', 'year'])
    df['year'] = df['year'].astype(int)

    # Step 6 — Drop duplicates on (country, year) keep first
    df = df.drop_duplicates(subset=['country', 'year'], keep='first')

    # Step 7 — Sort by country, year
    df = df.sort_values(by=['country', 'year'], ascending=True)

    result = df.to_dict('records')
    print(f'ACLED transformed: {len(result)} rows, total fatalities: {int(sum(r["total_fatalities"] for r in result))}')
    return result

if __name__ == '__main__': 
    from modules.defense.ingest import ( 
        ingest_milex, ingest_arms_exports, ingest_acled) 
    print('Testing transform...') 
    try:
        milex_raw = ingest_milex()
        print(transform_milex(milex_raw)[:2]) 
        
        arms_raw = ingest_arms_exports()
        print(transform_arms(arms_raw)[:2]) 
        
        acled_raw = ingest_acled()
        print(transform_acled(acled_raw)[:2]) 
        print('All transform functions OK') 
    except Exception as e:
        print(f"Transform test failed: {e}")
        import traceback
        traceback.print_exc()
