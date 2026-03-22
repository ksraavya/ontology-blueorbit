from __future__ import annotations
import time
from modules.defense.ingest import ( 
    ingest_milex, ingest_arms_exports, ingest_acled) 
from modules.defense.transform import ( 
    transform_milex, transform_arms, transform_acled) 
from modules.defense.compute import ( 
    compute_milex_metrics, 
    compute_arms_metrics, 
    compute_acled_metrics) 
from modules.defense.load import ( 
    load_milex, load_arms, load_acled, verify_loads) 

def run_defense_pipeline():
    start = time.time() 

    # STEP 1: SIPRI Military Spending 
    print('=' * 50) 
    print('STEP 1: SIPRI Military Spending') 
    print('=' * 50) 
    try: 
        raw = ingest_milex() 
        clean = transform_milex(raw) 
        enriched = compute_milex_metrics(clean) 
        count = load_milex(enriched) 
        print(f'STEP 1 COMPLETE: {count} records inserted') 
    except Exception as e: 
        print(f'STEP 1 FAILED: {e}') 

    # STEP 2: SIPRI Arms Exports 
    print('\n' + '=' * 50) 
    print('STEP 2: SIPRI Arms Exports') 
    print('=' * 50) 
    try: 
        raw_arms = ingest_arms_exports() 
        clean_arms = transform_arms(raw_arms) 
        enriched_arms = compute_arms_metrics(clean_arms) 
        count_arms = load_arms(enriched_arms) 
        print(f'STEP 2 COMPLETE: {count_arms} records inserted') 
    except Exception as e: 
        print(f'STEP 2 FAILED: {e}') 

    # STEP 3: ACLED Conflict Data 
    print('\n' + '=' * 50) 
    print('STEP 3: ACLED Conflict Data') 
    print('=' * 50) 
    try: 
        raw_acled = ingest_acled() 
        clean_acled = transform_acled(raw_acled) 
        enriched_acled = compute_acled_metrics(clean_acled) 
        count_acled = load_acled(enriched_acled) 
        print(f'STEP 3 COMPLETE: {count_acled} records inserted') 
    except Exception as e: 
        print(f'STEP 3 FAILED: {e}') 

    # STEP 4: Verification 
    print('\n' + '=' * 50) 
    print('STEP 4: Verification') 
    print('=' * 50) 
    try: 
        verify_loads() 
    except Exception as e: 
        print(f'Verification failed: {e}') 

    end = time.time() 
    print(f'\nTotal pipeline time: {end - start:.2f}s') 

if __name__ == '__main__': 
    run_defense_pipeline()
