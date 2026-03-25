from __future__ import annotations
import pandas as pd
from pathlib import Path

"""Defense module — data ingestion layer. 
Reads raw files from disk. No cleaning, no computing. 
All functions return raw pandas dataframes."""

# File path constants
BASE_DATA = Path('data/raw')
MILEX_FILE = BASE_DATA / 'sipri_milex.xlsx'
ARMS_1950_FILE = BASE_DATA / 'sipri_arms_1950_1980.csv'
ARMS_1981_FILE = BASE_DATA / 'sipri_arms_1981_2000.csv'
ACLED_VIOLENCE_FILE = BASE_DATA / 'acled_violence_events.xlsx'
ACLED_CIVILIAN_FATAL_FILE = BASE_DATA / 'acled_civilian_fatalities.xlsx'
ACLED_ALL_FATAL_FILE = BASE_DATA / 'acled_all_fatalities.xlsx'
ACLED_CIVILIAN_EVENTS_FILE = BASE_DATA / 'acled_civilian_events.xlsx'

def ingest_milex() -> pd.DataFrame:
    """Reads SIPRI military expenditure Excel file."""
    try:
        df = pd.read_excel(MILEX_FILE, sheet_name='Current US$', header=5)
        print(f'Milex raw shape: {df.shape}')
        return df
    except FileNotFoundError:
        raise FileNotFoundError(f'SIPRI Milex file not found at {MILEX_FILE}')
    except Exception as e:
        print(f"Error reading Milex: {e}")
        raise

def ingest_arms_exports() -> dict[str, pd.DataFrame]:
    """Reads both SIPRI arms transfer CSV files."""
    arms_data = {}
    
    # File 1: 1950-1980
    try:
        df_1950 = pd.read_csv(ARMS_1950_FILE, skiprows=10, header=0, low_memory=False, on_bad_lines='skip')
        print(f'Arms 1950-1980 raw shape: {df_1950.shape}')
        arms_data['1950_1980'] = df_1950
    except FileNotFoundError:
        raise FileNotFoundError(f'SIPRI Arms 1950-1980 file not found at {ARMS_1950_FILE}')
    
    # File 2: 1981-2000
    try:
        df_1981 = pd.read_csv(ARMS_1981_FILE, skiprows=10, header=0, low_memory=False, on_bad_lines='skip')
        print(f'Arms 1981-2000 raw shape: {df_1981.shape}')
        arms_data['1981_2000'] = df_1981
    except FileNotFoundError:
        raise FileNotFoundError(f'SIPRI Arms 1981-2000 file not found at {ARMS_1981_FILE}')
        
    return arms_data

def ingest_acled() -> dict[str, pd.DataFrame]:
    """Reads all 4 ACLED Excel files."""
    acled_data = {}
    
    files = {
        'violence_events': ACLED_VIOLENCE_FILE,
        'civilian_fatalities': ACLED_CIVILIAN_FATAL_FILE,
        'all_fatalities': ACLED_ALL_FATAL_FILE,
        'civilian_events': ACLED_CIVILIAN_EVENTS_FILE
    }
    
    for key, filepath in files.items():
        try:
            df = pd.read_excel(filepath, header=0)
            print(f'ACLED {key} raw shape: {df.shape}')
            acled_data[key] = df
        except FileNotFoundError:
            raise FileNotFoundError(f'ACLED {key} file not found at {filepath}')
            
    return acled_data

if __name__ == '__main__':
    print('Testing ingest...')
    try:
        df_milex = ingest_milex()
        arms = ingest_arms_exports()
        acled = ingest_acled()
        print('All ingest functions OK')
    except Exception as e:
        print(f"Ingest test failed: {e}")
