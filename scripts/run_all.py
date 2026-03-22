import sys
sys.path.insert(0, '.')
import os
import pandas as pd

from modules.geopolitics.loader import load_vdem, load_gdelt
from modules.geopolitics.cleaner import clean_vdem, clean_gdelt
from modules.geopolitics.inserter import insert_political_systems, insert_diplomatic_edges


def run_geopolitics(use_cache=True):
    print("=== Running Geopolitics Module ===")

    vdem_cache = "data/processed/vdem_clean.parquet"
    gdelt_cache = "data/processed/gdelt_clean.parquet"

    if use_cache and os.path.exists(vdem_cache) and os.path.exists(gdelt_cache):
        print("Loading from processed cache...")
        df_vdem_clean = pd.read_parquet(vdem_cache)
        df_gdelt_clean = pd.read_parquet(gdelt_cache)
    else:
        print("Processing raw files...")
        df_vdem = load_vdem()
        df_gdelt = load_gdelt()
        df_vdem_clean = clean_vdem(df_vdem)
        df_gdelt_clean = clean_gdelt(df_gdelt)
        df_vdem_clean.to_parquet(vdem_cache, index=False)
        df_gdelt_clean.to_parquet(gdelt_cache, index=False)

    insert_political_systems(df_vdem_clean)
    insert_diplomatic_edges(df_gdelt_clean)
    print("=== Geopolitics Module Complete ===")


def run_defense():
    print("Defense module not yet implemented")


def run_economy():
    print("Economy module not yet implemented")


def run_climate():
    print("Climate module not yet implemented")


if __name__ == "__main__":
    for name, fn in [
        ("Geopolitics", run_geopolitics),
        ("Defense",     run_defense),
        ("Economy",     run_economy),
        ("Climate",     run_climate),
    ]:
        try:
            fn()
        except Exception as e:
            print(f"❌ {name} module failed: {e}")