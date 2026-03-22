import sys
sys.path.insert(0, '.')
import os
import pandas as pd

from modules.geopolitics.loader import load_vdem, load_gdelt, load_unga
from modules.geopolitics.cleaner import clean_vdem, clean_gdelt, clean_unga
from modules.geopolitics.compute import compute_vote_similarity
from modules.geopolitics.inserter import insert_political_systems, insert_diplomatic_edges
from modules.geopolitics.inserter import insert_vote_similarity


def run_geopolitics(use_cache=True, use_unga_cache=True):
    print("=== Running Geopolitics Module ===")

    vdem_cache = "data/processed/vdem_clean.parquet"
    gdelt_cache = "data/processed/gdelt_clean.parquet"
    unga_cache  = "data/processed/unga_similarity.parquet"

    # ── V-Dem + GDELT (existing cache logic) ──────────────
    if use_cache and os.path.exists(vdem_cache) and os.path.exists(gdelt_cache):
        print("Loading V-Dem and GDELT from cache...")
        df_vdem_clean  = pd.read_parquet(vdem_cache)
        df_gdelt_clean = pd.read_parquet(gdelt_cache)
    else:
        print("Processing raw files...")
        df_vdem        = load_vdem()
        df_gdelt       = load_gdelt()
        df_vdem_clean  = clean_vdem(df_vdem)
        df_gdelt_clean = clean_gdelt(df_gdelt)
        df_vdem_clean.to_parquet(vdem_cache,  index=False)
        df_gdelt_clean.to_parquet(gdelt_cache, index=False)

    insert_political_systems(df_vdem_clean)
    insert_diplomatic_edges(df_gdelt_clean)

    # ── UNGA (separate independent cache) ─────────────────
    if use_unga_cache and os.path.exists(unga_cache):
        print("Loading UNGA from cache...")
        df_similarity = pd.read_parquet(unga_cache)
    else:
        print("Processing UNGA raw file...")
        df_unga       = load_unga()
        df_unga_clean = clean_unga(df_unga)
        df_similarity = compute_vote_similarity(df_unga_clean)
        df_similarity.to_parquet(unga_cache, index=False)
        print("Saved UNGA cache to data/processed/unga_similarity.parquet")

    insert_vote_similarity(df_similarity)

    print("=== Geopolitics Module Complete ===")


def run_defense():
    print("Defense module not yet implemented")


def run_economy():
    print("Economy module not yet implemented")


def run_climate():
    print("Climate module not yet implemented")


if __name__ == "__main__":
    # Normal run — everything from cache
    run_geopolitics(use_cache=True, use_unga_cache=True)

    # New GDELT files added — reprocess GDELT only
    # run_geopolitics(use_cache=False, use_unga_cache=True)

    # New UNGA year downloaded — reprocess UNGA only
    # run_geopolitics(use_cache=True, use_unga_cache=False)

    # Full fresh run of everything
    # run_geopolitics(use_cache=False, use_unga_cache=False)

    for name, fn in [
        ("Defense", run_defense),
        ("Economy", run_economy),
        ("Climate", run_climate),
    ]:
        try:
            fn()
        except Exception as e:
            print(f"❌ {name} module failed: {e}")
