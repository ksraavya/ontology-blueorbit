from __future__ import annotations

import sys
from typing import List
from pathlib import Path

import pandas as pd

# When running this file directly, Python won't automatically include the repo
# root on `sys.path`, so sibling imports like `modules.defense.cleaner` can
# fail. Add the project root explicitly.
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


REGION_LABELS: List[str] = [
    "Africa",
    "North Africa",
    "sub-Saharan Africa",
    "Americas",
    "North America",
    "Central America and the Caribbean",
    "South America",
    "Asia and Oceania",
    "Central Asia",
    "East Asia",
    "South Asia",
    "South East Asia",
    "Oceania",
    "Europe",
    "Eastern Europe",
    "Western Europe",
    "Middle East",
    "World total",
    "NATO",
]


def load_milex_spending(filepath: str) -> pd.DataFrame:
    df = pd.read_excel(filepath, sheet_name="Current US$", header=5)

    # Remove empty countries and aggregate region rows we don't want to model.
    df = df[df["Country"].notna() & ~df["Country"].isin(REGION_LABELS)].copy()

    year_cols = [c for c in df.columns if isinstance(c, int) and 2000 <= c <= 2024]
    df = df[["Country"] + year_cols].copy()

    melted = df.melt(
        id_vars=["Country"],
        var_name="year",
        value_name="expenditure_usd_millions",
    )

    result = melted.rename(columns={"Country": "country"})

    print("Result shape:", result.shape)
    print("Unique country count:", result["country"].nunique())
    print("First 10 rows:")
    print(result.head(10))

    return result


if __name__ == "__main__":
    primary = Path("data/raw/sipri_milex.xlsx")
    if primary.exists():
        df = load_milex_spending(str(primary))
    else:
        fallback = Path("data/raw/SIPRI-Milex-data-1949-2024_2.xlsx")
        print(f"Input file not found: {primary}. Trying fallback: {fallback}")
        df = load_milex_spending(str(fallback))

    # Temporary end-to-end flow for debugging/bootstrapping the pipeline.
    from modules.defense.cleaner import clean_milex_data

    df_clean = clean_milex_data(df)

    processed_dir = Path("data/processed")
    processed_dir.mkdir(parents=True, exist_ok=True)
    out_path = processed_dir / "clean_milex.csv"

    df_clean.to_csv(out_path, index=False)
    print(f"Saved to {out_path}")



