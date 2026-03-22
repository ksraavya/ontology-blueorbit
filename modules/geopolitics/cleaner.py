"""Clean V-Dem and GDELT geopolitics data for downstream use."""
from __future__ import annotations

import sys
sys.path.insert(0, ".")

from typing import Optional

import pandas as pd
import pycountry

from common.entity_mapper import normalize_entity


def code_to_name(code: Optional[str]) -> Optional[str]:
    """Convert 3-letter ISO country code to full country name.

    Args:
        code: 3-letter ISO alpha-3 code (e.g. 'USA', 'GBR').

    Returns:
        Full country name or None if invalid.
    """
    if not code or not isinstance(code, str):
        return None
    try:
        return pycountry.countries.get(alpha_3=code.strip().upper()).name
    except Exception:
        return None


def clean_vdem(df: pd.DataFrame) -> pd.DataFrame:
    """Clean V-Dem democracy data.

    Applies entity normalization to country names, casts types, drops invalid rows.
    Output columns: country, year, v2x_polyarchy.

    Args:
        df: Raw V-Dem DataFrame with country_name, year, v2x_polyarchy.

    Returns:
        Cleaned DataFrame with country, year, v2x_polyarchy.
    """
    df = df.copy()
    df["country"] = df["country_name"].apply(lambda x: normalize_entity(x, entity_type="country"))
    df = df.drop(columns=["country_name"])
    df["year"] = df["year"].astype(int)
    df["v2x_polyarchy"] = pd.to_numeric(df["v2x_polyarchy"], errors="coerce")
    df = df.dropna(subset=["country"])
    df = df[df["country"].astype(str).str.strip() != ""]
    df = df.dropna(subset=["v2x_polyarchy"])
    df = df[["country", "year", "v2x_polyarchy"]]
    print(f"V-Dem clean: {len(df)} rows remaining")
    return df.reset_index(drop=True)


def clean_gdelt(df: pd.DataFrame) -> pd.DataFrame:
    """Clean GDELT event data.

    Converts 3-letter ISO codes to country names, normalizes entities,
    drops self-pairs and invalid rows.
    Output columns: actor1, actor2, event_code, date.

    Args:
        df: Raw GDELT DataFrame with date, actor1_code, actor2_code, event_code.

    Returns:
        Cleaned DataFrame with actor1, actor2, event_code, date.
    """
    df = df.copy()
    df["actor1"] = (
        df["actor1_code"]
        .apply(code_to_name)
        .apply(lambda x: normalize_entity(x, entity_type="country") if isinstance(x, str) else None)
    )
    df["actor2"] = (
        df["actor2_code"]
        .apply(code_to_name)
        .apply(lambda x: normalize_entity(x, entity_type="country") if isinstance(x, str) else None)
    )
    df = df.drop(columns=["actor1_code", "actor2_code"])
    df["date"] = df["date"].astype(str)
    df = df.dropna(subset=["actor1", "actor2"])
    df = df[(df["actor1"] != "") & (df["actor2"] != "")]
    df = df[df["actor1"] != df["actor2"]]
    df = df[["actor1", "actor2", "event_code", "date"]]
    print(f"GDELT clean: {len(df)} rows remaining")
    return df.reset_index(drop=True)


if __name__ == "__main__":
    from modules.geopolitics.loader import load_vdem, load_gdelt

    df_vdem = load_vdem()
    df_clean_vdem = clean_vdem(df_vdem)
    print("V-Dem cleaned:")
    print(df_clean_vdem.head())
    print(df_clean_vdem.shape)

    df_gdelt = load_gdelt()
    df_clean_gdelt = clean_gdelt(df_gdelt)
    print("\nGDELT cleaned:")
    print(df_clean_gdelt.head())
    print(df_clean_gdelt.shape)

