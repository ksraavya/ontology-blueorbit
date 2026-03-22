"""Load V-Dem and GDELT geopolitics data."""
from __future__ import annotations
import sys
sys.path.insert(0, '.')

import glob
import os

import pandas as pd


def load_vdem(path: str = "data/raw/vdem_core.csv") -> pd.DataFrame:
    """Load V-Dem democracy data, select relevant columns, and filter year >= 2010.

    Args:
        path: Path to the V-Dem CSV file.

    Returns:
        DataFrame with columns: country_name, year, v2x_polyarchy (year >= 2010).

    Raises:
        FileNotFoundError: If the CSV file does not exist.
    """
    try:
        df = pd.read_csv(path)
    except FileNotFoundError as e:
        raise FileNotFoundError(
            f"V-Dem file not found: {path}. Please ensure the file exists."
        ) from e

    df = df[["country_name", "year", "v2x_polyarchy"]].copy()
    df = df[df["year"] >= 2010]
    print(f"V-Dem: loaded {len(df)} rows (year >= 2010)")
    return df


def load_gdelt(folder: str = "data/raw/gdelt/") -> pd.DataFrame:
    """Load GDELT event data from tab-separated files in a folder.

    Loads all files matching gdelt_*.CSV, selects date, actor1_code, actor2_code,
    event_code, filters by event_code prefix (04, 05, 08), and drops null/empty actors.

    Args:
        folder: Path to folder containing GDELT CSV files.

    Returns:
        DataFrame with columns: date, actor1_code, actor2_code, event_code.

    Raises:
        FileNotFoundError: If folder does not exist or no matching files found.
    """
    pattern = os.path.join(folder, "gdelt_*.CSV")
    files = sorted(glob.glob(pattern))

    if not files:
        if not os.path.isdir(folder):
            raise FileNotFoundError(
                f"GDELT folder not found: {folder}. Please ensure the folder exists."
            )
        raise FileNotFoundError(
            f"No GDELT files found matching: {pattern}. Please add gdelt_*.CSV files."
        )

    dfs = []
    for f in files:
        df = pd.read_csv(
            f,
            sep="\t",
            header=None,
            on_bad_lines="skip",
            low_memory=False,
        )
        dfs.append(df)

    df = pd.concat(dfs, ignore_index=True)

    # Select columns by index and rename
    df = df[[1, 7, 17, 26]].copy()
    df.columns = ["date", "actor1_code", "actor2_code", "event_code"]
    df["event_code"] = df["event_code"].astype(str).str.zfill(3)

    rows_before = len(df)

    # Filter: event_code starts with "04", "05", or "08"
    df = df[df["event_code"].astype(str).str.startswith(("04", "05", "08"))]

    # Drop null or empty actor1_code / actor2_code
    df = df.dropna(subset=["actor1_code", "actor2_code"])
    df = df[
        (df["actor1_code"].astype(str).str.strip() != "")
        & (df["actor2_code"].astype(str).str.strip() != "")
    ]

    rows_after = len(df)

    print(f"GDELT: {len(files)} file(s) found, {rows_before} rows before filter, {rows_after} rows after filter")
    return df.reset_index(drop=True)


if __name__ == "__main__":
    df_vdem = load_vdem()
    df_gdelt = load_gdelt()
    print(df_vdem.head())
    print(df_vdem.dtypes)
    print(df_gdelt.head())
    print(df_gdelt.dtypes)
