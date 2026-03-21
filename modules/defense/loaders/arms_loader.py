from __future__ import annotations

import sys
from pathlib import Path
from typing import List

import pandas as pd

# When running this file directly, Python won't automatically include the repo
# root on `sys.path`, so sibling imports like `modules.defense...` can fail.
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def load_arms_exports(filepath: str, period_label: str) -> pd.DataFrame:
    df = pd.read_csv(
        filepath,
        skiprows=10,
        header=0,
        low_memory=False,
        on_bad_lines="skip",
    )

    # Column index 3 = Supplier
    df = df.rename(columns={df.columns[3]: "country"})

    year_cols = [
        c
        for c in df.columns
        if str(c).strip().isdigit() and 1940 <= int(str(c).strip()) <= 2030
    ]

    df = df[["country"] + year_cols]

    df = df[df["country"].notna()]
    df = df[~df["country"].str.strip().str.endswith((" N", " S", " R"))]

    melted = df.melt(
        id_vars=["country"],
        var_name="year",
        value_name="tiv_millions",
    )

    melted["period"] = period_label

    print("Result shape:", melted.shape)
    print("Sample rows:")
    print(melted.head(5))
    print("Unique country count:", melted["country"].nunique())

    return melted


def load_all_arms_exports() -> pd.DataFrame:
    df1 = load_arms_exports("data/raw/sipri_arms_1950_1980.csv", "1950-1980")
    df2 = load_arms_exports("data/raw/sipri_arms_1981_2000.csv", "1981-2000")

    df = pd.concat([df1, df2], ignore_index=True)
    df = df.drop_duplicates(subset=["country", "year"], keep="first")

    print("Total rows:", df.shape[0])
    print("Unique countries:", df["country"].nunique())

    return df


if __name__ == "__main__":
    load_all_arms_exports()

