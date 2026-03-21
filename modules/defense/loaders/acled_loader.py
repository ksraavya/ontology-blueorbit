from __future__ import annotations

import pandas as pd


# Input files (already production-clean per project instructions)
VIOLENCE_EVENTS_FILE = "data/raw/acled_violence_events.xlsx"
CIVILIAN_FATALITIES_FILE = "data/raw/acled_civilian_fatalities.xlsx"
ALL_FATALITIES_FILE = "data/raw/acled_all_fatalities.xlsx"
CIVILIAN_EVENTS_FILE = "data/raw/acled_civilian_events.xlsx"


def load_acled_summary() -> pd.DataFrame:
    violence = pd.read_excel(VIOLENCE_EVENTS_FILE, header=0)
    violence.columns = ["country", "year", "violence_events"]

    civ_fatal = pd.read_excel(CIVILIAN_FATALITIES_FILE, header=0)
    civ_fatal.columns = ["country", "year", "civilian_fatalities"]

    all_fatal = pd.read_excel(ALL_FATALITIES_FILE, header=0)
    all_fatal.columns = ["country", "year", "total_fatalities"]

    civ_events = pd.read_excel(CIVILIAN_EVENTS_FILE, header=0)
    civ_events.columns = ["country", "year", "civilian_events"]

    df = violence.merge(civ_fatal, on=["country", "year"], how="outer")
    df = df.merge(all_fatal, on=["country", "year"], how="outer")
    df = df.merge(civ_events, on=["country", "year"], how="outer")

    # ACLED files are already integers, but outer joins can introduce NaNs.
    # Fill only numeric columns to avoid corrupting keys like `country`/`year`.
    numeric_cols = df.select_dtypes(include="number").columns
    df[numeric_cols] = df[numeric_cols].fillna(0)

    years = df["year"]
    year_min = int(years.min()) if len(df) else None
    year_max = int(years.max()) if len(df) else None

    print("ACLED merged shape:", df.shape)
    print("Columns:", df.columns.tolist())
    print("Year range:", year_min, year_max)
    print("Unique country count:", df["country"].nunique())

    return df


if __name__ == "__main__":
    df = load_acled_summary()
    print(df.head(10))

