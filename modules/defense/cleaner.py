from __future__ import annotations

import pandas as pd


def clean_milex_data(df: pd.DataFrame) -> pd.DataFrame:
    from common.country_mapper import normalize_country

    # 2. Strip whitespace from 'country'
    df["country"] = df["country"].astype(str).str.strip()

    # 3. Normalize each country value
    df["country"] = df["country"].apply(normalize_country)

    # 4. Convert 'year' to integer; drop rows where conversion fails
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df.dropna(subset=["year"])
    df["year"] = df["year"].astype(int)

    # 5. Convert expenditures to float; turn junk strings into NaN
    df["expenditure_usd_millions"] = pd.to_numeric(
        df["expenditure_usd_millions"], errors="coerce"
    )

    # 6. Drop rows where expenditure_usd_millions is NaN
    df = df.dropna(subset=["expenditure_usd_millions"])

    # 7. Drop rows where expenditure_usd_millions <= 0
    df = df[df["expenditure_usd_millions"] > 0].copy()

    # 8. Drop rows where country is NaN or empty string
    df = df[df["country"].notna() & (df["country"] != "")].copy()

    # 9. Drop duplicates on (country, year)
    df = df.drop_duplicates(subset=["country", "year"], keep="first")

    # 10. Sort by country then year
    df = df.sort_values(["country", "year"], ascending=[True, True])

    # 11. Print diagnostics
    print("Total rows remaining:", len(df))
    print("Unique country count:", df["country"].nunique())
    if len(df) > 0:
        print("Year range:", int(df["year"].min()), int(df["year"].max()))
    else:
        print("Year range: (empty)")

    top_2024 = (
        df[df["year"] == 2024]
        .sort_values("expenditure_usd_millions", ascending=False)
        .head(5)
    )
    print("Top 5 spenders in 2024:")
    print(top_2024)

    # 12. Return cleaned dataframe
    return df


def clean_arms_data(df: pd.DataFrame) -> pd.DataFrame:
    from common.country_mapper import normalize_country

    # 2. Strip whitespace from 'country'
    df["country"] = df["country"].astype(str).str.strip()

    # 3. Normalize each country value
    df["country"] = df["country"].apply(normalize_country)

    # 4. Convert 'year' to integer; drop rows where conversion fails
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df.dropna(subset=["year"])
    df["year"] = df["year"].astype(int)

    # 5. Convert 'tiv_millions' to float; drop NaN and non-positive values
    df["tiv_millions"] = pd.to_numeric(df["tiv_millions"], errors="coerce")
    df = df.dropna(subset=["tiv_millions"])
    df = df[df["tiv_millions"] > 0].copy()

    # 6. Drop rows where country is NaN or empty string
    df = df[df["country"].notna() & (df["country"] != "")].copy()
    # Remove non-country summary rows that sneak in from SIPRI CSV
    INVALID_NAMES = [
        'Total', 'total', 'TOTAL',
        'World', 'world',
        'Other', 'other',
        'Unknown', 'unknown',
        'Supplier', 'Recipient',  # header row repeats
    ]
    df = df[~df['country'].isin(INVALID_NAMES)]
    # 7. Keep the row with highest tiv_millions for each (country, year)
    df = df.sort_values("tiv_millions", ascending=False)
    df = df.drop_duplicates(subset=["country", "year"], keep="first")

    # 8. Sort by country, year ascending
    df = df.sort_values(["country", "year"], ascending=[True, True])

    # 9. Print summary stats
    print("Total rows remaining:", len(df))
    print("Unique country count:", df["country"].nunique())
    if len(df) > 0:
        print("Year range:", int(df["year"].min()), int(df["year"].max()))
    else:
        print("Year range: (empty)")

    # 10. Returns cleaned dataframe
    return df


def clean_acled_data(df: pd.DataFrame) -> pd.DataFrame:
    from common.country_mapper import normalize_country

    # 2. Strip whitespace from 'country'
    df["country"] = df["country"].astype(str).str.strip()

    # 3. Normalize each country value
    df["country"] = df["country"].apply(normalize_country)

    # 4. Convert 'year' to integer; drop rows where conversion fails
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df.dropna(subset=["year"])
    df["year"] = df["year"].astype(int)

    # 5. Convert metric columns to integer (coerce junk -> NaN, then fill 0)
    metric_cols = [
        "violence_events",
        "civilian_fatalities",
        "total_fatalities",
        "civilian_events",
    ]
    for col in metric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    # 6. Drop rows where 'country' is NaN or empty string
    df = df[df["country"].notna() & (df["country"] != "")].copy()

    # 7. Drop duplicates on (country, year) keeping first
    df = df.drop_duplicates(subset=["country", "year"], keep="first")

    # 8. Sort by country, year ascending
    df = df.sort_values(["country", "year"], ascending=[True, True])

    # 9. Prints requested diagnostics
    print("Total rows:", len(df))
    print("Unique country count:", df["country"].nunique())
    if len(df) > 0:
        print("Year range:", int(df["year"].min()), int(df["year"].max()))
    else:
        print("Year range: (empty)")

    total_fatalities = int(df["total_fatalities"].sum()) if len(df) else 0
    total_violence_events = int(df["violence_events"].sum()) if len(df) else 0
    print("Total fatalities sum:", total_fatalities)
    print("Total violence events sum:", total_violence_events)

    # 10. Returns the cleaned dataframe
    return df

