import sys

sys.path.insert(0, ".")

import pandas as pd
from itertools import combinations

from common.intelligence.normalization import normalize_by_max


def compute_vote_similarity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute pairwise vote similarity between all country pairs
    from UNGA voting data.

    Input DataFrame has columns: country, ms_vote, resolution, year

    Logic:
    - For each resolution, get all countries that voted
    - For each pair of countries that both voted on the same resolution,
      check if their votes match (both Y, both N, or both A)
    - Count agreements and total votes per pair per year
    - Compute similarity = agreements / total_votes

    Step by step:

    STEP 1 — Group by year for yearly similarity scores
    Process each year separately so you get yearly alignment scores

    STEP 2 — For each year:
        a. Get all resolutions in that year
        b. For each resolution get the votes as a dict
        c. For each pair of countries in that resolution:
           - Check if votes match
           - Increment agreements counter if match
           - Increment total counter regardless

    STEP 3 — Compute similarity per pair per year:
        similarity = agreements / total
        Only keep pairs where total >= 10
        (ignore pairs with less than 10 shared votes)

    STEP 4 — Normalize similarity scores:
        Use normalize_by_max(similarity, max_similarity)
        from common.intelligence.normalization

    STEP 5 — Return DataFrame with columns:
        country_a, country_b, year,
        agreements, total_votes,
        vote_similarity, normalized_weight
    """
    results = []

    for year in sorted(df["year"].unique()):
        year_df = df[df["year"] == year]

        pivot = year_df.pivot_table(
            index="resolution",
            columns="country",
            values="ms_vote",
            aggfunc="first",
        )

        countries = pivot.columns.tolist()

        for c1, c2 in combinations(countries, 2):
            both_voted = pivot[[c1, c2]].dropna()
            total = len(both_voted)

            if total < 10:
                continue

            agreements = (both_voted[c1] == both_voted[c2]).sum()
            similarity = agreements / total

            results.append(
                {
                    "country_a": c1,
                    "country_b": c2,
                    "year": year,
                    "agreements": int(agreements),
                    "total_votes": int(total),
                    "vote_similarity": round(float(similarity), 4),
                }
            )

        print(f"Year {year}: processed {len(countries)} countries")

    result_df = pd.DataFrame(results)

    max_sim = result_df["vote_similarity"].max()
    result_df["normalized_weight"] = result_df["vote_similarity"].apply(
        lambda x: normalize_by_max(x, max_sim)
    )

    # Keep only most recent year per country pair
    print(f"Before deduplication: {len(result_df)} pairs")

    result_df = result_df.sort_values("year", ascending=False)
    result_df = result_df.drop_duplicates(
        subset=["country_a", "country_b"],
        keep="first",
    )
    result_df = result_df.sort_values(["country_a", "country_b"])

    print(f"After keeping latest year per pair: {len(result_df)} pairs")
    print("Year distribution:")
    print(result_df["year"].value_counts().sort_index())

    print(f"Total pairs computed: {len(result_df)}")
    print(result_df.head())

    return result_df


if __name__ == "__main__":
    from modules.geopolitics.loader import load_unga
    from modules.geopolitics.cleaner import clean_unga

    df_unga = load_unga()
    df_clean = clean_unga(df_unga)
    df_similarity = compute_vote_similarity(df_clean)

    print("\nTop 10 most aligned pairs (latest year):")
    latest = df_similarity[
        df_similarity["year"] == df_similarity["year"].max()
    ].sort_values("vote_similarity", ascending=False).head(10)
    print(latest[["country_a", "country_b", "vote_similarity"]])
