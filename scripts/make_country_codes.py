"""
Build data/raw/country_codes.csv for BACI when the official file is missing.

    python scripts/make_country_codes.py

The official file ships inside the CEPII BACI zip (country_codes_V2025xx.csv).
If you have it, use it instead (check_data.py --fix-names picks it up).

BACI country codes are ISO 3166-1 numeric, except for a handful of CEPII
codes that keep the UN Comtrade reporter numbering. This script reads every
baci_trade_*.csv present, collects the codes actually used, maps ISO codes via
pycountry and the documented exceptions below, and fails loudly if any code
cannot be mapped (so nothing is silently guessed).
"""
from __future__ import annotations

import csv
import sys
from pathlib import Path

import pandas as pd
import pycountry

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"

# CEPII BACI codes that differ from ISO numeric (see BACI documentation /
# country_codes file). Value = ISO alpha-3 of the country it stands for.
BACI_EXCEPTIONS: dict[int, str] = {
    251: "FRA",  # France (incl. Monaco)
    490: "TWN",  # "Other Asia, nes" = Taiwan in Comtrade-derived data
    579: "NOR",  # Norway (incl. Svalbard and Jan Mayen)
    699: "IND",  # India
    757: "CHE",  # Switzerland (incl. Liechtenstein)
    842: "USA",  # USA (incl. Puerto Rico and US Virgin Islands)
}


def main() -> int:
    files = sorted(RAW.glob("baci_trade_*.csv"))
    if not files:
        print("No baci_trade_*.csv in data/raw")
        return 1
    codes: set[int] = set()
    for f in files:
        df = pd.read_csv(f, usecols=["i", "j"])
        codes |= set(int(c) for c in df["i"].unique()) | set(int(c) for c in df["j"].unique())
        print(f"  {f.name}: {len(codes)} codes so far")

    rows, unmapped = [], []
    for code in sorted(codes):
        c = None
        if code in BACI_EXCEPTIONS:
            c = pycountry.countries.get(alpha_3=BACI_EXCEPTIONS[code])
        else:
            c = pycountry.countries.get(numeric=str(code).zfill(3))
        if c is None:
            unmapped.append(code)
            continue
        rows.append({
            "country_code": code,
            "country_name": c.name,
            "country_iso2": c.alpha_2,
            "country_iso3": c.alpha_3,
        })

    if unmapped:
        print(f"Unmapped BACI codes: {unmapped}. Add them to BACI_EXCEPTIONS from the CEPII docs.")
        return 1

    out = RAW / "country_codes.csv"
    with open(out, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=["country_code", "country_name", "country_iso2", "country_iso3"])
        w.writeheader()
        w.writerows(rows)
    print(f"Wrote {out} ({len(rows)} codes, from {len(files)} BACI files)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
