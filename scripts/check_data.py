"""
Check data/raw before rebuilding the graph.

    python scripts/check_data.py            # report what is present / missing
    python scripts/check_data.py --fix-names  # rename raw downloads (BACI, GDELT)
                                              # to the names the loaders expect

Exit code 0 = every REQUIRED dataset is present, 1 = something required is missing.
Optional datasets only produce warnings; the matching pipeline step is skipped.
"""
from __future__ import annotations

import argparse
import csv
import glob
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"


@dataclass
class Dataset:
    key: str
    step: str
    pattern: str                 # glob relative to data/raw
    source: str
    required: bool = True
    columns: list[str] = field(default_factory=list)
    skiprows: int = 0
    note: str = ""


DATASETS: list[Dataset] = [
    # ── Defense (committed to git already) ──
    Dataset("sipri_milex", "defense", "sipri_milex.xlsx",
            "https://www.sipri.org/databases/milex  (Excel, all years)"),
    Dataset("sipri_arms_1950_1980", "defense", "sipri_arms_1950_1980.csv",
            "https://armstrade.sipri.org/  (trade registers export)"),
    Dataset("sipri_arms_1981_2000", "defense", "sipri_arms_1981_2000.csv",
            "https://armstrade.sipri.org/"),
    Dataset("acled", "defense", "acled_*.xlsx",
            "https://acleddata.com/  (4 aggregated xlsx files)"),
    Dataset("sipri_trade_register", "defense", "sipri_trade_register.csv",
            "https://armstrade.sipri.org/ -> Trade registers, all suppliers/recipients, CSV", required=False),
    # ── Economy ──
    Dataset("baci", "economy", "baci_trade_*.csv",
            "https://www.cepii.fr/CEPII/en/bdd_modele/bdd_modele_item.asp?id=37  (HS17, free signup)",
            columns=["t", "i", "j", "k", "v", "q"],
            note="One file per year, named baci_trade_<year>.csv. --fix-names renames BACI_HS17_Y2023_V202501.csv."),
    Dataset("country_codes", "economy", "country_codes.csv",
            "Ships inside the BACI zip as country_codes_V2025xx.csv",
            columns=["country_code", "country_name", "country_iso2", "country_iso3"]),
    Dataset("wb_gdp", "economy", "wb_gdp.csv",
            "https://data.worldbank.org/indicator/NY.GDP.MKTP.CD  → Download CSV, use the API_NY.GDP... file",
            columns=["Country Name", "Country Code"], skiprows=4),
    Dataset("wb_inflation", "economy", "wb_inflation.csv",
            "https://data.worldbank.org/indicator/FP.CPI.TOTL.ZG  → Download CSV",
            columns=["Country Name", "Country Code"], skiprows=4),
    Dataset("ofac_sdn", "sanctions", "ofac_sdn.csv",
            "https://sanctionslist.ofac.treas.gov/  → SDN.CSV (no header row)", required=False),
    Dataset("wto_rta", "sanctions", "wto_rta.csv",
            "https://rtais.wto.org/  → export all RTAs to CSV",
            columns=["RTA Name", "Status"], required=False),
    # ── Geopolitics ──
    Dataset("vdem", "geopolitics", "vdem_core.csv",
            "https://www.v-dem.net/data/the-v-dem-dataset/  (V-Dem Core CSV)",
            columns=["country_name", "year", "v2x_polyarchy"]),
    Dataset("gdelt", "geopolitics", "gdelt/gdelt_*.CSV",
            "http://data.gdeltproject.org/events/index.html  (daily *.export.CSV files)",
            note="Tab-separated, no header. --fix-names renames 20240101.export.CSV → gdelt_20240101.CSV."),
    Dataset("unga", "geopolitics", "unga/unga_votes_filtered.csv",
            "https://digitallibrary.un.org/  UNGA voting data, filtered to year >= 2010",
            columns=["ms_code", "ms_name", "ms_vote", "resolution", "year"]),
    # ── Climate ──
    Dataset("emdat", "climate", "emdat_disasters*.*",
            "https://public.emdat.be/  (free signup) → export as emdat_disasters.xlsx or .csv"),
]


def _header(path: Path, skiprows: int) -> list[str]:
    with open(path, newline="", encoding="utf-8-sig", errors="replace") as fh:
        reader = csv.reader(fh)
        for _ in range(skiprows):
            next(reader, None)
        return [h.strip() for h in next(reader, [])]


def fix_names() -> list[str]:
    done = []
    for p in RAW.glob("BACI_*_Y*_V*.csv"):
        m = re.search(r"_Y(\d{4})_", p.name)
        if m:
            dst = RAW / f"baci_trade_{m.group(1)}.csv"
            if not dst.exists():
                p.rename(dst)
                done.append(f"{p.name} → {dst.name}")
    for p in RAW.glob("country_codes_V*.csv"):
        dst = RAW / "country_codes.csv"
        if not dst.exists():
            p.rename(dst)
            done.append(f"{p.name} → {dst.name}")
    gd = RAW / "gdelt"
    if gd.is_dir():
        for p in gd.glob("*.export.CSV"):
            dst = gd / f"gdelt_{p.name.split('.')[0]}.CSV"
            if not dst.exists():
                p.rename(dst)
                done.append(f"gdelt/{p.name} → {dst.name}")
    for name, dst_name in (("API_NY.GDP.MKTP.CD", "wb_gdp.csv"), ("API_FP.CPI.TOTL.ZG", "wb_inflation.csv")):
        for p in RAW.glob(f"{name}*.csv"):
            dst = RAW / dst_name
            if not dst.exists():
                p.rename(dst)
                done.append(f"{p.name} → {dst.name}")
    return done


def check() -> tuple[dict[str, bool], list[str]]:
    status: dict[str, bool] = {}
    lines: list[str] = []
    for d in DATASETS:
        files = sorted(Path(f) for f in glob.glob(str(RAW / d.pattern)))
        if not files:
            status[d.key] = False
            tag = "MISSING " if d.required else "optional"
            lines.append(f"  [{tag}] {d.key:<22} data/raw/{d.pattern}\n             get it: {d.source}")
            if d.note:
                lines.append(f"             note:   {d.note}")
            continue
        problem = ""
        if d.columns and files[0].suffix.lower() == ".csv":
            try:
                hdr = _header(files[0], d.skiprows)
                missing = [c for c in d.columns if c not in hdr]
                if missing:
                    problem = f"columns missing in {files[0].name}: {missing} (found {hdr[:8]}...)"
            except Exception as exc:
                problem = f"could not read {files[0].name}: {exc}"
        extra = ""
        if d.key == "baci":
            years = sorted(int(m.group(1)) for f in files if (m := re.search(r"(\d{4})", f.name)))
            extra = f" years={years}"
        size_mb = sum(f.stat().st_size for f in files) / 1e6
        if problem:
            status[d.key] = False
            lines.append(f"  [BAD     ] {d.key:<22} {problem}")
        else:
            status[d.key] = True
            lines.append(f"  [ok      ] {d.key:<22} {len(files)} file(s), {size_mb:,.1f} MB{extra}")
    return status, lines


def baci_years() -> list[int]:
    return sorted(int(m.group(1)) for f in RAW.glob("baci_trade_*.csv")
                  if (m := re.search(r"baci_trade_(\d{4})\.csv$", f.name)))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--fix-names", action="store_true", help="rename raw downloads to expected names")
    args = ap.parse_args()

    RAW.mkdir(parents=True, exist_ok=True)
    if args.fix_names:
        for line in fix_names() or ["nothing to rename"]:
            print("renamed:", line)

    status, lines = check()
    print(f"Data folder: {RAW}\n")
    print("\n".join(lines))
    missing_required = [d.key for d in DATASETS if d.required and not status[d.key]]
    print()
    if missing_required:
        print(f"Missing required datasets: {', '.join(missing_required)}")
        print("Steps that need them will be skipped by rebuild_graph.py.")
        return 1
    print("All required datasets present. Next: python scripts/rebuild_graph.py")
    return 0


if __name__ == "__main__":
    os.chdir(ROOT)
    sys.exit(main())
