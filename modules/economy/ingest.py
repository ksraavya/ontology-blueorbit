from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from modules.economy.constants import TRADE_AGREEMENT_STATUS_ACTIVE

RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"

logger = logging.getLogger(__name__)

BACI_COLUMNS = ["t", "i", "j", "k", "v", "q"]
COUNTRY_CODE_COLUMNS = ["country_code", "country_name", "country_iso2", "country_iso3"]
WB_SKIPROWS = 4
WB_YEAR_WIDE = [str(y) for y in range(1960, 2026)]  # '1960' … '2025'
GDP_INFLATION_YEAR_COLS = [str(y) for y in range(2000, 2025)]  # '2000' … '2024'

WB_ORG_CODES = {
    "OEC": "OECD",
    "EUU": "European Union",
    "G7E": "G7",
    "ARB": "Arab League",
}


def _require_columns(df: pd.DataFrame, path: Path, required: list[str]) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing column(s) {missing} after loading {path}")


def _load_country_codes() -> pd.DataFrame:
    path = RAW_DIR / "country_codes.csv"
    if not path.is_file():
        raise FileNotFoundError(str(path.resolve()))
    try:
        df = pd.read_csv(path)
    except Exception:
        logger.exception("Failed to read %s", path)
        raise
    _require_columns(df, path, COUNTRY_CODE_COLUMNS)
    logger.info("Loaded country_codes.csv: %d rows", len(df))
    return df


def _load_baci_year(year: int) -> pd.DataFrame:
    path = RAW_DIR / f"baci_trade_{year}.csv"
    if not path.is_file():
        logger.warning("BACI file not found, skipping: %s", path.resolve())
        return pd.DataFrame(columns=BACI_COLUMNS)
    try:
        df = pd.read_csv(path)
    except Exception:
        logger.exception("Failed to read %s", path)
        raise
    _require_columns(df, path, BACI_COLUMNS)
    logger.info("Loaded %s: %d rows", path.name, len(df))
    return df


def _load_baci_all(years: list[int]) -> pd.DataFrame:
    cc = _load_country_codes()
    frames = [_load_baci_year(y) for y in years]
    df = pd.concat(frames, ignore_index=True)
    if df.empty:
        return df

    exp = cc[["country_code", "country_name"]].rename(columns={"country_name": "exporter_name"})
    df = df.merge(exp, left_on="i", right_on="country_code", how="left").drop(
        columns=["country_code"]
    )
    imp = cc[["country_code", "country_name"]].rename(columns={"country_name": "importer_name"})
    df = df.merge(imp, left_on="j", right_on="country_code", how="left").drop(
        columns=["country_code"]
    )
    df = df.dropna(subset=["exporter_name", "importer_name"])
    return df


def _baci_years_default(years: list[int] | None) -> list[int]:
    if years is None:
        return list(range(2018, 2025))
    return years


def load_trade_data(years: list[int] | None = None) -> list[dict]:
    years_list = _baci_years_default(years)
    df = _load_baci_all(years_list)
    if df.empty:
        logger.info("load_trade_data: returning 0 rows")
        return []
    k_str = df["k"].astype(str)
    df = df.loc[~k_str.str.startswith("27")].copy()
    df["value"] = df["v"] * 1000
    out = pd.DataFrame(
        {
            "source": df["exporter_name"],
            "target": df["importer_name"],
            "value": df["value"],
            "year": df["t"],
            "type": "trade",
        }
    )
    records = out.to_dict("records")
    logger.info("load_trade_data: returning %d rows", len(records))
    return records


def load_energy_data(years: list[int] | None = None) -> list[dict]:
    years_list = _baci_years_default(years)
    df = _load_baci_all(years_list)
    if df.empty:
        logger.info("load_energy_data: returning 0 rows")
        return []
    k_str = df["k"].astype(str)
    df = df.loc[k_str.str.startswith("27")].copy()
    df["value"] = df["v"] * 1000
    out = pd.DataFrame(
        {
            "source": df["exporter_name"],
            "target": df["importer_name"],
            "value": df["value"],
            "year": df["t"],
            "type": "energy",
        }
    )
    records = out.to_dict("records")
    logger.info("load_energy_data: returning %d rows", len(records))
    return records


def _load_wb_wide(path: Path) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(str(path.resolve()))
    try:
        df = pd.read_csv(path, skiprows=WB_SKIPROWS)
    except Exception:
        logger.exception("Failed to read %s", path)
        raise
    # World Bank CSVs end each row with a trailing comma, which pandas reads
    # as an empty "Unnamed: N" column. N grows by one every year the bank
    # adds a column (70 in the 2025 download, 71 in 2026), so drop any of
    # them instead of hardcoding the index.
    df = df.drop(columns=[c for c in df.columns if str(c).startswith("Unnamed:")])
    base_cols = ["Country Name", "Country Code", "Indicator Name", "Indicator Code"]
    _require_columns(df, path, base_cols + GDP_INFLATION_YEAR_COLS)
    return df


def load_gdp_data() -> list[dict]:
    path = RAW_DIR / "wb_gdp.csv"
    df = _load_wb_wide(path)
    for c in GDP_INFLATION_YEAR_COLS:
        if c not in df.columns:
            raise ValueError(f"Missing column '{c}' after loading {path}")
    keep = ["Country Name", "Country Code"] + GDP_INFLATION_YEAR_COLS
    df = df[keep]
    melted = pd.melt(
        df,
        id_vars=["Country Name", "Country Code"],
        var_name="year",
        value_name="value",
    )
    melted = melted.dropna(subset=["value"])
    out = pd.DataFrame(
        {
            "country": melted["Country Name"],
            "country_code": melted["Country Code"],
            "indicator": "gdp",
            "value": melted["value"],
            "year": melted["year"],
            "currency": "USD",
        }
    )
    records = out.to_dict("records")
    logger.info("load_gdp_data: returning %d rows", len(records))
    return records


def load_inflation_data() -> list[dict]:
    path = RAW_DIR / "wb_inflation.csv"
    df = _load_wb_wide(path)
    for c in GDP_INFLATION_YEAR_COLS:
        if c not in df.columns:
            raise ValueError(f"Missing column '{c}' after loading {path}")
    keep = ["Country Name", "Country Code"] + GDP_INFLATION_YEAR_COLS
    df = df[keep]
    melted = pd.melt(
        df,
        id_vars=["Country Name", "Country Code"],
        var_name="year",
        value_name="value",
    )
    melted = melted.dropna(subset=["value"])
    out = pd.DataFrame(
        {
            "country": melted["Country Name"],
            "country_code": melted["Country Code"],
            "indicator": "inflation",
            "value": melted["value"],
            "year": melted["year"],
            "currency": "PCT",
        }
    )
    records = out.to_dict("records")
    logger.info("load_inflation_data: returning %d rows", len(records))
    return records


def load_macro_data() -> list[dict]:
    return load_gdp_data() + load_inflation_data()

def load_org_data() -> list[dict]:
    path = RAW_DIR / "wb_gdp.csv"
    df = _load_wb_wide(path)
    keep = ["Country Name", "Country Code"] + GDP_INFLATION_YEAR_COLS
    df = df[keep]
    df = df[df["Country Code"].isin(WB_ORG_CODES.keys())]
    melted = pd.melt(
        df,
        id_vars=["Country Name", "Country Code"],
        var_name="year",
        value_name="value",
    )
    melted = melted.dropna(subset=["value"])
    records = []
    for _, row in melted.iterrows():
        records.append({
            "org_name": WB_ORG_CODES.get(row["Country Code"], row["Country Name"]),
            "country_code": row["Country Code"],
            "indicator": "gdp",
            "value": row["value"],
            "year": row["year"],
            "currency": "USD",
        })
    logger.info("load_org_data: returning %d rows", len(records))
    return records

# OFAC SDN.CSV has no country column: column 3 is the sanctions PROGRAM.
# Country-based programs map to the targeted country; thematic programs
# (terrorism, narcotics, cyber, human rights worldwide) have no single
# country and are skipped. Source: OFAC program list
# https://ofac.treasury.gov/sanctions-programs-and-country-information
OFAC_PROGRAM_COUNTRY: dict[str, str] = {
    # Russia (incl. Ukraine-related EOs targeting Russian actors / Crimea)
    "RUSSIA-EO14024": "Russian Federation", "RUSSIA-EO14065": "Russian Federation",
    "CAATSA - RUSSIA": "Russian Federation", "MAGNIT": "Russian Federation",
    "PEESA-EO14039": "Russian Federation",
    "UKRAINE-EO13660": "Russian Federation", "UKRAINE-EO13661": "Russian Federation",
    "UKRAINE-EO13662": "Russian Federation", "UKRAINE-EO13685": "Russian Federation",
    # Iran
    "IRAN": "Iran", "IRAN-EO13902": "Iran", "IRAN-EO13846": "Iran", "IRAN-EO13876": "Iran",
    "IRAN-EO13871": "Iran", "IRAN-HR": "Iran", "IRAN-TRA": "Iran", "IRAN-CON-ARMS-EO": "Iran",
    "IRGC": "Iran", "IFSR": "Iran", "IFCA": "Iran", "CAATSA - IRAN": "Iran", "HRIT-IR": "Iran",
    # North Korea
    "DPRK": "North Korea", "DPRK2": "North Korea", "DPRK3": "North Korea",
    "DPRK4": "North Korea", "DPRK-NKSPEA": "North Korea",
    # Others
    "BELARUS": "Belarus", "BELARUS-EO14038": "Belarus",
    "VENEZUELA": "Venezuela", "VENEZUELA-EO13850": "Venezuela", "VENEZUELA-EO13884": "Venezuela",
    "BURMA-EO14014": "Myanmar", "CUBA": "Cuba", "CUBA-EO14404": "Cuba",
    "IRAQ2": "Iraq", "IRAQ3": "Iraq", "DRCONGO": "Democratic Republic of the Congo",
    "NICARAGUA": "Nicaragua", "NICARAGUA-NHRAA": "Nicaragua",
    "SUDAN-EO14098": "Sudan", "DARFUR": "Sudan", "SOUTH SUDAN": "South Sudan",
    "LIBYA2": "Libya", "LIBYA3": "Libya", "SOMALIA": "Somalia",
    "CAR": "Central African Republic", "LEBANON": "Lebanon", "YEMEN": "Yemen",
    "MALI-EO13882": "Mali", "HRIT-SY": "Syria", "PAARSSR-EO13894": "Syria",
    "UHRPA": "China",
}


def load_sanctions_data() -> list[dict]:
    """
    Load OFAC SDN sanctions as country-level records.

    SDN.CSV (no header): 0 ent_num, 1 name, 2 type, 3 program(s), ...
    A cell can hold several programs: "UKRAINE-EO13662] [RUSSIA-EO14024".
    Each designated entity counts once per targeted country, so the edge
    weight downstream is the number of sanctioned entities: a real measure
    of sanctions intensity, not a yes/no flag.
    """
    path = RAW_DIR / "ofac_sdn.csv"
    if not path.is_file():
        raise FileNotFoundError(str(path.resolve()))

    try:
        df = pd.read_csv(path, header=None, low_memory=False, usecols=[0, 1, 3])
    except Exception:
        logger.exception("Failed to read %s", path)
        raise

    records: list[dict] = []
    skipped_thematic = 0
    for _, row in df.iterrows():
        programs = str(row[3]) if pd.notna(row[3]) else ""
        countries = set()
        for prog in programs.split("] ["):
            prog = prog.strip(" []")
            if prog in OFAC_PROGRAM_COUNTRY:
                countries.add(OFAC_PROGRAM_COUNTRY[prog])
            elif prog and prog != "-0-":
                skipped_thematic += 1
        for country in countries:
            records.append({
                "sanctioned_country": country,
                "entity_name": str(row[1]).strip(),
                "sanctioning_country": "United States",
                "source": "OFAC SDN",
            })

    logger.info(
        "load_sanctions_data: %d entity-country records (%d thematic program tags skipped)",
        len(records), skipped_thematic,
    )
    return records


def load_trade_agreements_data() -> list[dict]:
    """
    Load WTO Regional Trade Agreements data.
    Filters to Status == 'In Force' only.
    Explodes 'Current signatories' (semicolon-separated) into
    bilateral country pairs — every country in an agreement
    gets a HAS_TRADE_AGREEMENT_WITH edge to every other country.
    Returns one record per bilateral pair per agreement.
    """
    path = RAW_DIR / "wto_rta.csv"
    if not path.is_file():
        raise FileNotFoundError(str(path.resolve()))
 
    try:
        df = pd.read_csv(path, low_memory=False)
    except Exception:
        logger.exception("Failed to read %s", path)
        raise
 
    # Newer WTO RTA-IS exports call the column "Signatories" instead of
    # "Current signatories"; same ";"-separated content.
    if "Current signatories" not in df.columns and "Signatories" in df.columns:
        df = df.rename(columns={"Signatories": "Current signatories"})

    required = ["RTA Name", "Status", "Current signatories"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns {missing} in {path}")
 
    # Filter to active agreements only
    df = df[df["Status"] == TRADE_AGREEMENT_STATUS_ACTIVE].copy()
    df = df.dropna(subset=["Current signatories"])
    logger.info("WTO active agreements: %d", len(df))
 
    records = []
    for _, row in df.iterrows():
        rta_name = str(row["RTA Name"]).strip()
        signatories_raw = str(row["Current signatories"])
 
        # Split on semicolon
        parties = [p.strip() for p in signatories_raw.split(";")]
        parties = [p for p in parties if p]
 
        if len(parties) < 2:
            continue
 
        # Generate all bilateral pairs
        for i in range(len(parties)):
            for j in range(i + 1, len(parties)):
                records.append({
                    "country_a": parties[i],
                    "country_b": parties[j],
                    "agreement_name": rta_name,
                    "source": "WTO RTA",
                })
 
    logger.info("load_trade_agreements_data: returning %d bilateral pairs", len(records))
    return records

def load_all(years: list[int] | None = None) -> dict[str, list[dict]]:
    years_list = _baci_years_default(years)
 
    trade_all: list[dict] = []
    energy_all: list[dict] = []
 
    cc = _load_country_codes()
 
    for year in years_list:
        df = _load_baci_year(year)
        if df.empty:
            logger.warning("Skipping year %d — file not found or empty", year)
            continue
 
        exp = (
            cc[["country_code", "country_name"]]
            .rename(columns={"country_name": "exporter_name"})
        )
        df = df.merge(
            exp, left_on="i", right_on="country_code", how="left"
        ).drop(columns=["country_code"])
 
        imp = (
            cc[["country_code", "country_name"]]
            .rename(columns={"country_name": "importer_name"})
        )
        df = df.merge(
            imp, left_on="j", right_on="country_code", how="left"
        ).drop(columns=["country_code"])
 
        df = df.dropna(subset=["exporter_name", "importer_name"])
 
        k_str = df["k"].astype(str)
 
        trade_df = df.loc[~k_str.str.startswith("27")].copy()
        trade_df["value"] = trade_df["v"] * 1000
        trade_all.extend(
            pd.DataFrame({
                "source": trade_df["exporter_name"],
                "target": trade_df["importer_name"],
                "value":  trade_df["value"],
                "year":   trade_df["t"],
                "type":   "trade",
            }).to_dict("records")
        )
 
        energy_df = df.loc[k_str.str.startswith("27")].copy()
        energy_df["value"] = energy_df["v"] * 1000
        energy_all.extend(
            pd.DataFrame({
                "source": energy_df["exporter_name"],
                "target": energy_df["importer_name"],
                "value":  energy_df["value"],
                "year":   energy_df["t"],
                "type":   "energy",
            }).to_dict("records")
        )
 
        del df, trade_df, energy_df, k_str
        logger.info("Processed year %d: trade_total=%d energy_total=%d",
                    year, len(trade_all), len(energy_all))
 
    macro = load_macro_data()
    orgs = load_org_data()
    sanctions = load_sanctions_data()
    trade_agreements = load_trade_agreements_data()
 
    logger.info(
        "All data loaded: trade=%d, energy=%d, macro=%d, orgs=%d, "
        "sanctions=%d, trade_agreements=%d",
        len(trade_all), len(energy_all), len(macro), len(orgs),
        len(sanctions), len(trade_agreements),
    )
    return {
        "trade":             trade_all,
        "energy":            energy_all,
        "macro":             macro,
        "orgs":              orgs,
        "sanctions":         sanctions,
        "trade_agreements":  trade_agreements,
    }