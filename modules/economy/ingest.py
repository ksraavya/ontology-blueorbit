from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

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
    if "Unnamed: 70" not in df.columns:
        raise ValueError(f"Missing column 'Unnamed: 70' after loading {path}")
    base_cols = ["Country Name", "Country Code", "Indicator Name", "Indicator Code", "Unnamed: 70"]
    _require_columns(df, path, base_cols + WB_YEAR_WIDE)
    df = df.drop(columns=["Unnamed: 70"])
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


def load_all(years: list[int] | None = None) -> dict[str, list[dict]]:
    years_list = _baci_years_default(years)
    
    trade_all: list[dict] = []
    energy_all: list[dict] = []
    
    cc = _load_country_codes()
    
    for year in years_list:
        df = _load_baci_year(year)
        if df.empty:
            continue
        
        # Join country names
        exp = cc[["country_code", "country_name"]].rename(
            columns={"country_name": "exporter_name"}
        )
        df = df.merge(exp, left_on="i", right_on="country_code", how="left").drop(
            columns=["country_code"]
        )
        imp = cc[["country_code", "country_name"]].rename(
            columns={"country_name": "importer_name"}
        )
        df = df.merge(imp, left_on="j", right_on="country_code", how="left").drop(
            columns=["country_code"]
        )
        df = df.dropna(subset=["exporter_name", "importer_name"])
        
        # Split trade vs energy
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
        
        # Free memory immediately after processing each year
        del df, trade_df, energy_df, k_str
        logger.info("Processed year %d: trade_total=%d energy_total=%d",
                    year, len(trade_all), len(energy_all))
    
    macro = load_macro_data()
    orgs = load_org_data()
    
    logger.info(
        "All data loaded: trade=%d, energy=%d, macro=%d, orgs=%d",
        len(trade_all), len(energy_all), len(macro), len(orgs),
    )
    return {
        "trade":  trade_all,
        "energy": energy_all,
        "macro":  macro,
        "orgs":   orgs,
    }